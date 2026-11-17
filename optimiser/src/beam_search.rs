use crate::{
    AffinityGroup,
    COST_SCALE,
    Enforcement,
    MachineDeployment,
    Placement,
    Pod,
    PodAssignment,
    RESOURCE_SCALE,
    TopologyGroup,
    flat_slot_index,
    scaled_capacity,
    scaled_request,
};

// Score weights. Only ever compared against each other, never returned,
// so the absolute scale is arbitrary. Ratios tuned on the solver-bench beam arm.
/// Cost per newly opened node, on top of its price. About $0.05/h of price,
/// so the beam no longer pays much more per hour just to open fewer nodes.
const NODE_COUNT_COST: f64 = 10_500.0;
/// Cost scale for a low plugin score on the chosen MD.
const MD_PLUGIN_COST: f64 = 2_000.0;
/// Weight on the provisioned node cost term.
const NODE_PRICE_WEIGHT: f64 = 2.0;
/// Weight on leftover slot capacity, a tie-break for tighter packing.
const LEFTOVER_CAPACITY_WEIGHT: f64 = 2.0;
/// Cost per topology skew or min-domains violation.
const TOPOLOGY_VIOLATION_COST: f64 = 100_000.0;
/// Score scale for slot fill ratio when ranking used slots.
const FILL_RATIO_SCORE: f64 = 20_000.0;
/// Score scale for the MD plugin score when ranking slots and MDs.
const PLUGIN_SCORE_WEIGHT: f64 = 85.0;
/// Bonus for an MD that can take the pod on an already-used slot.
const REUSE_EXISTING_BONUS: f64 = 95_000.0;
/// Score scale for how well the pod fits the MD template.
const TEMPLATE_FIT_SCORE: f64 = 850.0;
/// Difficulty weight for pods with few valid placements. Ranking by hard
/// anti-affinity peer count as well placed fewer pods, so that term is gone.
const CHOICE_SCARCITY_WEIGHT: f64 = 1_750.0;
/// Difficulty weight on pod size (cpu + memory).
const POD_SIZE_WEIGHT: f64 = 1.75;

/// What one beam search reads but never changes.
struct BeamInputs<'a> {
    mds: &'a [MachineDeployment],
    pods: &'a [Pod],
    plugin_scores: &'a [f64],
    allowed_matrix: &'a [Vec<Vec<bool>>],
    /// Hard anti-affinity peers of each pod, see pairwise_exclusion_lists
    anti_peers: Vec<Vec<usize>>,
    plugin_weight: f64,
    topology_groups: &'a [TopologyGroup],
    affinity_groups: &'a [AffinityGroup],
    allocatable_fraction: f64,
    cpu_waste_weight: f64,
    memory_waste_weight: f64,
}

/// One slot as a beam state sees it. Capacity is in RESOURCE_SCALE units,
/// rounded like the CP-SAT model, so the two agree on what fits.
#[derive(Debug, Clone)]
struct BeamSlot {
    used: bool,
    /// Already provisioned: starts used, costs nothing to fill, and can never
    /// be opened as a new node.
    existing: bool,
    cpu_free: i64,
    memory_free: i64,
    /// What the fill ratio is measured against. Existing slots use their free
    /// capacity, template slots the whole template.
    cpu_capacity: i64,
    memory_capacity: i64,
}

/// A partial placement on the beam frontier
#[derive(Clone)]
struct BeamState {
    assignments: Vec<PodAssignment>,
    slots: Vec<Vec<BeamSlot>>,
    cost: f64,
    /// Pods per domain for each topology group, baselines included
    topology_domain_counts: Vec<Vec<i32>>,
    /// Term-matching pods per domain for each affinity group, baselines included
    affinity_domain_counts: Vec<Vec<i32>>,
    /// Placed matching pods per domain that don't carry the term themselves.
    /// They bind on their own, so they can support a carrier.
    affinity_noncarrier_counts: Vec<Vec<i32>>,
}

impl BeamState {
    fn initial(inputs: &BeamInputs) -> Self {
        let slots = inputs.mds
            .iter()
            .map(|md| {
                (0..md.total_slots())
                    .map(|slot_idx| {
                        let existing = md.is_existing_slot(slot_idx);
                        let (cpu_free, memory_free) = md.slot_capacity(slot_idx, inputs.allocatable_fraction);
                        let (cpu_capacity, memory_capacity) = if existing {
                            (cpu_free, memory_free)
                        } else {
                            (md.cpu, md.memory)
                        };
                        BeamSlot {
                            used: existing,
                            existing,
                            cpu_free: scaled_capacity(cpu_free),
                            memory_free: scaled_capacity(memory_free),
                            cpu_capacity: scaled_capacity(cpu_capacity),
                            memory_capacity: scaled_capacity(memory_capacity),
                        }
                    })
                    .collect()
            })
            .collect();

        Self {
            assignments: vec![PodAssignment::UNPLACED; inputs.pods.len()],
            slots,
            cost: 0.0,
            topology_domain_counts: inputs.topology_groups.iter().map(|g| g.domains.baseline_counts.clone()).collect(),
            affinity_domain_counts: inputs.affinity_groups.iter().map(|g| g.domains.baseline_counts.clone()).collect(),
            affinity_noncarrier_counts: inputs.affinity_groups.iter().map(|g| vec![0; g.domains.num_domains as usize]).collect(),
        }
    }

    /// Heuristic cost the frontier is ranked on. Not the CP-SAT objective:
    /// NODE_COUNT_COST also charges per node, and leftover capacity breaks
    /// ties towards tighter packing.
    fn total_cost(&self, inputs: &BeamInputs) -> f64 {
        let mut cost = 0.0;
        let mut new_slots_used = 0;

        for (md_idx, md) in inputs.mds.iter().enumerate() {
            let node_cost = md.node_cost(inputs.cpu_waste_weight, inputs.memory_waste_weight) * COST_SCALE;
            // Existing nodes are already paid for, as in the CP-SAT objective
            for slot in self.slots[md_idx].iter().filter(|slot| slot.used && !slot.existing) {
                new_slots_used += 1;
                cost += node_cost * NODE_PRICE_WEIGHT;
                cost += (slot.cpu_free as f64 * inputs.cpu_waste_weight
                    + slot.memory_free as f64 * inputs.memory_waste_weight)
                    / RESOURCE_SCALE
                    * LEFTOVER_CAPACITY_WEIGHT;
                cost += (1.0 - inputs.plugin_scores[md_idx]) * MD_PLUGIN_COST * inputs.plugin_weight;
            }
        }

        cost += new_slots_used as f64 * NODE_COUNT_COST;
        cost + self.topology_violation_cost(inputs.topology_groups)
    }

    /// Charges soft and hard groups alike. A hard group's skew is already blocked
    /// on each placement, but its min_domains can only be met once enough pods
    /// are down, so the cost steers towards it.
    fn topology_violation_cost(&self, topology_groups: &[TopologyGroup]) -> f64 {
        let mut penalty = 0.0;
        for (group_idx, group) in topology_groups.iter().enumerate() {
            let counts = &self.topology_domain_counts[group_idx];
            let max_count = counts.iter().max().copied().unwrap_or(0);
            let min_count = counts.iter().min().copied().unwrap_or(0);
            let domains_used = counts.iter().filter(|&&c| c > 0).count() as i32;

            let skew_violation = (max_count - min_count - group.max_skew).max(0);
            let domain_violation = (group.min_domains - domains_used).max(0);
            penalty += (skew_violation + domain_violation) as f64 * TOPOLOGY_VIOLATION_COST;
        }
        penalty
    }

    fn hard_topology_allows(&self, pod_idx: usize, md_idx: usize, slot_idx: usize, inputs: &BeamInputs) -> bool {
        let flat_idx = flat_slot_index(inputs.mds, md_idx, slot_idx);

        for (group_idx, group) in inputs.topology_groups.iter().enumerate() {
            if group.enforcement == Enforcement::Soft || !group.pods_in_group[pod_idx] {
                continue;
            }

            let domain = group.domains.slot_domains[flat_idx];
            // An unlabelled node is in no domain, and DoNotSchedule forbids it
            if domain < 0 {
                return false;
            }
            let mut counts = self.topology_domain_counts[group_idx].clone();
            counts[domain as usize] += 1;

            let max_count = counts.iter().max().copied().unwrap_or(0);
            let min_count = counts.iter().min().copied().unwrap_or(0);
            if max_count - min_count > group.max_skew {
                return false;
            }
        }
        true
    }

    fn hard_affinity_allows(&self, pod_idx: usize, md_idx: usize, slot_idx: usize, inputs: &BeamInputs) -> bool {
        let flat_idx = flat_slot_index(inputs.mds, md_idx, slot_idx);

        for (group_idx, group) in inputs.affinity_groups.iter().enumerate() {
            if group.enforcement == Enforcement::Soft || !group.pods_carrying_term[pod_idx] {
                continue;
            }

            let domain = group.domains.slot_domains[flat_idx];
            // An unlabelled slot is unconstrained, as in the CP-SAT model
            if domain < 0 {
                continue;
            }
            let count = self.affinity_domain_counts[group_idx][domain as usize];

            if group.colocate {
                // As the kube-scheduler: the carrier's domain needs a running match
                // or a placed non-carrying match. A fresh self-affinity set instead
                // shares one domain, the first carrier picks it and the rest join it.
                if group.domains.baseline_counts[domain as usize] == 0 {
                    let total_baseline: i32 = group.domains.baseline_counts.iter().sum();
                    let has_noncarrier_match = group.pods_matching_term.iter()
                        .zip(group.pods_carrying_term.iter())
                        .any(|(m, c)| *m && !*c);

                    if total_baseline == 0 && !has_noncarrier_match {
                        let placed_anywhere: i32 = self.affinity_domain_counts[group_idx].iter().sum();
                        let founds_colony = placed_anywhere == 0 && group.pods_matching_term[pod_idx];
                        if count == 0 && !founds_colony {
                            return false;
                        }
                    } else if self.affinity_noncarrier_counts[group_idx][domain as usize] == 0 {
                        return false;
                    }
                }
            } else if count > 0 {
                // Anti: the domain already holds a matching pod, running or placed
                return false;
            }
        }
        true
    }

    /// The first unused slot of an MD the pod may open. An MD's template slots
    /// all cost the same, so one per MD is enough to try.
    fn first_free_slot(&self, pod_idx: usize, md_idx: usize, inputs: &BeamInputs) -> Option<usize> {
        (0..self.slots[md_idx].len()).find(|&slot_idx| {
            // Existing slots start used, so this only ever finds template slots
            !self.slots[md_idx][slot_idx].used
                && inputs.allowed_matrix[pod_idx][md_idx][slot_idx]
                && self.hard_topology_allows(pod_idx, md_idx, slot_idx, inputs)
                && self.hard_affinity_allows(pod_idx, md_idx, slot_idx, inputs)
        })
    }

    /// Put a pod on a slot. A free slot becomes used, which total_cost charges for.
    fn place(&mut self, pod_idx: usize, md_idx: usize, slot_idx: usize, inputs: &BeamInputs) {
        let pod = &inputs.pods[pod_idx];
        let slot = &mut self.slots[md_idx][slot_idx];
        slot.used = true;
        slot.cpu_free -= scaled_request(pod.cpu);
        slot.memory_free -= scaled_request(pod.memory);
        self.assignments[pod_idx] = PodAssignment { md: md_idx as i32, slot: slot_idx as i32 };

        let flat_idx = flat_slot_index(inputs.mds, md_idx, slot_idx);
        for (group_idx, group) in inputs.topology_groups.iter().enumerate() {
            let domain = group.domains.slot_domains[flat_idx];
            if group.pods_in_group[pod_idx] && domain >= 0 {
                self.topology_domain_counts[group_idx][domain as usize] += 1;
            }
        }
        for (group_idx, group) in inputs.affinity_groups.iter().enumerate() {
            let domain = group.domains.slot_domains[flat_idx];
            if !group.pods_matching_term[pod_idx] || domain < 0 {
                continue;
            }
            self.affinity_domain_counts[group_idx][domain as usize] += 1;
            if !group.pods_carrying_term[pod_idx] {
                self.affinity_noncarrier_counts[group_idx][domain as usize] += 1;
            }
        }
    }
}

/// Average cpu and memory fill of a slot once the request is on it, in [0, 1].
/// A dimension with no capacity counts as full, which covers existing nodes
/// with nothing free.
fn fill_ratio_with_request(slot: &BeamSlot, cpu_request: i64, memory_request: i64) -> f64 {
    let filled = |free: i64, request: i64, capacity: i64| {
        if capacity > 0 {
            1.0 - (free - request) as f64 / capacity as f64
        } else {
            1.0
        }
    };
    (filled(slot.cpu_free, cpu_request, slot.cpu_capacity)
        + filled(slot.memory_free, memory_request, slot.memory_capacity))
        / 2.0
}

/// Hard anti-affinity peers of each pod (rule -1), stored both ways so a
/// placement check only reads the pod's own list, whichever pod declared it.
fn pairwise_exclusion_lists(pods: &[Pod]) -> Vec<Vec<usize>> {
    let mut anti_peers: Vec<Vec<usize>> = vec![vec![]; pods.len()];

    for (pod_idx, pod) in pods.iter().enumerate() {
        for (&peer, &rule) in pod.hard_peers.iter().zip(&pod.hard_rules) {
            // A pod can't be its own anti peer, or it could never be placed
            if rule == -1 && peer != pod_idx {
                anti_peers[pod_idx].push(peer);
                anti_peers[peer].push(pod_idx);
            }
        }
    }

    anti_peers
}

/// True if one of the pod's hard anti-affinity peers is already on this slot.
/// Only used slots can hold a peer, so opening a free slot never needs this.
fn shares_slot_with_excluded_pod(pod_idx: usize, slot: PodAssignment, state: &BeamState, inputs: &BeamInputs) -> bool {
    inputs.anti_peers[pod_idx].iter().any(|&peer| state.assignments[peer] == slot)
}

/// Pod indices in placement order, hardest pods first. A greedy placer fails
/// on the pods with the fewest options, so they go while there's still room.
fn hardest_first_order(
    pods: &[Pod],
    allowed_matrix: &[Vec<Vec<bool>>],
    affinity_groups: &[AffinityGroup],
) -> Vec<usize> {
    let mut ranked: Vec<(usize, bool, f64)> = pods
        .iter()
        .enumerate()
        .map(|(i, pod)| {
            let allowed_slots = allowed_matrix[i].iter().flatten().filter(|&&x| x).count();
            let footprint = pod.cpu + pod.memory;

            // A carrier can only be placed next to a match that's already down,
            // so non-carrying matches of a hard colocate group go first
            let is_colocate_support = affinity_groups.iter()
                .any(|g| g.enforcement == Enforcement::Hard && g.colocate && g.pods_matching_term[i] && !g.pods_carrying_term[i]);

            let difficulty = CHOICE_SCARCITY_WEIGHT / allowed_slots.max(1) as f64
                + footprint * POD_SIZE_WEIGHT;

            (i, is_colocate_support, difficulty)
        })
        .collect();

    ranked.sort_by(|a, b| b.1.cmp(&a.1).then(b.2.total_cmp(&a.2)));
    ranked.into_iter().map(|(idx, _, _)| idx).collect()
}

/// A used slot the pod fits on, with its ranking score
struct ScoredSlot {
    md_idx: usize,
    slot_idx: usize,
    score: f64,
}

/// Used slots the pod can go on, best first. Fuller slots rank higher, so the
/// beam tries best-fit packing before anything else.
fn rank_used_slots_for_pod(pod_idx: usize, state: &BeamState, inputs: &BeamInputs) -> Vec<ScoredSlot> {
    let pod = &inputs.pods[pod_idx];
    let cpu_request = scaled_request(pod.cpu);
    let memory_request = scaled_request(pod.memory);
    let mut candidates = Vec::new();

    for (md_idx, md_slots) in state.slots.iter().enumerate() {
        // No template-size check here: an existing slot's free capacity is
        // unrelated to the template, so only the slot's own free capacity counts
        for (slot_idx, slot) in md_slots.iter().enumerate() {
            if !slot.used
                || !inputs.allowed_matrix[pod_idx][md_idx][slot_idx]
                || cpu_request > slot.cpu_free
                || memory_request > slot.memory_free
            {
                continue;
            }
            let assignment = PodAssignment { md: md_idx as i32, slot: slot_idx as i32 };
            if shares_slot_with_excluded_pod(pod_idx, assignment, state, inputs)
                || !state.hard_topology_allows(pod_idx, md_idx, slot_idx, inputs)
                || !state.hard_affinity_allows(pod_idx, md_idx, slot_idx, inputs)
            {
                continue;
            }

            let score = fill_ratio_with_request(slot, cpu_request, memory_request) * FILL_RATIO_SCORE
                + inputs.plugin_scores[md_idx] * PLUGIN_SCORE_WEIGHT * inputs.plugin_weight;

            candidates.push(ScoredSlot { md_idx, slot_idx, score });
        }
    }

    candidates.sort_by(|a, b| b.score.total_cmp(&a.score));
    candidates
}

/// Up to `limit` MDs worth opening a new slot on for the pod, best first.
fn shortlist_mds(pod_idx: usize, state: &BeamState, limit: usize, inputs: &BeamInputs) -> Vec<usize> {
    let pod = &inputs.pods[pod_idx];
    let cpu_request = scaled_request(pod.cpu);
    let memory_request = scaled_request(pod.memory);
    let mut ranked_mds: Vec<(usize, f64)> = Vec::new();

    for (md_idx, md) in inputs.mds.iter().enumerate() {
        if !inputs.allowed_matrix[pod_idx][md_idx].iter().any(|&allowed| allowed) {
            continue;
        }

        // A pod bigger than the template can never open a new node here, but
        // it may still fit what's left on a used slot
        let fits_template = cpu_request <= scaled_capacity(md.cpu * inputs.allocatable_fraction)
            && memory_request <= scaled_capacity(md.memory * inputs.allocatable_fraction);
        let fits_used_slot = state.slots[md_idx].iter().any(|slot| {
            slot.used && cpu_request <= slot.cpu_free && memory_request <= slot.memory_free
        });
        let can_open_slot = fits_template && state.slots[md_idx].iter().any(|slot| !slot.used);

        if !fits_used_slot && !can_open_slot {
            continue;
        }

        let mut score = 0.0;
        if fits_used_slot {
            score += REUSE_EXISTING_BONUS;
        }
        let cpu_share = pod.cpu / md.cpu;
        let memory_share = pod.memory / md.memory;
        score += cpu_share.min(memory_share) * TEMPLATE_FIT_SCORE;
        score += inputs.plugin_scores[md_idx] * PLUGIN_SCORE_WEIGHT * inputs.plugin_weight;

        ranked_mds.push((md_idx, score));
    }

    ranked_mds.sort_by(|a, b| b.1.total_cmp(&a.1));
    ranked_mds.into_iter().take(limit).map(|(idx, _)| idx).collect()
}

/// The children of one state for one pod: the best used slots, then a new slot
/// on each shortlisted MD. Both kinds go back to run_beam_search, which ranks
/// them together on cost.
fn expand_state(
    state: &BeamState,
    pod_idx: usize,
    slot_candidates: usize,
    md_candidates: usize,
    inputs: &BeamInputs,
) -> Vec<BeamState> {
    let used_slots = rank_used_slots_for_pod(pod_idx, state, inputs)
        .into_iter()
        .take(slot_candidates)
        .map(|candidate| (candidate.md_idx, candidate.slot_idx));
    let new_slots = shortlist_mds(pod_idx, state, md_candidates, inputs)
        .into_iter()
        .filter_map(|md_idx| state.first_free_slot(pod_idx, md_idx, inputs).map(|slot_idx| (md_idx, slot_idx)));

    used_slots
        .chain(new_slots)
        .map(|(md_idx, slot_idx)| {
            let mut child = state.clone();
            child.place(pod_idx, md_idx, slot_idx, inputs);
            child.cost = child.total_cost(inputs);
            child
        })
        .collect()
}

/// Places pods hardest first, keeping the beam_width cheapest partial placements after each pod.
#[allow(clippy::too_many_arguments)]
pub fn run_beam_search(
    mds: &[MachineDeployment],
    pods: &[Pod],
    plugin_scores: &[f64],
    allowed_matrix: &[Vec<Vec<bool>>],
    beam_width: usize,
    slot_candidates: usize,
    md_candidates: usize,
    plugin_weight: f64,
    topology_groups: &[TopologyGroup],
    affinity_groups: &[AffinityGroup],
    allocatable_fraction: f64,
    cpu_waste_weight: f64,
    memory_waste_weight: f64,
) -> Placement {
    // A zero width would empty the frontier and leave no state to return
    assert!(beam_width >= 1, "beam_width must be at least 1");

    let inputs = BeamInputs {
        mds,
        pods,
        plugin_scores,
        allowed_matrix,
        anti_peers: pairwise_exclusion_lists(pods),
        plugin_weight,
        topology_groups,
        affinity_groups,
        allocatable_fraction,
        cpu_waste_weight,
        memory_waste_weight,
    };
    let mut frontier = vec![BeamState::initial(&inputs)];

    for pod_idx in hardest_first_order(pods, allowed_matrix, affinity_groups) {
        let mut next_frontier: Vec<BeamState> = frontier
            .iter()
            .flat_map(|state| expand_state(state, pod_idx, slot_candidates, md_candidates, &inputs))
            .collect();

        // No state can take the pod, so it stays unplaced in all of them and the
        // rest still get packed. The caller decides if a partial plan is any use.
        if next_frontier.is_empty() {
            continue;
        }

        next_frontier.sort_by(|a, b| a.cost.total_cmp(&b.cost));
        next_frontier.truncate(beam_width);
        frontier = next_frontier;
    }

    let best = frontier.swap_remove(0);
    let slots_used = best.slots.iter().map(|md_slots| md_slots.iter().map(|slot| slot.used).collect()).collect();
    Placement::new(best.assignments, slots_used)
}
