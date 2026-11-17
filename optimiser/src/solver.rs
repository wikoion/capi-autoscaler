use std::collections::{HashMap, HashSet};
use anyhow::{Result, bail};

use cp_sat::proto::{CpSolverStatus, SatParameters};

use crate::beam_search::run_beam_search;
use crate::optimise::Optimiser;
use crate::{
    COST_SCALE,
    OBJECTIVE_SCALE,
    SOFT_AFFINITY_FACTOR,
    SOFT_TOPOLOGY_PENALTY_WEIGHT,
    Domains,
    Enforcement,
    MachineDeployment,
    OptimiserConfig,
    Placement,
    Pod,
    PodAssignment,
    SolutionCost,
    SolveStats,
    SolverResult,
    SolverStatus,
    TopologyGroup,
    flat_slot_index,
    plugin_slot_weight,
    total_flat_slots,
    unplaced_pod_penalty
};

/// Beam width, used slot candidates and MD candidates per pod for every run_beam_search call.
const BEAM_WIDTH: usize = 4;
const SLOT_CANDIDATES: usize = 3;
const MD_CANDIDATES: usize = 3;

/// One CP-SAT solve of the shared model
struct CpSatAttempt {
    status: SolverStatus,
    solve_time_secs: f64,
    best_objective_bound: f64,
    /// None when CP-SAT found no placement in time
    solution: Option<Placement>,
}

/// The cheapest CP-SAT placement found across attempts
struct BestAttempt {
    status: SolverStatus,
    placement: Placement,
    cost: f64,
    attempt: i32,
}

impl From<CpSolverStatus> for SolverStatus {
    fn from(status: CpSolverStatus) -> Self {
        match status {
            CpSolverStatus::Unknown => SolverStatus::Unknown,
            CpSolverStatus::ModelInvalid => SolverStatus::ModelInvalid,
            CpSolverStatus::Feasible => SolverStatus::Feasible,
            CpSolverStatus::Infeasible => SolverStatus::Infeasible,
            CpSolverStatus::Optimal => SolverStatus::Optimal,
        }
    }
}

/// Price a placement exactly as the CP-SAT objective does, so CP-SAT attempts,
/// beam plans and the current state compare on one scale. Every term truncates
/// to integers where the model does, and the terms follow model_objective.
#[allow(clippy::too_many_arguments)]
pub fn calculate_solution_cost(
    assignments: &[PodAssignment],
    slots_used: &[Vec<bool>],
    mds: &[MachineDeployment],
    pods: &[Pod],
    plugin_scores: &[f64],
    plugin_weight: f64,
    topology_groups: &[TopologyGroup],
    cpu_waste_weight: f64,
    memory_waste_weight: f64,
    allow_partial: bool,
) -> f64 {
    // Only new slots cost anything, existing nodes are already paid for
    let mut provisioned_cost = 0.0;
    let mut plugin_cost = 0.0;
    for (md_idx, md) in mds.iter().enumerate() {
        let new_slots_used = slots_used[md_idx][md.existing_slots.len()..].iter().filter(|&&used| used).count() as f64;
        let node_cost = (md.node_cost(cpu_waste_weight, memory_waste_weight) * COST_SCALE) as i64 as f64;
        provisioned_cost += new_slots_used * node_cost;
        plugin_cost += new_slots_used * plugin_slot_weight(plugin_scores[md_idx]) as f64;
    }
    if allow_partial {
        let unplaced = assignments.iter().filter(|a| !a.is_placed()).count() as f64;
        provisioned_cost += unplaced * unplaced_pod_penalty(mds, cpu_waste_weight, memory_waste_weight) as i64 as f64;
    }

    // Soft affinity, each pod pair priced once
    let mut soft_affinity_penalty = 0.0;
    let mut priced_pairs: HashSet<(usize, usize)> = HashSet::new();
    for (pod_idx, pod) in pods.iter().enumerate() {
        // An unplaced pod shares no slot, so it can't break a preference
        if !assignments[pod_idx].is_placed() {
            continue;
        }
        for (&peer, &weight) in pod.soft_peers.iter().zip(&pod.soft_weights) {
            let penalty = (weight.abs() * 1000.0) as i64;
            // A zero penalty must not claim the pair, or it hides the peer's own weight
            if penalty == 0 || !assignments[peer].is_placed() {
                continue;
            }
            let pod_pair = (pod_idx.min(peer), pod_idx.max(peer));
            if !priced_pairs.insert(pod_pair) {
                continue;
            }

            let shares_slot = assignments[pod_idx] == assignments[peer];
            let preference_broken = if weight > 0.0 { !shares_slot } else { shares_slot };
            if preference_broken {
                soft_affinity_penalty += penalty as f64;
            }
        }
    }

    // Hard topology groups are model constraints and never charged
    let mut topology_penalty = 0.0;
    for group in topology_groups.iter().filter(|group| group.enforcement == Enforcement::Soft) {
        let mut domain_counts = group.domains.baseline_counts.clone();
        for (pod_idx, assignment) in assignments.iter().enumerate() {
            if !assignment.is_placed() || !group.pods_in_group[pod_idx] {
                continue;
            }
            let domain = group.domains.slot_domains[flat_slot_index(mds, assignment.md as usize, assignment.slot as usize)];
            // An unlabelled node is in no domain
            if domain >= 0 {
                domain_counts[domain as usize] += 1;
            }
        }

        let max_count = domain_counts.iter().max().copied().unwrap_or(0);
        let min_count = domain_counts.iter().min().copied().unwrap_or(0);
        let domains_used = domain_counts.iter().filter(|&&c| c > 0).count() as i32;
        let skew_violation = (max_count - min_count - group.max_skew).max(0);
        let domain_violation = (group.min_domains - domains_used).max(0);
        topology_penalty += (skew_violation + domain_violation) as f64 * SOFT_TOPOLOGY_PENALTY_WEIGHT;
    }

    provisioned_cost * OBJECTIVE_SCALE
        + plugin_cost * (OBJECTIVE_SCALE * plugin_weight) as i64 as f64
        + soft_affinity_penalty * SOFT_AFFINITY_FACTOR
        + topology_penalty * OBJECTIVE_SCALE
}

/// What the cluster costs today, the baseline a plan has to beat. Pending pods
/// are charged like unplaced pods in a partial solve, so a plan that places
/// them always beats a current state that doesn't.
#[allow(clippy::too_many_arguments)]
pub fn calculate_current_state_cost(
    mds: &[MachineDeployment],
    pods: &[Pod],
    plugin_scores: &[f64],
    current_replicas: Option<&[i32]>,
    plugin_weight: f64,
    topology_groups: &[TopologyGroup],
    cpu_waste_weight: f64,
    memory_waste_weight: f64,
) -> f64 {
    let placement = current_placement(mds, pods, current_replicas);
    calculate_solution_cost(&placement.assignments, &placement.slots_used, mds, pods, plugin_scores, plugin_weight, topology_groups, cpu_waste_weight, memory_waste_weight, true)
}

/// Rebuild today's placement from pod.current_md and each MD's node count.
/// Which node a pod is on isn't known, so pods are dealt round-robin over
/// their MD's nodes, which leaves no counted node empty.
fn current_placement(
    mds: &[MachineDeployment],
    pods: &[Pod],
    current_replicas: Option<&[i32]>,
) -> Placement {
    // current_md describes template nodes only, so existing slots would be counted twice
    debug_assert!(
        mds.iter().all(|md| md.existing_slots.is_empty())
            || pods.iter().all(|p| p.current_md.is_none()),
        "current-state cost cannot mix current_md pods with populated existing_slots"
    );

    let md_index: HashMap<&str, usize> = mds.iter()
        .enumerate()
        .map(|(i, md)| (md.name.as_str(), i))
        .collect();
    let mut pods_per_md: Vec<Vec<usize>> = vec![vec![]; mds.len()];
    for (pod_idx, pod) in pods.iter().enumerate() {
        if let Some(&md_idx) = pod.current_md.as_deref().and_then(|name| md_index.get(name)) {
            pods_per_md[md_idx].push(pod_idx);
        }
    }

    let mut assignments = vec![PodAssignment::UNPLACED; pods.len()];
    let mut slots_used = Vec::with_capacity(mds.len());
    for (md_idx, md) in mds.iter().enumerate() {
        let node_count = match current_replicas {
            Some(replicas) => replicas[md_idx] as usize,
            None => estimate_node_count(md, pods, &pods_per_md[md_idx]),
        }
        // More replicas than slots can't be represented, so count what fits
        .min(md.total_slots());

        let mut used = vec![false; md.total_slots()];
        used[..node_count].fill(true);
        slots_used.push(used);

        // With no nodes the MD's pods aren't running anywhere, so they stay pending
        if node_count > 0 {
            for (i, &pod_idx) in pods_per_md[md_idx].iter().enumerate() {
                assignments[pod_idx] = PodAssignment { md: md_idx as i32, slot: (i % node_count) as i32 };
            }
        }
    }

    Placement::new(assignments, slots_used)
}

/// Nodes an MD needs for the given pods, for when current_replicas isn't known
fn estimate_node_count(md: &MachineDeployment, pods: &[Pod], pod_indices: &[usize]) -> usize {
    let cpu: f64 = pod_indices.iter().map(|&i| pods[i].cpu).sum();
    let memory: f64 = pod_indices.iter().map(|&i| pods[i].memory).sum();
    // validate_inputs guarantees both template sizes are above zero
    let nodes_for_cpu = (cpu / md.cpu).ceil() as usize;
    let nodes_for_memory = (memory / md.memory).ceil() as usize;
    nodes_for_cpu.max(nodes_for_memory)
}

/// Whether a plan is worth switching to. With no baseline every plan is.
/// Otherwise it must be cheaper by at least threshold percent, so a zero
/// baseline, with nothing running and nothing pending, accepts nothing.
fn check_solution_acceptance(solution_cost: f64, current_state_cost: Option<f64>, threshold: f64) -> bool {
    match current_state_cost {
        None => true,
        Some(current) => {
            solution_cost < current && (current - solution_cost) / current * 100.0 >= threshold
        }
    }
}

/// Build the CP-SAT model once. Every attempt solves this same model and only
/// the hint changes between them.
fn build_model(
    mds: &[MachineDeployment],
    pods: &[Pod],
    plugin_scores: &[f64],
    allowed_matrix: &[Vec<Vec<bool>>],
    config: &OptimiserConfig,
) -> Optimiser {
    let mut opt = Optimiser::init(
        mds.to_vec(),
        pods.to_vec(),
        plugin_scores.to_vec(),
        allowed_matrix.to_vec(),
        config.allocatable_fraction,
        config.cpu_waste_weight,
        config.memory_waste_weight,
        config.allow_partial,
    );

    let soft_penalty = opt.model_soft_weights();
    let topology_penalty = opt.model_topology_constraints(&config.topology_groups);
    let affinity_penalty = opt.model_affinity_group_constraints(&config.affinity_groups);
    let objective = opt.model_objective(&soft_penalty, &topology_penalty, &affinity_penalty, config.plugin_cost_weight);
    opt.model.minimize(objective);
    opt
}

fn solve_attempt(opt: &Optimiser, params: &SatParameters) -> CpSatAttempt {
    let response = opt.model.solve_with_parameters(params);
    let status = SolverStatus::from(response.status());
    let found = matches!(status, SolverStatus::Optimal | SolverStatus::Feasible);
    CpSatAttempt {
        status,
        solve_time_secs: response.wall_time,
        best_objective_bound: response.best_objective_bound,
        solution: found.then(|| opt.read_solution(&response)),
    }
}

fn beam_search(
    mds: &[MachineDeployment],
    pods: &[Pod],
    plugin_scores: &[f64],
    allowed_matrix: &[Vec<Vec<bool>>],
    config: &OptimiserConfig,
) -> Placement {
    run_beam_search(
        mds,
        pods,
        plugin_scores,
        allowed_matrix,
        BEAM_WIDTH,
        SLOT_CANDIDATES,
        MD_CANDIDATES,
        config.plugin_cost_weight,
        &config.topology_groups,
        &config.affinity_groups,
        config.allocatable_fraction,
        config.cpu_waste_weight,
        config.memory_waste_weight,
    )
}

/// Beam search on its own, priced like a CP-SAT plan. Used for beam_only and
/// as the CP-SAT fallback. A plan that doesn't beat the current state still
/// succeeds with improvement_below_threshold set, the same as a CP-SAT plan.
fn run_beam_search_and_score(
    mds: &[MachineDeployment],
    pods: &[Pod],
    plugin_scores: &[f64],
    allowed_matrix: &[Vec<Vec<bool>>],
    current_state_cost: Option<f64>,
    attempts_run: i32,
    config: &OptimiserConfig,
) -> SolverResult {
    let beam = beam_search(mds, pods, plugin_scores, allowed_matrix, config);

    let stats = SolveStats {
        attempts_run,
        beam_used: true,
        beam_fallback_used: true,
        ..Default::default()
    };

    // Without allow_partial, a plan that leaves pods out is no plan at all
    if beam.unplaced_pods > 0 && !config.allow_partial {
        return SolverResult {
            success: false,
            placement: Placement { unplaced_pods: beam.unplaced_pods, ..Default::default() },
            cost: SolutionCost { current_state_cost, ..Default::default() },
            stats,
            ..Default::default()
        };
    }

    let objective = calculate_solution_cost(
        &beam.assignments,
        &beam.slots_used,
        mds,
        pods,
        plugin_scores,
        config.plugin_cost_weight,
        &config.topology_groups,
        config.cpu_waste_weight,
        config.memory_waste_weight,
        config.allow_partial,
    );

    SolverResult {
        success: true,
        status: SolverStatus::Feasible,
        message: String::new(),
        placement: beam,
        cost: SolutionCost {
            objective,
            current_state_cost,
            improvement_below_threshold: !check_solution_acceptance(objective, current_state_cost, config.improvement_threshold),
            ..Default::default()
        },
        stats,
    }
}

/// Validate input shapes and values before solving. Everything downstream
/// indexes into these arrays and divides by these sizes without checking, so
/// a bad input is a panic at best and a wrong plan at worst. Reject it here
/// with a named mismatch instead.
fn validate_inputs(
    mds: &[MachineDeployment],
    pods: &[Pod],
    plugin_scores: &[f64],
    allowed_matrix: &[Vec<Vec<bool>>],
    config: &OptimiserConfig,
) -> Result<()> {
    let total_flat_slots = total_flat_slots(mds);

    for md in mds {
        let positive = |amount: f64| amount.is_finite() && amount > 0.0;
        if !positive(md.cpu) || !positive(md.memory) {
            bail!(
                "MD {}: template cpu {} and memory {} must both be above 0",
                md.name, md.cpu, md.memory
            );
        }
    }

    if plugin_scores.len() != mds.len() {
        bail!(
            "plugin_scores has {} entries, expected one per MD ({})",
            plugin_scores.len(), mds.len()
        );
    }
    // Outside 0..=1 plugin_slot_weight can go negative, and the model would gain by opening nodes
    if let Some((md_idx, score)) = plugin_scores.iter().enumerate().find(|(_, score)| !(0.0..=1.0).contains(*score)) {
        bail!(
            "plugin score {} for MD {} out of range 0..=1",
            score, mds[md_idx].name
        );
    }

    if let Some(replicas) = &config.current_replicas {
        if replicas.len() != mds.len() {
            bail!(
                "current_replicas has {} entries, expected one per MD ({})",
                replicas.len(), mds.len()
            );
        }
        if let Some(&count) = replicas.iter().find(|&&count| count < 0) {
            bail!("current_replicas has a negative count {}", count);
        }
    }

    if allowed_matrix.len() != pods.len() {
        bail!(
            "allowed_matrix has {} pod rows, expected {}",
            allowed_matrix.len(), pods.len()
        );
    }
    for (pod_idx, row) in allowed_matrix.iter().enumerate() {
        if row.len() != mds.len() {
            bail!(
                "allowed_matrix[{}] has {} MD entries, expected {}",
                pod_idx, row.len(), mds.len()
            );
        }
        for (md_idx, slots) in row.iter().enumerate() {
            if slots.len() != mds[md_idx].total_slots() {
                bail!(
                    "allowed_matrix[{}][{}] has {} slot entries, expected total_slots {} of MD {}",
                    pod_idx, md_idx, slots.len(), mds[md_idx].total_slots(), mds[md_idx].name
                );
            }
        }
    }

    for (group_idx, tg) in config.topology_groups.iter().enumerate() {
        validate_domains(&format!("topology group {}", group_idx), &tg.domains, total_flat_slots)?;
        if tg.pods_in_group.len() != pods.len() {
            bail!(
                "topology group {}: pods_in_group has {} entries, expected pod count {}",
                group_idx, tg.pods_in_group.len(), pods.len()
            );
        }
    }

    for (group_idx, ag) in config.affinity_groups.iter().enumerate() {
        validate_domains(&format!("affinity group {}", group_idx), &ag.domains, total_flat_slots)?;
        for (name, flags) in [("pods_matching_term", &ag.pods_matching_term), ("pods_carrying_term", &ag.pods_carrying_term)] {
            if flags.len() != pods.len() {
                bail!(
                    "affinity group {}: {} has {} entries, expected pod count {}",
                    group_idx, name, flags.len(), pods.len()
                );
            }
        }
    }

    for (pod_idx, pod) in pods.iter().enumerate() {
        let valid_request = |amount: f64| amount.is_finite() && amount >= 0.0;
        if !valid_request(pod.cpu) || !valid_request(pod.memory) {
            bail!(
                "pod {}: cpu {} and memory {} must be finite and at least 0",
                pod_idx, pod.cpu, pod.memory
            );
        }
        for (name, peers) in [("hard_peers", &pod.hard_peers), ("soft_peers", &pod.soft_peers)] {
            if let Some(&peer) = peers.iter().find(|&&p| p >= pods.len()) {
                bail!(
                    "pod {}: {} index {} out of range for {} pods",
                    pod_idx, name, peer, pods.len()
                );
            }
        }
        // Peers and their rules or weights are read in pairs, zip would drop the extras
        if pod.hard_rules.len() != pod.hard_peers.len() {
            bail!(
                "pod {}: hard_rules has {} entries, expected one per hard_peers entry ({})",
                pod_idx, pod.hard_rules.len(), pod.hard_peers.len()
            );
        }
        if pod.soft_weights.len() != pod.soft_peers.len() {
            bail!(
                "pod {}: soft_weights has {} entries, expected one per soft_peers entry ({})",
                pod_idx, pod.soft_weights.len(), pod.soft_peers.len()
            );
        }
    }

    Ok(())
}

/// Shape checks a topology group and an affinity group share. `group` names
/// the group in the error, e.g. "topology group 2".
fn validate_domains(group: &str, domains: &Domains, total_flat_slots: usize) -> Result<()> {
    if domains.slot_domains.len() != total_flat_slots {
        bail!(
            "{}: slot_domains has {} entries, expected total flat slots {}",
            group, domains.slot_domains.len(), total_flat_slots
        );
    }
    if domains.num_domains < 0 || domains.baseline_counts.len() != domains.num_domains as usize {
        bail!(
            "{}: baseline_counts has {} entries, expected num_domains {}",
            group, domains.baseline_counts.len(), domains.num_domains
        );
    }
    if let Some(&domain) = domains.slot_domains.iter().find(|&&d| d < -1 || d >= domains.num_domains) {
        bail!(
            "{}: slot domain {} out of range -1..{}",
            group, domain, domains.num_domains
        );
    }
    Ok(())
}

/// Entry point. Validates the inputs, prices the current state, then runs
/// either beam search alone or the CP-SAT attempt loop with beam hints and fallback.
pub fn optimise_placement(
    mds: Vec<MachineDeployment>,
    pods: Vec<Pod>,
    plugin_scores: Vec<f64>,
    allowed_matrix: Vec<Vec<Vec<bool>>>,
    config: OptimiserConfig,
) -> Result<SolverResult> {
    validate_inputs(&mds, &pods, &plugin_scores, &allowed_matrix, &config)?;

    if !config.calculate_current_state && config.score_only {
        bail!("Incompatible parameters: current_state_cost must be true when using score_only");
    }

    let current_state_cost = config.calculate_current_state.then(|| {
        calculate_current_state_cost(
            &mds,
            &pods,
            &plugin_scores,
            config.current_replicas.as_deref(),
            config.plugin_cost_weight,
            &config.topology_groups,
            config.cpu_waste_weight,
            config.memory_waste_weight,
        )
    });

    if let (true, Some(cost)) = (config.score_only, current_state_cost) {
        return Ok(create_score_only_result(cost));
    }

    let result = if config.beam_only {
        run_beam_search_and_score(&mds, &pods, &plugin_scores, &allowed_matrix, current_state_cost, 0, &config)
    } else {
        run_cpsat_optimization_loop(&mds, &pods, &plugin_scores, &allowed_matrix, current_state_cost, &config)
    };
    debug_assert_assignments_allowed(&result, &allowed_matrix);
    Ok(result)
}

/// CP-SAT pins disallowed vars to zero (`pin_disallowed`), so this mainly
/// guards the beam search path in debug/test builds.
fn debug_assert_assignments_allowed(result: &SolverResult, allowed_matrix: &[Vec<Vec<bool>>]) {
    if !result.success || cfg!(not(debug_assertions)) {
        return;
    }
    for (pod_idx, assignment) in result.placement.assignments.iter().enumerate() {
        if !assignment.is_placed() {
            continue;
        }
        let allowed = allowed_matrix
            .get(pod_idx)
            .and_then(|row| row.get(assignment.md as usize))
            .and_then(|slots| slots.get(assignment.slot as usize))
            .copied()
            .unwrap_or(false);
        debug_assert!(
            allowed,
            "pod {} assigned to md {} slot {}, which its allowed_matrix entry forbids",
            pod_idx, assignment.md, assignment.slot
        );
    }
}

/// score_only prices the current state and solves nothing, so there is no
/// placement to report. placement stays empty, as on a failed solve, and
/// status stays Unknown because no solver ran.
fn create_score_only_result(current_state_cost: f64) -> SolverResult {
    SolverResult {
        success: true,
        cost: SolutionCost {
            objective: current_state_cost,
            current_state_cost: Some(current_state_cost),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Solve the CP-SAT model up to max_attempts times, stopping at the first plan
/// worth switching to. The beam hint goes in on seed_attempt and stays for
/// every attempt after it.
fn run_cpsat_optimization_loop(
    mds: &[MachineDeployment],
    pods: &[Pod],
    plugin_scores: &[f64],
    allowed_matrix: &[Vec<Vec<bool>>],
    current_state_cost: Option<f64>,
    config: &OptimiserConfig,
) -> SolverResult {
    let mut opt = build_model(mds, pods, plugin_scores, allowed_matrix, config);
    let params = SatParameters {
        max_time_in_seconds: Some(config.max_runtime_secs as f64),
        random_seed: Some(1729),
        randomize_search: Some(false),
        // Unset lets CP-SAT pick from the host core count
        num_workers: (config.num_workers > 0).then_some(config.num_workers),
        ..Default::default()
    };

    let mut beam_used = false;
    let mut best: Option<BestAttempt> = None;
    let mut total_solve_time = 0.0;
    let mut attempts_run = 0;
    // Every attempt solves the same model, so every bound holds. Keep the tightest.
    let mut best_bound: f64 = 0.0;

    for attempt in 1..=config.max_attempts {
        attempts_run = attempt;

        if config.seed_cpsat_with_beam && attempt == config.seed_attempt {
            let beam = beam_search(mds, pods, plugin_scores, allowed_matrix, config);
            // A beam plan with pods left out is only a fair start when partial plans are allowed
            if beam.unplaced_pods == 0 || config.allow_partial {
                opt.set_hint(&beam.assignments);
                beam_used = true;
            }
        }

        let CpSatAttempt { status, solve_time_secs, best_objective_bound, solution } = solve_attempt(&opt, &params);
        total_solve_time += solve_time_secs;

        // Infeasible and invalid are facts about the model, which every attempt
        // shares, so retrying with a hint can't change them
        if matches!(status, SolverStatus::Infeasible | SolverStatus::ModelInvalid) {
            break;
        }
        let Some(placement) = solution else {
            continue;
        };

        best_bound = best_bound.max(best_objective_bound);
        let cost = calculate_solution_cost(
            &placement.assignments,
            &placement.slots_used,
            mds,
            pods,
            plugin_scores,
            config.plugin_cost_weight,
            &config.topology_groups,
            config.cpu_waste_weight,
            config.memory_waste_weight,
            config.allow_partial,
        );
        if best.as_ref().is_none_or(|best| cost < best.cost) {
            best = Some(BestAttempt { status, placement, cost, attempt });
        }
        // Acceptance only gets easier as cost drops, so an accepted attempt is always the best one
        if check_solution_acceptance(cost, current_state_cost, config.improvement_threshold) {
            break;
        }
    }

    let cpsat_result = best.map(|best| SolverResult {
        success: true,
        status: best.status,
        message: String::new(),
        placement: best.placement,
        cost: SolutionCost {
            objective: best.cost,
            best_objective_bound: best_bound,
            current_state_cost,
            improvement_below_threshold: !check_solution_acceptance(best.cost, current_state_cost, config.improvement_threshold),
        },
        stats: SolveStats {
            solve_time_secs: total_solve_time,
            attempts_run,
            winning_attempt: best.attempt,
            beam_used,
            beam_fallback_used: false,
        },
    });

    // The beam only gets a say when CP-SAT found nothing cheaper than the
    // current state, and then the cheaper of the two plans wins
    let cpsat_improves = cpsat_result.as_ref()
        .is_some_and(|cpsat| current_state_cost.is_none_or(|current| cpsat.cost.objective < current));
    if config.beam_fallback && !cpsat_improves {
        let fallback = run_beam_search_and_score(mds, pods, plugin_scores, allowed_matrix, current_state_cost, config.max_attempts, config);
        let beats_cpsat = cpsat_result.as_ref().is_none_or(|cpsat| fallback.cost.objective < cpsat.cost.objective);
        if fallback.success && beats_cpsat {
            return fallback;
        }
    }

    cpsat_result.unwrap_or_else(|| SolverResult {
        success: false,
        message: "No valid solution found".to_string(),
        cost: SolutionCost { current_state_cost, ..Default::default() },
        stats: SolveStats { attempts_run, ..Default::default() },
        ..Default::default()
    })
}
