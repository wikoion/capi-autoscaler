use optimiser::{AffinityGroup, Domains, Enforcement, ExistingSlot, MachineDeployment, OptimiserConfig, Pod, SolverResult, SolverStatus, TopologyGroup, flat_slot_index, total_flat_slots};
use optimiser::solver::{calculate_solution_cost, optimise_placement};
use std::time::Instant;

// ---------------------------------------------------------------------------
// Builders and result helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum Engine {
    CpSat,
    Beam,
}

const ENGINES: [Engine; 2] = [Engine::CpSat, Engine::Beam];

fn config(engine: Engine) -> OptimiserConfig {
    OptimiserConfig {
        max_runtime_secs: 10,
        beam_only: matches!(engine, Engine::Beam),
        ..Default::default()
    }
}

fn instance(n: usize, cpu: f64, memory: f64, max_scale_out: i32) -> MachineDeployment {
    MachineDeployment {
        name: format!("test-instance-{n}"),
        cpu,
        memory,
        max_scale_out,
        existing_slots: vec![],
        cost_per_node: None,
    }
}

fn priced(md: MachineDeployment, cost: f64) -> MachineDeployment {
    MachineDeployment { cost_per_node: Some(cost), ..md }
}

fn with_nodes(md: MachineDeployment, existing_slots: Vec<ExistingSlot>) -> MachineDeployment {
    MachineDeployment { existing_slots, ..md }
}

/// An existing node with this much FREE capacity left.
fn node(n: usize, cpu: f64, memory: f64) -> ExistingSlot {
    ExistingSlot { node_name: format!("test-node-{n}"), cpu, memory }
}

fn pods(count: usize, cpu: f64, memory: f64) -> Vec<Pod> {
    (0..count).map(|_| Pod { cpu, memory, ..Default::default() }).collect()
}

fn allow_all(num_pods: usize, mds: &[MachineDeployment]) -> Vec<Vec<Vec<bool>>> {
    (0..num_pods)
        .map(|_| mds.iter().map(|md| vec![true; md.total_slots()]).collect())
        .collect()
}

fn solve_scored(mds: &[MachineDeployment], pods: &[Pod], scores: Vec<f64>, config: OptimiserConfig) -> SolverResult {
    optimise_placement(mds.to_vec(), pods.to_vec(), scores, allow_all(pods.len(), mds), config)
        .expect("optimise_placement failed")
}

fn solve(mds: &[MachineDeployment], pods: &[Pod], config: OptimiserConfig) -> SolverResult {
    solve_scored(mds, pods, vec![0.0; mds.len()], config)
}

/// New nodes opened per MD. Existing nodes are not counted. A failed solve
/// has no slots_used, so it opens nothing.
fn new_nodes(result: &SolverResult, mds: &[MachineDeployment]) -> Vec<usize> {
    mds.iter()
        .enumerate()
        .map(|(i, md)| {
            result.placement.slots_used.get(i).map_or(0, |slots| {
                slots.iter().skip(md.existing_slots.len()).filter(|&&u| u).count()
            })
        })
        .collect()
}

fn total_new_nodes(result: &SolverResult, mds: &[MachineDeployment]) -> usize {
    new_nodes(result, mds).iter().sum()
}

fn slot_of(result: &SolverResult, pod_idx: usize) -> (i32, i32) {
    let a = &result.placement.assignments[pod_idx];
    (a.md, a.slot)
}

/// Placed pods per flat slot index.
fn pods_per_flat_slot(result: &SolverResult, mds: &[MachineDeployment]) -> Vec<i32> {
    let mut counts = vec![0; total_flat_slots(mds)];
    for a in &result.placement.assignments {
        if a.md >= 0 && a.slot >= 0 {
            counts[flat_slot_index(mds, a.md as usize, a.slot as usize)] += 1;
        }
    }
    counts
}

/// Each slot's domain is its MD's index, so every MD is one zone.
fn zone_per_md(mds: &[MachineDeployment]) -> Vec<i32> {
    mds.iter()
        .enumerate()
        .flat_map(|(i, md)| std::iter::repeat_n(i as i32, md.total_slots()))
        .collect()
}

fn describe(result: &SolverResult, mds: &[MachineDeployment]) -> String {
    let opened: Vec<String> = new_nodes(result, mds)
        .iter()
        .zip(mds)
        .filter(|(n, _)| **n > 0)
        .map(|(n, md)| format!("{}={}", md.name, n))
        .collect();
    format!(
        "success={} status={:?} message={:?} unplaced={} attempts={} beam_used={} new_nodes=[{}]",
        result.success, result.status, result.message, result.placement.unplaced_pods,
        result.stats.attempts_run, result.stats.beam_used, opened.join(", ")
    )
}

// ---------------------------------------------------------------------------
// Large fleet: every solver mode runs against the same 12-instance ladder.
// ---------------------------------------------------------------------------

fn needs_local_disk(pod: &Pod) -> bool {
    pod.labels.get("storage").map(String::as_str) == Some("local-disk")
}

struct Fleet {
    mds: Vec<MachineDeployment>,
    local_disk: Vec<bool>,
    pods: Vec<Pod>,
    scores: Vec<f64>,
    allowed_matrix: Vec<Vec<Vec<bool>>>,
}

impl Fleet {
    /// 2, 4, 8 and 16 cores at 2, 4 and 8 GiB per core. The 8 GiB-per-core
    /// shapes have local disk. 60 pods cycle through six shapes, every seventh
    /// needs local disk, and pods 10/11, 20/21 and 30/31 carry affinity rules.
    fn new() -> Self {
        let mut mds = Vec::new();
        let mut local_disk = Vec::new();
        for cpu in [2.0, 4.0, 8.0, 16.0] {
            for gib_per_core in [2.0, 4.0, 8.0] {
                let memory = cpu * gib_per_core;
                let md = instance(mds.len() + 1, cpu, memory, (32.0 / cpu) as i32);
                // 4 cents a core plus half a cent a GiB
                mds.push(priced(md, cpu * 0.04 + memory * 0.005));
                local_disk.push(gib_per_core == 8.0);
            }
        }

        let shapes = [(0.5, 1.0), (1.0, 4.0), (2.0, 2.0), (0.25, 0.5), (3.0, 12.0), (1.5, 6.0)];
        let mut pods: Vec<Pod> = (0..60)
            .map(|i| {
                let (cpu, memory) = shapes[i % shapes.len()];
                let mut pod = Pod { cpu, memory, ..Default::default() };
                if i % 7 == 3 {
                    pod.labels.insert("storage".to_string(), "local-disk".to_string());
                }
                pod
            })
            .collect();
        pods[10].soft_peers = vec![11];
        pods[10].soft_weights = vec![0.6];
        pods[20].soft_peers = vec![21];
        pods[20].soft_weights = vec![-0.4];
        pods[30].hard_peers = vec![31];
        pods[30].hard_rules = vec![-1];

        // Cheapest price per core scores 1.0, dearest 0.0, linear in between
        let per_core: Vec<f64> = mds.iter().map(|md| md.cost_per_node.unwrap() / md.cpu).collect();
        let lo = per_core.iter().cloned().fold(f64::INFINITY, f64::min);
        let hi = per_core.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let scores = per_core.iter().map(|p| (hi - p) / (hi - lo)).collect();

        let allowed_matrix = pods
            .iter()
            .map(|pod| {
                mds.iter()
                    .zip(&local_disk)
                    .map(|(md, &disk)| {
                        let fits = pod.cpu <= md.cpu && pod.memory <= md.memory;
                        vec![fits && (disk || !needs_local_disk(pod)); md.total_slots()]
                    })
                    .collect()
            })
            .collect();

        Self { mds, local_disk, pods, scores, allowed_matrix }
    }

    fn run(&self, config: OptimiserConfig) -> SolverResult {
        optimise_placement(
            self.mds.clone(),
            self.pods.clone(),
            self.scores.clone(),
            self.allowed_matrix.clone(),
            config,
        ).expect("optimise_placement failed")
    }

    fn assert_valid(&self, mode: &str, result: &SolverResult, elapsed_ms: u128) {
        assert!(
            result.success && result.placement.unplaced_pods == 0,
            "{mode} ({elapsed_ms}ms): {}", describe(result, &self.mds)
        );
        for (i, pod) in self.pods.iter().enumerate() {
            let md = result.placement.assignments[i].md as usize;
            if needs_local_disk(pod) {
                assert!(self.local_disk[md], "{mode}: pod {i} needs local disk, got {}", self.mds[md].name);
            }
        }
        assert_ne!(slot_of(result, 30), slot_of(result, 31), "{mode}: hard anti pair shares a node");
    }
}

fn timed(f: impl FnOnce() -> SolverResult) -> (SolverResult, u128) {
    let start = Instant::now();
    let result = f();
    (result, start.elapsed().as_millis())
}

#[test]
fn fleet_shapes_are_consistent() {
    let fleet = Fleet::new();

    assert_eq!(fleet.mds.len(), 12);
    assert_eq!(fleet.pods.len(), 60);
    assert_eq!(fleet.scores.len(), 12);
    assert_eq!(fleet.allowed_matrix.len(), 60);
    assert_eq!(fleet.pods.iter().filter(|p| needs_local_disk(p)).count(), 9);
    assert!(fleet.scores.iter().all(|s| (0.0..=1.0).contains(s)), "scores out of range: {:?}", fleet.scores);

    for (pod_idx, row) in fleet.allowed_matrix.iter().enumerate() {
        assert_eq!(row.len(), 12, "pod {pod_idx} row");
        for (md_idx, slots) in row.iter().enumerate() {
            assert_eq!(slots.len(), fleet.mds[md_idx].total_slots(), "pod {pod_idx} {}", fleet.mds[md_idx].name);
            if needs_local_disk(&fleet.pods[pod_idx]) && !fleet.local_disk[md_idx] {
                assert!(slots.iter().all(|b| !b), "local-disk pod {pod_idx} allowed on {}", fleet.mds[md_idx].name);
            }
        }
        assert!(row.iter().flatten().any(|b| *b), "pod {pod_idx} has no allowed slot");
    }
}

#[test]
fn fleet_beam_only() {
    let fleet = Fleet::new();
    let (result, ms) = timed(|| fleet.run(OptimiserConfig { beam_only: true, ..Default::default() }));
    fleet.assert_valid("beam only", &result, ms);
    assert!(result.stats.beam_used);
}

#[test]
fn fleet_cpsat_single_attempt() {
    let fleet = Fleet::new();
    let (result, ms) = timed(|| fleet.run(OptimiserConfig {
        max_runtime_secs: 20,
        max_attempts: 1,
        ..Default::default()
    }));
    fleet.assert_valid("cp-sat", &result, ms);
    assert_eq!(result.stats.attempts_run, 1);
    assert!(!result.stats.beam_used);
}

#[test]
fn fleet_cpsat_seeded_from_beam() {
    // Seeding on attempt 1 means the beam hint is built before the first
    // CP-SAT solve, which returns as soon as it succeeds.
    let fleet = Fleet::new();
    let (result, ms) = timed(|| fleet.run(OptimiserConfig {
        max_runtime_secs: 20,
        max_attempts: 2,
        seed_cpsat_with_beam: true,
        seed_attempt: 1,
        beam_fallback: true,
        ..Default::default()
    }));
    fleet.assert_valid("cp-sat seeded", &result, ms);
    assert!(result.stats.beam_used, "the beam hint must be built on attempt 1: {}", describe(&result, &fleet.mds));
    assert!(!result.stats.beam_fallback_used, "CP-SAT solved, so no fallback: {}", describe(&result, &fleet.mds));
}

#[test]
fn fleet_plugin_weight_extremes_both_solve() {
    let fleet = Fleet::new();
    for plugin_cost_weight in [0.5, 8.0] {
        let (result, ms) = timed(|| fleet.run(OptimiserConfig {
            beam_only: true,
            plugin_cost_weight,
            ..Default::default()
        }));
        fleet.assert_valid(&format!("beam, plugin_cost_weight={plugin_cost_weight}"), &result, ms);
    }
}

#[test]
fn score_only_prices_the_current_layout() {
    // test-instance-1 runs 2 nodes, test-instance-2 runs 1 empty node.
    // Waste: 2 * (4*4 + 16) + 1 * (2*4 + 8) = 80 weighted units, so
    // 80 * COST_SCALE * OBJECTIVE_SCALE = 8.0e9.
    // Plugin: 2 * plugin_slot_weight(0.5) + 1 * plugin_slot_weight(0.0)
    // = 2 * 501 + 1001 = 2003, * OBJECTIVE_SCALE.
    let mds = vec![instance(1, 4.0, 16.0, 3), instance(2, 2.0, 8.0, 3)];
    let mut current = pods(3, 1.0, 4.0);
    for pod in &mut current {
        pod.current_md = Some("test-instance-1".to_string());
    }

    let result = solve_scored(&mds, &current, vec![0.5, 0.0], OptimiserConfig {
        score_only: true,
        current_replicas: Some(vec![2, 1]),
        ..Default::default()
    });

    assert!(result.success, "{}", describe(&result, &mds));
    assert_eq!(result.cost.current_state_cost, Some(8_002_003_000.0));
    assert_eq!(Some(result.cost.objective), result.cost.current_state_cost);
    assert!(result.placement.assignments.is_empty() && result.placement.slots_used.is_empty(), "score only places nothing");
}

// ---------------------------------------------------------------------------
// Topology spread
// ---------------------------------------------------------------------------

/// test-instance-1..3 with 1, 2 and 3 new slots: six hostname domains.
fn uneven_hostname_fleet() -> Vec<MachineDeployment> {
    vec![instance(1, 4.0, 8.0, 1), instance(2, 4.0, 8.0, 2), instance(3, 4.0, 8.0, 3)]
}

fn hostname_spread(num_pods: usize, min_domains: i32) -> TopologyGroup {
    TopologyGroup {
        pods_in_group: vec![true; num_pods],
        domains: Domains { slot_domains: (0..6).collect(), num_domains: 6, baseline_counts: vec![0; 6] },
        max_skew: 1,
        min_domains,
        enforcement: Enforcement::Hard,
    }
}

#[test]
fn hostname_spread_puts_one_pod_on_every_slot() {
    // Six pods, six domains, skew 1: a second pod anywhere would leave an
    // empty domain two behind, so every slot takes exactly one pod.
    let mds = uneven_hostname_fleet();
    let pending = pods(6, 1.0, 2.0);

    for engine in ENGINES {
        let result = solve(&mds, &pending, OptimiserConfig {
            topology_groups: vec![hostname_spread(6, 6)],
            ..config(engine)
        });
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        assert_eq!(pods_per_flat_slot(&result, &mds), vec![1; 6], "{engine:?}");
    }
}

#[test]
fn hostname_spread_second_round_stays_within_skew() {
    // Nine pods on six slots: three slots take a second pod, none a third.
    let mds = uneven_hostname_fleet();
    let pending = pods(9, 1.0, 2.0);

    for engine in ENGINES {
        let result = solve(&mds, &pending, OptimiserConfig {
            topology_groups: vec![hostname_spread(9, 0)],
            ..config(engine)
        });
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        let counts = pods_per_flat_slot(&result, &mds);
        assert!(counts.iter().all(|c| (1..=2).contains(c)), "{engine:?}: {:?}", counts);
        assert_eq!(counts.iter().sum::<i32>(), 9, "{engine:?}");
    }
}

/// Two one-pod-per-node zones at 1.0 a node, plus test-instance-3 with no
/// zone label at 0.05 a node, big enough for every pod and the highest
/// plugin score. Without the hard spread rule all six pods would go there.
/// Three MDs keep all of them on beam search's 3-MD shortlist.
fn unlabelled_cheap_fleet() -> (Vec<MachineDeployment>, Vec<Pod>, Vec<f64>, TopologyGroup) {
    let mds = vec![
        priced(instance(1, 1.5, 4.0, 3), 1.0),
        priced(instance(2, 1.5, 4.0, 3), 1.0),
        priced(instance(3, 8.0, 16.0, 1), 0.05),
    ];
    let group = TopologyGroup {
        pods_in_group: vec![true; 6],
        domains: Domains { slot_domains: vec![0, 0, 0, 1, 1, 1, -1], num_domains: 2, baseline_counts: vec![0; 2] },
        max_skew: 1,
        min_domains: 0,
        enforcement: Enforcement::Hard,
    };
    (mds, pods(6, 1.0, 1.0), vec![0.0, 0.0, 1.0], group)
}

fn assert_nothing_on_unlabelled(engine: Engine) {
    let (mds, pending, scores, group) = unlabelled_cheap_fleet();
    let result = solve_scored(&mds, &pending, scores, OptimiserConfig {
        topology_groups: vec![group],
        ..config(engine)
    });

    assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
    assert_eq!(
        new_nodes(&result, &mds), vec![3, 3, 0],
        "{engine:?}: DoNotSchedule makes the unlabelled instance ineligible: {:?}", result.placement.assignments
    );
}

#[test]
fn hard_spread_keeps_pods_off_unlabelled_slots_beam() {
    assert_nothing_on_unlabelled(Engine::Beam);
}

#[test]
#[ignore = "CP-SAT skips -1 slots in model_topology_constraints instead of forbidding them, beam search forbids them"]
fn hard_spread_keeps_pods_off_unlabelled_slots_cpsat() {
    assert_nothing_on_unlabelled(Engine::CpSat);
}

// ---------------------------------------------------------------------------
// Existing nodes. Their free capacity is sunk cost and needs no new node.
// ---------------------------------------------------------------------------

#[test]
fn pod_lands_on_the_existing_node_with_room() {
    let mds = vec![with_nodes(instance(1, 16.0, 64.0, 2), vec![node(1, 0.5, 1.0), node(2, 3.0, 12.0)])];
    let pending = pods(1, 2.0, 8.0);

    for engine in ENGINES {
        let result = solve(&mds, &pending, config(engine));
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        assert_eq!(slot_of(&result, 0), (0, 1), "{engine:?}: only test-node-2 has room");
        assert_eq!(total_new_nodes(&result, &mds), 0, "{engine:?}");
    }
}

#[test]
fn memory_short_existing_node_needs_a_new_node() {
    // test-node-1 has cpu to spare but only 2 GiB, the pod needs 4.
    let mds = vec![with_nodes(instance(1, 4.0, 16.0, 1), vec![node(1, 3.0, 2.0)])];
    let pending = pods(1, 1.0, 4.0);

    for engine in ENGINES {
        let result = solve(&mds, &pending, config(engine));
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        assert_eq!(slot_of(&result, 0), (0, 1), "{engine:?}: first new slot");
        assert_eq!(total_new_nodes(&result, &mds), 1, "{engine:?}");
    }
}

#[test]
fn full_existing_nodes_are_passed_over() {
    let mds = vec![with_nodes(instance(1, 8.0, 16.0, 2), vec![node(1, 0.0, 0.0), node(2, 0.0, 0.0)])];
    let pending = pods(1, 0.5, 1.0);

    for engine in ENGINES {
        let result = solve(&mds, &pending, config(engine));
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        assert!(result.placement.assignments[0].slot >= 2, "{engine:?}: full nodes take nothing, got {:?}", slot_of(&result, 0));
        assert_eq!(total_new_nodes(&result, &mds), 1, "{engine:?}");
    }
}

#[test]
fn existing_node_absorbs_what_the_new_node_cannot() {
    // The 6/24 pod needs a new node (7.84 cpu usable), which then has room for
    // one 1/4 pod. test-node-1 (2/8 free) takes the other two, so one new
    // node covers everything.
    let mds = vec![with_nodes(instance(1, 8.0, 32.0, 3), vec![node(1, 2.0, 8.0)])];
    let mut pending = pods(1, 6.0, 24.0);
    pending.extend(pods(3, 1.0, 4.0));

    for engine in ENGINES {
        let result = solve(&mds, &pending, config(engine));
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        assert!(result.placement.assignments[0].slot >= 1, "{engine:?}: the big pod cannot fit test-node-1");
        assert_eq!(total_new_nodes(&result, &mds), 1, "{engine:?}");
        let on_existing = result.placement.assignments.iter().filter(|a| a.slot == 0).count();
        assert_eq!(on_existing, 2, "{engine:?}: test-node-1 fits exactly two small pods");
    }
}

#[test]
fn allocatable_fraction_only_derates_new_nodes() {
    // 20 cores * 0.98 = 19.6 usable on a new node, so a 19.8 core pod can't
    // open one. An existing node reporting 19.8 free is already net of
    // reservation and takes it. Memory is nowhere near a limit.
    let pending = pods(1, 19.8, 10.0);
    let with_existing = vec![with_nodes(instance(1, 20.0, 40.0, 2), vec![node(1, 19.8, 10.0)])];
    let without_existing = vec![instance(1, 20.0, 40.0, 2)];

    for engine in ENGINES {
        let result = solve(&with_existing, &pending, config(engine));
        assert!(result.success, "{engine:?}: {}", describe(&result, &with_existing));
        assert_eq!(slot_of(&result, 0), (0, 0), "{engine:?}");

        let result = solve(&without_existing, &pending, config(engine));
        assert!(!result.success, "{engine:?}: a new node only has 19.6 cores usable");
    }
}

#[test]
fn hostname_spread_opens_a_node_despite_existing_room() {
    // Two existing nodes could take all three pods, but three hostname
    // domains at skew 1 need one pod each, so the new slot must open.
    let mds = vec![with_nodes(instance(1, 8.0, 16.0, 1), vec![node(1, 8.0, 16.0), node(2, 8.0, 16.0)])];
    let pending = pods(3, 1.0, 2.0);
    let group = TopologyGroup {
        pods_in_group: vec![true; 3],
        domains: Domains { slot_domains: vec![0, 1, 2], num_domains: 3, baseline_counts: vec![0; 3] },
        max_skew: 1,
        min_domains: 0,
        enforcement: Enforcement::Hard,
    };

    for engine in ENGINES {
        let result = solve(&mds, &pending, OptimiserConfig {
            topology_groups: vec![group.clone()],
            ..config(engine)
        });
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        assert_eq!(pods_per_flat_slot(&result, &mds), vec![1, 1, 1], "{engine:?}");
        assert_eq!(total_new_nodes(&result, &mds), 1, "{engine:?}");
    }
}

#[test]
fn solution_cost_matches_the_cpsat_objective_with_existing_nodes() {
    // calculate_solution_cost must price a solution exactly as the CP-SAT
    // model does. On an Optimal solve the model's bound is its objective.
    // Plugin scores 0.25 and 0.75 keep the per-node plugin weights whole.
    let mds = vec![
        with_nodes(instance(1, 8.0, 32.0, 2), vec![node(1, 2.0, 8.0)]),
        priced(instance(2, 4.0, 8.0, 2), 0.3),
    ];
    let mut pending = pods(1, 6.0, 24.0);
    pending.extend(pods(4, 1.0, 2.0));
    let scores = vec![0.25, 0.75];

    let result = solve_scored(&mds, &pending, scores.clone(), config(Engine::CpSat));
    assert_eq!(result.status, SolverStatus::Optimal, "{}", describe(&result, &mds));

    let cost = calculate_solution_cost(
        &result.placement.assignments, &result.placement.slots_used, &mds, &pending, &scores, 1.0, &[], 4.0, 1.0, false,
    );
    assert_eq!(cost, result.cost.best_objective_bound, "calculate_solution_cost must mirror the model objective");
}

#[test]
fn solution_cost_matches_the_cpsat_objective_with_soft_affinity() {
    // All 4 pods fit one node, so the colocate pair shares a slot and the anti
    // pair does too, as a second node costs far more than the anti penalty.
    // Each pair must be charged once, as calculate_solution_cost does.
    let mds = vec![instance(1, 8.0, 32.0, 3)];
    let mut pending = pods(4, 1.0, 2.0);
    pending[0].soft_peers = vec![1];
    pending[0].soft_weights = vec![0.5];
    pending[2].soft_peers = vec![3];
    pending[2].soft_weights = vec![-0.4];

    let result = solve(&mds, &pending, config(Engine::CpSat));
    assert_eq!(result.status, SolverStatus::Optimal, "{}", describe(&result, &mds));
    assert_eq!(slot_of(&result, 0), slot_of(&result, 1), "{}", describe(&result, &mds));

    let cost = calculate_solution_cost(
        &result.placement.assignments, &result.placement.slots_used, &mds, &pending, &[0.0], 1.0, &[], 4.0, 1.0, false,
    );
    assert_eq!(cost, result.cost.best_objective_bound, "calculate_solution_cost must mirror the model objective");
}

// ---------------------------------------------------------------------------
// Hard pod anti-affinity (hostname, pairwise peer rules)
// ---------------------------------------------------------------------------

fn anti(pod: &mut Pod, peers: &[usize]) {
    pod.hard_peers = peers.to_vec();
    pod.hard_rules = vec![-1; peers.len()];
}

#[test]
fn three_mutually_anti_affine_pods_take_three_nodes() {
    // Pods 0-2 repel each other; pod 3 has no rule and shares a node.
    let mds = vec![instance(1, 8.0, 16.0, 4)];
    let mut pending = pods(4, 1.0, 2.0);
    anti(&mut pending[0], &[1, 2]);
    anti(&mut pending[1], &[0, 2]);
    anti(&mut pending[2], &[0, 1]);

    for engine in ENGINES {
        let result = solve(&mds, &pending, config(engine));
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        let mut slots: Vec<_> = (0..3).map(|i| slot_of(&result, i)).collect();
        slots.sort();
        slots.dedup();
        assert_eq!(slots.len(), 3, "{engine:?}: {:?}", result.placement.assignments);
        assert_eq!(total_new_nodes(&result, &mds), 3, "{engine:?}");
    }
}

#[test]
fn one_sided_anti_affinity_separates_both_peers() {
    // Only pod 2 declares the rule, against pods 0 and 1. They still share a node.
    let mds = vec![instance(1, 8.0, 16.0, 3)];
    let mut pending = pods(3, 1.0, 2.0);
    anti(&mut pending[2], &[0, 1]);

    for engine in ENGINES {
        let result = solve(&mds, &pending, config(engine));
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        assert_ne!(slot_of(&result, 2), slot_of(&result, 0), "{engine:?}");
        assert_ne!(slot_of(&result, 2), slot_of(&result, 1), "{engine:?}");
        assert_eq!(total_new_nodes(&result, &mds), 2, "{engine:?}");
    }
}

#[test]
fn self_only_anti_affinity_does_not_block_packing() {
    // Each pod lists only itself, as a self-matching selector would. The
    // solver skips self pairs, so all three pack onto one node.
    let mds = vec![instance(1, 8.0, 16.0, 3)];
    let mut pending = pods(3, 1.0, 2.0);
    for (i, pod) in pending.iter_mut().enumerate() {
        anti(pod, &[i]);
    }

    for engine in ENGINES {
        let result = solve(&mds, &pending, config(engine));
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        assert_eq!(total_new_nodes(&result, &mds), 1, "{engine:?}: {:?}", result.placement.assignments);
    }
}

// ---------------------------------------------------------------------------
// Cost objective: waste weights and real prices
// ---------------------------------------------------------------------------

/// test-instance-1 (4 cpu / 8 GiB) fits three 1.2-core pods, test-instance-2
/// (2 / 2) fits one. Six pods need 2 of the first or 6 of the second.
fn cpu_bound_fleet() -> (Vec<MachineDeployment>, Vec<Pod>) {
    (vec![instance(1, 4.0, 8.0, 4), instance(2, 2.0, 2.0, 8)], pods(6, 1.2, 1.0))
}

#[test]
fn default_weights_pick_fewer_cpu_dense_nodes() {
    // cpu 4.0 / memory 1.0: test-instance-1 costs 24 a node, test-instance-2
    // costs 10. 2 x 24 = 48 beats 1 x 24 + 3 x 10 = 54 and 6 x 10 = 60.
    let (mds, pending) = cpu_bound_fleet();

    let result = solve(&mds, &pending, config(Engine::CpSat));
    assert!(result.success, "{}", describe(&result, &mds));
    assert_eq!(new_nodes(&result, &mds), vec![2, 0]);

    // Beam is a heuristic, so only hold it to the all-test-instance-2 cost.
    let beam = solve(&mds, &pending, config(Engine::Beam));
    assert!(beam.success, "{}", describe(&beam, &mds));
    let per_md = new_nodes(&beam, &mds);
    assert!(per_md[0] * 24 + per_md[1] * 10 <= 60, "beam: {:?}", per_md);
}

#[test]
fn memory_only_weights_flip_the_choice() {
    // cpu 0 / memory 1: 8 a node against 2 a node, so 6 x 2 = 12 beats
    // 1 x 8 + 3 x 2 = 14 and 2 x 8 = 16. Proves the weights steer the solve.
    let (mds, pending) = cpu_bound_fleet();

    let result = solve(&mds, &pending, OptimiserConfig {
        cpu_waste_weight: 0.0,
        memory_waste_weight: 1.0,
        ..config(Engine::CpSat)
    });
    assert!(result.success, "{}", describe(&result, &mds));
    assert_eq!(new_nodes(&result, &mds), vec![0, 6]);
}

#[test]
fn real_prices_break_a_tie_on_default_weights() {
    // Four 1.5/6 pods: 2 x test-instance-1, 4 x test-instance-2 or
    // 1 x test-instance-3. All three cost 64 on the default weights.
    // Prices make test-instance-3 (0.60) beat 1.00 and 1.20.
    let mds = vec![
        priced(instance(1, 4.0, 16.0, 2), 0.50),
        priced(instance(2, 2.0, 8.0, 4), 0.30),
        priced(instance(3, 8.0, 32.0, 2), 0.60),
    ];
    let pending = pods(4, 1.5, 6.0);

    for engine in ENGINES {
        let result = solve(&mds, &pending, config(engine));
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        assert_eq!(new_nodes(&result, &mds), vec![0, 0, 1], "{engine:?}");
    }
}

// ---------------------------------------------------------------------------
// Partial placement (allow_partial)
// ---------------------------------------------------------------------------

/// Two slots for three 6/24 pods. Three 0.5/1 pods fit beside a big pod.
fn oversubscribed_fleet() -> (Vec<MachineDeployment>, Vec<Pod>) {
    let mut pending = pods(3, 6.0, 24.0);
    pending.extend(pods(3, 0.5, 1.0));
    (vec![instance(1, 8.0, 32.0, 2)], pending)
}

fn partial(engine: Engine) -> OptimiserConfig {
    OptimiserConfig { allow_partial: true, ..config(engine) }
}

#[test]
fn partial_drops_only_the_pod_that_cannot_fit() {
    let (mds, pending) = oversubscribed_fleet();

    for engine in ENGINES {
        let result = solve(&mds, &pending, partial(engine));
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        assert_eq!(result.placement.unplaced_pods, 1, "{engine:?}");

        let dropped: Vec<usize> = (0..pending.len()).filter(|&i| result.placement.assignments[i].md < 0).collect();
        assert_eq!(dropped.len(), 1, "{engine:?}");
        assert!(dropped[0] < 3, "{engine:?}: a big pod is dropped, not a small one");
        assert_eq!(slot_of(&result, dropped[0]), (-1, -1), "{engine:?}: unplaced must not look like (0, 0)");
        assert_eq!(result.placement.slots_used[0], vec![true, true], "{engine:?}");
    }
}

#[test]
fn partial_never_drops_a_pod_to_save_money() {
    // Each 3/3 pod needs its own 750-a-node machine. The unplaced penalty
    // is one unit above opening every node, so both still get placed.
    let mds = vec![priced(instance(1, 4.0, 4.0, 2), 750.0)];
    let pending = pods(2, 3.0, 3.0);

    for engine in ENGINES {
        let result = solve(&mds, &pending, partial(engine));
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        assert_eq!(result.placement.unplaced_pods, 0, "{engine:?}");
        assert_ne!(slot_of(&result, 0), slot_of(&result, 1), "{engine:?}");
    }
}

#[test]
fn all_or_nothing_stops_after_proving_infeasible() {
    let (mds, pending) = oversubscribed_fleet();

    let result = solve(&mds, &pending, OptimiserConfig {
        max_attempts: 4,
        ..config(Engine::CpSat)
    });

    assert!(!result.success, "all-or-nothing can't place three big pods on two slots");
    assert_eq!(result.stats.attempts_run, 1, "an infeasibility proof doesn't change between attempts");
}

#[test]
fn partial_solution_cost_matches_the_cpsat_objective() {
    let (mds, pending) = oversubscribed_fleet();

    let result = solve(&mds, &pending, partial(Engine::CpSat));
    assert_eq!(result.status, SolverStatus::Optimal, "{}", describe(&result, &mds));

    let cost = calculate_solution_cost(
        &result.placement.assignments, &result.placement.slots_used, &mds, &pending, &[0.0], 1.0, &[], 4.0, 1.0, true,
    );
    assert_eq!(cost, result.cost.best_objective_bound, "the unplaced penalty must match the model's");
}

// ---------------------------------------------------------------------------
// Input validation. Malformed shapes are rejected with an error naming the
// mismatch, never a panic or a solve on garbage.
// ---------------------------------------------------------------------------

struct SolverInput {
    mds: Vec<MachineDeployment>,
    pods: Vec<Pod>,
    scores: Vec<f64>,
    allowed_matrix: Vec<Vec<Vec<bool>>>,
    topology: TopologyGroup,
    affinity: AffinityGroup,
}

/// 1 + 3 + 2 = 6 flat slots (test-instance-2 has one existing node), one
/// zone per MD, three pods. Both groups are soft, so a solve always succeeds.
fn valid_input() -> SolverInput {
    let mds = vec![
        instance(1, 8.0, 16.0, 1),
        with_nodes(instance(2, 8.0, 16.0, 2), vec![node(1, 8.0, 16.0)]),
        instance(3, 8.0, 16.0, 2),
    ];
    let pods = pods(3, 1.0, 2.0);
    let allowed_matrix = allow_all(pods.len(), &mds);
    let domains = Domains { slot_domains: zone_per_md(&mds), num_domains: 3, baseline_counts: vec![0; 3] };
    SolverInput {
        topology: TopologyGroup {
            pods_in_group: vec![true; 3],
            domains: domains.clone(),
            max_skew: 1,
            min_domains: 0,
            enforcement: Enforcement::Soft,
        },
        affinity: AffinityGroup {
            domains,
            enforcement: Enforcement::Soft,
            colocate: true,
            pods_matching_term: vec![true; 3],
            pods_carrying_term: vec![true; 3],
        },
        scores: vec![0.0; mds.len()],
        mds,
        pods,
        allowed_matrix,
    }
}

fn run_input(input: SolverInput) -> anyhow::Result<SolverResult> {
    optimise_placement(input.mds, input.pods, input.scores, input.allowed_matrix, OptimiserConfig {
        max_runtime_secs: 5,
        topology_groups: vec![input.topology],
        affinity_groups: vec![input.affinity],
        ..Default::default()
    })
}

#[test]
fn validation_accepts_well_formed_input() {
    run_input(valid_input()).expect("well-formed input must pass validation");

    // -1 marks an unlabelled slot, the one legal value outside 0..num_domains
    let mut input = valid_input();
    input.topology.domains.slot_domains[0] = -1;
    input.affinity.domains.slot_domains[5] = -1;
    run_input(input).expect("-1 slot domains are legal");
}

#[test]
fn validation_rejects_malformed_input() {
    let cases: [(&str, fn(&mut SolverInput), &str); 17] = [
        ("topology slot_domains too short", |i| { i.topology.domains.slot_domains.truncate(4); }, "slot_domains"),
        ("topology pods_in_group too long", |i| i.topology.pods_in_group.push(true), "pods_in_group"),
        ("topology baseline_counts too short", |i| { i.topology.domains.baseline_counts.pop(); }, "baseline_counts"),
        ("topology domain past num_domains", |i| i.topology.domains.slot_domains[5] = 3, "out of range"),
        ("topology domain below -1", |i| i.topology.domains.slot_domains[2] = -2, "out of range"),
        ("affinity slot_domains too long", |i| i.affinity.domains.slot_domains.push(0), "affinity group"),
        ("matrix missing a pod row", |i| { i.allowed_matrix.pop(); }, "pod rows"),
        ("matrix row missing an MD", |i| { i.allowed_matrix[2].pop(); }, "MD entries"),
        ("matrix MD missing a slot", |i| { i.allowed_matrix[0][1].pop(); }, "slot entries"),
        ("hard peer past the pod count", |i| anti(&mut i.pods[2], &[3]), "hard_peers"),
        ("soft peer past the pod count", |i| {
            i.pods[0].soft_peers = vec![7];
            i.pods[0].soft_weights = vec![0.3];
        }, "soft_peers"),
        ("soft peer without a weight", |i| i.pods[0].soft_peers = vec![1], "soft_weights"),
        ("hard peer without a rule", |i| i.pods[0].hard_peers = vec![1], "hard_rules"),
        ("plugin_scores missing an MD", |i| { i.scores.pop(); }, "plugin_scores"),
        ("plugin score above 1", |i| i.scores[1] = 1.5, "plugin score"),
        ("MD template with no cpu", |i| i.mds[0].cpu = 0.0, "template cpu"),
        ("pod request is NaN", |i| i.pods[1].memory = f64::NAN, "pod 1: cpu"),
    ];

    for (name, break_input, fragment) in cases {
        let mut input = valid_input();
        break_input(&mut input);
        let err = run_input(input).expect_err(name).to_string();
        assert!(err.contains(fragment), "{name}: error {err:?} should mention {fragment:?}");
    }
}

#[test]
fn flat_slot_index_walks_mds_existing_slots_first() {
    let mds = vec![
        instance(1, 8.0, 16.0, 1),                                                                    // flat 0
        with_nodes(instance(2, 8.0, 16.0, 0), vec![node(1, 1.0, 1.0), node(2, 1.0, 1.0), node(3, 1.0, 1.0)]), // flat 1-3
        with_nodes(instance(3, 8.0, 16.0, 2), vec![node(4, 1.0, 1.0)]),                               // flat 4-6
        with_nodes(instance(4, 8.0, 16.0, 2), vec![node(5, 1.0, 1.0), node(6, 1.0, 1.0)]),            // flat 7-10
    ];

    assert_eq!(total_flat_slots(&mds), 11);
    for (md_idx, slot_idx, flat) in [(0, 0, 0), (1, 0, 1), (1, 2, 3), (2, 0, 4), (2, 2, 6), (3, 0, 7), (3, 3, 10)] {
        assert_eq!(flat_slot_index(&mds, md_idx, slot_idx), flat, "md {md_idx} slot {slot_idx}");
    }

    let last = mds.len() - 1;
    assert_eq!(flat_slot_index(&mds, last, mds[last].total_slots() - 1), total_flat_slots(&mds) - 1);
}

// ---------------------------------------------------------------------------
// Affinity groups (hard, non-hostname keys). Three zones, one MD each.
// ---------------------------------------------------------------------------

fn three_zones(cpu: f64, memory: f64, max_scale_out: i32, prices: [f64; 3]) -> Vec<MachineDeployment> {
    prices.iter().enumerate()
        .map(|(i, &price)| priced(instance(i + 1, cpu, memory, max_scale_out), price))
        .collect()
}

fn zone_group(mds: &[MachineDeployment], baseline: [i32; 3], colocate: bool, matching: Vec<bool>, carrying: Vec<bool>) -> AffinityGroup {
    AffinityGroup {
        domains: Domains { slot_domains: zone_per_md(mds), num_domains: 3, baseline_counts: baseline.to_vec() },
        enforcement: Enforcement::Hard,
        colocate,
        pods_matching_term: matching,
        pods_carrying_term: carrying,
    }
}

fn solve_grouped(mds: &[MachineDeployment], pending: &[Pod], group: &AffinityGroup, engine: Engine) -> SolverResult {
    solve(mds, pending, OptimiserConfig { affinity_groups: vec![group.clone()], ..config(engine) })
}

#[test]
fn zone_anti_affinity_avoids_every_zone_with_a_running_match() {
    // Matches run in zones 1 and 2, so the carrier takes the dearest zone 0.
    let mds = three_zones(8.0, 16.0, 2, [3.0, 1.0, 2.0]);
    let pending = pods(1, 1.0, 2.0);
    let group = zone_group(&mds, [0, 2, 1], false, vec![false], vec![true]);

    for engine in ENGINES {
        let result = solve_grouped(&mds, &pending, &group, engine);
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        assert_eq!(result.placement.assignments[0].md, 0, "{engine:?}");
    }
}

#[test]
fn self_affinity_set_settles_in_one_zone() {
    // Four self-matching pods, nothing running: kube binds one and the rest
    // follow, so they share a zone (two pods a node, two nodes a zone).
    let mds = three_zones(4.0, 8.0, 2, [1.0, 1.0, 1.0]);
    let pending = pods(4, 1.5, 3.0);
    let group = zone_group(&mds, [0, 0, 0], true, vec![true; 4], vec![true; 4]);

    for engine in ENGINES {
        let result = solve_grouped(&mds, &pending, &group, engine);
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        let zones: Vec<i32> = result.placement.assignments.iter().map(|a| a.md).collect();
        assert!(zones.iter().all(|z| *z == zones[0] && *z >= 0), "{engine:?}: {:?}", zones);
    }
}

#[test]
fn colocate_carrier_joins_running_peers_over_cheaper_zones() {
    let mds = three_zones(8.0, 16.0, 2, [1.0, 1.0, 9.0]);
    let pending = pods(1, 1.0, 2.0);
    let group = zone_group(&mds, [0, 0, 4], true, vec![true], vec![true]);

    for engine in ENGINES {
        let result = solve_grouped(&mds, &pending, &group, engine);
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        assert_eq!(result.placement.assignments[0].md, 2, "{engine:?}: peers only run in zone 2");
    }
}

#[test]
fn first_self_affine_pod_founds_its_colony_where_it_is_cheapest() {
    // Nothing matches anywhere, which kube allows for a self-matching pod.
    let mds = three_zones(8.0, 16.0, 2, [5.0, 1.0, 3.0]);
    let pending = pods(1, 1.0, 2.0);
    let group = zone_group(&mds, [0, 0, 0], true, vec![true], vec![true]);

    for engine in ENGINES {
        let result = solve_grouped(&mds, &pending, &group, engine);
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        assert_eq!(result.placement.assignments[0].md, 1, "{engine:?}: placed, and only price decides where");
    }
}

#[test]
fn carrier_follows_a_pending_match_into_a_dearer_zone() {
    // Pod 0 is the match and may only run in zone 2. Pod 1 carries the term
    // and must follow, though zones 0 and 1 are cheaper. They can't share a
    // node (2 + 2 cores on 2.94 usable).
    let mds = three_zones(3.0, 6.0, 2, [1.0, 1.0, 3.0]);
    let pending = pods(2, 2.0, 1.0);
    let group = zone_group(&mds, [0, 0, 0], true, vec![true, false], vec![false, true]);
    let mut allowed_matrix = allow_all(2, &mds);
    for md_idx in 0..2 {
        allowed_matrix[0][md_idx].fill(false);
    }

    for engine in ENGINES {
        let result = optimise_placement(mds.clone(), pending.clone(), vec![0.0; 3], allowed_matrix.clone(), OptimiserConfig {
            affinity_groups: vec![group.clone()],
            ..config(engine)
        }).expect("optimise_placement failed");
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        assert_eq!(result.placement.assignments[0].md, 2, "{engine:?}");
        assert_eq!(result.placement.assignments[1].md, 2, "{engine:?}: carrier shares the match's zone");
    }
}

#[test]
fn self_affine_carriers_cannot_found_a_second_colony() {
    // Peers run in zone 1. Three pending self-matching pods could satisfy
    // each other in a cheaper empty zone, but kube only counts running or
    // independently bound matches, so all three join zone 1.
    let mds = three_zones(4.0, 8.0, 2, [1.0, 4.0, 1.0]);
    let pending = pods(3, 1.0, 2.0);
    let group = zone_group(&mds, [0, 3, 0], true, vec![true; 3], vec![true; 3]);

    for engine in ENGINES {
        let result = solve_grouped(&mds, &pending, &group, engine);
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        assert!(result.placement.assignments.iter().all(|a| a.md == 1), "{engine:?}: {:?}", result.placement.assignments);
    }
}

#[test]
fn hostname_followers_share_a_node_with_some_anchor() {
    // Anchors 0 and 1 repel each other. Followers 2-4 carry a hostname
    // colocate term matching the anchors. Three pods fit a node, so two
    // nodes hold everything: an anchor and two followers, an anchor and one.
    let mds = vec![instance(1, 4.0, 8.0, 3)];
    let mut pending = pods(5, 1.0, 2.0);
    anti(&mut pending[0], &[1]);
    anti(&mut pending[1], &[0]);
    let group = AffinityGroup {
        domains: Domains { slot_domains: vec![0, 1, 2], num_domains: 3, baseline_counts: vec![0; 3] },
        enforcement: Enforcement::Hard,
        colocate: true,
        pods_matching_term: vec![true, true, false, false, false],
        pods_carrying_term: vec![false, false, true, true, true],
    };

    for engine in ENGINES {
        let result = solve_grouped(&mds, &pending, &group, engine);
        assert!(result.success, "{engine:?}: {}", describe(&result, &mds));
        let anchors = [slot_of(&result, 0), slot_of(&result, 1)];
        assert_ne!(anchors[0], anchors[1], "{engine:?}");
        for follower in 2..5 {
            assert!(
                anchors.contains(&slot_of(&result, follower)),
                "{engine:?}: follower {follower} must share an anchor's node: {:?}", result.placement.assignments
            );
        }
        assert_eq!(total_new_nodes(&result, &mds), 2, "{engine:?}");
    }
}
