use std::collections::HashMap;

pub mod beam_search;
pub mod optimise;
pub mod solver;

const SOFT_AFFINITY_FACTOR: f64 = 200.0;
const SOFT_TOPOLOGY_PENALTY_WEIGHT: f64 = 5000.0;
const OBJECTIVE_SCALE: f64 = 1000.0;

/// Unit convention: cpu is in CORES, memory is in GiB. Resource quantities are
/// converted to fixed-point integers at this scale inside the CP-SAT model,
/// giving ~1 millicore / ~1 MiB granularity.
pub const RESOURCE_SCALE: f64 = 1000.0;

/// Fixed-point scale for per-node cost in the objective (5 decimal places,
/// enough for fractional $/h instance prices).
pub const COST_SCALE: f64 = 100_000.0;

/// A pod request in RESOURCE_SCALE units. Rounded up, so a sum of requests
/// never comes out smaller than the real total.
pub fn scaled_request(amount: f64) -> i64 {
    (amount * RESOURCE_SCALE).ceil() as i64
}

/// A slot capacity in RESOURCE_SCALE units. Rounded down, so a slot is never
/// filled past its real capacity. The CP-SAT model and beam search both use
/// this and scaled_request, so a beam placement always fits the model.
pub fn scaled_capacity(amount: f64) -> i64 {
    (amount * RESOURCE_SCALE) as i64
}

/// Plugin weight every new slot pays, even on an MD with a perfect score, so
/// opening a node is never free.
const PLUGIN_BASE_WEIGHT: f64 = 1.0;
/// Extra plugin weight for a new slot on an MD scored 0, scaled down linearly to
/// nothing at a score of 1.
const PLUGIN_LOW_SCORE_WEIGHT: f64 = 1000.0;

/// Plugin weight of opening one new slot on an MD with this score. The model
/// and calculate_solution_cost both call this, so they can't drift apart.
/// Truncated to an integer, as the model needs.
pub fn plugin_slot_weight(plugin_score: f64) -> i64 {
    (PLUGIN_BASE_WEIGHT + (1.0 - plugin_score) * PLUGIN_LOW_SCORE_WEIGHT) as i64
}

/// Objective penalty for one unplaced pod in partial mode. One unit above the
/// cost of opening every allowed node, so dropping a placeable pod is never
/// cheaper than placing it. Used by the CP-SAT objective and calculate_solution_cost.
pub fn unplaced_pod_penalty(mds: &[MachineDeployment], cpu_waste_weight: f64, memory_waste_weight: f64) -> f64 {
    let max_fleet_cost: f64 = mds
        .iter()
        .map(|md| md.max_scale_out as f64 * md.node_cost(cpu_waste_weight, memory_waste_weight))
        .sum();
    max_fleet_cost * COST_SCALE + COST_SCALE
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum SolverStatus {
    #[default]
    Unknown = 0,
    ModelInvalid = 1,
    Feasible = 2,
    Infeasible = 3,
    Optimal = 4,
}

/// An already-provisioned node belonging to a MachineDeployment, exposed to the
/// solver as a placement target with its remaining free capacity.
#[derive(Debug, Clone)]
pub struct ExistingSlot {
    pub node_name: String,
    /// FREE cpu in cores (allocatable minus current requests), not total capacity.
    pub cpu: f64,
    /// FREE memory in GiB (allocatable minus current requests), not total capacity.
    pub memory: f64,
}

/// Slot-index convention: for an MD with `E = existing_slots.len()`, slot indices
/// `0..E` refer to `existing_slots` (already-provisioned nodes) and indices
/// `E..E + max_scale_out` refer to new nodes that would be created from the
/// `cpu`/`memory` template. `max_scale_out` counts new nodes only.
#[derive(Debug, Clone)]
pub struct MachineDeployment {
    pub name: String,
    /// Node template capacity in cores.
    pub cpu: f64,
    /// Node template capacity in GiB.
    pub memory: f64,
    pub max_scale_out: i32,
    pub existing_slots: Vec<ExistingSlot>,
    /// Real per-node cost (e.g. instance $/h). When set, the objective
    /// minimises actual provisioned cost; when None, cost is derived from
    /// capacity via the configured waste weights.
    pub cost_per_node: Option<f64>,
}

impl MachineDeployment {
    /// Cost of provisioning one node of this MD, used by the objective.
    /// Falls back to resource-weighted capacity when no real price is set.
    pub fn node_cost(&self, cpu_waste_weight: f64, memory_waste_weight: f64) -> f64 {
        self.cost_per_node
            .unwrap_or(self.cpu * cpu_waste_weight + self.memory * memory_waste_weight)
    }

    pub fn total_slots(&self) -> usize {
        self.existing_slots.len() + self.max_scale_out as usize
    }

    pub fn is_existing_slot(&self, slot_idx: usize) -> bool {
        slot_idx < self.existing_slots.len()
    }

    /// Usable (cpu, memory) for a slot. Existing slots carry their own free
    /// capacity, which is already net of system reservation and running pods,
    /// so `allocatable_fraction` applies to template (new-node) slots only.
    pub fn slot_capacity(&self, slot_idx: usize, allocatable_fraction: f64) -> (f64, f64) {
        match self.existing_slots.get(slot_idx) {
            Some(slot) => (slot.cpu, slot.memory),
            None => (self.cpu * allocatable_fraction, self.memory * allocatable_fraction),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Pod {
    /// Requested cpu in cores.
    pub cpu: f64,
    /// Requested memory in GiB.
    pub memory: f64,
    pub labels: HashMap<String, String>,
    pub hard_peers: Vec<usize>,
    pub hard_rules: Vec<i32>,
    pub soft_peers: Vec<usize>,
    pub soft_weights: Vec<f64>,
    pub current_md: Option<String>,
}

/// `slot` follows the MachineDeployment slot-index convention: values below
/// `existing_slots.len()` refer to an existing node (`existing_slots[slot]`),
/// values at or above it refer to a new node to be provisioned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PodAssignment {
    pub md: i32,
    pub slot: i32,
}

impl PodAssignment {
    /// {-1, -1}. A default of {0, 0} would look like a real slot.
    pub const UNPLACED: Self = Self { md: -1, slot: -1 };

    pub fn is_placed(&self) -> bool {
        self.md >= 0 && self.slot >= 0
    }
}

/// Where each pod goes and which slots end up used
#[derive(Debug, Clone, Default)]
pub struct Placement {
    pub assignments: Vec<PodAssignment>,
    pub slots_used: Vec<Vec<bool>>,
    pub unplaced_pods: i32,
}

impl Placement {
    pub fn new(assignments: Vec<PodAssignment>, slots_used: Vec<Vec<bool>>) -> Self {
        let unplaced_pods = assignments.iter().filter(|a| !a.is_placed()).count() as i32;
        Self { assignments, slots_used, unplaced_pods }
    }
}

/// What a plan costs, and whether it's worth switching to
#[derive(Debug, Clone, Default)]
pub struct SolutionCost {
    pub objective: f64,
    /// Tightest CP-SAT lower bound on the objective, 0.0 when no CP-SAT attempt ran
    pub best_objective_bound: f64,
    /// What the current placement costs. None when calculate_current_state is
    /// off, and then every plan is accepted.
    pub current_state_cost: Option<f64>,
    pub improvement_below_threshold: bool,
}

/// How the solve went. Read by logs and the bench, never by placement decisions.
#[derive(Debug, Clone, Default)]
pub struct SolveStats {
    pub solve_time_secs: f64,
    pub attempts_run: i32,
    pub winning_attempt: i32,
    pub beam_used: bool,
    pub beam_fallback_used: bool,
}

#[derive(Debug, Clone, Default)]
pub struct SolverResult {
    pub success: bool,
    pub status: SolverStatus,
    pub message: String,
    pub placement: Placement,
    pub cost: SolutionCost,
    pub stats: SolveStats,
}

#[derive(Debug, Clone)]
pub struct OptimiserConfig {
    // CP-SAT
    pub max_runtime_secs: i32,
    /// CP-SAT worker threads. 0 lets CP-SAT decide from the host core count,
    /// which oversubscribes under a container cpu quota; set to the quota.
    pub num_workers: i32,
    pub max_attempts: i32,

    // Beam search
    pub beam_only: bool,
    pub beam_fallback: bool,
    pub seed_cpsat_with_beam: bool,
    pub seed_attempt: i32,

    // Cost weights
    /// Weight of one wasted CPU core in the waste objective. Default 4.0:
    /// on typical cloud pricing (e.g. AWS m5) 1 vCPU costs about as much as
    /// 4 GiB of memory, so cores are weighted 4x per unit against GiB.
    pub cpu_waste_weight: f64,
    /// Weight of one wasted GiB of memory in the waste objective. Default 1.0.
    pub memory_waste_weight: f64,
    /// Amplifies plugin score influence. Default 1.0, values > 1.0 make plugin scores dominate waste.
    pub plugin_cost_weight: f64,

    // Constraints
    /// Topology spread constraint groups.
    pub topology_groups: Vec<TopologyGroup>,
    /// Affinity groups represent pod affinity topology groups e.g. which pods can place with which
    /// matching the topology key.
    pub affinity_groups: Vec<AffinityGroup>,
    /// Fraction of node capacity usable by workloads (accounts for kubelet/system reservation).
    /// Applied as a hard constraint in both CP-SAT and beam search.
    /// Default 0.98 (2% overhead). Set lower for nodes with higher system reservation.
    pub allocatable_fraction: f64,
    /// Allow best-effort solutions: pods that cannot be placed anywhere are
    /// left unassigned ({md: -1, slot: -1}, counted in
    /// Placement::unplaced_pods) instead of failing the whole solve.
    /// Default false (all-or-nothing).
    pub allow_partial: bool,

    // Current state
    pub calculate_current_state: bool,
    /// Current replica count per MD (for accurate current state cost calculation)
    pub current_replicas: Option<Vec<i32>>,
    pub improvement_threshold: f64,
    pub score_only: bool,
}

impl Default for OptimiserConfig {
    fn default() -> Self {
        Self {
            max_runtime_secs: 12,
            num_workers: 0,
            max_attempts: 1,

            beam_only: false,
            beam_fallback: false,
            seed_cpsat_with_beam: false,
            seed_attempt: 3,

            cpu_waste_weight: 4.0,
            memory_waste_weight: 1.0,
            plugin_cost_weight: 1.0,

            topology_groups: Vec::new(),
            affinity_groups: Vec::new(),
            allocatable_fraction: 0.98,
            allow_partial: false,

            calculate_current_state: true,
            current_replicas: None,
            improvement_threshold: 0.0,
            score_only: false,
        }
    }
}

/// Whether a topology or affinity constraint must hold
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Enforcement {
    /// DoNotSchedule, or required affinity: the model is infeasible if it's broken
    Hard,
    /// ScheduleAnyway, or preferred affinity: breaking it adds a penalty to the objective
    Soft,
}

/// How slots map to the domains of one topology key, e.g. zones. With a zone
/// key, slots in the same zone share a domain. With hostname, every slot gets its own.
#[derive(Debug, Clone)]
pub struct Domains {
    /// Domain index for each slot, -1 for a node without the label. Flattened:
    /// slot index = sum of prior MDs' total_slots() (existing + new slots) +
    /// slot_idx. Existing slots come first within each MD, per the
    /// MachineDeployment slot-index convention.
    pub slot_domains: Vec<i32>,
    /// Number of distinct domains.
    pub num_domains: i32,
    /// Pods already running per domain before this solve.
    pub baseline_counts: Vec<i32>,
}

/// Maps to a Kubernetes topologySpreadConstraint
#[derive(Debug, Clone)]
pub struct TopologyGroup {
    /// Which pods are subject to this topology constraint. Length = num_pods.
    pub pods_in_group: Vec<bool>,
    pub domains: Domains,
    /// Maximum allowed skew between the most-populated and least-populated domains.
    pub max_skew: i32,
    /// Minimum number of domains that must have at least one pod.
    pub min_domains: i32,
    pub enforcement: Enforcement,
}

/// Affinity groups are used to model pods that carry topologyKeys that aren't hostname. Since these
/// require similar matching semantics as TopologyGroups, the implementation looks similar.
///
/// Maps to a Kubernetes Pod Affinity with non hostname topologyKey
#[derive(Debug, Clone)]
pub struct AffinityGroup {
    pub domains: Domains,
    pub enforcement: Enforcement,
    /// The difference between affinity and anti-affinity
    pub colocate: bool,
    /// The pods that match a given topology key e.g. zone=us-west-1a
    pub pods_matching_term: Vec<bool>,
    /// The pods that carry the topology term
    pub pods_carrying_term: Vec<bool>,
}

/// Compute the flat slot index from (md_idx, slot_idx).
/// Flattened over each MD's total slots (existing + new).
pub fn flat_slot_index(mds: &[MachineDeployment], md_idx: usize, slot_idx: usize) -> usize {
    let mut idx = 0;
    for i in 0..md_idx {
        idx += mds[i].total_slots();
    }
    idx + slot_idx
}

pub fn total_flat_slots(mds: &[MachineDeployment]) -> usize {
    mds.iter().map(|md| md.total_slots()).sum()
}
