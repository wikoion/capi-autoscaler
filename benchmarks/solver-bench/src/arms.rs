//! Solver configurations per benchmark arm. The prod arm mirrors
//! construct_scale_up_solver_input in autoscaler/src/optimiser.rs; the RQ3
//! cpsat arms strip the hint and fallback so beam vs CP-SAT stays clean.

use optimiser::OptimiserConfig;

use crate::generator::Instance;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Arm {
    /// Production config at a given budget (RQ2)
    Prod { budget: i32, max_attempts: i32 },
    /// Beam search only, width fixed at 3 inside the solver (RQ3)
    Beam,
    /// Single-attempt CP-SAT at a given budget, no hint, no fallback (RQ3)
    CpSat { budget: i32 },
}

impl Arm {
    pub fn parse(s: &str, budget: i32, max_attempts: i32) -> Option<Self> {
        match s {
            "prod" => Some(Self::Prod { budget, max_attempts }),
            "beam" => Some(Self::Beam),
            _ => {
                let secs: i32 = s.strip_prefix("cpsat-")?.parse().ok()?;
                Some(Self::CpSat { budget: secs })
            }
        }
    }

    pub fn name(&self) -> String {
        match self {
            Self::Prod { budget, .. } => format!("prod-{budget}"),
            Self::Beam => "beam".to_string(),
            Self::CpSat { budget } => format!("cpsat-{budget}"),
        }
    }

    pub fn budget_secs(&self) -> i32 {
        match self {
            Self::Prod { budget, .. } | Self::CpSat { budget } => *budget,
            Self::Beam => 0,
        }
    }

    pub fn config(&self, inst: &Instance, num_workers: i32) -> OptimiserConfig {
        // Prod base (autoscaler/src/optimiser.rs:692). improvement_threshold 0
        // instead of prod's 10: dead code at current cost 0, zero removes the
        // ambiguity. calculate_current_state false keeps every solve accepted.
        let base = OptimiserConfig {
            max_runtime_secs: 30,
            improvement_threshold: 0.0,
            score_only: false,
            calculate_current_state: false,
            max_attempts: 5,
            seed_cpsat_with_beam: true,
            seed_attempt: 3,
            beam_fallback: true,
            beam_only: false,
            current_replicas: Some(vec![0; inst.mds.len()]),
            allocatable_fraction: 1.0,
            num_workers,
            allow_partial: true,
            topology_groups: inst.topology_groups.clone(),
            affinity_groups: inst.affinity_groups.clone(),
            ..Default::default()
        };

        match self {
            Self::Prod { budget, max_attempts } => OptimiserConfig {
                max_runtime_secs: *budget,
                max_attempts: *max_attempts,
                ..base
            },
            Self::Beam => OptimiserConfig { beam_only: true, ..base },
            Self::CpSat { budget } => OptimiserConfig {
                max_runtime_secs: *budget,
                max_attempts: 1,
                seed_cpsat_with_beam: false,
                beam_fallback: false,
                ..base
            },
        }
    }
}
