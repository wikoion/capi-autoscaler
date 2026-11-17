//! Sweep loops. One row per solve; deterministic cell order.

use std::path::Path;
use std::time::Instant;

use anyhow::Result;
use optimiser::solver::optimise_placement;

use crate::arms::Arm;
use crate::csvout::CsvWriter;
use crate::generator::{Instance, Profile, SlotsLevel, build};

pub const POD_COUNTS: [i32; 3] = [25, 50, 100];
pub const MD_COUNTS: [usize; 3] = [5, 10, 15];

pub fn num_workers() -> i32 {
    // Cgroup-aware, same as prod (autoscaler/src/optimiser.rs:706)
    std::thread::available_parallelism().map_or(0, |n| n.get() as i32)
}

pub struct Cell {
    pub profile: Profile,
    pub pod_count: i32,
    pub md_count: usize,
    pub slots: SlotsLevel,
    pub arm: Arm,
    pub rep: i32,
}

pub fn run_cell(sweep: &str, cell: &Cell, workers: i32) -> Result<Vec<String>> {
    let inst = build(cell.profile, cell.pod_count, cell.md_count, cell.slots);
    let config = cell.arm.config(&inst, workers);

    let started = Instant::now();
    let result = optimise_placement(
        inst.mds.clone(),
        inst.pods.clone(),
        inst.plugin_scores.clone(),
        inst.allowed_matrix.clone(),
        config,
    )?;
    let wall = started.elapsed().as_secs_f64();

    let (nodes, cost) = fleet_from_slots(&inst, &result.placement.slots_used);
    let instance_id = format!(
        "{}-{}-{}-{}",
        cell.profile.name(), cell.pod_count, cell.md_count, cell.slots.name()
    );

    eprintln!(
        "{sweep} {} arm={} rep={} status={:?} objective={:.1} unplaced={} wall={:.2}s nodes={} cost={:.3}",
        instance_id, cell.arm.name(), cell.rep, result.status, result.cost.objective,
        result.placement.unplaced_pods, wall, nodes, cost
    );

    // Gap is only meaningful when CP-SAT ran and returned a solution
    let gap = if result.stats.attempts_run > 0 && result.success && result.cost.objective > 0.0 {
        format!("{}", (result.cost.objective - result.cost.best_objective_bound) / result.cost.objective)
    } else {
        String::new()
    };

    Ok(vec![
        sweep.to_string(),
        cell.arm.name(),
        cell.arm.budget_secs().to_string(),
        cell.profile.name().to_string(),
        cell.pod_count.to_string(),
        cell.md_count.to_string(),
        cell.slots.name().to_string(),
        cell.rep.to_string(),
        instance_id,
        inst.pods.len().to_string(),
        inst.mds.len().to_string(),
        inst.total_slots.to_string(),
        workers.to_string(),
        result.success.to_string(),
        format!("{:?}", result.status),
        format!("{}", result.cost.objective),
        format!("{}", result.stats.solve_time_secs),
        format!("{wall}"),
        result.placement.unplaced_pods.to_string(),
        result.stats.beam_used.to_string(),
        result.stats.beam_fallback_used.to_string(),
        result.stats.attempts_run.to_string(),
        result.stats.winning_attempt.to_string(),
        nodes.to_string(),
        format!("{cost}"),
        format!("{}", result.cost.best_objective_bound),
        gap,
    ])
}

fn fleet_from_slots(inst: &Instance, slots_used: &[Vec<bool>]) -> (usize, f64) {
    let mut nodes = 0;
    let mut cost = 0.0;
    for (md_idx, slots) in slots_used.iter().enumerate() {
        let used = slots.iter().filter(|s| **s).count();
        nodes += used;
        if let Some(pool) = inst.pools.get(md_idx) {
            cost += used as f64 * pool.cost;
        }
    }
    (nodes, cost)
}

/// RQ2: factorial sweep under the production config.
pub fn run_rq2(out: &Path, reps: i32, budget: i32, max_attempts: i32) -> Result<()> {
    let mut csv = CsvWriter::create(out)?;
    let workers = num_workers();
    let arm = Arm::Prod { budget, max_attempts };

    for profile in Profile::ALL {
        for pod_count in POD_COUNTS {
            for md_count in MD_COUNTS {
                for slots in SlotsLevel::ALL {
                    for rep in 1..=reps {
                        let cell = Cell { profile, pod_count, md_count, slots, arm, rep };
                        csv.write_row(&run_cell("rq2", &cell, workers)?)?;
                    }
                }
            }
        }
    }
    Ok(())
}

/// RQ3: budget ladder on the cluster-shaped instances (15 MDs, full slots).
pub fn run_rq3(out: &Path, reps: i32) -> Result<()> {
    let mut csv = CsvWriter::create(out)?;
    let workers = num_workers();
    let arms = [
        Arm::Beam,
        Arm::CpSat { budget: 1 },
        Arm::CpSat { budget: 2 },
        Arm::CpSat { budget: 10 },
        Arm::CpSat { budget: 30 },
    ];

    for profile in Profile::ALL {
        for pod_count in POD_COUNTS {
            for arm in arms {
                for rep in 1..=reps {
                    let cell = Cell {
                        profile, pod_count, md_count: 15, slots: SlotsLevel::High, arm, rep,
                    };
                    csv.write_row(&run_cell("rq3", &cell, workers)?)?;
                }
            }
        }
    }
    Ok(())
}

/// RQ4: consolidation headroom. The hardest cluster-shaped instance
/// (heavy-constrained, 100 pods) at the production 30s budget vs a 600s
/// budget a background scale-down pass could afford.
pub fn run_rq4(out: &Path, reps: i32) -> Result<()> {
    let mut csv = CsvWriter::create(out)?;
    let workers = num_workers();
    let arms = [Arm::CpSat { budget: 30 }, Arm::CpSat { budget: 600 }];

    for arm in arms {
        for rep in 1..=reps {
            let cell = Cell {
                profile: Profile::HeavyConstrained,
                pod_count: 100,
                md_count: 15,
                slots: SlotsLevel::High,
                arm,
                rep,
            };
            csv.write_row(&run_cell("rq4", &cell, workers)?)?;
        }
    }
    Ok(())
}
