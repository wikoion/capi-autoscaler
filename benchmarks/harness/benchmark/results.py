"""CSV output for benchmark results."""

import csv
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .config import RESULTS_DIR
from .metrics import TimeSeriesPoint


@dataclass
class RunResult:
    autoscaler: str
    scenario_name: str
    pod_count: int
    profile: str
    run_num: int
    run_id: str
    first_unschedulable_s: float | None
    all_scheduled_s: float | None
    all_running_s: float | None
    final_nodes: int
    final_cpu_util_pct: float
    final_mem_util_pct: float
    timed_out: bool
    final_instance_counts: dict[str, int] = field(default_factory=dict)
    final_cost_per_hour: float = 0.0
    time_series: list[TimeSeriesPoint] = field(default_factory=list)
    solver_solves: list = field(default_factory=list)

    @property
    def latency_s(self) -> float | None:
        """End-to-end latency: time from pod injection to all pods running."""
        return self.all_running_s


def write_results(results: list[RunResult], prefix: str = "benchmark") -> tuple[Path, Path]:
    """Write summary and time-series CSVs."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    summary_path = RESULTS_DIR / f"{prefix}_{timestamp}_summary.csv"
    timeseries_path = RESULTS_DIR / f"{prefix}_{timestamp}_timeseries.csv"

    # Summary CSV
    with open(summary_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "autoscaler",
            "scenario",
            "pod_count",
            "profile",
            "run_num",
            "run_id",
            "latency_s",
            "first_unschedulable_s",
            "all_scheduled_s",
            "all_running_s",
            "worker_nodes",
            "workload_cpu_util_pct",
            "workload_mem_util_pct",
            "cost_per_hour",
            "instance_counts",
            "timed_out",
        ])
        for r in results:
            writer.writerow([
                r.autoscaler,
                r.scenario_name,
                r.pod_count,
                r.profile,
                r.run_num,
                r.run_id,
                r.latency_s if r.latency_s is not None else "N/A",
                r.first_unschedulable_s if r.first_unschedulable_s is not None else "N/A",
                r.all_scheduled_s if r.all_scheduled_s is not None else "N/A",
                r.all_running_s if r.all_running_s is not None else "N/A",
                r.final_nodes,
                r.final_cpu_util_pct,
                r.final_mem_util_pct,
                round(r.final_cost_per_hour, 4),
                ";".join(f"{t}={n}" for t, n in sorted(r.final_instance_counts.items())),
                r.timed_out,
            ])

    # Time-series CSV
    with open(timeseries_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "autoscaler",
            "scenario",
            "run_id",
            "elapsed_s",
            "ready_nodes",
            "worker_nodes",
            "pending_pods",
            "scheduled_pods",
            "running_pods",
            "workload_cpu_requested_m",
            "worker_cpu_allocatable_m",
            "workload_mem_requested_mi",
            "worker_mem_allocatable_mi",
        ])
        for r in results:
            for ts in r.time_series:
                writer.writerow([
                    r.autoscaler,
                    r.scenario_name,
                    r.run_id,
                    ts.elapsed_s,
                    ts.ready_nodes,
                    ts.worker_nodes,
                    ts.pending_pods,
                    ts.scheduled_pods,
                    ts.running_pods,
                    ts.workload_cpu_requested_m,
                    ts.worker_cpu_allocatable_m,
                    ts.workload_mem_requested_mi,
                    ts.worker_mem_allocatable_mi,
                ])

    # Solver CSV, only written when at least one run captured solver metrics
    if any(r.solver_solves for r in results):
        solver_path = RESULTS_DIR / f"{prefix}_{timestamp}_solver.csv"
        with open(solver_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "autoscaler",
                "scenario",
                "run_id",
                "attempt",
                "status",
                "objective",
                "unplaced",
                "used_beam",
                "solve_time_secs",
            ])
            for r in results:
                for s in r.solver_solves:
                    writer.writerow([
                        r.autoscaler,
                        r.scenario_name,
                        r.run_id,
                        s.attempt,
                        s.status,
                        s.objective,
                        s.unplaced,
                        str(s.used_beam).lower(),
                        s.solve_time_secs,
                    ])
        print(f"Solver:      {solver_path}")

    print(f"Summary:     {summary_path}")
    print(f"Time-series: {timeseries_path}")
    return summary_path, timeseries_path
