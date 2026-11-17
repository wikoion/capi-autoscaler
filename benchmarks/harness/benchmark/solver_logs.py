"""Scrape solver metrics from the capi-autoscaler pod log after a run.

deploy_autoscaler rollout-restarts the controller at the start of every run,
so the current pod's full log spans exactly the current run.
"""

import re
from dataclasses import dataclass

AUTOSCALER_NAMESPACE = "capi-autoscaler-system"
AUTOSCALER_LABEL = "app=capi-autoscaler"

# Matches the line emitted per solve in autoscaler/src/optimiser.rs:
# Solver result: status=Optimal objective=-1234.5 unplaced=0 used_beam=false solve_time=2.41s
SOLVER_RESULT_RE = re.compile(
    r"Solver result: status=(?P<status>\S+) objective=(?P<objective>-?[\d.]+) "
    r"unplaced=(?P<unplaced>\d+) used_beam=(?P<used_beam>true|false) "
    r"solve_time=(?P<solve_time>[\d.]+)s"
)


@dataclass
class SolverSolve:
    attempt: int
    status: str
    objective: float
    unplaced: int
    used_beam: bool
    solve_time_secs: float


def collect_solver_metrics(mgmt_client) -> list[SolverSolve]:
    """Parse Solver result lines from the running controller pod. Never raises;
    a scrape failure must not fail a benchmark run."""
    # Imported here so the analysis package can import this module for the
    # regex without needing click/kubernetes installed
    import click
    from kubernetes import client

    try:
        core_v1 = client.CoreV1Api(mgmt_client)
        pods = core_v1.list_namespaced_pod(
            namespace=AUTOSCALER_NAMESPACE, label_selector=AUTOSCALER_LABEL
        ).items
        running = [p for p in pods if p.status.phase == "Running"]
        if not running:
            click.echo("  solver metrics: no running capi-autoscaler pod found")
            return []
        pod = max(running, key=lambda p: p.metadata.creation_timestamp)

        restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
        if restarts > 0:
            click.echo(
                f"  solver metrics: pod restarted {restarts}x mid-run, "
                "earlier solve attempts may be missing from the log"
            )

        log = core_v1.read_namespaced_pod_log(
            name=pod.metadata.name, namespace=AUTOSCALER_NAMESPACE
        )
        solves = []
        for i, m in enumerate(SOLVER_RESULT_RE.finditer(log), start=1):
            solves.append(SolverSolve(
                attempt=i,
                status=m.group("status"),
                objective=float(m.group("objective")),
                unplaced=int(m.group("unplaced")),
                used_beam=m.group("used_beam") == "true",
                solve_time_secs=float(m.group("solve_time")),
            ))
        click.echo(f"  solver metrics: captured {len(solves)} solve(s)")
        return solves
    except Exception as e:
        click.echo(f"  solver metrics: scrape failed, continuing without ({e})")
        return []
