"""Benchmark orchestrator: reset -> deploy -> inject -> collect -> clean."""

import time
import uuid

import click

from .cluster import (
    deploy_autoscaler,
    get_eks_client,
    get_mgmt_client,
    get_workload_client,
    remove_autoscaler,
    reset_cluster,
    reset_karpenter_cluster,
    set_karpenter_consolidation,
)
from .config import INSTANCE_HOURLY_COST, TIMEOUT_S, POLL_INTERVAL_S, WorkloadScenario
from .metrics import MetricsCollector
from .results import RunResult, write_results
from .solver_logs import collect_solver_metrics
from .workload import clear_pods, inject_pods


def run_benchmark(
    autoscaler: str,
    scenarios: list[WorkloadScenario],
    runs: int = 3,
    output_prefix: str = "benchmark",
    consolidation: bool = False,
) -> list[RunResult]:
    """Run benchmarks for all scenarios with the given autoscaler."""
    all_results: list[RunResult] = []

    # Set up clients
    if autoscaler == "karpenter":
        workload_client = get_eks_client()
        mgmt_client = None
    else:
        mgmt_client = get_mgmt_client()
        workload_client = get_workload_client(mgmt_client)

    if consolidation:
        set_karpenter_consolidation(workload_client, True)

    for scenario in scenarios:
        for run_num in range(1, runs + 1):
            # One retry per run: transient connection resets must cost at most
            # one run, never the whole leg's results
            for attempt in range(2):
                run_id = f"{scenario.name}-{run_num}-{uuid.uuid4().hex[:6]}"
                click.echo(f"\n{'='*60}")
                click.echo(
                    f"Run {run_num}/{runs}: {autoscaler} | {scenario} | id={run_id}"
                )
                click.echo(f"{'='*60}")

                try:
                    result = _execute_single_run(
                        autoscaler=autoscaler,
                        scenario=scenario,
                        run_id=run_id,
                        run_num=run_num,
                        mgmt_client=mgmt_client,
                        workload_client=workload_client,
                        consolidation=consolidation,
                    )
                except Exception as e:
                    if attempt == 0:
                        click.echo(f"Run failed, retrying once: {e}")
                        continue
                    click.echo(f"Run failed twice, skipping: {e}")
                    break
                all_results.append(result)
                break

    # Clean up after all runs; completed results must be written regardless
    try:
        if autoscaler == "karpenter":
            if consolidation:
                click.echo("Restoring NodePool disruption defaults...")
                set_karpenter_consolidation(workload_client, False)
            click.echo("Final Karpenter reset...")
            reset_karpenter_cluster(workload_client)
        elif mgmt_client is not None:
            click.echo("Removing autoscaler...")
            remove_autoscaler(mgmt_client, autoscaler)
            click.echo("Tearing down worker nodes...")
            reset_cluster(mgmt_client)
    except Exception as e:
        click.echo(f"Cleanup failed (continuing to write results): {e}")

    write_results(all_results, output_prefix)
    return all_results


def _execute_single_run(
    autoscaler: str,
    scenario: WorkloadScenario,
    run_id: str,
    run_num: int,
    mgmt_client,
    workload_client,
    consolidation: bool = False,
) -> RunResult:
    """Execute a single benchmark run."""

    # 1. Remove autoscaler + clear leftover pods before reset so the autoscaler
    #    doesn't fight the scale-to-0
    if mgmt_client is not None:
        click.echo("Step 1a: Removing autoscaler and clearing pods...")
        remove_autoscaler(mgmt_client, autoscaler)
        clear_pods(workload_client)

    # 1b. Reset cluster to 0 workers
    if autoscaler == "karpenter":
        click.echo("Step 1b: Resetting Karpenter cluster...")
        reset_karpenter_cluster(workload_client)
    elif mgmt_client is not None:
        click.echo("Step 1b: Resetting cluster...")
        reset_cluster(mgmt_client)

    # 2. Deploy/verify autoscaler
    click.echo("Step 2: Deploying autoscaler...")
    if mgmt_client is not None:
        deploy_autoscaler(mgmt_client, autoscaler)

    # 3. Start metrics collection
    click.echo("Step 3: Starting metrics collection...")
    collector = MetricsCollector(
        workload_client=workload_client,
        mgmt_client=mgmt_client,
        run_id=run_id,
        pod_count=scenario.pod_count,
    )
    collector.start()

    # 4. Inject workload pods
    click.echo(
        f"Step 4: Injecting {scenario.pod_count} {scenario.profile.name} pods..."
    )
    inject_pods(workload_client, scenario, run_id)

    # 5. Wait for completion or timeout. Consolidation runs hold the full
    #    window so Karpenter can consolidate; final state is read at the end.
    click.echo("Step 5: Waiting for pods to be scheduled and running...")
    deadline = time.monotonic() + TIMEOUT_S
    all_running_logged = False
    while time.monotonic() < deadline:
        if collector.is_complete and not all_running_logged:
            click.echo(
                f"  All pods running after {collector.all_running_time:.1f}s"
            )
            all_running_logged = True
            if not consolidation:
                break
        remaining = int(deadline - time.monotonic())
        if remaining % 30 < POLL_INTERVAL_S:
            ts = collector.time_series[-1] if collector.time_series else None
            if ts:
                click.echo(
                    f"  [{ts.elapsed_s:.0f}s] nodes={ts.ready_nodes} "
                    f"pending={ts.pending_pods} running={ts.running_pods}"
                )
        time.sleep(POLL_INTERVAL_S)
    else:
        if not collector.is_complete:
            click.echo(f"  TIMEOUT after {TIMEOUT_S}s")

    # 6. Finalize metrics
    click.echo("Step 6: Finalizing metrics...")
    collector.stop()

    # 6b. Scrape solver metrics before clear_pods so cleanup solves are excluded
    solver_solves = []
    if autoscaler == "capi-autoscaler" and mgmt_client is not None:
        click.echo("Step 6b: Collecting solver metrics...")
        solver_solves = collect_solver_metrics(mgmt_client)

    # Build result from the first all-running sample when there is one;
    # the last sample can catch Karpenter mid-consolidation. Consolidation
    # runs want exactly that post-consolidation state, so they use the last
    # sample instead.
    if consolidation:
        last_ts = collector.time_series[-1] if collector.time_series else None
        instance_counts = collector.latest_instance_counts
    else:
        last_ts = collector.steady_point or (
            collector.time_series[-1] if collector.time_series else None
        )
        instance_counts = (
            collector.steady_instance_counts or collector.latest_instance_counts
        )
    result = RunResult(
        autoscaler="karpenter-consolidation" if consolidation else autoscaler,
        scenario_name=scenario.name,
        pod_count=scenario.pod_count,
        profile=scenario.profile.name,
        run_num=run_num,
        run_id=run_id,
        first_unschedulable_s=collector.first_unschedulable_time,
        all_scheduled_s=collector.all_scheduled_time,
        all_running_s=collector.all_running_time,
        final_nodes=last_ts.worker_nodes if last_ts else 0,
        final_cpu_util_pct=(
            round(last_ts.workload_cpu_requested_m / max(last_ts.worker_cpu_allocatable_m, 1) * 100, 1)
            if last_ts
            else 0.0
        ),
        final_mem_util_pct=(
            round(
                last_ts.workload_mem_requested_mi / max(last_ts.worker_mem_allocatable_mi, 1) * 100, 1
            )
            if last_ts
            else 0.0
        ),
        timed_out=collector.all_running_time is None,
        solver_solves=solver_solves,
        final_instance_counts=instance_counts,
        final_cost_per_hour=sum(
            n * INSTANCE_HOURLY_COST.get(t, 0.0)
            for t, n in instance_counts.items()
        ),
        time_series=collector.time_series,
    )

    # 7. Clean up pods
    click.echo("Step 7: Cleaning up workload pods...")
    clear_pods(workload_client)

    return result
