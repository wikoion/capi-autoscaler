"""Click-based CLI entry point for the benchmark harness."""

import subprocess
import sys
from pathlib import Path

import click

from .config import AUTOSCALERS, PROFILES, build_scenarios, BENCHMARKS_ROOT
from .runner import run_benchmark


@click.group()
def main():
    """CAPI Autoscaler Benchmark Harness."""
    pass


@main.command()
@click.option(
    "--autoscaler",
    type=click.Choice(AUTOSCALERS),
    required=True,
    help="Which autoscaler to benchmark.",
)
@click.option(
    "--scenario",
    "scenario_filter",
    type=str,
    default="all",
    help="Scenario filter: 'all', a profile name (small/medium/large/mixed), or 'profile-count' (e.g. small-10).",
)
@click.option(
    "--pods",
    type=int,
    default=None,
    help="Override pod count (single count instead of default list).",
)
@click.option(
    "--profile",
    type=click.Choice(list(PROFILES.keys())),
    default=None,
    help="Override resource profile.",
)
@click.option(
    "--runs",
    type=int,
    default=3,
    help="Number of runs per scenario.",
)
@click.option(
    "--output-prefix",
    type=str,
    default="benchmark",
    help="Prefix for output CSV files.",
)
@click.option(
    "--teardown",
    is_flag=True,
    default=False,
    help="Tear down all infrastructure after benchmarks complete.",
)
@click.option(
    "--consolidation",
    is_flag=True,
    default=False,
    help="Karpenter only: enable consolidation and hold each run for the full "
    "timeout window, recording the post-consolidation cluster state.",
)
def run(autoscaler, scenario_filter, pods, profile, runs, output_prefix, teardown, consolidation):
    """Run benchmark scenarios."""

    if consolidation and autoscaler != "karpenter":
        click.echo("--consolidation is only valid with --autoscaler karpenter", err=True)
        sys.exit(1)

    # Build scenario list
    if profile and pods:
        scenarios = build_scenarios(profiles=[profile], pod_counts=[pods])
    elif profile:
        scenarios = build_scenarios(profiles=[profile])
    elif pods:
        profiles_list = (
            [scenario_filter]
            if scenario_filter in PROFILES
            else list(PROFILES.keys())
        )
        scenarios = build_scenarios(profiles=profiles_list, pod_counts=[pods])
    elif scenario_filter == "all":
        scenarios = build_scenarios()
    elif scenario_filter in PROFILES:
        scenarios = build_scenarios(profiles=[scenario_filter])
    else:
        click.echo(f"Unknown scenario filter: {scenario_filter}", err=True)
        sys.exit(1)

    click.echo(f"Autoscaler: {autoscaler}")
    click.echo(f"Scenarios:  {len(scenarios)}")
    click.echo(f"Runs/each:  {runs}")
    click.echo(f"Total runs: {len(scenarios) * runs}")
    click.echo()

    results = run_benchmark(
        autoscaler=autoscaler,
        scenarios=scenarios,
        runs=runs,
        output_prefix=output_prefix,
        consolidation=consolidation,
    )

    # Print summary
    click.echo(f"\n{'='*60}")
    click.echo("BENCHMARK COMPLETE")
    click.echo(f"{'='*60}")
    for r in results:
        status = "TIMEOUT" if r.timed_out else f"{r.latency_s:.1f}s"
        click.echo(
            f"  {r.autoscaler:20s} | {r.scenario_name:15s} | run {r.run_num} | {status}"
        )

    if teardown:
        click.echo("\nTearing down infrastructure...")
        scripts_dir = BENCHMARKS_ROOT / "scripts"
        subprocess.run(
            [str(scripts_dir / "cleanup-all.sh")],
            check=True,
        )


@main.command()
@click.option(
    "--target",
    type=click.Choice(["capa", "eks"]),
    required=True,
    help="Which infrastructure to set up.",
)
def setup(target):
    """Set up benchmark infrastructure (CAPA management cluster or EKS)."""
    scripts_dir = BENCHMARKS_ROOT / "scripts"

    if target == "capa":
        click.echo("Setting up CAPA management cluster...")
        subprocess.run(
            [str(scripts_dir / "create-aws-mgmt-cluster.sh")],
            check=True,
        )
    elif target == "eks":
        click.echo("Setting up EKS cluster with Karpenter...")
        subprocess.run(
            [str(scripts_dir / "create-eks-cluster.sh")],
            check=True,
        )


@main.command()
def reset():
    """Reset the workload cluster (scale all MachineDeployments to 0)."""
    from .cluster import get_mgmt_client, reset_cluster

    mgmt_client = get_mgmt_client()
    reset_cluster(mgmt_client)
    click.echo("Cluster reset complete.")


@main.command()
@click.option(
    "--date",
    "date_filter",
    type=str,
    default=None,
    help="Only include summary CSVs whose timestamp starts with this date (YYYYMMDD). Defaults to all.",
)
def summarise(date_filter):
    """Aggregate summary CSVs into a per-autoscaler, per-scenario comparison.

    Files are processed in chronological order; if a later file contains rows
    for an (autoscaler, scenario) pair seen earlier, the later rows replace the
    earlier ones (so scenario reruns supersede invalidated runs).
    """
    import csv as csvmod
    from collections import defaultdict
    from .config import RESULTS_DIR

    files = sorted(RESULTS_DIR.glob("benchmark_*_summary.csv"))
    if date_filter:
        files = [f for f in files if f.name.startswith(f"benchmark_{date_filter}")]
    if not files:
        click.echo("No summary CSVs found.", err=True)
        sys.exit(1)

    # (autoscaler, scenario) -> list of row dicts; later files replace earlier
    rows_by_key: dict[tuple[str, str], list[dict]] = {}
    for f in files:
        file_keys_seen: set[tuple[str, str]] = set()
        with open(f, newline="") as fh:
            for row in csvmod.DictReader(fh):
                key = (row["autoscaler"], row["scenario"])
                if key not in file_keys_seen:
                    rows_by_key[key] = []  # later file supersedes earlier rows
                    file_keys_seen.add(key)
                rows_by_key[key].append(row)

    def fmean(rows, field):
        vals = [float(r[field]) for r in rows if r[field] not in ("N/A", "", None)]
        return sum(vals) / len(vals) if vals else None

    scenarios = sorted({k[1] for k in rows_by_key}, key=lambda s: int(s.rsplit("-", 1)[1]))
    autoscalers = sorted({k[0] for k in rows_by_key})

    header = (
        f"{'scenario':<14}{'autoscaler':<17}{'ok':>5}{'latency_s':>11}"
        f"{'nodes':>7}{'cpu%':>7}{'mem%':>7}{'$/hour':>9}"
    )
    click.echo(header)
    click.echo("-" * len(header))
    for scenario in scenarios:
        for autoscaler in autoscalers:
            rows = rows_by_key.get((autoscaler, scenario))
            if not rows:
                continue
            ok = [r for r in rows if r["timed_out"] != "True"]
            lat = fmean(ok, "latency_s")
            nodes = fmean(ok, "worker_nodes")
            cpu = fmean(ok, "workload_cpu_util_pct")
            mem = fmean(ok, "workload_mem_util_pct")
            cost = fmean(ok, "cost_per_hour") if ok and "cost_per_hour" in ok[0] else None
            click.echo(
                f"{scenario:<14}{autoscaler:<17}{f'{len(ok)}/{len(rows)}':>5}"
                + (f"{lat:>11.1f}" if lat is not None else f"{'—':>11}")
                + (f"{nodes:>7.1f}" if nodes is not None else f"{'—':>7}")
                + (f"{cpu:>7.1f}" if cpu is not None else f"{'—':>7}")
                + (f"{mem:>7.1f}" if mem is not None else f"{'—':>7}")
                + (f"{cost:>9.2f}" if cost is not None else f"{'—':>9}")
            )


@main.command()
@click.option("--all", "cleanup_all", is_flag=True, help="Clean up all resources.")
def cleanup(cleanup_all):
    """Clean up benchmark resources."""
    if cleanup_all:
        scripts_dir = BENCHMARKS_ROOT / "scripts"
        click.echo("Cleaning up all benchmark resources...")
        subprocess.run(
            [str(scripts_dir / "cleanup-all.sh")],
            check=True,
        )
    else:
        click.echo("Use --all to clean up all resources, or use 'make cleanup'.")


if __name__ == "__main__":
    main()
