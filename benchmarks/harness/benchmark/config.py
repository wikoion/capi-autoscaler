"""Benchmark configuration: scenarios, resource profiles, constants."""

from dataclasses import dataclass, field
from pathlib import Path

BENCHMARKS_ROOT = Path(__file__).resolve().parent.parent.parent
MANIFESTS_DIR = BENCHMARKS_ROOT / "manifests"
TEMPLATES_DIR = MANIFESTS_DIR / "workloads"
RESULTS_DIR = BENCHMARKS_ROOT / "results"

WORKLOAD_NAMESPACE = "benchmark-workload"
MGMT_CLUSTER_CONTEXT = "kind-capi-bench"
WORKLOAD_CLUSTER_NAME = "bench-cluster"
KUBECONFIG_SECRET_NAME = f"{WORKLOAD_CLUSTER_NAME}-kubeconfig"

# MachineDeployment names to reset between runs.
# MUST cover every worker pool in manifests/capa-workload-cluster.yaml:
# reset_cluster only scales these, and any pool missing from this list keeps
# its nodes running (and billing) after teardown.
MACHINE_DEPLOYMENTS = [
    "bench-cluster-workers-small",
    "bench-cluster-workers-medium",
    "bench-cluster-workers-large",
    "bench-cluster-workers-general-xl",
    "bench-cluster-workers-general-4xl",
    "bench-cluster-workers-compute-l",
    "bench-cluster-workers-compute-xl",
    "bench-cluster-workers-compute-4xl",
    "bench-cluster-workers-memory-m",
    "bench-cluster-workers-memory-l",
    "bench-cluster-workers-memory-xl",
    "bench-cluster-workers-memory-4xl",
    "bench-cluster-workers-zone-a",
    "bench-cluster-workers-zone-b",
    "bench-cluster-workers-zone-c",
]

# Autoscaler identifiers
AUTOSCALERS = ["capi-autoscaler", "cas", "karpenter"]

# On-demand $/hour per instance type (us-east-1); must match the
# wikoion.dev/cost-per-hour annotations in manifests/capa-workload-cluster.yaml
INSTANCE_HOURLY_COST = {
    "t3.medium": 0.0416,
    "m5.large": 0.096,
    "m5.xlarge": 0.192,
    "m5.2xlarge": 0.384,
    "m5.4xlarge": 0.768,
    "m6i.large": 0.096,
    "c5.xlarge": 0.17,
    "c5.2xlarge": 0.34,
    "c5.4xlarge": 0.68,
    "r5.large": 0.126,
    "r5.xlarge": 0.252,
    # r5.2xlarge is billed at a deliberately discounted benchmark rate (public on-demand 0.504):
    # the cost-mix profile tests whether autoscalers can chase true prices
    "r5.2xlarge": 0.277,
    "r5.4xlarge": 1.008,
}

POLL_INTERVAL_S = 5
TIMEOUT_S = 900  # 15 minutes


@dataclass(frozen=True)
class ResourceProfile:
    name: str
    cpu: str
    memory: str
    template: str  # Template filename in workloads/
    pod_counts: tuple[int, ...] | None = None  # None -> DEFAULT_POD_COUNTS

    def __str__(self) -> str:
        return self.name


# Three research cohorts. unconstrained and heavy-constrained render the same
# template with rules off/on, so the delta is attributable to rule handling;
# light-constrained is topology spread only. Unused templates stay on disk.
PROFILES = {
    "unconstrained": ResourceProfile("unconstrained", "mixed", "mixed", "affinity-pods.yaml.tmpl", pod_counts=(25, 50, 100)),
    "light-constrained": ResourceProfile("light-constrained", "mixed", "mixed", "topology-pods.yaml.tmpl", pod_counts=(25, 50, 100)),
    "heavy-constrained": ResourceProfile("heavy-constrained", "mixed", "mixed", "affinity-pods.yaml.tmpl", pod_counts=(25, 50, 100)),
}


@dataclass(frozen=True)
class WorkloadScenario:
    name: str
    pod_count: int
    profile: ResourceProfile

    def __str__(self) -> str:
        return f"{self.profile.name}-{self.pod_count}"


DEFAULT_POD_COUNTS = [25, 50, 100]


def build_scenarios(
    profiles: list[str] | None = None,
    pod_counts: list[int] | None = None,
) -> list[WorkloadScenario]:
    """Build list of scenarios from profiles and pod counts."""
    profile_names = profiles or list(PROFILES.keys())

    scenarios = []
    for pname in profile_names:
        profile = PROFILES[pname]
        # Explicit pod_counts wins, then the profile's own default list
        counts = pod_counts or profile.pod_counts or DEFAULT_POD_COUNTS
        for count in counts:
            scenarios.append(
                WorkloadScenario(
                    name=f"{pname}-{count}",
                    pod_count=count,
                    profile=profile,
                )
            )
    return scenarios
