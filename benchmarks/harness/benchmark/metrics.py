"""Metrics collection: pod watcher and node poller threads."""

import time
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone

from kubernetes import client, watch

from .config import WORKLOAD_NAMESPACE, POLL_INTERVAL_S


@dataclass
class TimeSeriesPoint:
    elapsed_s: float
    ready_nodes: int
    pending_pods: int
    scheduled_pods: int
    running_pods: int
    # Workload-only resource requests (benchmark pods only)
    workload_cpu_requested_m: int
    workload_mem_requested_mi: int
    # Worker node allocatable (excludes control plane / system-only nodes)
    worker_cpu_allocatable_m: int
    worker_mem_allocatable_mi: int
    worker_nodes: int


@dataclass
class MetricsCollector:
    """Collects pod and node metrics in background threads."""

    workload_client: client.ApiClient
    mgmt_client: client.ApiClient | None = None
    run_id: str = ""
    pod_count: int = 0

    # Timestamps
    start_time: float = 0.0
    first_unschedulable_time: float | None = None
    all_scheduled_time: float | None = None
    all_running_time: float | None = None

    # Time-series data
    time_series: list[TimeSeriesPoint] = field(default_factory=list)

    # Latest snapshot of ready worker nodes by instance type, for cost reporting
    latest_instance_counts: dict[str, int] = field(default_factory=dict)
    # Snapshot at the first sample with every pod running. The final sample can
    # exclude nodes Karpenter has tainted for consolidation, inflating
    # utilisation and undercounting cost.
    steady_point: TimeSeriesPoint | None = None
    steady_instance_counts: dict[str, int] | None = None

    # Control
    _stop_event: threading.Event = field(default_factory=threading.Event)
    _threads: list[threading.Thread] = field(default_factory=list)

    def start(self) -> None:
        self.start_time = time.monotonic()
        self._stop_event.clear()

        pod_thread = threading.Thread(
            target=self._poll_pods, name="pod-poller", daemon=True
        )
        node_thread = threading.Thread(
            target=self._poll_nodes, name="node-poller", daemon=True
        )
        self._threads = [pod_thread, node_thread]
        for t in self._threads:
            t.start()

    def stop(self) -> None:
        self._stop_event.set()
        for t in self._threads:
            t.join(timeout=10)

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.start_time

    @property
    def is_complete(self) -> bool:
        return self.all_running_time is not None

    def _poll_pods(self) -> None:
        core_v1 = client.CoreV1Api(self.workload_client)
        while not self._stop_event.is_set():
            try:
                # Without a timeout a dead connection hangs this thread forever
                # and all_running_time never gets set (false TIMEOUT)
                pods = core_v1.list_namespaced_pod(
                    namespace=WORKLOAD_NAMESPACE,
                    label_selector=f"bench-run={self.run_id}",
                    _request_timeout=30,
                )
                pending = 0
                scheduled = 0
                running = 0
                unschedulable = False

                for pod in pods.items:
                    phase = pod.status.phase
                    if phase == "Running":
                        running += 1
                        scheduled += 1
                    elif phase == "Pending":
                        pending += 1
                        # Check for unschedulable condition
                        if pod.status.conditions:
                            for cond in pod.status.conditions:
                                if (
                                    cond.type == "PodScheduled"
                                    and cond.status == "False"
                                    and cond.reason == "Unschedulable"
                                ):
                                    unschedulable = True
                    else:
                        scheduled += 1

                total_benchmark_pods = len(pods.items)

                if unschedulable and self.first_unschedulable_time is None:
                    self.first_unschedulable_time = self.elapsed

                if (
                    total_benchmark_pods > 0
                    and pending == 0
                    and self.all_scheduled_time is None
                ):
                    self.all_scheduled_time = self.elapsed

                if (
                    total_benchmark_pods > 0
                    and running == total_benchmark_pods
                    and self.all_running_time is None
                ):
                    self.all_running_time = self.elapsed

            except Exception:
                pass  # Transient API errors during scaling

            self._stop_event.wait(POLL_INTERVAL_S)

    def _poll_nodes(self) -> None:
        core_v1 = client.CoreV1Api(self.workload_client)
        while not self._stop_event.is_set():
            try:
                nodes = core_v1.list_node(_request_timeout=30)
                ready_nodes = 0
                worker_cpu_alloc_m = 0
                worker_mem_alloc_mi = 0
                worker_nodes = 0
                instance_counts: dict[str, int] = {}

                for node in nodes.items:
                    is_ready = False
                    if node.status.conditions:
                        for cond in node.status.conditions:
                            if cond.type == "Ready" and cond.status == "True":
                                is_ready = True
                                ready_nodes += 1
                                break

                    # Skip nodes that can't run benchmark pods:
                    # - Control plane nodes (CAPA)
                    # - Nodes with NoSchedule taints (EKS system node group)
                    labels = node.metadata.labels or {}
                    is_control_plane = (
                        "node-role.kubernetes.io/control-plane" in labels
                        or "node-role.kubernetes.io/master" in labels
                    )
                    has_noschedule = False
                    if node.spec.taints:
                        for taint in node.spec.taints:
                            if taint.effect == "NoSchedule":
                                has_noschedule = True
                                break
                    is_worker = not is_control_plane and not has_noschedule
                    if is_worker and is_ready and node.status.allocatable:
                        worker_nodes += 1
                        cpu = node.status.allocatable.get("cpu", "0")
                        mem = node.status.allocatable.get("memory", "0")
                        worker_cpu_alloc_m += _parse_cpu_millicores(cpu)
                        worker_mem_alloc_mi += _parse_memory_mi(mem)
                        itype = labels.get("node.kubernetes.io/instance-type", "unknown")
                        instance_counts[itype] = instance_counts.get(itype, 0) + 1

                # Workload-only resource requests (benchmark pods only)
                bench_pods = core_v1.list_namespaced_pod(
                    namespace=WORKLOAD_NAMESPACE,
                    label_selector=f"bench-run={self.run_id}",
                    _request_timeout=30,
                )
                workload_cpu_req_m = 0
                workload_mem_req_mi = 0
                pending = 0
                scheduled = 0
                running = 0

                for pod in bench_pods.items:
                    phase = pod.status.phase
                    if phase == "Running":
                        running += 1
                    elif phase == "Pending":
                        pending += 1
                    else:
                        scheduled += 1

                    # Sum resource requests for all benchmark pods
                    for container in pod.spec.containers or []:
                        if container.resources and container.resources.requests:
                            cpu = container.resources.requests.get("cpu", "0")
                            mem = container.resources.requests.get("memory", "0")
                            workload_cpu_req_m += _parse_cpu_millicores(cpu)
                            workload_mem_req_mi += _parse_memory_mi(mem)

                point = TimeSeriesPoint(
                    elapsed_s=round(self.elapsed, 1),
                    ready_nodes=ready_nodes,
                    pending_pods=pending,
                    scheduled_pods=scheduled + running,
                    running_pods=running,
                    workload_cpu_requested_m=workload_cpu_req_m,
                    workload_mem_requested_mi=workload_mem_req_mi,
                    worker_cpu_allocatable_m=worker_cpu_alloc_m,
                    worker_mem_allocatable_mi=worker_mem_alloc_mi,
                    worker_nodes=worker_nodes,
                )
                self.time_series.append(point)
                self.latest_instance_counts = instance_counts
                # Keep the all-running sample that saw the most allocatable:
                # Karpenter can taint nodes for consolidation within seconds,
                # shrinking the counted fleet on later samples
                if (self.pod_count > 0 and running == self.pod_count
                        and (self.steady_point is None
                             or point.worker_cpu_allocatable_m
                             > self.steady_point.worker_cpu_allocatable_m)):
                    self.steady_point = point
                    self.steady_instance_counts = dict(instance_counts)

            except Exception:
                pass  # Transient API errors during scaling

            self._stop_event.wait(POLL_INTERVAL_S)


def _parse_cpu_millicores(cpu_str: str) -> int:
    """Parse a Kubernetes CPU quantity to millicores."""
    if cpu_str.endswith("m"):
        return int(cpu_str[:-1])
    try:
        return int(float(cpu_str) * 1000)
    except ValueError:
        return 0


def _parse_memory_mi(mem_str: str) -> int:
    """Parse a Kubernetes memory quantity to MiB."""
    suffixes = {
        "Ki": 1 / 1024,
        "Mi": 1,
        "Gi": 1024,
        "Ti": 1024 * 1024,
        "K": 1000 / (1024 * 1024),
        "M": 1000_000 / (1024 * 1024),
        "G": 1000_000_000 / (1024 * 1024),
    }
    for suffix, factor in suffixes.items():
        if mem_str.endswith(suffix):
            try:
                return int(float(mem_str[: -len(suffix)]) * factor)
            except ValueError:
                return 0
    try:
        # Plain bytes
        return int(int(mem_str) / (1024 * 1024))
    except ValueError:
        return 0
