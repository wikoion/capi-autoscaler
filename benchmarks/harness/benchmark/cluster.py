"""Cluster lifecycle: reset, deploy autoscaler, kubeconfig management."""

import base64
import subprocess
import tempfile
import time
from pathlib import Path

import yaml
from kubernetes import client, config

from .config import (
    MACHINE_DEPLOYMENTS,
    MANIFESTS_DIR,
    MGMT_CLUSTER_CONTEXT,
    WORKLOAD_CLUSTER_NAME,
    WORKLOAD_NAMESPACE,
    KUBECONFIG_SECRET_NAME,
    POLL_INTERVAL_S,
)


def get_mgmt_client() -> client.ApiClient:
    """Get a Kubernetes API client for the management cluster."""
    cfg = client.Configuration()
    config.load_kube_config(context=MGMT_CLUSTER_CONTEXT, client_configuration=cfg)
    return client.ApiClient(cfg)


def get_workload_kubeconfig(mgmt_client: client.ApiClient) -> str:
    """Extract workload cluster kubeconfig from the management cluster secret."""
    core_v1 = client.CoreV1Api(mgmt_client)
    secret = core_v1.read_namespaced_secret(
        name=KUBECONFIG_SECRET_NAME, namespace="default"
    )
    kubeconfig_b64 = secret.data["value"]
    return base64.b64decode(kubeconfig_b64).decode("utf-8")


def get_workload_client(mgmt_client: client.ApiClient) -> client.ApiClient:
    """Get a Kubernetes API client for the workload cluster."""
    kubeconfig_yaml = get_workload_kubeconfig(mgmt_client)

    # Write to temp file for kubernetes client
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".kubeconfig", delete=False, prefix="bench-"
    )
    tmp.write(kubeconfig_yaml)
    tmp.flush()

    cfg = client.Configuration()
    config.load_kube_config(config_file=tmp.name, client_configuration=cfg)
    return client.ApiClient(cfg)


def get_eks_client(cluster_name: str = "bench-karpenter") -> client.ApiClient:
    """Get a Kubernetes API client for the EKS cluster.

    Finds the kubeconfig context matching the cluster name (set by eksctl during
    cluster creation) and loads it.
    """
    # Find the context matching this cluster name
    result = subprocess.run(
        ["kubectl", "config", "get-contexts", "-o", "name"],
        capture_output=True,
        text=True,
    )
    contexts = result.stdout.strip().split("\n")
    matching = [c for c in contexts if cluster_name in c]
    if not matching:
        raise RuntimeError(
            f"No kubeconfig context found for EKS cluster '{cluster_name}'. "
            f"Available contexts: {contexts}"
        )
    # Prefer the eksctl-written context: create-eks-cluster.sh refreshes it on
    # every create, while a leftover arn:aws:eks context can still point at a
    # torn-down cluster's endpoint
    eksctl_contexts = [c for c in matching if c.endswith(".eksctl.io")]
    context_name = (eksctl_contexts or matching)[0]
    _get_echo()(f"Using EKS context: {context_name}")

    cfg = client.Configuration()
    config.load_kube_config(context=context_name, client_configuration=cfg)
    return client.ApiClient(cfg)


def reset_cluster(mgmt_client: client.ApiClient) -> None:
    """Scale all MachineDeployments to 0 replicas and wait for machines to deprovision."""
    apps_client = client.CustomObjectsApi(mgmt_client)
    click_echo = _get_echo()

    click_echo("Scaling all MachineDeployments to 0 replicas...")
    for md_name in MACHINE_DEPLOYMENTS:
        try:
            apps_client.patch_namespaced_custom_object(
                group="cluster.x-k8s.io",
                version="v1beta1",
                namespace="default",
                plural="machinedeployments",
                name=md_name,
                body={"spec": {"replicas": 0}},
            )
            click_echo(f"  Scaled {md_name} to 0")
        except client.exceptions.ApiException as e:
            click_echo(f"  Warning: Could not scale {md_name}: {e.reason}")

    # Wait for all machines to be deprovisioned
    click_echo("Waiting for machines to deprovision...")
    deadline = time.monotonic() + 600  # 10 minute timeout
    while time.monotonic() < deadline:
        try:
            machines = apps_client.list_namespaced_custom_object(
                group="cluster.x-k8s.io",
                version="v1beta1",
                namespace="default",
                plural="machines",
                label_selector=f"cluster.x-k8s.io/cluster-name={WORKLOAD_CLUSTER_NAME}",
            )
        except Exception as e:
            # Transient connection resets must not kill a whole bench leg
            click_echo(f"  Warning: machine list failed, retrying: {e}")
            time.sleep(POLL_INTERVAL_S)
            continue
        worker_machines = [
            m
            for m in machines.get("items", [])
            if "control-plane" not in m["metadata"]["name"]
        ]
        if len(worker_machines) == 0:
            click_echo("All worker machines deprovisioned.")
            return
        click_echo(
            f"  {len(worker_machines)} worker machines remaining..."
        )
        time.sleep(POLL_INTERVAL_S)

    click_echo("WARNING: Timed out waiting for machines to deprovision")


def reset_karpenter_cluster(eks_client: client.ApiClient) -> None:
    """Reset Karpenter-managed cluster by deleting workload pods and NodeClaims.

    Waits for Karpenter to consolidate nodes back to the system node group only (2 nodes).
    """
    click_echo = _get_echo()

    # 1. Delete all deployments in benchmark-workload namespace
    click_echo("Deleting benchmark workload deployments...")
    apps_v1 = client.AppsV1Api(eks_client)
    try:
        apps_v1.delete_collection_namespaced_deployment(
            namespace=WORKLOAD_NAMESPACE,
            label_selector="app=benchmark-workload",
        )
    except client.exceptions.ApiException:
        pass  # Namespace may not exist yet

    # 2. Delete all NodeClaims to force Karpenter node termination
    click_echo("Deleting all Karpenter NodeClaims...")
    custom_api = client.CustomObjectsApi(eks_client)
    try:
        nodeclaims = custom_api.list_cluster_custom_object(
            group="karpenter.sh",
            version="v1",
            plural="nodeclaims",
        )
        for nc in nodeclaims.get("items", []):
            nc_name = nc["metadata"]["name"]
            try:
                custom_api.delete_cluster_custom_object(
                    group="karpenter.sh",
                    version="v1",
                    plural="nodeclaims",
                    name=nc_name,
                )
                click_echo(f"  Deleted NodeClaim {nc_name}")
            except client.exceptions.ApiException as e:
                click_echo(f"  Warning: Could not delete NodeClaim {nc_name}: {e.reason}")
    except client.exceptions.ApiException as e:
        click_echo(f"  Warning: Could not list NodeClaims: {e.reason}")

    # 3. Wait for Karpenter-managed nodes to terminate (only system node group remains = 2 nodes)
    click_echo("Waiting for Karpenter nodes to terminate...")
    deadline = time.monotonic() + 600  # 10 minute timeout
    while time.monotonic() < deadline:
        core_v1 = client.CoreV1Api(eks_client)
        nodes = core_v1.list_node()
        # System node group nodes don't have the karpenter.sh/nodepool label
        karpenter_nodes = [
            n for n in nodes.items
            if n.metadata.labels and "karpenter.sh/nodepool" in n.metadata.labels
        ]
        if len(karpenter_nodes) == 0:
            click_echo("All Karpenter-managed nodes terminated.")
            return
        click_echo(f"  {len(karpenter_nodes)} Karpenter nodes remaining...")
        time.sleep(POLL_INTERVAL_S)

    click_echo("WARNING: Timed out waiting for Karpenter nodes to terminate")


def set_karpenter_consolidation(eks_client: client.ApiClient, enabled: bool) -> None:
    """Patch the bench-pool NodePool disruption block. Enabled switches to
    WhenEmptyOrUnderutilized/1m for the consolidation scenario; disabled
    restores the manifest defaults (WhenEmpty/5m)."""
    click_echo = _get_echo()
    if enabled:
        disruption = {"consolidationPolicy": "WhenEmptyOrUnderutilized", "consolidateAfter": "1m"}
    else:
        disruption = {"consolidationPolicy": "WhenEmpty", "consolidateAfter": "5m"}

    custom_api = client.CustomObjectsApi(eks_client)
    custom_api.patch_cluster_custom_object(
        group="karpenter.sh",
        version="v1",
        plural="nodepools",
        name="bench-pool",
        body={"spec": {"disruption": disruption}},
    )
    click_echo(f"NodePool bench-pool disruption set to {disruption}")


def deploy_autoscaler(
    mgmt_client: client.ApiClient,
    autoscaler: str,
) -> None:
    """Deploy the specified autoscaler's manifests."""
    click_echo = _get_echo()

    if autoscaler == "capi-autoscaler":
        click_echo("Deploying capi-autoscaler (Autoscale CR)...")
        _kubectl_apply(MANIFESTS_DIR / "autoscale-benchmark.yaml")
        click_echo("Restarting capi-autoscaler controller...")
        subprocess.run(
            ["kubectl", "--context", MGMT_CLUSTER_CONTEXT, "-n", "capi-autoscaler-system",
             "rollout", "restart", "deployment/capi-autoscaler"],
            check=True, capture_output=True, text=True,
        )
        subprocess.run(
            ["kubectl", "--context", MGMT_CLUSTER_CONTEXT, "-n", "capi-autoscaler-system",
             "wait", "--for=condition=available", "deployment/capi-autoscaler", "--timeout=120s"],
            check=True, capture_output=True, text=True,
        )

    elif autoscaler == "cas":
        click_echo("Deploying Cluster Autoscaler (CAS)...")
        _kubectl_apply(MANIFESTS_DIR / "cas" / "cluster-autoscaler.yaml")

    elif autoscaler == "karpenter":
        click_echo("Karpenter is pre-deployed on EKS cluster, skipping deploy step.")

    else:
        raise ValueError(f"Unknown autoscaler: {autoscaler}")


def remove_autoscaler(
    mgmt_client: client.ApiClient,
    autoscaler: str,
) -> None:
    """Remove the specified autoscaler's manifests."""
    click_echo = _get_echo()

    if autoscaler == "capi-autoscaler":
        click_echo("Removing capi-autoscaler (Autoscale CRD)...")
        _kubectl_delete(MANIFESTS_DIR / "autoscale-benchmark.yaml")
        # The controller installs a placement-gate MAP in the workload cluster on
        # startup; if it survives into a cas leg every pod stays gated and cas
        # times out. Remove it with the rest of the capi artifacts.
        click_echo("Removing placement-gate MutatingAdmissionPolicy...")
        custom = client.CustomObjectsApi(get_workload_client(mgmt_client))
        for plural in ("mutatingadmissionpolicybindings", "mutatingadmissionpolicies"):
            try:
                custom.delete_cluster_custom_object(
                    group="admissionregistration.k8s.io",
                    version="v1beta1",
                    plural=plural,
                    name="capi-autoscaler-placement-gate",
                )
            except client.exceptions.ApiException as e:
                if e.status != 404:
                    click_echo(f"Warning: failed to delete {plural}: {e.reason}")

    elif autoscaler == "cas":
        click_echo("Removing Cluster Autoscaler (CAS)...")
        _kubectl_delete(MANIFESTS_DIR / "cas" / "cluster-autoscaler.yaml")

    elif autoscaler == "karpenter":
        click_echo("Karpenter managed by EKS setup, skipping removal.")


def _kubectl_apply(manifest_path: Path) -> None:
    result = subprocess.run(
        ["kubectl", "--context", MGMT_CLUSTER_CONTEXT, "apply", "-f", str(manifest_path)],
        capture_output=True,
        text=True,
    )
    if result.stdout:
        _get_echo()(result.stdout.strip())
    if result.returncode != 0:
        raise RuntimeError(f"kubectl apply failed: {result.stderr.strip()}")


def _kubectl_delete(manifest_path: Path) -> None:
    result = subprocess.run(
        ["kubectl", "--context", MGMT_CLUSTER_CONTEXT, "delete", "-f", str(manifest_path),
         "--ignore-not-found"],
        capture_output=True,
        text=True,
    )
    if result.stdout:
        _get_echo()(result.stdout.strip())
    if result.returncode != 0:
        _get_echo()(f"Warning: kubectl delete failed: {result.stderr.strip()}")


def _get_echo():
    """Get click.echo if available, otherwise print."""
    try:
        import click
        return click.echo
    except ImportError:
        return print
