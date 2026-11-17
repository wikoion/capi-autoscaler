"""Workload pod injection via Jinja2 template rendering."""

import math
import yaml
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from kubernetes import client

from .config import TEMPLATES_DIR, WORKLOAD_NAMESPACE, WorkloadScenario


def _get_jinja_env() -> Environment:
    return Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        keep_trailing_newline=True,
    )


def render_template(scenario: WorkloadScenario, run_id: str) -> str:
    """Render a workload template with the given scenario parameters."""
    env = _get_jinja_env()
    template = env.get_template(scenario.profile.template)

    context: dict = {"run_id": run_id}

    if scenario.profile.name in ("heavy-constrained", "unconstrained"):
        total = scenario.pod_count
        context["profile"] = scenario.profile.name
        context["constrained"] = scenario.profile.name == "heavy-constrained"
        context["total_replicas"] = total
        context["web_replicas"] = round(total * 0.20)
        context["cache_replicas"] = round(total * 0.12)
        context["pair_replicas"] = round(total * 0.10)
        context["batch_replicas"] = round(total * 0.10)
        context["db_replicas"] = round(total * 0.06)
        context["api_replicas"] = round(total * 0.12)
        context["memcache_replicas"] = round(total * 0.20)
        context["standalone_replicas"] = round(total * 0.06)
        # Ensure we hit exactly total
        context["filler_replicas"] = total - (
            context["web_replicas"]
            + context["cache_replicas"]
            + context["pair_replicas"]
            + context["batch_replicas"]
            + context["db_replicas"]
            + context["api_replicas"]
            + context["memcache_replicas"]
            + context["standalone_replicas"]
        )
    elif scenario.profile.name == "light-constrained":
        total = scenario.pod_count
        context["profile"] = scenario.profile.name
        context["total_replicas"] = total
        context["zonespread_replicas"] = round(total * 0.25)
        context["hostspread_replicas"] = round(total * 0.15)
        context["affinity_replicas"] = round(total * 0.20)
        # Ensure we hit exactly total
        context["filler_replicas"] = total - (
            context["zonespread_replicas"]
            + context["hostspread_replicas"]
            + context["affinity_replicas"]
        )
    else:
        context["replicas"] = scenario.pod_count

    # Rounding can push the components past total, e.g. heavy-constrained at 9
    if context.get("filler_replicas", 0) < 0:
        raise ValueError(
            f"{scenario.profile.name} components round to more than {scenario.pod_count} pods"
        )

    return template.render(**context)


def inject_pods(
    api_client: client.ApiClient,
    scenario: WorkloadScenario,
    run_id: str,
) -> None:
    """Render workload template and apply to cluster."""
    apps_v1 = client.AppsV1Api(api_client)
    core_v1 = client.CoreV1Api(api_client)

    # Ensure namespace exists
    try:
        core_v1.create_namespace(
            client.V1Namespace(metadata=client.V1ObjectMeta(name=WORKLOAD_NAMESPACE))
        )
    except client.exceptions.ApiException as e:
        if e.status != 409:  # Already exists
            raise

    rendered = render_template(scenario, run_id)
    docs = list(yaml.safe_load_all(rendered))

    for doc in docs:
        if doc is None:
            continue
        if doc["kind"] == "Deployment":
            try:
                apps_v1.create_namespaced_deployment(
                    namespace=WORKLOAD_NAMESPACE, body=doc
                )
            except client.exceptions.ApiException as e:
                if e.status == 409:
                    apps_v1.replace_namespaced_deployment(
                        name=doc["metadata"]["name"],
                        namespace=WORKLOAD_NAMESPACE,
                        body=doc,
                    )
                else:
                    raise


def clear_pods(api_client: client.ApiClient) -> None:
    """Delete all benchmark workload deployments."""
    apps_v1 = client.AppsV1Api(api_client)
    try:
        apps_v1.delete_collection_namespaced_deployment(
            namespace=WORKLOAD_NAMESPACE,
            label_selector="app=benchmark-workload",
        )
    except client.exceptions.ApiException:
        pass  # Namespace may not exist yet
