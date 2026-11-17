use kube::{Client, api::{Api, ListParams, PatchParams}, runtime::reflector::Store};
use thiserror::Error;
use std::sync::Arc;
use crate::{machine_deployment::MachineDeployment, machine_set::MachineSet};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, LabelSelector};
use k8s_openapi::jiff::Timestamp;
use kube_core::{ObjectList, Selector};
use k8s_openapi::api::core::v1::{Node, Pod};

pub mod api;
pub mod controller;
pub mod optimiser;
pub mod machine_deployment;
pub mod machine_set;
pub mod machine;

use crate::{api::Autoscale, machine_deployment::MachineDeployment as MD};

pub struct ReconcileContext<'a> {
    pub autoscale_name: &'a str,
    pub autoscale_api: &'a Api<Autoscale>,
    pub time: Timestamp,
    pub mds: &'a [MachineDeployment],
    pub ctx: Arc<Context>,
    pub observed_generation: Option<i64>,
    pub node_selector: Option<LabelSelector>,
}


/// In-memory placement-control progress: nodes already handed their pods and
/// which node claimed each (md, solver slot). Lost on restart, rebuilt from
/// live pod state by the first reconcile.
#[derive(Default)]
pub struct PlacedState {
    pub recovered: bool,
    pub nodes: std::collections::HashSet<String>,
    pub claimed_slots: std::collections::HashMap<(String, i32), String>,
    /// "namespace/name" -> when we nominated the pod; backstop for clearing
    /// nominations the resource-fit check cannot fault (e.g. affinity-blocked)
    pub nominated_at: std::collections::HashMap<String, k8s_openapi::jiff::Timestamp>,
}

#[derive(Clone)]
pub struct Context {
    pub client: Client,
    pub remote_client: Client,
    pub namespace: String,
    pub cluster_id: String,
    pub pod_store: Option<Store<Pod>>,
    pub placed: Arc<std::sync::Mutex<PlacedState>>,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Optimiser error: {0}")]
    OptimiserError(#[from] anyhow::Error),
    #[error("parse expression error: {0}")]
    ParseExpressionError(#[from] kube_core::ParseExpressionError),
    #[error("jiff error: {0}")]
    JiffError(#[from] k8s_openapi::jiff::Error),
    #[error("kube error: {0}")]
    KubeError(#[from] kube::Error),
    #[error("Invalid configuration: `{0}`")]
    InvalidConfigError(String),
    #[error("No autoscale resources found")]
    MissingAutoscale,
    #[error("Failed to parse pod affinity or anti affinity")]
    ParsePodAffinity,
    #[error("Found autoscale resource missing a name")]
    MissingAutoscaleName,
    #[error("More than one autoscale resources found, there must only be one per cluster_id: `{0}`")]
    DuplicateAutoscale(String),
    #[error("Autoscale listing error: {0}")]
    AutoscaleError(#[source] kube::Error),
    #[error("Node listing error: {0}")]
    NodeError(#[source] kube::Error),
    #[error("MachineDeployment listing error: {0}")]
    MachineDeploymentError(#[source] kube::Error),
    #[error("MachineDeployment replicas changed")]
    MachineDeploymentReplicasError,
    #[error("Failed to find machineset in owner annotations for node: `{0}`")]
    MachineSetLookupError(String),
    #[error("Improvement below threshold: {0}")]
    OptimiserImprovementThresholdError(f64),
    #[error("Optimiser failed with reason: {0}")]
    OptimiserFailedError(String),
    #[error("Missing expected status condition")]
    MissingStatusCondition,
    #[error("Machineset has no MachineDeployment owner, possibly orphaned: `{0}`")]
    OrphanedMachineDeploymentError(String),
    #[error("DaemonSet query error: {0}")]
    DaemonSetError(#[source] kube::Error),
    #[error("Pod listing error: {0}")]
    PodError(#[source] kube::Error),
    #[error("ReplicaSet listing error: {0}")]
    ReplicaSetError(#[source] kube::Error),
    #[error("yaml error: {0}")]
    YamlError(#[from] serde_yaml::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn get_machine_deployment(name: &str, ctx: Arc<Context>) -> Result<MD> {
    let machine_deployment_api = Api::<MD>::namespaced(ctx.client.clone(), &ctx.namespace);

    let machine_deployment = machine_deployment_api
        .get(name)
        .await
        .map_err(Error::MachineDeploymentError)?;

    Ok(machine_deployment)
}

pub async fn get_autoscale(ctx: Arc<Context>) -> Result<Autoscale> {
    let autoscale_api = Api::<Autoscale>::namespaced(ctx.client.clone(), &ctx.namespace);

    let autoscale_list = autoscale_api
        .list(&ListParams::default().labels(&format!("wikoion.dev/cluster-id={}", ctx.cluster_id)))
        .await
        .map_err(Error::AutoscaleError)?
        .items;
    let autoscale = match autoscale_list.len() {
        1 => autoscale_list.into_iter().next().unwrap(),
        0 => return Err(Error::MissingAutoscale),
        _ => return Err(Error::DuplicateAutoscale("".to_string()))
    };

    Ok(autoscale)
}

pub async fn get_machine_deployments(ctx: Arc<Context>) -> Result<Vec<MachineDeployment>> {
    let machine_deployment_api = Api::<MachineDeployment>::namespaced(ctx.client.clone(), &ctx.namespace);

    let machine_deployment_list = machine_deployment_api
        .list(&ListParams::default().labels(&format!("wikoion.dev/cluster-id={}", ctx.cluster_id)))
        .await
        .map_err(Error::MachineDeploymentError)?
        .items;

    Ok(machine_deployment_list)
}

pub async fn get_machine_sets(rc: &ReconcileContext<'_>) -> Result<ObjectList<MachineSet>> {
    let machine_set_api = Api::<MachineSet>::namespaced(rc.ctx.clone().client.clone(), &rc.ctx.namespace);

    // MachineSets only carry CAPI's own labels, custom ones like
    // wikoion.dev/cluster-id don't propagate from the MD, so filtering on
    // them returns nothing.
    let machine_set_list = machine_set_api
        .list(&ListParams::default().labels(&format!("cluster.x-k8s.io/cluster-name={}", rc.ctx.clone().cluster_id)))
        .await?;

    Ok(machine_set_list)
}

pub async fn get_remote_nodes(rc: &ReconcileContext<'_>) -> Result<ObjectList<Node>> {
    let node_api = Api::<Node>::all(rc.ctx.remote_client.clone());
    let mut lp = ListParams::default();

    if let Some(selector) = rc.node_selector.clone() {
	let ls: Selector = selector.try_into()?;
	lp = lp.labels_from(&ls);
    } 

    let node_list = node_api
        .list(&lp)
        .await
        .map_err(Error::NodeError)?;

    Ok(node_list)
}

pub async fn get_remote_pods(
    rc: &ReconcileContext<'_>,
    selector: &Option<LabelSelector>,
) -> Result<ObjectList<Pod>> {
    let pod_api = Api::<Pod>::all(rc.ctx.remote_client.clone());
    let mut lp = ListParams::default();

    if let Some(sel) = selector {
	let ls: Selector = sel.clone().try_into()?;
	lp = lp.labels_from(&ls);
    } 

    let pod_list = pod_api
        .list(&lp)
        .await
        .map_err(Error::NodeError)?;

    Ok(pod_list)
}

pub async fn scale_md(md_name: &str, replicas: i32, ctx: Arc<Context>) -> Result<()> {
    let machine_deployment_api = Api::<MD>::namespaced(ctx.client.clone(), &ctx.namespace);

    let patch_body = serde_json::json!({
        "apiVersion": "cluster.x-k8s.io/v1beta1",
        "kind": "MachineDeployment",
        "metadata": {
            "name": md_name,
            "namespace": &ctx.namespace
        },
        "spec": {
            "replicas": replicas
        }
    });

    let params = PatchParams::apply("capi-autoscaler").force();
    machine_deployment_api.patch(md_name, &params, &kube::api::Patch::Apply(&patch_body)).await?;

    Ok(())
}

pub const PLACEMENT_GATE: &str = "wikoion.dev/placement";

// Pods gated only by our placement gate are planning demand; a gate we
// don't own means another controller is holding the pod back
pub fn has_non_placement_gate(pod: &Pod) -> bool {
    pod.spec.as_ref()
        .and_then(|s| s.scheduling_gates.as_ref())
        .is_some_and(|g| g.iter().any(|g| g.name != PLACEMENT_GATE))
}

// A nominated pod's capacity is already planned; counting it as pending
// double-provisions while the scheduler works through the nomination.
// The controller clears nominations that go stale (clear_stale_nominations),
// which puts the pod back into planning.
pub fn has_nominated_node(pod: &Pod) -> bool {
    pod.status.as_ref()
        .is_some_and(|s| s.nominated_node_name.is_some())
}

pub fn is_unschedulable(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| {s.conditions.as_ref()})
        .map(|conditions| {
            conditions.iter()
                .any(|c| {
                    c.type_ == "PodScheduled" && c.status == "False"
                })
        })
        .unwrap_or(false)
}

pub fn is_node_ready(node: &Node) -> bool {
    node.status
        .as_ref()
        .and_then(|s| {s.conditions.as_ref()})
        .map(|conditions| {
            conditions.iter()
                .any(|c| {
                    c.type_ == "Ready" && c.status == "True"
                })
        })
        .unwrap_or(false)
}
