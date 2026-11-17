use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::sync::Arc;
use capi_autoscaler::{Context, ReconcileContext};
use capi_autoscaler::optimiser::{self, ScaleUpResult};
use capi_autoscaler::api::Autoscale;
use capi_autoscaler::api::autoscale_types::AutoscaleSpec;
use capi_autoscaler::machine_set::*;
use capi_autoscaler::machine_deployment::*;
use http::{Method, Request, Response};
use http_body_util::Full;
use k8s_openapi::api::apps::v1::DaemonSet;
use k8s_openapi::api::core::v1::{Container, Node, NodeCondition, NodeStatus, Pod, PodSpec, PodStatus, ResourceRequirements};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference, Time};
use k8s_openapi::jiff::Timestamp;
use kube::{Client, Resource};
use kube::api::Api;
use kube::client::Body;
use kube_core::{ListMeta, ObjectList, TypeMeta};
use tower::service_fn;

// ---------------------------------------------------------------------------
// Mock management and workload API server
// ---------------------------------------------------------------------------

struct Store {
    pods: Vec<Arc<Pod>>,
    nodes: Vec<Node>,
    mds: Vec<MachineDeployment>,
    mss: Vec<MachineSet>,
}

fn list<T: Clone>(items: &[T]) -> ObjectList<T> {
    ObjectList { types: TypeMeta::default(), metadata: ListMeta::default(), items: items.to_vec() }
}

fn find<'a, T: Resource>(items: &'a [T], name: &str) -> Option<&'a T> {
    items.iter().find(|item| item.meta().name.as_deref() == Some(name))
}

fn mock_client(namespace: &str, store: Arc<Store>) -> Client {
    let svc = service_fn(move |req: Request<Body>| {
        let store = store.clone();
        async move {
            let path = req.uri().path().to_string();
            let name = path.rsplit('/').next().unwrap_or("").to_string();

            let response = match (req.method().clone(), path.as_str()) {
                (Method::GET, p) if p.starts_with("/api/v1/nodes") => match find(&store.nodes, &name) {
                    Some(node) => serde_json::to_vec(node).unwrap(),
                    None => serde_json::to_vec(&list(&store.nodes)).unwrap(),
                },
                (Method::GET, p) if p.contains("/machinesets") => match find(&store.mss, &name) {
                    Some(ms) => serde_json::to_vec(ms).unwrap(),
                    None => serde_json::to_vec(&list(&store.mss)).unwrap(),
                },
                (Method::GET | Method::PATCH, p) if p.contains("/machinedeployments/") => {
                    serde_json::to_vec(&find(&store.mds, &name)).unwrap()
                }
                (Method::POST, p) if p.contains("/events") => {
                    // Echo a minimal valid Event for create()
                    serde_json::to_vec(&serde_json::json!({
                        "metadata": {"name": "evt", "namespace": "default"},
                        "involvedObject": {}
                    })).unwrap()
                }
                (Method::GET, p) if p.starts_with("/api/v1/pods") => {
                    let pods: Vec<Pod> = store.pods.iter().map(|p| (**p).clone()).collect();
                    serde_json::to_vec(&list(&pods)).unwrap()
                }
                (Method::GET, p) if p.contains("/daemonsets") => {
                    serde_json::to_vec(&list::<DaemonSet>(&[])).unwrap()
                }
                _ => panic!("unexpected request: {} {}", req.method(), req.uri()),
            };

            Ok::<_, Infallible>(Response::new(Full::from(response)))
        }
    });

    Client::new(svc, namespace)
}

// ---------------------------------------------------------------------------
// Fixtures. test-instance-N is an MD, test-instance-N-ms its MachineSet,
// test-node-N a workload node and test-pod-N a pod.
// ---------------------------------------------------------------------------

fn ms_name(n: usize) -> String {
    format!("test-instance-{n}-ms")
}

/// An MD whose status reports `replicas` machines ready, as CAPA does once
/// machines are up, whether or not their nodes have joined.
fn instance(n: usize, cpu: &str, memory: &str, replicas: i32, max_replicas: i32) -> MachineDeployment {
    let annotations = BTreeMap::from([
        ("capacity.cluster-autoscaler.kubernetes.io/cpu".to_string(), cpu.to_string()),
        ("capacity.cluster-autoscaler.kubernetes.io/memory".to_string(), memory.to_string()),
        ("cluster.x-k8s.io/cluster-api-autoscaler-node-group-max-size".to_string(), max_replicas.to_string()),
    ]);

    MachineDeployment {
        metadata: ObjectMeta {
            name: Some(format!("test-instance-{n}")),
            annotations: Some(annotations),
            ..Default::default()
        },
        spec: MachineDeploymentSpec {
            cluster_name: "test-cluster".to_string(),
            replicas: Some(replicas),
            selector: MachineDeploymentSelector { match_expressions: None, match_labels: None },
            template: MachineDeploymentTemplate { metadata: None, spec: None },
            min_ready_seconds: None,
            paused: None,
            progress_deadline_seconds: None,
            revision_history_limit: None,
            strategy: None,
        },
        status: Some(MachineDeploymentStatus {
            available_replicas: Some(replicas),
            conditions: None,
            observed_generation: None,
            phase: None,
            ready_replicas: Some(replicas),
            replicas: Some(replicas),
            selector: None,
            unavailable_replicas: None,
            updated_replicas: Some(replicas),
        }),
    }
}

/// test-instance-N-ms, owned by test-instance-N.
fn machine_set(n: usize) -> MachineSet {
    MachineSet {
        metadata: ObjectMeta {
            name: Some(ms_name(n)),
            owner_references: Some(vec![OwnerReference {
                api_version: "cluster.x-k8s.io/v1beta2".to_string(),
                kind: "MachineDeployment".to_string(),
                name: format!("test-instance-{n}"),
                ..Default::default()
            }]),
            ..Default::default()
        },
        spec: MachineSetSpec {
            cluster_name: "test-cluster".to_string(),
            deletion: None,
            machine_naming: None,
            replicas: Some(1),
            selector: MachineSetSelector { match_expressions: None, match_labels: None },
            template: MachineSetTemplate {
                metadata: None,
                spec: MachineSetTemplateSpec {
                    bootstrap: MachineSetTemplateSpecBootstrap { config_ref: None, data_secret_name: None },
                    cluster_name: "test-cluster".to_string(),
                    deletion: None,
                    failure_domain: None,
                    infrastructure_ref: MachineSetTemplateSpecInfrastructureRef {
                        api_group: "infrastructure.cluster.x-k8s.io".to_string(),
                        kind: "DockerMachineTemplate".to_string(),
                        name: "test-infra".to_string(),
                    },
                    min_ready_seconds: None,
                    provider_id: None,
                    readiness_gates: None,
                    taints: None,
                    version: None,
                },
            },
        },
        status: None,
    }
}

/// test-node-N, owned by the given MachineSet.
fn node(n: usize, owner_ms: &str, cpu: &str, memory: &str, ready: bool) -> Node {
    Node {
        metadata: ObjectMeta {
            name: Some(format!("test-node-{n}")),
            annotations: Some(BTreeMap::from([
                ("cluster.x-k8s.io/owner-name".to_string(), owner_ms.to_string()),
            ])),
            ..Default::default()
        },
        status: Some(NodeStatus {
            allocatable: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity(cpu.to_string())),
                ("memory".to_string(), Quantity(memory.to_string())),
            ])),
            conditions: Some(vec![NodeCondition {
                type_: "Ready".to_string(),
                status: if ready { "True" } else { "False" }.to_string(),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn container(requests: Option<BTreeMap<String, Quantity>>) -> Container {
    Container {
        name: "test-container".to_string(),
        resources: Some(ResourceRequirements { requests, ..Default::default() }),
        ..Default::default()
    }
}

/// test-pod-N, bound to `node` when given, pending otherwise.
fn pod(n: usize, node: Option<&str>, cpu: &str, memory: &str) -> Pod {
    let requests = BTreeMap::from([
        ("cpu".to_string(), Quantity(cpu.to_string())),
        ("memory".to_string(), Quantity(memory.to_string())),
    ]);
    Pod {
        metadata: ObjectMeta { name: Some(format!("test-pod-{n}")), ..Default::default() },
        spec: Some(PodSpec {
            node_name: node.map(str::to_string),
            containers: vec![container(Some(requests))],
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn with_status(mut pod: Pod, status: PodStatus) -> Pod {
    pod.status = Some(status);
    pod
}

fn secs(s: u64) -> kube_core::Duration {
    std::time::Duration::from_secs(s).into()
}

fn autoscale() -> Autoscale {
    Autoscale::new("test-autoscale", AutoscaleSpec {
        node_selector: None,
        scan_interval: secs(15),
        pod_scale_up_delay: secs(15),
        max_scale_out: 8,
        scaling_timeout: secs(900),
        solver_timeout: secs(20),
        unready_node_limit: 4,
        min_improvement_percent: Some(5),
    })
}

/// Runs plan_scale_up against the mock cluster for every pod with no node.
async fn plan(store: &Arc<Store>, force_replan: bool) -> ScaleUpResult {
    let ctx = Arc::new(Context {
        client: mock_client("test-ns", store.clone()),
        remote_client: mock_client("default", store.clone()),
        namespace: "test-ns".to_string(),
        cluster_id: "test-cluster".to_string(),
        pod_store: None,
        placed: Default::default(),
    });
    let autoscale_api = Api::<Autoscale>::namespaced(ctx.client.clone(), &ctx.namespace);
    let rc = ReconcileContext {
        autoscale_name: "test-autoscale",
        autoscale_api: &autoscale_api,
        time: Timestamp::now(),
        mds: &store.mds,
        ctx: ctx.clone(),
        observed_generation: None,
        node_selector: None,
    };

    let pending: Vec<Arc<Pod>> = store.pods.iter()
        .filter(|p| p.spec.as_ref().and_then(|s| s.node_name.as_ref()).is_none())
        .cloned()
        .collect();

    optimiser::plan_scale_up(&store.mds, &pending, &autoscale(), &rc, force_replan)
        .await
        .expect("plan_scale_up failed")
}

fn targets(result: &ScaleUpResult) -> BTreeMap<&str, i32> {
    result.target_replicas.iter().map(|(md, t)| (md.as_str(), t.target)).collect()
}

// ---------------------------------------------------------------------------
// compute_existing_slots
// ---------------------------------------------------------------------------

fn node_to_md(pairs: &[(usize, usize)]) -> HashMap<String, String> {
    pairs.iter().map(|(node, md)| (format!("test-node-{node}"), format!("test-instance-{md}"))).collect()
}

#[test]
fn existing_slots_cover_only_ready_mapped_nodes() {
    let nodes = list(&[
        node(1, &ms_name(1), "4", "8Gi", true),
        node(2, &ms_name(2), "16", "32Gi", true),
        node(3, &ms_name(1), "4", "8Gi", false), // not Ready
        node(4, "unknown-ms", "8", "16Gi", true), // no MD
    ]);

    // A bound pod with no requests counts as nothing used
    let mut no_requests = pod(5, Some("test-node-2"), "0", "0");
    no_requests.spec.as_mut().unwrap().containers = vec![container(None)];

    let pods = vec![
        pod(1, Some("test-node-1"), "1500m", "2560Mi"),
        pod(2, Some("test-node-1"), "500m", "512Mi"),
        pod(3, Some("test-node-2"), "2", "4Gi"),
        pod(4, None, "1", "1Gi"), // pending, reserves nothing
        no_requests,
    ];

    let slots = optimiser::compute_existing_slots(&nodes, &pods, &node_to_md(&[(1, 1), (2, 2), (3, 1)]));

    assert_eq!(slots.len(), 2, "{:?}", slots);
    let first = &slots["test-instance-1"];
    assert_eq!(first.len(), 1, "test-node-3 is not Ready: {:?}", first);
    assert_eq!(first[0].node_name, "test-node-1");
    assert_eq!((first[0].cpu, first[0].memory), (2.0, 5.0), "4 - 1.5 - 0.5 cores, 8 - 2.5 - 0.5 GiB");
    let second = &slots["test-instance-2"];
    assert_eq!((second[0].cpu, second[0].memory), (14.0, 28.0));
}

#[test]
fn existing_slots_reserve_nominated_capacity_once() {
    let nodes = list(&[node(1, &ms_name(1), "8", "32Gi", true)]);

    let nominated_to = |node: &str| PodStatus { nominated_node_name: Some(node.to_string()), ..Default::default() };
    let pods = vec![
        // Pending but nominated: its requests are held on test-node-1
        with_status(pod(1, None, "2", "8Gi"), nominated_to("test-node-1")),
        // Bound with a leftover nomination: counted once
        with_status(pod(2, Some("test-node-1"), "1", "2Gi"), nominated_to("test-node-1")),
        // Nominated somewhere else entirely
        with_status(pod(3, None, "4", "4Gi"), nominated_to("test-node-9")),
    ];

    let slots = optimiser::compute_existing_slots(&nodes, &pods, &node_to_md(&[(1, 1)]));

    let slot = &slots["test-instance-1"][0];
    assert_eq!((slot.cpu, slot.memory), (5.0, 22.0), "8 - 2 - 1 cores, 32 - 8 - 2 GiB");
}

#[test]
fn existing_slots_free_failed_and_terminating_pods() {
    let nodes = list(&[node(1, &ms_name(1), "4", "16Gi", true)]);

    let phase = |p: &str| PodStatus { phase: Some(p.to_string()), ..Default::default() };
    let mut terminating = pod(2, Some("test-node-1"), "1", "2Gi");
    terminating.metadata.deletion_timestamp = Some(Time(Timestamp::now()));

    let pods = vec![
        with_status(pod(1, Some("test-node-1"), "2", "4Gi"), phase("Failed")),
        terminating,
        with_status(pod(3, Some("test-node-1"), "250m", "1Gi"), phase("Running")),
    ];

    let slots = optimiser::compute_existing_slots(&nodes, &pods, &node_to_md(&[(1, 1)]));

    let slot = &slots["test-instance-1"][0];
    assert_eq!((slot.cpu, slot.memory), (3.75, 15.0), "only the Running pod holds capacity");
}

// ---------------------------------------------------------------------------
// plan_scale_up
// ---------------------------------------------------------------------------

#[tokio::test]
async fn plan_covers_existing_room_and_one_new_large_node() {
    // test-node-1 and test-node-2 each have room for one small pending pod.
    // test-pod-4 needs 8 cores and 32Gi, which only test-instance-2 offers.
    let store = Arc::new(Store {
        pods: vec![
            Arc::new(pod(1, Some("test-node-1"), "3", "12Gi")),
            Arc::new(pod(2, Some("test-node-2"), "1500m", "3Gi")),
            Arc::new(pod(3, None, "500m", "1Gi")),
            Arc::new(pod(4, None, "8", "32Gi")),
            Arc::new(pod(5, None, "1", "2Gi")),
        ],
        nodes: vec![
            node(1, &ms_name(1), "4", "16Gi", true),
            node(2, &ms_name(3), "2", "4Gi", true),
        ],
        mds: vec![
            instance(1, "4", "16Gi", 1, 4),
            instance(2, "16", "64Gi", 0, 2),
            instance(3, "2", "4Gi", 1, 6),
        ],
        mss: vec![machine_set(1), machine_set(3)],
    });

    let result = plan(&store, false).await;

    assert_eq!(targets(&result), BTreeMap::from([("test-instance-2", 1)]));
    assert!(result.unplaced_pods.is_empty(), "{:?}", result.unplaced_pods.iter().map(|p| &p.pod_name).collect::<Vec<_>>());
    assert_eq!(result.pod_placements.len(), 3, "every pending pod has a placement");
    let big = result.pod_placements.iter().find(|p| p.pod_name == "test-pod-4").unwrap();
    assert_eq!(big.md, "test-instance-2");
    assert!(big.node.is_none() && big.new_node_slot.is_some(), "test-pod-4 goes on a new node: {:?}", big);
}

#[tokio::test]
async fn plan_uses_the_existing_node_that_has_room() {
    // test-node-1 is nearly full, test-node-2 has 6 cores and 28Gi free.
    let store = Arc::new(Store {
        pods: vec![
            Arc::new(pod(1, Some("test-node-1"), "7", "30Gi")),
            Arc::new(pod(2, Some("test-node-2"), "2", "4Gi")),
            Arc::new(pod(3, None, "4", "16Gi")),
        ],
        nodes: vec![
            node(1, &ms_name(1), "8", "32Gi", true),
            node(2, &ms_name(1), "8", "32Gi", true),
        ],
        mds: vec![instance(1, "8", "32Gi", 2, 5)],
        mss: vec![machine_set(1)],
    });

    let result = plan(&store, false).await;

    assert!(result.target_replicas.is_empty(), "no new node needed: {:?}", result.target_replicas);
    assert_eq!(result.existing_node_placements.len(), 1);
    assert_eq!(result.existing_node_placements[0].pod_name, "test-pod-3");
    assert_eq!(result.existing_node_placements[0].node_name, "test-node-2");
}

#[tokio::test]
async fn plan_maps_results_to_mds_by_name_not_index() {
    // test-instance-2 has no capacity annotations, so parse_mds drops it and
    // the solver sees [test-instance-1, test-instance-3]. The pod only fits
    // test-instance-3, solver index 1, which is test-instance-2 by kube index.
    let mut unannotated = instance(2, "8", "32Gi", 0, 3);
    unannotated.metadata.annotations = None;

    let store = Arc::new(Store {
        pods: vec![Arc::new(pod(1, None, "6", "20Gi"))],
        nodes: vec![],
        mds: vec![instance(1, "2", "4Gi", 0, 3), unannotated, instance(3, "8", "32Gi", 0, 3)],
        mss: vec![],
    });

    let result = plan(&store, false).await;

    assert_eq!(targets(&result), BTreeMap::from([("test-instance-3", 1)]));
    assert!(result.existing_node_placements.is_empty());
}

#[tokio::test]
async fn plan_waits_while_a_ready_machine_has_no_node() {
    // CAPA reports both machines ready, but only test-node-1 has joined.
    // Planning now would count the missing node's capacity twice.
    let store = Arc::new(Store {
        pods: vec![Arc::new(pod(1, None, "1", "2Gi"))],
        nodes: vec![node(1, &ms_name(1), "4", "8Gi", true)],
        mds: vec![instance(1, "4", "8Gi", 2, 6)],
        mss: vec![machine_set(1)],
    });

    let result = plan(&store, false).await;

    assert!(result.target_replicas.is_empty(), "{:?}", result.target_replicas);
    assert!(result.pod_placements.is_empty(), "the solver must not run");
}

#[tokio::test]
async fn plan_scales_for_the_pods_that_fit_and_reports_the_rest() {
    // test-pod-1 and test-pod-2 share one node. test-pod-3 asks for more
    // cores than any test-instance-1 node has.
    let store = Arc::new(Store {
        pods: vec![
            Arc::new(pod(1, None, "1", "2Gi")),
            Arc::new(pod(2, None, "1", "2Gi")),
            Arc::new(pod(3, None, "6", "2Gi")),
        ],
        nodes: vec![],
        mds: vec![instance(1, "4", "8Gi", 0, 2)],
        mss: vec![],
    });

    let result = plan(&store, false).await;

    assert_eq!(targets(&result), BTreeMap::from([("test-instance-1", 1)]));
    let unplaced: Vec<&str> = result.unplaced_pods.iter().map(|p| p.pod_name.as_str()).collect();
    assert_eq!(unplaced, vec!["test-pod-3"]);
}

#[tokio::test]
async fn force_replan_orders_on_top_of_committed_machines() {
    // Three machines committed, one node joined and full. A normal pass
    // waits. A forced pass plans the two pending pods (one a node) on top of
    // the 3 committed machines, with headroom counted from spec: 3 + 2.
    let store = Arc::new(Store {
        pods: vec![
            Arc::new(pod(1, Some("test-node-1"), "4", "8Gi")),
            Arc::new(pod(2, None, "3", "6Gi")),
            Arc::new(pod(3, None, "3", "6Gi")),
        ],
        nodes: vec![node(1, &ms_name(1), "4", "8Gi", true)],
        mds: vec![instance(1, "4", "8Gi", 3, 6)],
        mss: vec![machine_set(1)],
    });

    let waiting = plan(&store, false).await;
    assert!(waiting.target_replicas.is_empty(), "{:?}", waiting.target_replicas);

    let forced = plan(&store, true).await;
    let target = &forced.target_replicas["test-instance-1"];
    assert_eq!((target.original, target.target), (3, 5), "{:?}", forced.target_replicas);
}
