use core::num;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Debug;
use std::sync::Arc;
use anyhow::bail;
use k8s_openapi::api::autoscaling::v1::CrossVersionObjectReference;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::{Api, Resource, ResourceExt};
use kube::api::ListParams;
use kube::runtime::reflector::Lookup;
use optimiser::AffinityGroup;
use optimiser::{
    total_flat_slots, flat_slot_index,
    Domains,
    Enforcement,
    ExistingSlot,
    MachineDeployment as SolverMachineDeployment,
    OptimiserConfig, 
    Pod as SolverPod, 
    TopologyGroup,
    SolverResult,
    solver::optimise_placement
};
use tracing::{debug, info, instrument, warn};
use crate::api::autoscale_types::{PlannedPlacement, ScalingTarget};
use crate::machine_set::MachineSet;
use crate::api::Autoscale;
use crate::{
    Context, 
    Error, 
    ReconcileContext, 
    Result, 
    get_autoscale, 
    get_machine_sets, 
    get_remote_nodes, 
    is_node_ready, 
    is_unschedulable, 
    scale_md
};
use crate::machine_deployment::MachineDeployment;
use kube_core::{ObjectList, Selector, SelectorExt};
use k8s_openapi::api::apps::v1::DaemonSet;
use k8s_openapi::api::core::v1::{Affinity, Node, NodeAffinity, NodeSelectorRequirement, Pod, PodAffinityTerm, TopologySpreadConstraint, WeightedPodAffinityTerm};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube_quantity::ParsedQuantity;

pub const HOSTNAME_TOP_KEY: &str = "kubernetes.io/hostname";

#[derive(Debug)]
struct SolverInput {
    mds: Vec<SolverMachineDeployment>,
    pods: Vec<SolverPod>,
    plugin_scores: Vec<f64>,
    allowed_matrix: Vec<Vec<Vec<bool>>>,
    config: OptimiserConfig,
}

struct MachineDeploymentDisplay(pub SolverMachineDeployment);

impl std::fmt::Display for MachineDeploymentDisplay {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MachineDeployment {{ name: {}, cpu: {}, memory: {}, max_scale_out: {}, existing_slots: {} }}",
            self.0.name, self.0.cpu, self.0.memory, self.0.max_scale_out, self.0.existing_slots.len()
        )
    }
}

struct MachineDeploymentResources {
    cpu: f64,
    memory: f64,
}

/// New-node allocatable = advertised - kernel reserve - 100Mi eviction; CPU is
/// full nominal. The kernel reserve is a fixed chunk plus a linear share of RAM,
/// so a flat ratio over-reserves on large nodes. Fitted to the bench AMI, refit per AMI.
const MEMORY_OVERHEAD_FIXED_GIB: f64 = 0.15;
const MEMORY_OVERHEAD_RATE: f64 = 0.0274;
const MEMORY_EVICTION_THRESHOLD_GIB: f64 = 0.1;

fn get_md_resources(md: &MachineDeployment) -> Option<MachineDeploymentResources> {
    let cpu_annotation = "capacity.cluster-autoscaler.kubernetes.io/cpu";
    let mem_annotation = "capacity.cluster-autoscaler.kubernetes.io/memory";

    let mut cpu = 0.0;
    let mut memory = 0.0;

    if let Some(annotations) = md.metadata.annotations.as_ref() {
        if let Some(cpu_str) = annotations.get(cpu_annotation) {
            cpu = quantity_to_cores(&Quantity(cpu_str.to_string())).unwrap_or(0.0);
        }

        if let Some(mem_str) = annotations.get(mem_annotation) {
            let advertised = quantity_to_gib(&Quantity(mem_str.to_string())).unwrap_or(0.0);
            memory = (advertised
                - MEMORY_OVERHEAD_FIXED_GIB
                - advertised * MEMORY_OVERHEAD_RATE
                - MEMORY_EVICTION_THRESHOLD_GIB)
                .max(0.0);
        }
    };

    if cpu > 0.0 && memory > 0.0 {
        Some(MachineDeploymentResources {
            cpu: cpu,
            memory: memory,
        })
    } else {
        None
    }
}

/// Real per-node price from the `wikoion.dev/cost-per-hour` annotation. When
/// present, the solver minimises actual $/h instead of resource-weighted waste.
fn get_md_cost_per_hour(md: &MachineDeployment) -> Option<f64> {
    let cost = md.metadata.annotations.as_ref()?
        .get("wikoion.dev/cost-per-hour")?
        .parse::<f64>();
    match cost {
        Ok(v) if v > 0.0 => Some(v),
        Ok(_) => None,
        Err(e) => {
            warn!("Failed to parse cost-per-hour annotation for MD {:?}: {}", md.metadata.name, e);
            None
        }
    }
}

fn get_max_md_replicas(md: &MachineDeployment) -> Option<usize> {
    let max_replicas_annotation = "cluster.x-k8s.io/cluster-api-autoscaler-node-group-max-size";
        
    let annotations = md.metadata.annotations.as_ref()?;
    let max_str = annotations.get(max_replicas_annotation)?;

    match max_str.parse::<usize>() {
        Ok(v) if v > 0 => Some(v),
        Ok(_) => {
            warn!("max-size annotation is 0 for MD {:?}, skipping", md.metadata.name);
            None
        }
        Err(e) => {
            warn!("Failed to parse max-size annotation '{}' for MD {:?}: {}",
                max_str, md.metadata.name, e);
            None
        }
    }
}

// Converts kube MDs to solver MDs. max_scale_out becomes remaining headroom
// (max_replicas minus ready or committed nodes), not the raw annotation value
fn parse_mds(mds: &[MachineDeployment], existing_slots: &HashMap<String, Vec<ExistingSlot>>, force_replan: bool) -> (Vec<SolverMachineDeployment>, Vec<i32>) {
    let mut solver_mds: Vec<SolverMachineDeployment> = Vec::new();
    let mut current_replicas: Vec<i32> = Vec::new();

    for md in mds.iter() {
        let replicas = md.status.as_ref()
            .and_then(|s| s.ready_replicas)
            .unwrap_or(0);
        // Under force_replan headroom counts committed machines (spec) rather
        // than ready ones, so we don't double-order capacity a stuck machine
        // may still deliver.
        let headroom_base = if force_replan {
            md.spec.replicas.unwrap_or(0).max(replicas)
        } else {
            replicas
        };
        let mut max_scale_out: usize = 0;
        if let Some(max_replicas) = get_max_md_replicas(md) {
            max_scale_out = max_replicas.saturating_sub(headroom_base as usize);
        }

        if let Some(resources) = get_md_resources(md) {
            if let Some(name) = md.name() {
                let solver_md = SolverMachineDeployment {
                    name: name.to_string(),
                    cpu: resources.cpu,
                    memory: resources.memory,
                    max_scale_out: max_scale_out as i32,
                    existing_slots: existing_slots.get(name.as_ref()).cloned().unwrap_or_default(),
                    cost_per_node: get_md_cost_per_hour(md),
                };

                solver_mds.push(solver_md);
                current_replicas.push(replicas);
            } else {
                warn!("Failed to parse md name, field was None");
                debug!("Failed md: {:?}", md);
            }
        } else {
            warn!("Failed to parse md resources, field was None");
            debug!("Failed md resources: {:?}", md);
        }
    }

    (solver_mds, current_replicas)
}

pub fn build_node_to_md_map(machine_sets: &ObjectList<MachineSet>, nodes: &ObjectList<Node>) -> HashMap<String, String> {
    let owner_annotation = "cluster.x-k8s.io/owner-name";

    let ms_to_md_map: HashMap<String, String> = machine_sets.items.iter()
        .filter_map(|ms| {
            let ms_name = ms.metadata.name.as_deref()?;
            let md_name = ms.metadata.owner_references.as_ref()?
                .iter()
                .find(|r| r.kind == "MachineDeployment")
                .map(|r| r.name.clone())?;
            Some((ms_name.to_string(), md_name))
        }).collect();

    let node_to_md_map: HashMap<String, String> = nodes.items.iter()
        .filter_map(|n| {
            let ms_name = n.metadata.annotations.as_ref()?.get(owner_annotation)?;
            let md_name = ms_to_md_map.get(ms_name)?.clone();
            let node_name = n.metadata.name.clone()?;

            Some((node_name, md_name))
        }).collect();

    node_to_md_map
}

const BYTES_PER_GIB: f64 = 1024.0 * 1024.0 * 1024.0;

/// CPU quantity in cores ("500m" -> 0.5, "4" -> 4.0).
pub(crate) fn quantity_to_cores(q: &Quantity) -> Option<f64> {
    ParsedQuantity::try_from(q).ok().and_then(|p| p.to_bytes_f64())
}

/// Memory quantity in GiB ("4Gi" -> 4.0), the solver's memory unit.
pub(crate) fn quantity_to_gib(q: &Quantity) -> Option<f64> {
    ParsedQuantity::try_from(q)
        .ok()
        .and_then(|p| p.to_bytes_f64())
        .map(|bytes| bytes / BYTES_PER_GIB)
}

/// Sum of container resource requests for a pod, as (cores, GiB).
pub(crate) fn pod_requests(pod: &Pod) -> (f64, f64) {
    let mut cpu = 0.0;
    let mut memory = 0.0;

    if let Some(spec) = &pod.spec {
        for c in &spec.containers {
            if let Some(requests) = c.resources.as_ref().and_then(|r| r.requests.as_ref()) {
                if let Some(c) = requests.get("cpu").and_then(quantity_to_cores) {
                    cpu += c;
                }
                if let Some(m) = requests.get("memory").and_then(quantity_to_gib) {
                    memory += m;
                }
            }
        }
    }

    (cpu, memory)
}

/// The node a pod's requests are reserved against: its bound node, else its
/// nominated node. Succeeded/Failed and terminating pods reserve nothing.
pub(crate) fn pod_reserved_node(pod: &Pod) -> Option<&str> {
    if pod.metadata.deletion_timestamp.is_some() {
        return None;
    }
    if let Some(phase) = pod.status.as_ref().and_then(|s| s.phase.as_deref()) {
        if phase == "Succeeded" || phase == "Failed" {
            return None;
        }
    }
    pod.spec.as_ref().and_then(|s| s.node_name.as_deref())
        .or_else(|| pod.status.as_ref().and_then(|s| s.nominated_node_name.as_deref()))
}

pub fn count_ready_nodes_per_md(
    nodes: &ObjectList<Node>,
    node_to_md_map: &HashMap<String, String>,
) -> HashMap<String, i32> {
    let mut counts: HashMap<String, i32> = HashMap::new();
    for node in &nodes.items {
        if !is_node_ready(node) {
            continue;
        }
        let Some(node_name) = node.metadata.name.as_deref() else { continue };
        if let Some(md_name) = node_to_md_map.get(node_name) {
            *counts.entry(md_name.clone()).or_insert(0) += 1;
        }
    }
    counts
}

/// Upper bound on existing slots passed to the solver per MD, to keep the
/// CP-SAT model (pods x slots boolean vars) bounded on large clusters.
const MAX_EXISTING_SLOTS_PER_MD: usize = 32;

/// Free capacity of each Ready node, grouped by owning MD. Free capacity is
/// node allocatable minus the summed requests of all pods bound to the node
/// (DaemonSet pods included), so no additional overhead subtraction applies.
pub fn compute_existing_slots(
    nodes: &ObjectList<Node>,
    all_pods: &[Pod],
    node_to_md_map: &HashMap<String, String>,
) -> HashMap<String, Vec<ExistingSlot>> {
    let mut used_per_node: HashMap<&str, (f64, f64)> = HashMap::new();
    for pod in all_pods {
        if let Some(node_name) = pod_reserved_node(pod) {
            let (cpu, memory) = pod_requests(pod);
            let used = used_per_node.entry(node_name).or_insert((0.0, 0.0));
            used.0 += cpu;
            used.1 += memory;
        }
    }

    let mut slots: HashMap<String, Vec<ExistingSlot>> = HashMap::new();
    for node in &nodes.items {
        if !is_node_ready(node) {
            continue;
        }
        let Some(node_name) = node.metadata.name.as_deref() else { continue };
        let Some(md_name) = node_to_md_map.get(node_name) else { continue };
        let Some(allocatable) = node.status.as_ref().and_then(|s| s.allocatable.as_ref()) else { continue };

        let alloc_cpu = allocatable.get("cpu").and_then(quantity_to_cores).unwrap_or(0.0);
        let alloc_memory = allocatable.get("memory").and_then(quantity_to_gib).unwrap_or(0.0);
        if alloc_cpu <= 0.0 || alloc_memory <= 0.0 {
            warn!("Node {} has no parseable allocatable cpu/memory, skipping as existing capacity", node_name);
            continue;
        }

        let (used_cpu, used_memory) = used_per_node.get(node_name).copied().unwrap_or((0.0, 0.0));
        slots.entry(md_name.clone()).or_default().push(ExistingSlot {
            node_name: node_name.to_string(),
            cpu: (alloc_cpu - used_cpu).max(0.0),
            memory: (alloc_memory - used_memory).max(0.0),
        });
    }

    for (md_name, md_slots) in slots.iter_mut() {
        if md_slots.len() > MAX_EXISTING_SLOTS_PER_MD {
            md_slots.sort_by(|a, b| (b.cpu + b.memory)
                .partial_cmp(&(a.cpu + a.memory))
                .unwrap_or(std::cmp::Ordering::Equal));
            warn!(
                "MD {} has {} existing nodes; passing only the {} with most free capacity to the solver",
                md_name, md_slots.len(), MAX_EXISTING_SLOTS_PER_MD
            );
            md_slots.truncate(MAX_EXISTING_SLOTS_PER_MD);
        }
    }

    slots
}

async fn parse_pods(pods: &[Arc<Pod>], node_to_md_map: &HashMap<String, String>) -> Result<Vec<SolverPod>> {
    let mut solver_pods: Vec<SolverPod> = Vec::new();
    for (i, pod) in pods.iter().enumerate() {
        let mut labels: HashMap<String, String> = HashMap::new();
        let mut md_name = None;

        if let Some(spec) = &pod.spec {
            if !is_unschedulable(pod) {
                if let Some(node_name) = &spec.node_name {
                    debug!("Matching node to md");
                    md_name = node_to_md_map.get(node_name).cloned();
                }
            }
        }
        let (cpu, memory) = pod_requests(pod);

        if let Some(pod_labels) = &pod.metadata.labels {
            labels = pod_labels.clone().into_iter().collect();
        }

        let affinity = parse_affinities(i, pods).ok_or(Error::ParsePodAffinity)?;

        let solver_pod = SolverPod {
            hard_peers: affinity.hard_peers,
            hard_rules: affinity.hard_rules,
            soft_peers: affinity.soft_peers,
            soft_weights: affinity.soft_weights,
            current_md: md_name,
            cpu: cpu,
            memory: memory,
            labels: labels,
        };
        solver_pods.push(solver_pod);
    }

    debug!("Finished parsing pods");
    Ok(solver_pods)
}

fn parse_affinities(pod_idx: usize, pods: &[Arc<Pod>]) -> Option<SolverAffinity> {
    let affinity = pods.get(pod_idx)
        .and_then(|p| p.spec.as_ref())
        .and_then(|s| s.affinity.as_ref());

    let pod_aff = affinity.and_then(|a| a.pod_affinity.as_ref());
    let soft_aff = pod_aff.and_then(|a| a.preferred_during_scheduling_ignored_during_execution.as_deref()).unwrap_or(&[]);

    let pod_anti_aff = affinity.and_then(|a| a.pod_anti_affinity.as_ref());
    let hard_anti_aff = pod_anti_aff.and_then(|a| a.required_during_scheduling_ignored_during_execution.as_deref()).unwrap_or(&[]);
    let soft_anti_aff = pod_anti_aff.and_then(|a| a.preferred_during_scheduling_ignored_during_execution.as_deref()).unwrap_or(&[]);

    // Hard colocate goes through generate_affinity_groups, never peer rules:
    // peer equalities chain every match to identical assignments. Only hard anti is pairwise.
    let (hard_anti_aff_rules, hard_anti_aff_peers) = parse_hard_pod_affinities(pod_idx, hard_anti_aff, pods, -1);

    let (soft_aff_rules, soft_aff_peers) = parse_soft_pod_affinities(pod_idx, soft_aff, pods, 100);
    let (soft_anti_aff_rules, soft_anti_aff_peers) = parse_soft_pod_affinities(pod_idx, soft_anti_aff, pods, -100);

    Some(SolverAffinity {
        hard_peers: hard_anti_aff_peers,
        hard_rules: hard_anti_aff_rules,
        soft_peers: [soft_aff_peers, soft_anti_aff_peers].concat(),
        soft_weights: [soft_aff_rules, soft_anti_aff_rules].concat(),
    })
}

fn parse_soft_pod_affinities(pod_idx: usize, soft_aff: &[WeightedPodAffinityTerm], pods: &[Arc<Pod>], divisor: i32) -> (Vec<f64>, Vec<usize>) {
    let (soft_aff_rules, soft_aff_peers): (Vec<f64>, Vec<usize>) = soft_aff.iter()
                                           .filter(|aff| aff.pod_affinity_term.topology_key == HOSTNAME_TOP_KEY)
                                           .map(|aff| {
                                               let pods = calculate_pods_in_group(pods, &aff.pod_affinity_term.label_selector);
                                               let rule = aff.weight as f64 / divisor as f64;

                                               // A pod is never its own peer
                                               let indicies: Vec<(f64, usize)> = pods.iter()
                                                   .enumerate()
                                                   .filter(|(i, pod)| **pod && *i != pod_idx)
                                                   .map(|(i, _)| (rule, i) )
                                                   .collect();

                                                indicies
                                            })
                                           .flatten()
                                           .unzip();
    (soft_aff_rules, soft_aff_peers)
}

fn parse_hard_pod_affinities(pod_idx: usize, hard_aff: &[PodAffinityTerm], pods: &[Arc<Pod>], rule: i32) -> (Vec<i32>, Vec<usize>) {
    let (hard_aff_rules, hard_aff_peers): (Vec<i32>, Vec<usize>) = hard_aff.iter()
                                 .filter(|aff| aff.topology_key == HOSTNAME_TOP_KEY)
                                 .map(|aff| {
                                    let pods = calculate_pods_in_group(pods, &aff.label_selector);

                                    // A pod is never its own peer: a self-matching anti-affinity
                                    // selector would otherwise bar the pod from every slot
                                    let indicies: Vec<usize> = pods.iter()
                                        .enumerate()
                                        .filter(|(i, pod)| **pod && *i != pod_idx)
                                        .map(|(i, _)| i )
                                        .collect();

                                     indicies
                                 })
                                .flatten()
                                .map(|i| (rule, i)) 
                                .unzip();

    (hard_aff_rules, hard_aff_peers)
}

struct SolverAffinity {
    hard_peers: Vec<usize>,
    hard_rules: Vec<i32>,
    soft_peers: Vec<usize>,
    soft_weights: Vec<f64>,
}

struct DaemonsetOverhead {
    cpu: f64,
    memory: f64,
}

async fn get_daemonset_overheads(rc: &ReconcileContext<'_>) -> Result<Vec<(String, Option<BTreeMap<String, String>>, DaemonsetOverhead)>> {
    let ds_api = Api::<DaemonSet>::all(rc.ctx.remote_client.clone());
    let daemonsets = ds_api.list(&ListParams::default()).await.map_err(Error::DaemonSetError)?;

    let mut overheads = Vec::new();
    for ds in &daemonsets.items {
        let mut cpu = 0.0;
        let mut memory = 0.0;

        if let Some(spec) = &ds.spec {
            let node_selector = spec.template.spec.as_ref().and_then(|s| s.node_selector.clone());

            if let Some(pod_spec) = &spec.template.spec {
                for c in &pod_spec.containers {
                    if let Some(requests) = c.resources.as_ref().and_then(|r| r.requests.as_ref()) {
                        if let Some(c) = requests.get("cpu").and_then(quantity_to_cores) {
                            cpu += c;
                        }
                        if let Some(m) = requests.get("memory").and_then(quantity_to_gib) {
                            memory += m;
                        }
                    }
                }
            }

            let ds_name = ds.metadata.name.as_deref().unwrap_or("unknown");
            debug!("DaemonSet {} overhead: cpu={}, memory={}, node_selector={:?}", ds_name, cpu, memory, node_selector);
            overheads.push((ds_name.to_string(), node_selector, DaemonsetOverhead { cpu, memory }));
        }
    }

    Ok(overheads)
}

fn match_node_expr(
    expr: &Vec<NodeSelectorRequirement>,
    labels: Option<&BTreeMap<String,String>>,
) -> bool {
    expr.iter().all(|e| {
            match e.operator.as_str() {
               "In" => {
                   labels.is_some_and(|l| {
                       e.values.as_ref().is_some_and(|vals| {
                           vals.iter().any(|v| l.get(&e.key) == Some(v))
                       })
                   })
               },
               "NotIn" => {
                   labels.is_none_or(|l| {
                       e.values.as_ref().is_some_and(|vals| {
                           vals.iter().all(|v| l.get(&e.key) != Some(v))
                       })
                   })
               },
               "Exists" => {
                   labels.is_some_and(|l| l.contains_key(&e.key))
               },
               "DoesNotExist" => {
                   labels.is_none_or(|l| !l.contains_key(&e.key))
               },
               "Gt" => {
                   labels.is_some_and(|l| {
                       e.values.as_ref().is_some_and(|vals| {
                           if vals.len() == 1 {
                               let Some(node_val) = l.get(&e.key) else {
                                   return false;
                               };
                               let parsed_node_val: Result<i64, _> = node_val.parse();
                               let parsed_selector_val: Result<i64, _> = vals[0].parse();

                               parsed_selector_val.is_ok_and(|s| {
                                   parsed_node_val.is_ok_and(|n| s < n)
                               })
                           } else {
                               false
                           }
                       })
                   })
               },
               "Lt" => {
                   labels.is_some_and(|l| {
                       e.values.as_ref().is_some_and(|vals| {
                           if vals.len() == 1 {
                               let Some(node_val) = l.get(&e.key) else {
                                   return false;
                               };
                               let parsed_node_val: Result<i64, _> = node_val.parse();
                               let parsed_selector_val: Result<i64, _> = vals[0].parse();

                               parsed_selector_val.is_ok_and(|s| {
                                   parsed_node_val.is_ok_and(|n| s > n)
                               })
                           } else {
                               false
                           }
                       })
                   })
               },
               _ => false,
            }
    })
}

fn matches_node_affinity(
    affinity: &Option<NodeAffinity>,
    node_labels: &Option<&BTreeMap<String, String>>,
    md: &MachineDeployment,
) -> bool {
    let md_labels = get_expected_node_labels_from_md(md);
    let labels = node_labels.or(md_labels.as_ref());

    if let Some(selector) = affinity.as_ref().and_then(|o| o.required_during_scheduling_ignored_during_execution.as_ref()) {
        return selector.node_selector_terms.iter()
            .any(|t| {
                let match_expr = t.match_expressions.as_ref().is_some_and(|v| !v.is_empty());
                let match_fields = t.match_fields.as_ref().is_some_and(|v| !v.is_empty());

                (match_expr || match_fields) &&
                    t.match_expressions.as_ref().is_none_or(|v| {
                        match_node_expr(v, labels)
                    }) 
                // Nodes that don't exist can't work with In operator since they won't get a name
                // until they exist, NotIn however is implicitely always true
                && t.match_fields.as_ref().is_none_or(|v| v.iter().all(|e| e.operator == "NotIn"))
            });
    }
    true
}

fn pod_fits_on_md(pod: &Pod, cpu: f64, mem: f64) -> bool {
    let (req_cpu, req_mem) = pod_requests(pod);
    req_cpu <= cpu && req_mem <= mem
}

fn pod_allowed_on_md(
    pod: &Pod,
    node_labels: &Option<&BTreeMap<String, String>>, 
    md: &MachineDeployment,
) -> bool {
    pod.spec.as_ref().is_some_and(|p| {
        matches_node_selector(&p.node_selector, node_labels, md) &&
            p.affinity.as_ref()
            .is_none_or(|a| matches_node_affinity(&a.node_affinity, node_labels, md))
    })
}

/// Selector key the MD never declared, so the model may be wrong, not the match.
fn undeclared_selector_key(
    ds_selector: &Option<BTreeMap<String, String>>,
    md: &MachineDeployment,
) -> Option<String> {
    let declared = get_expected_node_labels_from_md(md);
    ds_selector.as_ref()?.keys()
        .find(|k| declared.as_ref().is_none_or(|l| !l.contains_key(*k)))
        .cloned()
}

fn matches_node_selector(
    ds_selector: &Option<BTreeMap<String, String>>,
    node_labels: &Option<&BTreeMap<String, String>>,
    md: &MachineDeployment,
) -> bool {
    let md_labels = get_expected_node_labels_from_md(md);
    let labels = node_labels.or(md_labels.as_ref());

    match ds_selector {
        None => true,
        Some(selector) => {
            selector.iter().all(|(k,v)| labels.is_some_and(|l| l.get(k) == Some(v)))
        }
    }
}

// Gathers the full solver input: existing-node free capacity, MD templates minus
// daemonset overhead, pod requests, topology/affinity groups and the allowed matrix
async fn construct_scale_up_solver_input(
    mds: &[MachineDeployment],
    pods: &[Arc<Pod>],
    autoscale: &Autoscale,
    rc: &ReconcileContext<'_>,
    nodes: &ObjectList<Node>,
    node_to_md_map: &HashMap<String, String>,
    force_replan: bool,
) -> Result<SolverInput> {
    info!("Computing free capacity on existing nodes");
    let pod_api = Api::<Pod>::all(rc.ctx.remote_client.clone());
    let all_pods = pod_api.list(&ListParams::default()).await.map_err(Error::PodError)?;
    let existing_slots = compute_existing_slots(nodes, &all_pods.items, node_to_md_map);
    debug!("Computed existing slots: {:?}", existing_slots);

    info!("Parsing MDs");
    let (mut solver_mds, current_replicas) = parse_mds(mds, &existing_slots, force_replan);
    debug!("Parsed MDs: {:?}, current_replicas: {:?}", solver_mds, current_replicas);

    info!("Querying DaemonSet overheads");
    let ds_overheads = get_daemonset_overheads(rc).await?;

    // Subtract daemonset overhead from each MD's capacity.
    // parse_mds() produces solver MDs in the same order as the input mds slice,
    // but skips MDs that lack resources or a name. We need to correlate by name.
    for solver_md in solver_mds.iter_mut() {
        // Find the original kube MD by name
        if let Some(kube_md) = mds.iter().find(|md| md.metadata.name.as_deref() == Some(&solver_md.name)) {
            for (ds_name, ds_selector, overhead) in &ds_overheads {
                if matches_node_selector(ds_selector, &None, kube_md) {
                    solver_md.cpu = (solver_md.cpu - overhead.cpu).max(0.0);
                    solver_md.memory = (solver_md.memory - overhead.memory).max(0.0);
                } else if let Some(missing) = undeclared_selector_key(ds_selector, kube_md) {
                    warn!(
                        "DaemonSet {} selects label {} which MD {} does not declare in its capacity labels annotation; \
                         its overhead is not subtracted from new-node capacity; declare the label if nodes will carry it",
                        ds_name, missing, solver_md.name
                    );
                }
            }
            debug!("MD {} capacity after daemonset overhead: cpu={}, memory={}", solver_md.name, solver_md.cpu, solver_md.memory);
        }
    }

    let num_mds = solver_mds.len();

    let plugin_scores = vec![0.0; num_mds];

    info!("Parsing pods");
    let solver_pods = parse_pods(pods, node_to_md_map).await?;
    debug!("Parsed pods: {:?}", solver_pods);

    let timeout: std::time::Duration = autoscale.spec.solver_timeout.into();
    let improvement_threshold = autoscale.spec.min_improvement_percent.unwrap_or(10) as f64;
    let solver_md_name_to_md_map = generate_solver_md_name_to_md_map(&solver_mds, mds);
    let topology_groups = generate_topology_groups(&solver_mds, pods, &all_pods, nodes, &solver_md_name_to_md_map);
    let affinity_groups = generate_affinity_groups(&solver_mds, pods, &all_pods, nodes, &solver_md_name_to_md_map);

    let config = OptimiserConfig {
        max_runtime_secs: timeout.as_secs() as i32,
        improvement_threshold,
        score_only: false,
        calculate_current_state: false,
        max_attempts: 5,
        seed_cpsat_with_beam: true,
        seed_attempt: 3,
        beam_fallback: true,
        beam_only: false,
        current_replicas: Some(current_replicas),
        // get_md_resources already models the kernel reserve; a fraction here would derate twice
        allocatable_fraction: 1.0,
        // available_parallelism is cgroup-aware, CP-SAT's own detection is not
        num_workers: std::thread::available_parallelism().map_or(0, |n| n.get() as i32),
        // Best-effort: an unplaceable pod must not block scaling for the
        // placeable ones (see report_unplaced_pods)
        allow_partial: true,
	topology_groups,
	affinity_groups,
        ..Default::default()
    };


    // allowed_matrix[pod_idx][md_idx][slot_idx] = can this pod be placed on this slot?
    let allowed_matrix = generate_allowed_matrix(pods, &all_pods, &solver_mds, nodes, &config, &solver_md_name_to_md_map);

    Ok(SolverInput {
        mds: solver_mds,
        pods: solver_pods,
        plugin_scores,
        allowed_matrix,
        config,
    })
}

fn generate_topology_groups<'a>(
    mds: &'a [SolverMachineDeployment],
    pods: &'a [Arc<Pod>],
    all_pods: &ObjectList<Pod>,
    nodes: &'a ObjectList<Node>,
    solver_md_name_to_md_map: &BTreeMap<&str, &MachineDeployment>,
) -> Vec<TopologyGroup> {
    let topology_groups: Vec<TopologyGroup> = pods.iter()
        .filter_map(|pod| pod.as_ref().spec.as_ref()?.topology_spread_constraints.as_ref())
	.flatten()
        .map(|topology| (dedup_key(topology), topology))
        .collect::<BTreeMap<_, _>>()
        .into_values()
	.map(|topology| generate_topology_group(mds, nodes, pods, all_pods, &topology, solver_md_name_to_md_map))
	.collect();

    topology_groups
}

fn dedup_key(t: &TopologySpreadConstraint) -> (String, i32, String, i32, String) {
    let selector_json = t.label_selector.as_ref()
        .and_then(|s| serde_json::to_string(&normalise_selector(s)).ok())
        .unwrap_or_default();

    (
        t.topology_key.clone(),
        t.max_skew,
        t.when_unsatisfiable.clone(),
        t.min_domains.unwrap_or(0),
        selector_json,
    )
}

fn normalise_selector(s: &LabelSelector) -> LabelSelector {
    let mut s = s.clone();
    if let Some(exprs) = s.match_expressions.as_mut() {
        for e in exprs.iter_mut() {
            if let Some(v) = e.values.as_mut() {
                v.sort();
            }
        }
        exprs.sort_by(|a, b| (&a.key, &a.operator).cmp(&(&b.key, &b.operator)));
    }
    s
}

fn generate_topology_group(
    mds: &[SolverMachineDeployment],
    nodes: &ObjectList<Node>,
    pods: &[Arc<Pod>],
    all_pods: &ObjectList<Pod>,
    topology: &TopologySpreadConstraint,
    solver_md_name_to_md_map: &BTreeMap<&str, &MachineDeployment>,
) -> TopologyGroup {
    let domain_indicies = calculate_domain_indicies(mds, nodes, topology.topology_key.as_ref(), solver_md_name_to_md_map);
    let enforcement = if topology.when_unsatisfiable == "DoNotSchedule" { Enforcement::Hard } else { Enforcement::Soft };
    let num_domains = domain_indicies.by_label_val.len();
    let pods_in_group = calculate_pods_in_group(pods, &topology.label_selector);
    let baseline_counts = calculate_baseline_counts(all_pods, &topology.label_selector, &domain_indicies, num_domains);
    let group = TopologyGroup{
        max_skew: topology.max_skew,
        min_domains: topology.min_domains.unwrap_or(0),
        domains: Domains {
            num_domains: num_domains as i32,
            slot_domains: domain_indicies.md_domains,
            baseline_counts,
        },
        enforcement,
        pods_in_group,
    };
    group
}

fn generate_affinity_groups<'a>(
    mds: &'a [SolverMachineDeployment],
    pods: &'a [Arc<Pod>],
    all_pods: &ObjectList<Pod>,
    nodes: &'a ObjectList<Node>,
    solver_md_name_to_md_map: &BTreeMap<&str, &MachineDeployment>,
) -> Vec<AffinityGroup> {
    // Hard colocate terms of every topology key become domain groups ("at least
    // one match in my domain"); hostname anti stays on the exact peer-rule path.
    // Identical terms merge into one group, pods_carrying_term marks the declarers.
    let mut groups: BTreeMap<(String, bool, String), (&PodAffinityTerm, bool, Vec<bool>)> = BTreeMap::new();

    for (pod_idx, pod) in pods.iter().enumerate() {
        let affinity = pod.spec.as_ref().and_then(|s| s.affinity.as_ref());

        let pod_aff = affinity.and_then(|a| a.pod_affinity.as_ref());
        let hard_aff = pod_aff.and_then(|a| a.required_during_scheduling_ignored_during_execution.as_deref()).unwrap_or(&[]);
        let _soft_aff = pod_aff.and_then(|a| a.preferred_during_scheduling_ignored_during_execution.as_deref()).unwrap_or(&[]);

        let pod_anti_aff = affinity.and_then(|a| a.pod_anti_affinity.as_ref());
        let hard_anti_aff = pod_anti_aff.and_then(|a| a.required_during_scheduling_ignored_during_execution.as_deref()).unwrap_or(&[]);
        let _soft_anti_aff = pod_anti_aff.and_then(|a| a.preferred_during_scheduling_ignored_during_execution.as_deref()).unwrap_or(&[]);

        let terms = hard_aff.iter().map(|aff| (aff, true))
            .chain(hard_anti_aff.iter().map(|aff| (aff, false)));

        for (aff, colocate) in terms {
            if aff.topology_key == HOSTNAME_TOP_KEY && !colocate {
                continue;
            }
            let entry = groups.entry(affinity_dedup_key(aff, colocate))
                .or_insert_with(|| (aff, colocate, vec![false; pods.len()]));
            entry.2[pod_idx] = true;
        }
    }

    groups.into_values()
        .map(|(aff, colocate, pods_carrying_term)| {
            generate_hard_affinity_group(mds, nodes, pods, all_pods, aff, solver_md_name_to_md_map, colocate, pods_carrying_term)
        })
        .chain(generate_bound_anti_affinity_groups(mds, pods, all_pods, nodes, solver_md_name_to_md_map))
        .collect()
}

/// Anti terms carried by bound pods, which the scheduler keeps enforcing
/// against incoming pods. A matching pending pod goes in pods_carrying_term
/// with the carriers' domains as baseline, so the baseline pin keeps it out.
fn generate_bound_anti_affinity_groups(
    mds: &[SolverMachineDeployment],
    pods: &[Arc<Pod>],
    all_pods: &ObjectList<Pod>,
    nodes: &ObjectList<Node>,
    solver_md_name_to_md_map: &BTreeMap<&str, &MachineDeployment>,
) -> Vec<AffinityGroup> {
    let mut terms: BTreeMap<(String, bool, String), (&PodAffinityTerm, Vec<&str>)> = BTreeMap::new();

    for bound in all_pods.items.iter() {
        // Nominated pods carry their terms to the reserved node too
        let Some(node_name) = pod_reserved_node(bound) else { continue };
        let Some(spec) = bound.spec.as_ref() else { continue };
        let anti = spec.affinity.as_ref()
            .and_then(|a| a.pod_anti_affinity.as_ref())
            .and_then(|a| a.required_during_scheduling_ignored_during_execution.as_deref())
            .unwrap_or(&[]);

        // Hostname terms block via the matrix, see anti_affinity_blocked_nodes
        for term in anti.iter().filter(|t| t.topology_key != HOSTNAME_TOP_KEY) {
            let entry = terms.entry(affinity_dedup_key(term, false))
                .or_insert_with(|| (term, Vec::new()));
            entry.1.push(node_name);
        }
    }

    terms.into_values()
        .filter_map(|(term, carrier_nodes)| {
            let pods_carrying_term = calculate_pods_in_group(pods, &term.label_selector);
            if !pods_carrying_term.contains(&true) {
                return None;
            }

            let domain_indicies = calculate_domain_indicies(mds, nodes, term.topology_key.as_str(), solver_md_name_to_md_map);
            let num_domains = domain_indicies.by_label_val.len();
            let mut baseline_counts = vec![0; num_domains];
            for name in carrier_nodes {
                if let Some(domain) = domain_indicies.by_node_name.get(name) {
                    baseline_counts[*domain as usize] += 1;
                }
            }

            Some(AffinityGroup {
                domains: Domains {
                    slot_domains: domain_indicies.md_domains,
                    num_domains: num_domains as i32,
                    baseline_counts,
                },
                enforcement: Enforcement::Hard,
                colocate: false,
                // All false so the pairwise anti rule builds nothing here
                pods_matching_term: vec![false; pods.len()],
                pods_carrying_term,
            })
        })
        .collect()
}

fn affinity_dedup_key(t: &PodAffinityTerm, colocate: bool) -> (String, bool, String) {
    let selector_json = t.label_selector.as_ref()
        .and_then(|s| serde_json::to_string(&normalise_selector(s)).ok())
        .unwrap_or_default();

    (t.topology_key.clone(), colocate, selector_json)
}

fn generate_hard_affinity_group(
    mds: &[SolverMachineDeployment],
    nodes: &ObjectList<Node>,
    pods: &[Arc<Pod>],
    all_pods: &ObjectList<Pod>,
    affinity: &PodAffinityTerm,
    solver_md_name_to_md_map: &BTreeMap<&str, &MachineDeployment>,
    colocate: bool,
    pods_carrying_term: Vec<bool>,
) -> AffinityGroup {
    let domain_indicies = calculate_domain_indicies(mds, nodes, affinity.topology_key.as_str(), solver_md_name_to_md_map);
    let num_domains = domain_indicies.by_label_val.len();
    let baseline_counts = calculate_baseline_counts(all_pods, &affinity.label_selector, &domain_indicies, num_domains);
    // Selector-only matching; namespaces are ignored, same as the topology path
    let pods_matching_term = calculate_pods_in_group(pods, &affinity.label_selector);

    AffinityGroup {
        domains: Domains {
            slot_domains: domain_indicies.md_domains,
            num_domains: num_domains as i32,
            baseline_counts,
        },
        enforcement: Enforcement::Hard,
        colocate,
        pods_matching_term,
        pods_carrying_term,
    }
}



fn calculate_baseline_counts(
    pods: &ObjectList<Pod>,
    selector: &Option<LabelSelector>,
    domain_indicies: &DomainIndex,
    num_domains: usize,
) -> Vec<i32> {
    selector.clone()
	.and_then(|s| Selector::try_from(s).ok())
	.map(|sel| {
	    pods.items.iter()
		.filter(|pod| sel.matches(pod.labels()))
		.filter_map(|p| pod_reserved_node(p))
		.filter_map(|name| domain_indicies.by_node_name.get(name))
		.fold(vec![0; num_domains], |mut counts, domain| {
		    counts[*domain as usize] += 1;
		    counts
		})
	}).unwrap_or_else(|| vec![0; num_domains])
}

fn calculate_pods_in_group(
    pods: &[Arc<Pod>],
    selector: &Option<LabelSelector>,
) -> Vec<bool> {
    // Kube semantics: a nil labelSelector matches no pods, only an empty
    // non-nil selector matches all
    let pods_in_group: Vec<bool> = pods.iter().map(|pod| {
	selector.as_ref().is_some_and(|s| {
	    let ls: Option<Selector> = s.clone().try_into().ok();
	    ls.is_some_and(|sel| sel.matches(pod.labels()))
	})
    })
    .collect();

    pods_in_group
}

struct DomainIndex {
    by_label_val: BTreeMap<String, i32>,
    by_node_name: BTreeMap<String,i32>,
    md_domains: Vec<i32>,
}

fn calculate_domain_indicies(
    mds: &[SolverMachineDeployment], 
    nodes: &ObjectList<Node>,
    topology_key: &str,
    solver_md_name_to_md_map: &BTreeMap<&str, &MachineDeployment>,
) -> DomainIndex {
    let mut label_vals: BTreeMap<String, i32> = BTreeMap::new();
    let mut node_names: BTreeMap<String, i32> = BTreeMap::new();
    
    let total_slots = total_flat_slots(mds);
    let mut md_domains: Vec<i32> = vec![-1; total_slots];

    let node_name_node_map: BTreeMap<&str, &Node> = nodes.iter()
        .filter_map(|node| Some((node.metadata.name.as_deref()?, node)))
        .collect();


    mds.iter().enumerate().for_each(|(md_idx, solver_md)| {
        solver_md.existing_slots.iter()
            .enumerate()
            .for_each(|slot| {
                if let Some(node) = node_name_node_map.get(slot.1.node_name.as_str()) {
                    let labels = node.labels();

                    if let Some(label_val) = labels.get(topology_key) {
                        let next = label_vals.len() as i32;
                        let slot_idx = flat_slot_index(mds, md_idx, slot.0);

                        let idx = label_vals.entry(label_val.to_string()).or_insert(next);

                        if let Some(name) = node.name() {
                            node_names.entry(name.to_string()).or_insert(*idx);
                            md_domains[slot_idx] = *idx;
                        };
                    }
                }
            });

        if let Some(md) = solver_md_name_to_md_map.get(solver_md.name.as_str()) {
            let start = solver_md.existing_slots.len();
            let end = solver_md.total_slots();

            if topology_key == "kubernetes.io/hostname" {
                (start..end).for_each(|slot_idx| {
                    let next = label_vals.len() as i32;
                    let flat_slot_idx = flat_slot_index(mds, md_idx, slot_idx as usize);

                    let idx = label_vals.entry(flat_slot_idx.to_string()).or_insert(next);
                    md_domains[flat_slot_idx] = *idx;
                });
            } else if let Some(label_val) = md.labels().get(topology_key) {
                let next = label_vals.len() as i32;
                let idx = label_vals.entry(label_val.to_string()).or_insert(next);

                (start..end).for_each(|slot_idx| {
                    let flat_slot_idx = flat_slot_index(mds, md_idx, slot_idx as usize);

                    md_domains[flat_slot_idx] = *idx;
                });
            }
        };
    });

    // separately iterate through nodes in case any don't have an owning machine deployment
    nodes.items.iter().for_each(|node| {
       let labels = node.labels();

        if let Some(label_val) = labels.get(topology_key) {
            let next = label_vals.len() as i32;
            let idx = label_vals.entry(label_val.to_string()).or_insert(next);

            if let Some(name) = node.name() {
                node_names.entry(name.to_string()).or_insert(*idx);
            };
        }
    });

    DomainIndex {
	by_label_val: label_vals,
	by_node_name: node_names,
	md_domains,
    }
}

fn generate_solver_md_name_to_md_map<'a>(
    solver_mds: &'a Vec<SolverMachineDeployment>,
    mds: &'a [MachineDeployment],
) -> BTreeMap<&'a str, &'a MachineDeployment> {
    let solver_md_name_to_md_map: BTreeMap<&str, &MachineDeployment> = solver_mds.iter()
        .filter_map(|s_md| {
            let md = mds.iter().find(|m| m.metadata.name.as_ref().is_some_and(|n| n == s_md.name.as_str()))?;
            
            Some((s_md.name.as_str(), md))
        })
        .collect();

    solver_md_name_to_md_map
}

fn generate_allowed_matrix(
    pods: &[Arc<Pod>],
    all_pods: &ObjectList<Pod>,
    solver_mds: &Vec<SolverMachineDeployment>,
    nodes: &ObjectList<Node>,
    config: &OptimiserConfig,
    solver_md_name_to_md_map: &BTreeMap<&str, &MachineDeployment>,
) -> Vec<Vec<Vec<bool>>> {
    let name_to_node_map: BTreeMap<&str, &Node> = nodes.iter()
        .filter_map(|n| n.metadata.name.as_ref().map(|name| (name.as_str(), n)))
        .collect();

    let allowed_matrix: Vec<Vec<Vec<bool>>> = pods.iter()
        .map(|pod| {
        let blocked_nodes = anti_affinity_blocked_nodes(pod, all_pods);
        solver_mds.iter().map(|solver_md| {
            let Some(&md) = solver_md_name_to_md_map.get(solver_md.name.as_str()) else {
                warn!("No kube MachineDeployment named {} while building allowed matrix", solver_md.name);
                return vec![false; solver_md.total_slots()];
            };

            if solver_md.existing_slots.is_empty() {
                let (cpu, mem) = solver_md.slot_capacity(0, config.allocatable_fraction);
                let fits = pod_fits_on_md(pod, cpu, mem);
                let allowed = pod_allowed_on_md(&pod, &None, md);

                vec![fits && allowed; solver_md.total_slots()]
            } else {
                let allowed_existing: Vec<bool> = solver_md.existing_slots.iter()
                    .enumerate()
                    .map(|(slot_idx, slot)| {
                        let node_name: &str = slot.node_name.as_ref();
                        let node = name_to_node_map.get(node_name);
                        let node_labels = node.and_then(|n| n.metadata.labels.as_ref());

                        let (cpu, mem) = solver_md.slot_capacity(slot_idx, config.allocatable_fraction);
                        let fits = pod_fits_on_md(pod, cpu, mem);
                        let allowed = pod_allowed_on_md(&pod, &node_labels, md);

                        fits && allowed && !blocked_nodes.contains(node_name)
                    })
                    .collect();

                let (cpu, mem) = solver_md.slot_capacity(solver_md.existing_slots.len(), config.allocatable_fraction);
                let fits = pod_fits_on_md(pod, cpu, mem);
                let allowed = pod_allowed_on_md(&pod, &None, md);

                let allowed_new: Vec<bool> = (solver_md.existing_slots.len()..solver_md.total_slots())
                    .map(|_| fits && allowed)
                    .collect();

                [allowed_existing, allowed_new].concat()
            }
        }).collect()
        })
        .collect();

    allowed_matrix
}

/// Nodes blocked for this pod by required hostname anti terms, in both
/// directions: our terms against running pods, and bound pods' terms against
/// us. Peer rules only see pending pods, so running pods block via the matrix.
fn anti_affinity_blocked_nodes(pod: &Pod, all_pods: &ObjectList<Pod>) -> BTreeSet<String> {
    let terms = pod.spec.as_ref()
        .and_then(|s| s.affinity.as_ref())
        .and_then(|a| a.pod_anti_affinity.as_ref())
        .and_then(|a| a.required_during_scheduling_ignored_during_execution.as_deref())
        .unwrap_or(&[]);

    let mut blocked = BTreeSet::new();
    for term in terms.iter().filter(|t| t.topology_key == HOSTNAME_TOP_KEY) {
        let Some(sel) = term.label_selector.clone().and_then(|s| Selector::try_from(s).ok()) else { continue };
        for name in all_pods.items.iter()
            .filter(|p| sel.matches(p.labels()))
            .filter_map(|p| pod_reserved_node(p))
        {
            blocked.insert(name.to_string());
        }
    }

    // Reverse: a bound pod's own hostname anti term blocks its node for us
    for bound in all_pods.items.iter() {
        let Some(node_name) = pod_reserved_node(bound) else { continue };
        let Some(spec) = bound.spec.as_ref() else { continue };
        let bound_terms = spec.affinity.as_ref()
            .and_then(|a| a.pod_anti_affinity.as_ref())
            .and_then(|a| a.required_during_scheduling_ignored_during_execution.as_deref())
            .unwrap_or(&[]);
        for term in bound_terms.iter().filter(|t| t.topology_key == HOSTNAME_TOP_KEY) {
            let Some(sel) = term.label_selector.clone().and_then(|s| Selector::try_from(s).ok()) else { continue };
            if sel.matches(pod.labels()) {
                blocked.insert(node_name.to_string());
                break;
            }
        }
    }
    blocked
}

/// CAS scale-from-zero contract: the annotation must declare every label a
/// selector will ask about, kubelet defaults like kubernetes.io/os included.
fn get_expected_node_labels_from_md(md: &MachineDeployment) -> Option<BTreeMap<String, String>> {
    const CAS_CAPACITY_ANNOTATION: &str = "capacity.cluster-autoscaler.kubernetes.io/labels";

    md.annotations().get(CAS_CAPACITY_ANNOTATION).map(|v| v.split(",")
        .filter_map(|l| l.split_once("="))
        .map(|e| (e.0.to_string(), e.1.to_string()))
        .collect())
}

/// A pending pod the solver placed onto an existing node's free capacity
/// rather than a new node.
#[derive(Debug, Clone)]
pub struct PodNodePlacement {
    pub pod_name: String,
    pub pod_namespace: String,
    pub node_name: String,
}

/// A pending pod the solver could not place on any MachineDeployment within
/// max-size limits.
#[derive(Debug, Clone)]
pub struct UnplacedPod {
    pub pod_name: String,
    pub pod_namespace: String,
}

#[derive(Debug, Clone, Default)]
pub struct ScaleUpResult {
    pub target_replicas: HashMap<String, ScalingTarget>,
    /// Empty when the solver didn't run (plan reuse / nodes in flight).
    pub existing_node_placements: Vec<PodNodePlacement>,
    /// Full per-pod plan, for comparing against actual scheduler placement.
    pub pod_placements: Vec<PlannedPlacement>,
    /// Pods the solver could not place anywhere; scaling proceeds for the rest.
    pub unplaced_pods: Vec<UnplacedPod>,
}

/// Emit a Warning Event on the workload-cluster pod so operators can see why
/// it stays Pending. Failures are logged, never fatal.
async fn emit_unplaceable_event(pod: &Pod, client: kube::Client) {
    use k8s_openapi::api::core::v1::{Event, ObjectReference};

    let namespace = pod.metadata.namespace.clone().unwrap_or_else(|| "default".to_string());
    let event = Event {
        metadata: kube_core::ObjectMeta {
            generate_name: pod.metadata.name.clone().map(|n| format!("{n}-unplaceable-")),
            namespace: Some(namespace.clone()),
            ..Default::default()
        },
        involved_object: ObjectReference {
            kind: Some("Pod".to_string()),
            name: pod.metadata.name.clone(),
            namespace: Some(namespace.clone()),
            uid: pod.metadata.uid.clone(),
            ..Default::default()
        },
        reason: Some("UnplaceablePod".to_string()),
        message: Some(
            "capi-autoscaler: no MachineDeployment satisfies this pod's resource requests within max-size limits; pod excluded from scaling plan".to_string(),
        ),
        type_: Some("Warning".to_string()),
        ..Default::default()
    };

    let events = Api::<Event>::namespaced(client, &namespace);
    if let Err(e) = events.create(&Default::default(), &event).await {
        warn!("Failed to emit UnplaceablePod event for {:?}: {}", pod.metadata.name, e);
    }
}

fn md_ready_nodes(ready_nodes: &HashMap<String, i32>, md_name: Option<&str>) -> i32 {
    md_name.and_then(|n| ready_nodes.get(n)).copied().unwrap_or(0)
}

// If we have a scaling plan we want to return it rather than call the optimiser again
#[instrument(skip_all, fields(autoscale_name))]
pub async fn plan_scale_up(
    mds: &[MachineDeployment],
    pods: &[Arc<Pod>],
    autoscale: &Autoscale,
    rc: &ReconcileContext<'_>,
    force_replan: bool,
) -> Result<ScaleUpResult> {
    let autoscale_name = autoscale.metadata.name.as_deref()
        .ok_or(Error::MissingAutoscaleName)?;
    tracing::Span::current().record("autoscale_name", autoscale_name);

    let scaling_plan = autoscale.status.as_ref().and_then(|s| s.scaling_plan.clone());

    // Readiness is judged by Ready Kubernetes nodes, not MD status.ready_replicas.
    // CAPA marks machines ready before their nodes join the cluster, and gating
    // on that makes a plan look satisfied while every pod is still pending.
    let machine_sets = get_machine_sets(rc).await?;
    let nodes = get_remote_nodes(rc).await?;
    let node_to_md_map = build_node_to_md_map(&machine_sets, &nodes);
    let ready_nodes = count_ready_nodes_per_md(&nodes, &node_to_md_map);

    let nodes_in_flight = mds.iter().any(|md| {
        let spec = md.spec.replicas.unwrap_or(0);
        spec > md_ready_nodes(&ready_nodes, md.metadata.name.as_deref())
    });

    let plan_already_met = scaling_plan.as_ref().map_or(false, |plan| {
        plan.iter().all(|(md_name, target)| {
            let md = mds.iter().find(|md| md.metadata.name.as_deref() == Some(md_name.as_str()));
            let spec = md.and_then(|md| md.spec.replicas).unwrap_or(0);
            let ready = md_ready_nodes(&ready_nodes, Some(md_name.as_str()));
            spec >= target.target && ready >= target.target
        })
    });

    if let Some(plan) = scaling_plan.filter(|_| !plan_already_met && !force_replan) {
        return Ok(ScaleUpResult { target_replicas: plan, ..Default::default() });
    }

    if nodes_in_flight && !plan_already_met && !force_replan {
        info!("Nodes still provisioning, waiting for them to be ready before re-evaluating");
        return Ok(ScaleUpResult::default());
    }

    if force_replan {
        warn!("Pods pending past scaling_timeout; re-planning despite in-flight machines (headroom from spec)");
    }

    solve_scale_up(mds, pods, autoscale, rc, &nodes, &node_to_md_map, &ready_nodes, force_replan).await
}

// Fresh solve: build the solver input, run it, map results back to kube MDs by name
async fn solve_scale_up(
    mds: &[MachineDeployment],
    pods: &[Arc<Pod>],
    autoscale: &Autoscale,
    rc: &ReconcileContext<'_>,
    nodes: &ObjectList<Node>,
    node_to_md_map: &HashMap<String, String>,
    ready_nodes: &HashMap<String, i32>,
    force_replan: bool,
) -> Result<ScaleUpResult> {
    info!("Constructing solver input...");
    let solver_input = construct_scale_up_solver_input(mds, pods, autoscale, rc, nodes, node_to_md_map, force_replan).await?;
    debug!("Constructed solver input: {:?}", solver_input);

    // The solver's MD list can be shorter than `mds` (parse_mds skips MDs
    // without capacity annotations), so result indices refer to this list,
    // and results must be mapped back to kube MDs by name.
    let solver_md_meta: Vec<(String, Vec<String>)> = solver_input.mds.iter()
        .map(|md| (md.name.clone(), md.existing_slots.iter().map(|s| s.node_name.clone()).collect()))
        .collect();

    let result = optimise_placement(solver_input.mds, solver_input.pods, solver_input.plugin_scores, solver_input.allowed_matrix, solver_input.config)?;

    info!(
        "Solver result: status={:?} objective={} unplaced={} used_beam={} solve_time={:.2}s",
        result.status, result.cost.objective, result.placement.unplaced_pods, result.stats.beam_used, result.stats.solve_time_secs
    );

    if !result.success { return Err(Error::OptimiserFailedError(result.message)) };

    let target_replicas = build_target_replicas(mds, &solver_md_meta, &result, ready_nodes, force_replan);
    let pod_deployments = resolve_pod_deployments(pods, rc.ctx.remote_client.clone()).await?;
    let pod_placements = build_pod_placements(pods, &solver_md_meta, &result, &pod_deployments);

    let existing_node_placements: Vec<PodNodePlacement> = pod_placements.iter()
        .filter_map(|p| Some(PodNodePlacement {
            pod_name: p.pod_name.clone(),
            pod_namespace: p.pod_namespace.clone(),
            node_name: p.node.clone()?,
        }))
        .collect();

    if !existing_node_placements.is_empty() {
        info!(
            "Solver placed {} pending pod(s) onto free capacity of existing nodes: {:?}",
            existing_node_placements.len(),
            existing_node_placements.iter()
                .map(|p| format!("{}/{} -> {}", p.pod_namespace, p.pod_name, p.node_name))
                .collect::<Vec<_>>()
        );
    }

    let unplaced_pods = report_unplaced_pods(pods, &result, rc).await;

    Ok(ScaleUpResult { target_replicas, existing_node_placements, pod_placements, unplaced_pods })
}

fn build_target_replicas(
    mds: &[MachineDeployment],
    solver_md_meta: &[(String, Vec<String>)],
    result: &SolverResult,
    ready_nodes: &HashMap<String, i32>,
    force_replan: bool,
) -> HashMap<String, ScalingTarget> {
    solver_md_meta.iter()
        .enumerate()
        .filter_map(|(solver_idx, (md_name, existing_nodes))| {
            let md = mds.iter().find(|md| md.metadata.name.as_deref() == Some(md_name.as_str()))?;
            // Slots below existing_nodes.len() are already-provisioned
            // nodes; only slots above them represent new nodes.
            let new_nodes = result.placement.slots_used[solver_idx].iter()
                .skip(existing_nodes.len())
                .filter(|slot| **slot)
                .count() as i32;
            let spec_replicas = md.spec.replicas.unwrap_or(0);
            // Under force_replan headroom was computed from spec, so new
            // nodes are IN ADDITION to committed machines.
            let base = if force_replan {
                spec_replicas.max(md_ready_nodes(ready_nodes, Some(md_name.as_str())))
            } else {
                md_ready_nodes(ready_nodes, Some(md_name.as_str()))
            };
            let desired_total = base + new_nodes;
            if desired_total > spec_replicas {
                return Some((md_name.clone(), ScalingTarget {
                    original: spec_replicas,
                    current: spec_replicas,
                    target: desired_total
                }))
            }
            None
        })
        .collect()
}

/// Owner deployment per pending pod (pod -> ReplicaSet -> Deployment),
/// keyed "namespace/pod-name". Pods without a Deployment owner are absent.
/// A ReplicaSet listing error fails the plan: a silently missing deployment
/// would drop out of the gating scope and reopen double provisioning.
async fn resolve_pod_deployments(pods: &[Arc<Pod>], client: kube::Client) -> Result<HashMap<String, String>> {
    use k8s_openapi::api::apps::v1::ReplicaSet;

    let namespaces: BTreeSet<String> = pods.iter()
        .filter(|p| p.metadata.owner_references.as_ref()
            .is_some_and(|refs| refs.iter().any(|o| o.kind == "ReplicaSet")))
        .filter_map(|p| p.metadata.namespace.clone())
        .collect();

    let mut rs_owners: HashMap<(String, String), String> = HashMap::new();
    for ns in &namespaces {
        let rs_api = Api::<ReplicaSet>::namespaced(client.clone(), ns);
        let rss = rs_api.list(&ListParams::default()).await.map_err(Error::ReplicaSetError)?;
        for rs in rss.items {
            let Some(rs_name) = rs.metadata.name else { continue };
            let Some(deployment) = rs.metadata.owner_references.as_ref()
                .and_then(|refs| refs.iter().find(|o| o.kind == "Deployment"))
                .map(|o| o.name.clone()) else { continue };
            rs_owners.insert((ns.clone(), rs_name), deployment);
        }
    }

    let mut pod_deployments = HashMap::new();
    for pod in pods {
        let Some(ns) = pod.metadata.namespace.clone() else { continue };
        let Some(pod_name) = pod.metadata.name.clone() else { continue };
        let Some(rs_name) = pod.metadata.owner_references.as_ref()
            .and_then(|refs| refs.iter().find(|o| o.kind == "ReplicaSet"))
            .map(|o| o.name.clone()) else { continue };
        if let Some(deployment) = rs_owners.get(&(ns.clone(), rs_name)) {
            pod_deployments.insert(format!("{ns}/{pod_name}"), deployment.clone());
        }
    }
    Ok(pod_deployments)
}

fn build_pod_placements(
    pods: &[Arc<Pod>],
    solver_md_meta: &[(String, Vec<String>)],
    result: &SolverResult,
    pod_deployments: &HashMap<String, String>,
) -> Vec<PlannedPlacement> {
    result.placement.assignments.iter()
        .zip(pods.iter())
        .filter_map(|(assignment, pod)| {
            if assignment.md < 0 || assignment.slot < 0 { return None; }
            let (md_name, existing_nodes) = solver_md_meta.get(assignment.md as usize)?;
            let pod_name = pod.metadata.name.clone().unwrap_or_default();
            let pod_namespace = pod.metadata.namespace.clone().unwrap_or_default();
            // Only slots within the existing prefix map to concrete nodes
            let node = existing_nodes.get(assignment.slot as usize).cloned();
            Some(PlannedPlacement {
                new_node_slot: node.is_none().then_some(assignment.slot),
                deployment: pod_deployments.get(&format!("{pod_namespace}/{pod_name}")).cloned(),
                pod_name,
                pod_namespace,
                md: md_name.clone(),
                node,
                // Stamped by the controller when the wave commits
                committed_time: None,
            })
        })
        .collect()
}

// Best-effort placement: pods the solver could not place anywhere are
// reported, given a Warning event, and excluded from the plan; scaling
// proceeds for everything else.
async fn report_unplaced_pods(
    pods: &[Arc<Pod>],
    result: &SolverResult,
    rc: &ReconcileContext<'_>,
) -> Vec<UnplacedPod> {
    let mut unplaced_pods: Vec<UnplacedPod> = Vec::new();
    if result.placement.unplaced_pods == 0 {
        return unplaced_pods;
    }

    for (assignment, pod) in result.placement.assignments.iter().zip(pods.iter()) {
        if assignment.md >= 0 {
            continue;
        }
        unplaced_pods.push(UnplacedPod {
            pod_name: pod.metadata.name.clone().unwrap_or_default(),
            pod_namespace: pod.metadata.namespace.clone().unwrap_or_default(),
        });
        emit_unplaceable_event(pod, rc.ctx.remote_client.clone()).await;
    }
    warn!(
        "{} pod(s) cannot be placed on any MachineDeployment within max-size limits: {:?}",
        unplaced_pods.len(),
        unplaced_pods.iter()
            .map(|p| format!("{}/{}", p.pod_namespace, p.pod_name))
            .collect::<Vec<_>>()
    );
    unplaced_pods
}

// Commits the next batch of the plan, at most batch_size new replicas per call
pub async fn scale_up(
    target_replicas: &HashMap<String, ScalingTarget>,
    batch_size: i32,
    ctx: Arc<Context>,
) -> Result<()> {
    let mut current_batch = 0;

    for (md, replicas) in target_replicas.iter() {
        if (replicas.target - replicas.current) + current_batch >= batch_size {
            let batch_target = batch_size - current_batch;
            info!("Scaling Machine Deployment from: {} to: {}", replicas.original, replicas.current + batch_target);
            scale_md(md, replicas.current + batch_target, ctx.clone()).await?;

            // Exit early since we've reached our max batch size
            break;
        }
        current_batch += replicas.target - replicas.current;
        info!("Scaling Machine Deployment from: {} to: {}", replicas.original, replicas.target);
        scale_md(md, replicas.target, ctx.clone()).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::machine_deployment::{MachineDeploymentSelector, MachineDeploymentSpec, MachineDeploymentStatus, MachineDeploymentTemplate};
    use k8s_openapi::api::core::v1::{
        Container, NodeSelector, NodeSelectorTerm, PodAffinity, PodAntiAffinity, PodSpec, PodStatus,
        PreferredSchedulingTerm, ResourceRequirements,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelectorRequirement, ObjectMeta, Time};
    use k8s_openapi::jiff::Timestamp;
    use kube_core::{ListMeta, TypeMeta};

    const ZONE_KEY: &str = "topology.kubernetes.io/zone";
    const CAS_LABELS: &str = "capacity.cluster-autoscaler.kubernetes.io/labels";
    const CAS_CPU: &str = "capacity.cluster-autoscaler.kubernetes.io/cpu";
    const CAS_MEMORY: &str = "capacity.cluster-autoscaler.kubernetes.io/memory";
    const MAX_SIZE: &str = "cluster.x-k8s.io/cluster-api-autoscaler-node-group-max-size";
    const COST: &str = "wikoion.dev/cost-per-hour";

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn app(n: usize) -> String {
        format!("test-app-{n}")
    }

    fn zone(n: usize) -> String {
        format!("test-zone-{n}")
    }

    fn list<T: Clone>(items: Vec<T>) -> ObjectList<T> {
        ObjectList { types: TypeMeta::default(), metadata: ListMeta::default(), items }
    }

    fn arcs(pods: Vec<Pod>) -> Vec<Arc<Pod>> {
        pods.into_iter().map(Arc::new).collect()
    }

    /// Kube MD test-instance-{n} with these annotations and replica counts.
    fn instance(n: usize, annotations: &[(&str, &str)], spec_replicas: i32, ready_replicas: i32) -> MachineDeployment {
        let mut md = MachineDeployment::new(&format!("test-instance-{n}"), MachineDeploymentSpec {
            cluster_name: "test-cluster".to_string(),
            min_ready_seconds: None,
            paused: None,
            progress_deadline_seconds: None,
            replicas: Some(spec_replicas),
            revision_history_limit: None,
            selector: MachineDeploymentSelector { match_expressions: None, match_labels: None },
            strategy: None,
            template: MachineDeploymentTemplate { metadata: None, spec: None },
        });
        md.metadata.annotations = Some(labels(annotations));
        md.status = Some(MachineDeploymentStatus {
            available_replicas: None,
            conditions: None,
            observed_generation: None,
            phase: None,
            ready_replicas: Some(ready_replicas),
            replicas: None,
            selector: None,
            unavailable_replicas: None,
            updated_replicas: None,
        });
        md
    }

    /// MD test-instance-{n} that only declares the labels its new nodes get.
    fn labelled_instance(n: usize, declared: &str) -> MachineDeployment {
        instance(n, &[(CAS_LABELS, declared)], 0, 0)
    }

    /// MD test-instance-{n} whose new nodes land in test-zone-{z}.
    fn zoned_instance(n: usize, z: Option<usize>) -> MachineDeployment {
        let mut md = instance(n, &[], 0, 0);
        md.metadata.labels = z.map(|z| labels(&[(ZONE_KEY, zone(z).as_str())]));
        md
    }

    fn solver_instance(n: usize, cpu: f64, memory: f64, max_scale_out: i32, existing_slots: Vec<ExistingSlot>) -> SolverMachineDeployment {
        SolverMachineDeployment {
            name: format!("test-instance-{n}"),
            cpu,
            memory,
            max_scale_out,
            existing_slots,
            cost_per_node: None,
        }
    }

    fn slot(n: usize, cpu: f64, memory: f64) -> ExistingSlot {
        ExistingSlot { node_name: format!("test-node-{n}"), cpu, memory }
    }

    fn node(n: usize, node_labels: &[(&str, &str)]) -> Node {
        Node {
            metadata: ObjectMeta {
                name: Some(format!("test-node-{n}")),
                labels: Some(labels(node_labels)),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn pod(n: usize, cpu: &str, memory: &str) -> Pod {
        Pod {
            metadata: ObjectMeta { name: Some(format!("test-pod-{n}")), ..Default::default() },
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "test-container".to_string(),
                    resources: Some(ResourceRequirements {
                        requests: Some([
                            ("cpu".to_string(), Quantity(cpu.to_string())),
                            ("memory".to_string(), Quantity(memory.to_string())),
                        ].into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Small pod test-pod-{n} labelled app=test-app-{a}.
    fn app_pod(n: usize, a: usize) -> Pod {
        let mut pod = pod(n, "100m", "128Mi");
        pod.metadata.labels = Some(labels(&[("app", app(a).as_str())]));
        pod
    }

    fn on_node(mut pod: Pod, n: usize) -> Pod {
        pod.spec.as_mut().unwrap().node_name = Some(format!("test-node-{n}"));
        pod
    }

    fn with_selector(mut pod: Pod, disk: &str) -> Pod {
        pod.spec.as_mut().unwrap().node_selector = Some(labels(&[("test.dev/disk", disk)]));
        pod
    }

    fn with_affinity(mut pod: Pod, affinity: Affinity) -> Pod {
        pod.spec.as_mut().unwrap().affinity = Some(affinity);
        pod
    }

    fn terminating(mut pod: Pod) -> Pod {
        pod.metadata.deletion_timestamp = Some(Time(Timestamp::now()));
        pod
    }

    /// Required pod affinity and anti-affinity terms.
    fn required(colocate: Vec<PodAffinityTerm>, anti: Vec<PodAffinityTerm>) -> Affinity {
        Affinity {
            pod_affinity: Some(PodAffinity {
                required_during_scheduling_ignored_during_execution: Some(colocate),
                ..Default::default()
            }),
            pod_anti_affinity: Some(PodAntiAffinity {
                required_during_scheduling_ignored_during_execution: Some(anti),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Term on topology_key selecting app=test-app-{a}.
    fn term(topology_key: &str, a: usize) -> PodAffinityTerm {
        PodAffinityTerm {
            topology_key: topology_key.to_string(),
            label_selector: Some(LabelSelector {
                match_labels: Some(labels(&[("app", app(a).as_str())])),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Selector for app In these apps, values left in the order given.
    fn app_in(apps: &[usize]) -> LabelSelector {
        LabelSelector {
            match_expressions: Some(vec![LabelSelectorRequirement {
                key: "app".to_string(),
                operator: "In".to_string(),
                values: Some(apps.iter().map(|a| app(*a)).collect()),
            }]),
            ..Default::default()
        }
    }

    fn requirement(key: &str, operator: &str, values: &[&str]) -> NodeSelectorRequirement {
        NodeSelectorRequirement {
            key: key.to_string(),
            operator: operator.to_string(),
            values: (!values.is_empty()).then(|| values.iter().map(|v| v.to_string()).collect()),
        }
    }

    fn expr_term(exprs: Vec<NodeSelectorRequirement>) -> NodeSelectorTerm {
        NodeSelectorTerm { match_expressions: Some(exprs), match_fields: None }
    }

    fn field_term(operator: &str) -> NodeSelectorTerm {
        NodeSelectorTerm { match_expressions: None, match_fields: Some(vec![requirement("metadata.name", operator, &["test-node-9"])]) }
    }

    fn node_affinity_pod(terms: Vec<NodeSelectorTerm>) -> Pod {
        with_affinity(pod(1, "100m", "128Mi"), Affinity {
            node_affinity: Some(NodeAffinity {
                required_during_scheduling_ignored_during_execution: Some(NodeSelector { node_selector_terms: terms }),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    fn config(allocatable_fraction: f64) -> OptimiserConfig {
        OptimiserConfig { allocatable_fraction, ..Default::default() }
    }

    /// test-instance-1 in test-zone-1 owns test-node-1 plus 2 new slots,
    /// test-instance-2 in test-zone-2 has 1 new slot, test-instance-3 has no
    /// zone and 1 new slot. test-node-2 (test-zone-3) and test-node-3
    /// (test-zone-2) belong to no MD.
    fn zoned_cluster() -> (Vec<MachineDeployment>, Vec<SolverMachineDeployment>, ObjectList<Node>) {
        let kube_mds = vec![zoned_instance(1, Some(1)), zoned_instance(2, Some(2)), zoned_instance(3, None)];
        let solver_mds = vec![
            solver_instance(1, 4.0, 16.0, 2, vec![slot(1, 4.0, 16.0)]),
            solver_instance(2, 4.0, 16.0, 1, vec![]),
            solver_instance(3, 4.0, 16.0, 1, vec![]),
        ];
        let nodes = list(vec![
            node(1, &[(ZONE_KEY, zone(1).as_str())]),
            node(2, &[(ZONE_KEY, zone(3).as_str())]),
            node(3, &[(ZONE_KEY, zone(2).as_str())]),
        ]);
        (kube_mds, solver_mds, nodes)
    }

    /// Zones are numbered as first seen: test-zone-1 is 0, test-zone-2 is 1,
    /// and test-zone-3 is 2 from test-node-2. The unzoned MD's slot gets -1.
    const ZONED_SLOT_DOMAINS: [i32; 5] = [0, 0, 0, 1, -1];

    #[test]
    fn node_expressions_follow_kube_operator_rules() {
        let node_labels = labels(&[("test.dev/tier", "gold"), ("test.dev/gen", "12")]);
        let labelled = Some(&node_labels);
        let cases: Vec<(&str, Vec<NodeSelectorRequirement>, Option<&BTreeMap<String, String>>, bool)> = vec![
            ("In matches any listed value", vec![requirement("test.dev/tier", "In", &["silver", "gold"])], labelled, true),
            ("In with no values matches nothing", vec![requirement("test.dev/tier", "In", &[])], labelled, false),
            ("NotIn passes a missing key", vec![requirement("test.dev/rack", "NotIn", &["test-rack-1"])], labelled, true),
            ("NotIn rejects a listed value", vec![requirement("test.dev/tier", "NotIn", &["bronze", "gold"])], labelled, false),
            ("Exists needs the key", vec![requirement("test.dev/gpu", "Exists", &[])], labelled, false),
            ("DoesNotExist rejects a present key", vec![requirement("test.dev/gen", "DoesNotExist", &[])], labelled, false),
            ("Gt compares as integers", vec![requirement("test.dev/gen", "Gt", &["9"])], labelled, true),
            ("Gt takes a negative bound", vec![requirement("test.dev/gen", "Gt", &["-3"])], labelled, true),
            ("Gt is strict", vec![requirement("test.dev/gen", "Gt", &["12"])], labelled, false),
            ("Gt on a non-integer label fails", vec![requirement("test.dev/tier", "Gt", &["1"])], labelled, false),
            ("Gt with two values fails", vec![requirement("test.dev/gen", "Gt", &["1", "2"])], labelled, false),
            ("Lt compares as integers", vec![requirement("test.dev/gen", "Lt", &["20"])], labelled, true),
            ("Lt on a missing key fails", vec![requirement("test.dev/rack", "Lt", &["20"])], labelled, false),
            ("unknown operators never match", vec![requirement("test.dev/tier", "Matches", &["gold"])], labelled, false),
            ("an empty list matches", vec![], labelled, true),
            ("requirements are ANDed", vec![
                requirement("test.dev/tier", "In", &["gold"]),
                requirement("test.dev/gen", "Lt", &["10"]),
            ], labelled, false),
            ("no labels pass NotIn", vec![requirement("test.dev/tier", "NotIn", &["gold"])], None, true),
            ("no labels pass DoesNotExist", vec![requirement("test.dev/tier", "DoesNotExist", &[])], None, true),
            ("no labels fail Gt", vec![requirement("test.dev/gen", "Gt", &["1"])], None, false),
            ("no labels fail Exists", vec![requirement("test.dev/tier", "Exists", &[])], None, false),
        ];
        for (case, exprs, node_labels, expected) in cases {
            assert_eq!(match_node_expr(&exprs, node_labels), expected, "{case}");
        }
    }

    #[test]
    fn md_label_annotation_parses_pairs_and_skips_junk() {
        let md = labelled_instance(1, "test.dev/tier=gold,test.dev/note=a=b,junk,,test.dev/empty=");
        assert_eq!(
            get_expected_node_labels_from_md(&md),
            Some(labels(&[("test.dev/tier", "gold"), ("test.dev/note", "a=b"), ("test.dev/empty", "")])),
            "splits on the first '=' and drops entries without one"
        );
        assert_eq!(get_expected_node_labels_from_md(&labelled_instance(2, "")), Some(BTreeMap::new()));
        assert_eq!(get_expected_node_labels_from_md(&instance(3, &[], 0, 0)), None);
    }

    #[test]
    fn node_selector_prefers_real_labels_even_when_empty() {
        let md = labelled_instance(1, "test.dev/tier=gold,test.dev/disk=local");
        let gold = Some(labels(&[("test.dev/tier", "gold")]));
        let no_labels = BTreeMap::new();
        let silver_node = labels(&[("test.dev/tier", "silver")]);

        assert!(matches_node_selector(&gold, &None, &md), "a new node is judged by the MD's labels");
        assert!(!matches_node_selector(&gold, &Some(&no_labels), &md), "a real node with no labels can't borrow the MD's");
        assert!(!matches_node_selector(&gold, &Some(&silver_node), &md));
        assert!(!matches_node_selector(&Some(labels(&[("test.dev/tier", "gold"), ("test.dev/disk", "network")])), &None, &md));
        assert!(matches_node_selector(&Some(BTreeMap::new()), &Some(&no_labels), &md), "an empty selector matches anything");

        let gold_with_gpu = Some(labels(&[("test.dev/tier", "gold"), ("test.dev/gpu", "yes")]));
        assert_eq!(undeclared_selector_key(&gold_with_gpu, &md), Some("test.dev/gpu".to_string()));
        assert_eq!(undeclared_selector_key(&Some(silver_node), &md), None, "declared key with another value");
        assert_eq!(undeclared_selector_key(&None, &md), None);
        assert_eq!(undeclared_selector_key(&gold, &instance(2, &[], 0, 0)), Some("test.dev/tier".to_string()));
    }

    #[test]
    fn node_affinity_terms_or_together_and_match_fields_need_notin() {
        let md = labelled_instance(1, "test.dev/tier=gold");
        let tier_in = |tier: &str| expr_term(vec![requirement("test.dev/tier", "In", &[tier])]);
        let allowed = |pod: &Pod| pod_allowed_on_md(pod, &None, &md);

        assert!(allowed(&node_affinity_pod(vec![tier_in("silver"), tier_in("gold")])), "any term can match");
        assert!(!allowed(&node_affinity_pod(vec![tier_in("silver")])));
        assert!(!allowed(&node_affinity_pod(vec![expr_term(vec![])])), "an empty term matches nothing");
        assert!(allowed(&node_affinity_pod(vec![field_term("NotIn")])), "a new node's name is never in the list");
        assert!(!allowed(&node_affinity_pod(vec![field_term("In")])), "a new node has no name to match yet");
        let gold_and_named = NodeSelectorTerm { match_fields: field_term("In").match_fields, ..tier_in("gold") };
        assert!(!allowed(&node_affinity_pod(vec![gold_and_named])));

        let preferred_only = with_affinity(pod(2, "100m", "128Mi"), Affinity {
            node_affinity: Some(NodeAffinity {
                preferred_during_scheduling_ignored_during_execution: Some(vec![PreferredSchedulingTerm {
                    weight: 100,
                    preference: tier_in("silver"),
                }]),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert!(allowed(&preferred_only), "preferred node affinity never filters");
        let pod_affinity_only = with_affinity(pod(3, "100m", "128Mi"), required(vec![], vec![term(HOSTNAME_TOP_KEY, 1)]));
        assert!(allowed(&pod_affinity_only));
        assert!(!allowed(&Pod::default()), "a pod without a spec is never allowed");
    }

    #[test]
    fn md_capacity_annotations_parse_and_derate() {
        let resources = |annotations: &[(&str, &str)]| {
            get_md_resources(&instance(1, annotations, 0, 0))
                .map(|r| (r.cpu, (r.memory * 1e4).round() / 1e4))
        };
        // Memory loses 0.15 GiB, 2.74% and the 100Mi eviction threshold,
        // the bench AMI fit
        assert_eq!(resources(&[(CAS_CPU, "1500m"), (CAS_MEMORY, "16384Mi")]), Some((1.5, 15.3116)));
        assert_eq!(resources(&[(CAS_CPU, "4"), (CAS_MEMORY, "32Gi")]), Some((4.0, 30.8732)));
        assert_eq!(resources(&[(CAS_CPU, "4")]), None, "no memory annotation");
        assert_eq!(resources(&[(CAS_CPU, "4"), (CAS_MEMORY, "200Mi")]), None, "derates below zero");
        assert_eq!(resources(&[(CAS_CPU, "0"), (CAS_MEMORY, "8Gi")]), None);
        assert_eq!(resources(&[(CAS_CPU, "lots"), (CAS_MEMORY, "8Gi")]), None);
    }

    #[test]
    fn headroom_counts_committed_machines_only_under_force_replan() {
        fn shape(extra: &[(&'static str, &'static str)]) -> Vec<(&'static str, &'static str)> {
            [(CAS_CPU, "4"), (CAS_MEMORY, "16Gi")].iter().chain(extra).copied().collect()
        }
        let mds = vec![
            // 4 machines committed, 2 nodes ready
            instance(1, &shape(&[(MAX_SIZE, "6"), (COST, "0.2")]), 4, 2),
            // No capacity annotations, so the solver never sees it
            instance(2, &[(MAX_SIZE, "5")], 0, 0),
            // More ready than spec, so both paths count the ready nodes
            instance(3, &shape(&[(MAX_SIZE, "5"), (COST, "-1")]), 2, 3),
            // Already past its max
            instance(4, &shape(&[(MAX_SIZE, "1"), (COST, "cheap")]), 2, 2),
            // No max-size annotation, but it owns a running node
            instance(5, &shape(&[]), 1, 1),
        ];
        let existing = HashMap::from([("test-instance-5".to_string(), vec![slot(1, 2.0, 6.0)])]);

        for (force_replan, headroom) in [(false, [4, 2, 0, 0]), (true, [2, 2, 0, 0])] {
            let (solver_mds, current_replicas) = parse_mds(&mds, &existing, force_replan);
            let names: Vec<&str> = solver_mds.iter().map(|md| md.name.as_str()).collect();
            assert_eq!(names, ["test-instance-1", "test-instance-3", "test-instance-4", "test-instance-5"]);
            assert_eq!(solver_mds.iter().map(|md| md.max_scale_out).collect::<Vec<_>>(), headroom, "force_replan {force_replan}");
            assert_eq!(current_replicas, [2, 3, 2, 1], "current replicas are the ready count either way");
            assert_eq!(solver_mds.iter().map(|md| md.cost_per_node).collect::<Vec<_>>(), [Some(0.2), None, None, None]);
            assert_eq!(solver_mds.iter().map(|md| md.existing_slots.len()).collect::<Vec<_>>(), [0, 0, 0, 1]);
        }
    }

    #[test]
    fn matrix_rows_follow_each_slot_kind() {
        let kube_mds = vec![labelled_instance(1, "test.dev/disk=local"), labelled_instance(2, "test.dev/disk=network")];
        let solver_mds = vec![
            solver_instance(1, 4.0, 16.0, 2, vec![slot(1, 3.0, 12.0), slot(2, 0.5, 2.0)]),
            solver_instance(2, 4.0, 16.0, 1, vec![]),
            // Has no kube MD, so nothing may land on it
            solver_instance(3, 4.0, 16.0, 2, vec![]),
        ];
        let nodes = list(vec![node(1, &[("test.dev/disk", "local")]), node(2, &[("test.dev/disk", "local")])]);
        let running = list(vec![on_node(app_pod(9, 1), 1)]);
        let pods = arcs(vec![
            with_selector(pod(1, "1", "1Gi"), "local"),
            pod(2, "2", "4Gi"),
            pod(3, "6", "1Gi"),
            with_selector(pod(4, "250m", "1Gi"), "network"),
            with_affinity(pod(5, "500m", "1Gi"), required(vec![], vec![term(HOSTNAME_TOP_KEY, 1)])),
        ]);

        let map = generate_solver_md_name_to_md_map(&solver_mds, &kube_mds);
        let matrix = generate_allowed_matrix(&pods, &running, &solver_mds, &nodes, &config(1.0), &map);

        let orphan = vec![false, false];
        assert_eq!(matrix, vec![
            // test-node-2 has half a core free
            vec![vec![true, false, true, true], vec![false], orphan.clone()],
            vec![vec![true, false, true, true], vec![true], orphan.clone()],
            vec![vec![false; 4], vec![false], orphan.clone()],
            // Only test-instance-2 declares network disks
            vec![vec![false; 4], vec![true], orphan.clone()],
            // test-pod-9 on test-node-1 repels it, test-node-2 has just enough
            vec![vec![false, true, true, true], vec![true], orphan.clone()],
        ]);
    }

    #[test]
    fn matrix_derates_only_new_slots() {
        let kube_mds = vec![instance(1, &[], 0, 0)];
        let solver_mds = vec![solver_instance(1, 4.0, 16.0, 1, vec![slot(1, 3.9, 15.0)])];
        let pods = arcs(vec![pod(1, "3800m", "2Gi"), pod(2, "3500m", "14848Mi")]);
        let map = generate_solver_md_name_to_md_map(&solver_mds, &kube_mds);
        let matrix_at = |fraction: f64| generate_allowed_matrix(&pods, &list(vec![]), &solver_mds, &list(vec![]), &config(fraction), &map);

        // At 0.9 a new node offers 3.6 cores and 14.4 GiB, the existing
        // node's free capacity is used as is
        assert_eq!(matrix_at(0.9), vec![vec![vec![true, false]], vec![vec![true, false]]]);
        assert_eq!(matrix_at(1.0), vec![vec![vec![true, true]], vec![vec![true, true]]]);
    }

    #[test]
    fn running_pods_block_nodes_both_ways_unless_finished() {
        let pending = with_affinity(app_pod(1, 1), required(vec![], vec![term(HOSTNAME_TOP_KEY, 2), term(ZONE_KEY, 3)]));

        let mut nominated = app_pod(12, 2);
        nominated.status = Some(PodStatus { nominated_node_name: Some("test-node-2".to_string()), ..Default::default() });
        let mut failed = on_node(app_pod(13, 2), 3);
        failed.status = Some(PodStatus { phase: Some("Failed".to_string()), ..Default::default() });
        let selectorless = PodAffinityTerm { topology_key: HOSTNAME_TOP_KEY.to_string(), ..Default::default() };
        let carrying = |n: usize, anti: PodAffinityTerm| with_affinity(app_pod(n, 4), required(vec![], vec![anti]));

        let running = list(vec![
            on_node(app_pod(11, 2), 1),
            nominated,
            failed,
            terminating(on_node(app_pod(14, 2), 4)),
            // Only the zone term selects it, and that is left to the affinity groups
            on_node(app_pod(15, 3), 5),
            // These carry their own terms against test-app-1
            on_node(carrying(16, term(HOSTNAME_TOP_KEY, 1)), 6),
            on_node(carrying(17, term(ZONE_KEY, 1)), 7),
            on_node(carrying(18, selectorless), 8),
        ]);

        assert_eq!(
            anti_affinity_blocked_nodes(&pending, &running),
            BTreeSet::from(["test-node-1".to_string(), "test-node-2".to_string(), "test-node-6".to_string()])
        );
    }

    #[test]
    fn hostname_terms_become_peer_rules_without_self() {
        let soft = |weight: i32, topology_key: &str, a: usize| WeightedPodAffinityTerm { weight, pod_affinity_term: term(topology_key, a) };
        // Hard colocate goes to the affinity groups, never to peer rules
        let mut carrier = with_affinity(app_pod(1, 1), required(
            vec![term(HOSTNAME_TOP_KEY, 2)],
            vec![term(HOSTNAME_TOP_KEY, 1), term(ZONE_KEY, 2)],
        ));
        let affinity = carrier.spec.as_mut().unwrap().affinity.as_mut().unwrap();
        affinity.pod_affinity.as_mut().unwrap().preferred_during_scheduling_ignored_during_execution =
            Some(vec![soft(50, HOSTNAME_TOP_KEY, 2), soft(80, ZONE_KEY, 2)]);
        affinity.pod_anti_affinity.as_mut().unwrap().preferred_during_scheduling_ignored_during_execution =
            Some(vec![soft(30, HOSTNAME_TOP_KEY, 1)]);
        let pods = arcs(vec![carrier, app_pod(2, 1), app_pod(3, 2), app_pod(4, 2)]);

        let rules = parse_affinities(0, &pods).unwrap();
        assert_eq!(rules.hard_peers, [1], "the self-matching anti term skips the carrier itself");
        assert_eq!(rules.hard_rules, [-1]);
        assert_eq!(rules.soft_peers, [2, 3, 1], "soft affinity peers come before soft anti peers");
        assert_eq!(rules.soft_weights, [0.5, 0.5, -0.3]);

        let plain = parse_affinities(1, &pods).unwrap();
        assert!(plain.hard_peers.is_empty() && plain.soft_peers.is_empty());
    }

    #[test]
    fn spread_constraints_merge_when_equivalent_and_count_running_pods() {
        let (kube_mds, solver_mds, nodes) = zoned_cluster();
        let map = generate_solver_md_name_to_md_map(&solver_mds, &kube_mds);
        let spread = |mut pod: Pod, max_skew: i32, hard: bool, min_domains: Option<i32>, selector: LabelSelector| {
            pod.spec.as_mut().unwrap().topology_spread_constraints = Some(vec![TopologySpreadConstraint {
                topology_key: ZONE_KEY.to_string(),
                max_skew,
                when_unsatisfiable: (if hard { "DoNotSchedule" } else { "ScheduleAnyway" }).to_string(),
                min_domains,
                label_selector: Some(selector),
                ..Default::default()
            }]);
            pod
        };
        let app_2 = LabelSelector { match_labels: Some(labels(&[("app", app(2).as_str())])), ..Default::default() };
        let pods = arcs(vec![
            spread(app_pod(1, 1), 1, true, None, app_in(&[1, 9])),
            // The same constraint with the selector values the other way round
            spread(app_pod(2, 1), 1, true, None, app_in(&[9, 1])),
            spread(app_pod(3, 1), 2, true, None, app_in(&[1, 9])),
            spread(app_pod(4, 2), 1, false, Some(3), app_2),
            app_pod(5, 1),
        ]);
        let running = list(vec![
            on_node(app_pod(21, 1), 1),
            // test-node-2 has no MD, its zone still counts
            on_node(app_pod(22, 1), 2),
            on_node(app_pod(23, 2), 1),
            // Not scheduled, so not counted
            app_pod(24, 1),
        ]);

        let groups = generate_topology_groups(&solver_mds, &pods, &running, &nodes, &map);

        let summary: Vec<_> = groups.iter()
            .map(|g| (g.max_skew, g.enforcement, g.min_domains, g.pods_in_group.clone(), g.domains.baseline_counts.clone()))
            .collect();
        assert_eq!(summary, vec![
            (1, Enforcement::Hard, 0, vec![true, true, true, false, true], vec![1, 0, 1]),
            (1, Enforcement::Soft, 3, vec![false, false, false, true, false], vec![1, 0, 0]),
            (2, Enforcement::Hard, 0, vec![true, true, true, false, true], vec![1, 0, 1]),
        ]);
        for g in &groups {
            assert_eq!(g.domains.num_domains, 3);
            assert_eq!(g.domains.slot_domains, ZONED_SLOT_DOMAINS);
        }
    }

    #[test]
    fn colocate_terms_merge_by_selector_and_skip_soft_terms() {
        let (kube_mds, solver_mds, nodes) = zoned_cluster();
        let map = generate_solver_md_name_to_md_map(&solver_mds, &kube_mds);
        let zone_term_in = |apps: &[usize]| PodAffinityTerm {
            topology_key: ZONE_KEY.to_string(),
            label_selector: Some(app_in(apps)),
            ..Default::default()
        };
        let soft_only = Affinity {
            pod_affinity: Some(PodAffinity {
                preferred_during_scheduling_ignored_during_execution: Some(vec![WeightedPodAffinityTerm {
                    weight: 100,
                    pod_affinity_term: term(ZONE_KEY, 1),
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let pods = arcs(vec![
            with_affinity(app_pod(1, 1), required(vec![zone_term_in(&[1, 2])], vec![])),
            with_affinity(app_pod(2, 1), required(vec![zone_term_in(&[2, 1])], vec![])),
            with_affinity(app_pod(3, 2), soft_only),
            // Hostname anti stays on the peer rules
            with_affinity(app_pod(4, 3), required(vec![], vec![term(HOSTNAME_TOP_KEY, 1)])),
            with_affinity(app_pod(5, 3), required(vec![], vec![term(ZONE_KEY, 2)])),
        ]);
        let running = list(vec![on_node(app_pod(21, 2), 1), on_node(app_pod(22, 1), 2)]);

        let groups = generate_affinity_groups(&solver_mds, &pods, &running, &nodes, &map);

        // Anti sorts before colocate on the same topology key
        let summary: Vec<_> = groups.iter()
            .map(|g| (g.colocate, g.pods_carrying_term.clone(), g.pods_matching_term.clone(), g.domains.baseline_counts.clone()))
            .collect();
        assert_eq!(summary, vec![
            (false, vec![false, false, false, false, true], vec![false, false, true, false, false], vec![1, 0, 0]),
            (true, vec![true, true, false, false, false], vec![true, true, true, false, false], vec![1, 0, 1]),
        ]);
        for g in &groups {
            assert_eq!(g.enforcement, Enforcement::Hard);
            assert_eq!(g.domains.num_domains, 3);
            assert_eq!(g.domains.slot_domains, ZONED_SLOT_DOMAINS);
        }
    }

    #[test]
    fn hostname_colocate_gives_every_new_slot_its_own_domain() {
        let kube_mds = vec![instance(1, &[], 0, 0), instance(2, &[], 0, 0)];
        let solver_mds = vec![
            solver_instance(1, 4.0, 16.0, 2, vec![slot(1, 4.0, 16.0), slot(2, 4.0, 16.0)]),
            solver_instance(2, 4.0, 16.0, 1, vec![slot(3, 4.0, 16.0)]),
        ];
        let hostname = |n: usize| node(n, &[(HOSTNAME_TOP_KEY, format!("test-node-{n}").as_str())]);
        // test-node-2 is missing its hostname label
        let nodes = list(vec![hostname(1), node(2, &[]), hostname(3)]);
        let pods = arcs(vec![with_affinity(app_pod(1, 1), required(vec![term(HOSTNAME_TOP_KEY, 2)], vec![])), app_pod(2, 2)]);
        let running = list(vec![on_node(app_pod(21, 2), 3)]);
        let map = generate_solver_md_name_to_md_map(&solver_mds, &kube_mds);

        let groups = generate_affinity_groups(&solver_mds, &pods, &running, &nodes, &map);

        assert_eq!(groups.len(), 1);
        let g = &groups[0];
        assert!(g.colocate && g.enforcement == Enforcement::Hard);
        assert_eq!(g.pods_carrying_term, [true, false]);
        assert_eq!(g.pods_matching_term, [false, true]);
        // Flat slots: test-node-1, test-node-2, 2 new, test-node-3, 1 new
        assert_eq!(g.domains.slot_domains, [0, -1, 1, 2, 3, 4]);
        assert_eq!(g.domains.num_domains, 5);
        assert_eq!(g.domains.baseline_counts, [0, 0, 0, 1, 0], "test-pod-21 runs on test-node-3");
    }

    #[test]
    fn running_anti_carriers_merge_per_term_and_count_per_zone() {
        let (kube_mds, solver_mds, nodes) = zoned_cluster();
        let map = generate_solver_md_name_to_md_map(&solver_mds, &kube_mds);
        let carrier = |n: usize, node_n: usize, anti: PodAffinityTerm| {
            on_node(with_affinity(app_pod(n, 5), required(vec![], vec![anti])), node_n)
        };
        let running = list(vec![
            carrier(31, 3, term(ZONE_KEY, 1)),
            carrier(32, 2, term(ZONE_KEY, 1)),
            terminating(carrier(33, 1, term(ZONE_KEY, 1))),
            // No pending pod is test-app-9, so this term builds nothing
            carrier(34, 1, term(ZONE_KEY, 9)),
            // Hostname terms block through the allowed matrix instead
            carrier(35, 1, term(HOSTNAME_TOP_KEY, 1)),
        ]);
        let pods = arcs(vec![app_pod(1, 1), app_pod(2, 2)]);

        let groups = generate_affinity_groups(&solver_mds, &pods, &running, &nodes, &map);

        assert_eq!(groups.len(), 1);
        let g = &groups[0];
        assert!(g.enforcement == Enforcement::Hard && !g.colocate);
        assert_eq!(g.pods_carrying_term, [true, false], "the pending pod the carriers select");
        assert_eq!(g.pods_matching_term, [false, false], "running carriers build no pairwise rules");
        assert_eq!(g.domains.baseline_counts, [0, 1, 1], "test-node-3 is in test-zone-2, test-node-2 in test-zone-3");
        assert_eq!(g.domains.slot_domains, ZONED_SLOT_DOMAINS);
    }

    #[test]
    fn nil_selector_matches_nothing_and_empty_selector_matches_everything() {
        let pods = arcs(vec![app_pod(1, 1), app_pod(2, 2), pod(3, "100m", "128Mi")]);
        let expression = |operator: &str, values: Option<Vec<String>>| Some(LabelSelector {
            match_expressions: Some(vec![LabelSelectorRequirement { key: "app".to_string(), operator: operator.to_string(), values }]),
            ..Default::default()
        });
        let cases = [
            ("nil", None, [false, false, false]),
            ("empty", Some(LabelSelector::default()), [true, true, true]),
            ("matchLabels", Some(LabelSelector { match_labels: Some(labels(&[("app", app(2).as_str())])), ..Default::default() }), [false, true, false]),
            ("NotIn passes unlabelled pods", expression("NotIn", Some(vec![app(1)])), [false, true, true]),
            ("DoesNotExist", expression("DoesNotExist", None), [false, false, true]),
        ];
        for (case, selector, expected) in cases {
            assert_eq!(calculate_pods_in_group(&pods, &selector), expected, "{case}");
        }
    }
}


