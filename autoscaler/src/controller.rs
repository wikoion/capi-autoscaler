use futures::StreamExt;
use k8s_openapi::api::core::v1::{Node, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, LabelSelector};
use k8s_openapi::jiff::Timestamp;
use kube::api::{Api, ListParams, Patch, PatchParams};
use kube::ResourceExt;
use kube::runtime::reflector::Store;
use kube::runtime::{Controller, PredicateConfig, WatchStreamExt, controller::Action, reflector, reflector::ObjectRef, watcher, watcher::Config};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::{sync::Arc, time::Duration};

use crate::api::Autoscale;
use crate::api::autoscale_types::{AutoscaleStatus, ConditionType, PlannedPlacement, ScalingTarget, ScalingUpReason};
use std::collections::HashMap;
use crate::{PLACEMENT_GATE, ReconcileContext, get_autoscale, get_machine_deployments, get_remote_nodes, has_nominated_node, has_non_placement_gate, is_node_ready, is_unschedulable};
use crate::machine_deployment::MachineDeployment;
use crate::{Context, Error, Result, optimiser};

use tracing::{Level, event, info, instrument, span, warn, error};

fn hash<T: Hash + ?Sized>(t: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    t.hash(&mut hasher);
    hasher.finish()
}


fn parse_autoscale_ref(autoscale: &Autoscale, namespace: &str) -> Result<ObjectRef<Autoscale>> {
    let autoscale_name = autoscale.metadata.name.as_deref()
        .ok_or(Error::MissingAutoscaleName)?;
    let autoscale_ref = ObjectRef::new(&autoscale_name).within(namespace);

    Ok(autoscale_ref)
}

// Wires the two clusters together: watches the Autoscale CR on the management
// cluster and unschedulable pods on the workload cluster, any pod event requeues
// the single Autoscale.
pub async fn controller(mut ctx: Context) -> Result<()> {
    let (reader, writer) = reflector::store();
    ctx.pod_store = Some(reader);
    let ctx = Arc::new(ctx);

    let autoscale_config = Config::default()
        .labels(&format!("wikoion.dev/cluster-id={}", ctx.cluster_id));

    let autoscale_api = Api::<Autoscale>::namespaced(ctx.client.clone(), &ctx.namespace);

    let pod_api = Api::<Pod>::all(ctx.remote_client.clone());
    let pod_cfg = watcher::Config::default();

    let autoscale = get_autoscale(ctx.clone()).await?;
    let autoscale_ref = parse_autoscale_ref(&autoscale, &ctx.namespace)?;

    // Gating is best-effort: on clusters without the MutatingAdmissionPolicy
    // API pods just arrive ungated and only nomination applies
    if let Err(e) = install_placement_policy(&ctx).await {
        warn!("Failed to install placement gate policy, continuing without gating: {e:?}");
    }

    // The reflector stores every pod; the predicate only shapes the trigger
    // stream, where hashing the uid dedups repeat events for the same pod
    let pod_stream = reflector(writer, watcher(pod_api, pod_cfg))
        .default_backoff()
        .applied_objects()
        .predicate_filter({
            move |pod: &Pod| {
                if is_unschedulable(pod) && !has_non_placement_gate(pod) && !has_nominated_node(pod) {
                    let uid = &pod.uid();
                    Some(hash(uid))
                } else {
                    None
                }
            }
        }, PredicateConfig::default());

    // Readiness transitions requeue promptly so the placement pass runs as
    // soon as a new node joins, not on the next scan_interval
    let node_api = Api::<Node>::all(ctx.remote_client.clone());
    let node_stream = watcher(node_api, watcher::Config::default())
        .default_backoff()
        .applied_objects()
        .predicate_filter({
            move |node: &Node| {
                Some(hash(&(node.uid(), is_node_ready(node))))
            }
        }, PredicateConfig::default());

    let node_autoscale_ref = autoscale_ref.clone();

    Controller::new(autoscale_api, autoscale_config)
        .shutdown_on_signal()
        .watches_stream(pod_stream, {
            move |_pod| {
                let autoscale_ref = autoscale_ref.clone();
                Some(autoscale_ref)
            }
        })
        .watches_stream(node_stream, {
            move |_node| {
                Some(node_autoscale_ref.clone())
            }
        })
        .run(reconcile, error_policy, ctx.clone())
        .filter_map(|x| async move { std::result::Result::ok(x) })
        .for_each(|_| futures::future::ready(()))
        .await;

    Ok(())
}

async fn patch_status(autoscale_api: &Api<Autoscale>, autoscale_name: &str, new_status: AutoscaleStatus) -> Result<()> {
    let patch = serde_json::json!({"status": new_status});

    autoscale_api.patch_status(
        &autoscale_name,
        &PatchParams::default(),
        &Patch::Merge(patch)
    ).await?;

    Ok(())
}

// Enters WaitingForPendingPods; the condition's last_transition_time starts the
// pod_scale_up_delay clock that handle_waiting_for_pods checks
async fn handle_init_status(rc: &ReconcileContext<'_>, prev: Option<&AutoscaleStatus>) -> Result<()> {
    let new_status = AutoscaleStatus {
        observed_generation: rc.observed_generation,
        last_update_time: Some(rc.time),
        conditions: vec![
            ScalingUpReason::WaitingForPendingPods.build(),
        ],
        scaling_plan: None,
        // Carry the previous wave's plan forward until the cycle settles
        planned_placements: prev.and_then(|s| s.planned_placements.clone()),
    };


    patch_status(rc.autoscale_api, rc.autoscale_name, new_status).await?;
    Ok(())
}

// Ends the scaling cycle: report plan vs actual, clear the plan, back to Idle.
// Returns whether any nomination was cleared on the way out.
async fn handle_transition_idle(rc: &ReconcileContext<'_>, prev: Option<&AutoscaleStatus>) -> Result<bool> {
    let mut cleared_any = false;
    if let Some(planned) = prev.and_then(|s| s.planned_placements.as_ref()) {
        // Best-effort report before the plan is cleared
        if let Err(e) = report_placement_outcome(rc, planned).await {
            warn!("Failed to report placement outcome: {e:?}");
        }
        // Ownership ends with the plan: a nomination we set that never bound
        // must be cleared now, or the pod stays hidden from planning forever
        cleared_any = clear_plan_nominations(rc, planned).await?;

        // Cycle over: the plan's pods must not stay gated. Never-planned
        // gated pods are the next cycle's demand and keep their gate.
        ungate_plan_pods(rc, planned).await?;
    }

    // Plan-scoped in-memory state dies with the plan
    {
        let mut placed = rc.ctx.placed.lock().unwrap();
        placed.claimed_slots.clear();
        placed.nodes.clear();
        placed.nominated_at.clear();
    }

    let new_status = AutoscaleStatus {
        observed_generation: rc.observed_generation,
        last_update_time: Some(rc.time),
        conditions: vec![
            ScalingUpReason::Idle.build(),
        ],
        scaling_plan: None,
        planned_placements: None,
    };

    patch_status(rc.autoscale_api, rc.autoscale_name, new_status).await?;
    Ok(cleared_any)
}

// Clear nominations still pending on the outgoing plan's pods
async fn clear_plan_nominations(rc: &ReconcileContext<'_>, planned: &[PlannedPlacement]) -> Result<bool> {
    let Some(store) = rc.ctx.pod_store.as_ref() else { return Ok(false) };
    let mut cleared_any = false;
    for pod in store.state().iter() {
        if !(is_unschedulable(pod) && has_nominated_node(pod) && owned_nomination(rc, planned, pod)) { continue; }
        let (Some(ns), Some(name)) = (pod.metadata.namespace.as_deref(), pod.metadata.name.as_deref()) else { continue };
        if clear_nomination(rc, ns, name).await? {
            info!("cleared nomination on {ns}/{name} with the outgoing plan");
            cleared_any = true;
        }
    }
    Ok(cleared_any)
}

/// Log how the scheduler's actual placement compared with the solver's plan.
async fn report_placement_outcome(rc: &ReconcileContext<'_>, planned: &[PlannedPlacement]) -> Result<()> {
    let machine_sets = crate::get_machine_sets(rc).await?;
    let nodes = get_remote_nodes(rc).await?;
    let node_to_md_map = optimiser::build_node_to_md_map(&machine_sets, &nodes);

    let pod_api = Api::<Pod>::all(rc.ctx.remote_client.clone());
    let pods = pod_api.list(&ListParams::default()).await.map_err(Error::PodError)?;
    let actual_nodes: HashMap<(String, String), Option<String>> = pods.items.iter()
        .filter_map(|p| {
            let ns = p.metadata.namespace.clone()?;
            let name = p.metadata.name.clone()?;
            Some(((ns, name), p.spec.as_ref().and_then(|s| s.node_name.clone())))
        })
        .collect();

    let (mut md_match, mut diverged, mut pending, mut missing) = (0usize, 0usize, 0usize, 0usize);
    for plan in planned {
        let key = (plan.pod_namespace.clone(), plan.pod_name.clone());
        match actual_nodes.get(&key) {
            None => missing += 1,
            Some(None) => pending += 1,
            Some(Some(node)) => {
                let actual_md = node_to_md_map.get(node);
                if actual_md == Some(&plan.md) {
                    md_match += 1;
                } else {
                    diverged += 1;
                    info!(
                        "placement diverged: pod {}/{} planned md={} node={:?}, actual md={:?} node={}",
                        plan.pod_namespace, plan.pod_name, plan.md, plan.node, actual_md, node
                    );
                }
            }
        }
    }

    let node_pinned = planned.iter().filter(|p| p.node.is_some()).count();
    let node_match = planned.iter()
        .filter(|p| p.node.is_some())
        .filter(|p| {
            actual_nodes.get(&(p.pod_namespace.clone(), p.pod_name.clone()))
                .and_then(|n| n.as_ref()) == p.node.as_ref()
        })
        .count();

    info!(
        "placement outcome: planned={} md_match={} diverged={} pending={} missing={} existing_node_pinned={} node_match={}",
        planned.len(), md_match, diverged, pending, missing, node_pinned, node_match
    );

    Ok(())
}

// Null the pod's nominatedNodeName; a pod gone (404) or already bound (422)
// needs no clearing
async fn clear_nomination(rc: &ReconcileContext<'_>, ns: &str, name: &str) -> Result<bool> {
    let pod_api = Api::<Pod>::namespaced(rc.ctx.remote_client.clone(), ns);
    let patch = serde_json::json!({"status": {"nominatedNodeName": null}});
    match pod_api.patch_status(name, &PatchParams::default(), &Patch::Merge(patch)).await {
        Ok(_) => Ok(true),
        Err(kube::Error::Api(ae)) if ae.code == 404 || ae.code == 422 => Ok(false),
        Err(e) => Err(e.into()),
    }
}

fn in_plan(planned: &[PlannedPlacement], pod: &Pod) -> bool {
    planned.iter().any(|p| {
        Some(p.pod_namespace.as_str()) == pod.metadata.namespace.as_deref()
            && Some(p.pod_name.as_str()) == pod.metadata.name.as_deref()
    })
}

fn pod_key(pod: &Pod) -> Option<String> {
    Some(format!("{}/{}", pod.metadata.namespace.as_deref()?, pod.metadata.name.as_deref()?))
}

fn has_placement_gate(pod: &Pod) -> bool {
    pod.spec.as_ref()
        .and_then(|s| s.scheduling_gates.as_ref())
        .is_some_and(|g| g.iter().any(|g| g.name == PLACEMENT_GATE))
}

// Gated and in the plan: waiting for its node, not demand. An ungated plan
// pod (nomination cleared) must still count as demand to be re-planned.
fn gated_plan_pod(planned: &[PlannedPlacement], pod: &Pod) -> bool {
    has_placement_gate(pod) && in_plan(planned, pod)
}

// A nomination is ours to manage when its pod is in the plan, or we stamped
// it ourselves (replacement pods handed a deleted planned pod's slot)
fn owned_nomination(rc: &ReconcileContext<'_>, planned: &[PlannedPlacement], pod: &Pod) -> bool {
    in_plan(planned, pod) || pod_key(pod)
        .is_some_and(|k| rc.ctx.placed.lock().unwrap().nominated_at.contains_key(&k))
}

// KEP-5278: whoever sets nominatedNodeName owns clearing it, so only pods in
// our plan are candidates; scheduler preemption nominations are never touched.
// A nomination is stale when its node is gone, or no longer has room for the
// pod because another pod took the space; clearing puts the pod back into
// planning. A slow scheduler is not staleness: while the node fits we wait.
// Returns whether anything was cleared.
async fn clear_stale_nominations(
    rc: &ReconcileContext<'_>,
    pod_store: &Store<Pod>,
    planned: &[PlannedPlacement],
) -> Result<bool> {
    let all_pods = pod_store.state();
    let (candidates, orphans): (Vec<&Arc<Pod>>, Vec<&Arc<Pod>>) = all_pods.iter()
        .filter(|pod| is_unschedulable(pod) && has_nominated_node(pod))
        .partition(|pod| owned_nomination(rc, planned, pod));
    if candidates.is_empty() && orphans.is_empty() {
        return Ok(false);
    }

    let nodes = get_remote_nodes(rc).await?;
    // Free capacity from bound, still-running pods only; candidates reserve
    // their space in the admission walk below
    let mut free: HashMap<String, (f64, f64)> = nodes.iter()
        .filter(|n| is_node_ready(n))
        .filter_map(|n| {
            let alloc = n.status.as_ref()?.allocatable.as_ref()?;
            let cpu = optimiser::quantity_to_cores(alloc.get("cpu")?)?;
            let mem = optimiser::quantity_to_gib(alloc.get("memory")?)?;
            Some((n.name_any(), (cpu, mem)))
        })
        .collect();
    for pod in all_pods.iter() {
        if pod.spec.as_ref().and_then(|s| s.node_name.as_deref()).is_none() { continue; }
        if let Some(node) = optimiser::pod_reserved_node(pod) {
            if let Some(f) = free.get_mut(node) {
                let (cpu, mem) = optimiser::pod_requests(pod);
                f.0 -= cpu;
                f.1 -= mem;
            }
        }
    }

    // An owned nomination with no stamp (restart, lost entry) is stamped now,
    // never cleared for it; the backstop clock starts here. The key tie-break
    // keeps admission order stable when stamps share one reconcile's time.
    let mut ordered: Vec<(Timestamp, String, &Arc<Pod>, String)> = Vec::new();
    {
        let mut placed = rc.ctx.placed.lock().unwrap();
        for pod in &candidates {
            let Some(node) = pod.status.as_ref().and_then(|s| s.nominated_node_name.clone()) else { continue };
            let (Some(ns), Some(name)) = (pod.metadata.namespace.as_deref(), pod.metadata.name.as_deref()) else { continue };
            let key = format!("{ns}/{name}");
            let stamp = *placed.nominated_at.entry(key.clone()).or_insert(rc.time);
            ordered.push((stamp, key, pod, node));
        }
        // Stamps for pods that bound, vanished or left the plan are done with
        let live: std::collections::HashSet<&String> = ordered.iter().map(|(_, k, _, _)| k).collect();
        placed.nominated_at.retain(|k, _| live.contains(k));
    }
    ordered.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));

    let mut cleared_any = false;
    for (stamp, key, pod, node) in &ordered {
        // Resource fit can't fault an affinity-blocked nomination, so a pod
        // still pending well past its nomination is cleared regardless.
        // Expired pods reserve nothing, so a fresh one behind can take the space
        let expired = rc.time.duration_since(*stamp).unsigned_abs() >= NOMINATION_MAX_WAIT;

        let stale = expired || if let Some(f) = free.get_mut(node) {
            let (need_cpu, need_mem) = optimiser::pod_requests(pod);
            if need_cpu <= f.0 + 1e-9 && need_mem <= f.1 + 1e-9 {
                // Fits: admit it, reserving its space against later candidates
                f.0 -= need_cpu;
                f.1 -= need_mem;
                false
            } else {
                true
            }
        } else if nodes.iter().any(|n| n.name_any() == *node) {
            // Present but not Ready: still provisioning or recovering, keep waiting
            false
        } else {
            // Nominated node no longer exists
            true
        };
        if !stale { continue; }
        let (Some(ns), Some(name)) = (pod.metadata.namespace.as_deref(), pod.metadata.name.as_deref()) else { continue };
        if clear_nomination(rc, ns, name).await? {
            info!("cleared stale nomination on {ns}/{name} (node {node})");
            cleared_any = true;
        }
        rc.ctx.placed.lock().unwrap().nominated_at.remove(key);
    }

    // Unowned nominations are someone else's to clear, except when the
    // nominated node no longer exists: nothing can ever bind the pod there,
    // so clearing cannot fight the scheduler and puts the pod back into planning
    for pod in orphans {
        let Some(node) = pod.status.as_ref().and_then(|s| s.nominated_node_name.as_deref()) else { continue };
        if nodes.iter().any(|n| n.name_any() == node) { continue; }
        let (Some(ns), Some(name)) = (pod.metadata.namespace.as_deref(), pod.metadata.name.as_deref()) else { continue };
        if clear_nomination(rc, ns, name).await? {
            info!("cleared orphaned nomination on {ns}/{name} (node {node} gone)");
            cleared_any = true;
        }
    }

    Ok(cleared_any)
}

// Called when pending pods are detected. We wait pod_scale_up_delay to let more pods
// accumulate, then plan via optimiser::plan_scale_up, commit the first batch via
// optimiser::scale_up (at most max_scale_out replicas per call) and set status
// WaitingForNodes. Later batches are handled from handle_waiting_for_nodes.
async fn handle_waiting_for_pods(
    rc: &ReconcileContext<'_>,
    pod_store: &Store<Pod>,
    autoscale: Arc<Autoscale>,
    cond_idx: usize,
    mut conds: Vec<Condition>
) -> Result<bool> {
    let planned = autoscale.status.as_ref()
        .and_then(|s| s.planned_placements.as_deref())
        .unwrap_or(&[]);
    let unscheduled_pods: Vec<Arc<Pod>> = pod_store.state()
	.into_iter()
	.filter(| pod | is_unschedulable(pod) && !has_non_placement_gate(pod) && !has_nominated_node(pod) && !gated_plan_pod(planned, pod) )
	.collect();
    if unscheduled_pods.is_empty() {
	// Plan pods still gated are waiting on nodes, not settled; tearing
	// down here would ungate them mid-provisioning
	if pod_store.state().iter().any(|pod| gated_plan_pod(planned, pod)) {
	    return Ok(false);
	}
	return handle_transition_idle(&rc, autoscale.status.as_ref()).await;
    }

    let Some(mut status) = autoscale.status.clone() else {
        warn!("Condition index not found, requeuing");
        return Err(Error::MissingStatusCondition)
    };

    let Some(cond) = conds.get(cond_idx) else {
        warn!("Condition index not found, requeuing");
        return Err(Error::MissingStatusCondition)
    };

    let elapsed = rc.time.duration_since(cond.last_transition_time.0).unsigned_abs();
    if elapsed >= autoscale.spec.pod_scale_up_delay.into() {
        // A second threshold: pods still pending past scaling_timeout means the
        // last scale up went wrong (e.g. machines that never joined as nodes),
        // so force a re-plan rather than wait on the in-flight guard forever.
        let force_replan = elapsed >= autoscale.spec.scaling_timeout.into();

        // Call the optimiser library to generate a scaling plan and initiate scaling
        let scale_up_result = optimiser::plan_scale_up(rc.mds, &unscheduled_pods, &autoscale, rc, force_replan).await?;
        optimiser::scale_up(&scale_up_result.target_replicas, autoscale.spec.max_scale_out, rc.ctx.clone()).await?;

        let target_replicas = scale_up_result.target_replicas;
        let new_replicas = target_replicas.values().fold(0, |acc, x| acc + x.target - x.current);

        status.scaling_plan = Some(target_replicas);

        // Later waves override earlier entries for the same pod
        if !scale_up_result.pod_placements.is_empty() {
            let mut new_placements = scale_up_result.pod_placements;
            // Per-wave cutoff: carried-forward entries keep their own stamp
            for p in &mut new_placements {
                p.committed_time = Some(rc.time);
            }
            let mut merged: Vec<PlannedPlacement> = status.planned_placements.take().unwrap_or_default();
            merged.retain(|p| !new_placements.iter()
                .any(|n| n.pod_namespace == p.pod_namespace && n.pod_name == p.pod_name));
            merged.extend(new_placements);
            status.planned_placements = Some(merged);
        }

        conds.remove(cond_idx);
        conds.push(ScalingUpReason::WaitingForNodes{ ready: 0, desired: new_replicas as usize }.build());

        status.last_update_time = Some(rc.time);
        status.conditions = conds;

        patch_status(rc.autoscale_api, rc.autoscale_name, status).await?;

        // Existing-capacity placements have their node already; nominate now
        // and lift the gate so the scheduler can bind
        for p in &scale_up_result.existing_node_placements {
            if nominate_pod(rc, &p.pod_namespace, &p.pod_name, &p.node_name).await? {
                remove_placement_gate(rc, &p.pod_namespace, &p.pod_name).await?;
            }
        }
    }

    Ok(false)
}

async fn handle_waiting_for_nodes(
    rc: &ReconcileContext<'_>,
    cond_idx: usize,
    mut conds: Vec<Condition>,
    autoscale: Arc<Autoscale>,
) -> Result<bool> {
    let Some(mut status) = autoscale.status.clone() else {
        warn!("Condition index not found, requeuing");
        return Err(Error::MissingStatusCondition)
    };
    let Some(cond) = conds.get(cond_idx) else {
        warn!("Condition index not found, requeuing");
        return Err(Error::MissingStatusCondition)
    };
    let elapsed = rc.time.duration_since(cond.last_transition_time.0).unsigned_abs();
    if elapsed >= autoscale.spec.scaling_timeout.into() {
        info!("Scaling timed out, falling back to idle...");
	return handle_transition_idle(&rc, autoscale.status.as_ref()).await;
    }

    let nodes = get_remote_nodes(rc).await?;
    let unready_nodes = nodes.iter()
	.fold(0, |acc, n| if !is_node_ready(n) { acc + 1 } else { acc } );

    if unready_nodes > autoscale.spec.unready_node_limit {
        info!("Max Unready Node count reached, requeuing...");
        return Ok(false);
    }

    // Judge progress by Ready Kubernetes nodes, not MD status.ready_replicas
    // (CAPA machine-ready precedes node join; see optimiser::count_ready_nodes_per_md)
    let machine_sets = crate::get_machine_sets(rc).await?;
    let node_to_md_map = optimiser::build_node_to_md_map(&machine_sets, &nodes);

    if let Some(planned) = status.planned_placements.as_deref() {
        place_pods_on_ready_nodes(rc, planned, &nodes, &node_to_md_map).await?;
    }

    // Used to work out if we're within our batch size
    let ready_node_counts = optimiser::count_ready_nodes_per_md(&nodes, &node_to_md_map);

    let mut plan = status.scaling_plan.clone();

    // Update scaling plan replica counts
    if let Some(ref target_replicas) = status.scaling_plan {
        // total_ready measures the whole plan by ready nodes; batch_ready measures
        // replicas committed to spec since current was last patched, i.e. the batch in flight
        let mut total_ready = 0i32;
        let mut batch_ready = 0i32;
        let mut total_desired = 0i32;

        for (md_name, target) in target_replicas {
            let md = rc.mds.iter()
                .find(|md| md.metadata.name.as_deref() == Some(md_name.as_str()));
            let current = md.and_then(|md| md.spec.replicas).unwrap_or(target.original);
            let ready = ready_node_counts.get(md_name.as_str()).copied().unwrap_or(0);
            total_ready += (ready - target.original).max(0);
            batch_ready += (current - target.current).max(0);
            total_desired += target.target - target.original;

            // Update current ready replicas in scaling plan
	    if let Some(st) = plan.as_mut().and_then(|p| p.get_mut(md_name)) {
		st.current = current;
	    }
        }

        status.scaling_plan = plan;

        if total_ready >= total_desired {
            // A barrier that never lifted must not leave plan pods gated,
            // or they re-enter planning as fresh demand
            if let Some(planned) = status.planned_placements.as_deref() {
                force_release_deferred(rc, planned).await?;
            }
            // Ready counts alone don't settle the plan: a pod still gated is
            // waiting on a slot claim, so hold WaitingForNodes and keep
            // placing. The untouched condition keeps the scaling_timeout
            // backstop ticking.
            let all_pods = rc.ctx.pod_store.as_ref().map(|s| s.state()).unwrap_or_default();
            let unsettled = status.planned_placements.as_deref()
                .is_some_and(|planned| all_pods.iter().any(|pod| gated_plan_pod(planned, pod)));
            if !unsettled {
                info!("Scaling at desired, falling back to analysing...");
                let new_cond = ScalingUpReason::Idle.build();
                conds.remove(cond_idx);
                conds.push(new_cond);
                status.scaling_plan = None;
            }
        } else if batch_ready >= autoscale.spec.max_scale_out {
            if let Some(ref plan) = status.scaling_plan {
                optimiser::scale_up(plan, autoscale.spec.max_scale_out, rc.ctx.clone()).await?;
            }
        } else {
            let new_cond = ScalingUpReason::WaitingForNodes {
                ready: total_ready as usize,
                desired: total_desired as usize,
            }.build();

            // Only replace the condition when progress actually changed:
            // rebuilding it refreshes last_transition_time, which resets the
            // scaling_timeout clock and can leave WaitingForNodes stuck forever.
            if conds[cond_idx].message != new_cond.message {
                conds.remove(cond_idx);
                conds.push(new_cond);
            }
        }
    } else {
        info!("No scaling plan detected, failing back to analysing...");
        let new_cond = ScalingUpReason::Idle.build();
        conds.remove(cond_idx);
        conds.push(new_cond);
    }

    status.last_update_time = Some(rc.time);
    status.conditions = conds;

    patch_status(rc.autoscale_api, rc.autoscale_name, status).await?;

    Ok(false)
}

const NOMINATION_MAX_WAIT: Duration = Duration::from_secs(120);

// Install the MutatingAdmissionPolicy + binding that gate labelled pods.
// Applied as parsed yaml because SSA needs apiVersion/kind in the payload
// and k8s-openapi structs don't serialise TypeMeta.
async fn install_placement_policy(ctx: &Context) -> Result<()> {
    use k8s_openapi::api::admissionregistration::v1beta1::{MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding};
    use serde::Deserialize as _;

    const MANIFEST: &str = include_str!("../manifests/placement-gate.yaml");
    let params = PatchParams::apply("capi-autoscaler").force();

    for doc in serde_yaml::Deserializer::from_str(MANIFEST) {
        let value = serde_yaml::Value::deserialize(doc)?;
        let Some(name) = value.get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .map(str::to_string) else { continue };

        match value.get("kind").and_then(|k| k.as_str()) {
            Some("MutatingAdmissionPolicy") => {
                let api = Api::<MutatingAdmissionPolicy>::all(ctx.remote_client.clone());
                api.patch(&name, &params, &Patch::Apply(&value)).await?;
            }
            Some("MutatingAdmissionPolicyBinding") => {
                let api = Api::<MutatingAdmissionPolicyBinding>::all(ctx.remote_client.clone());
                api.patch(&name, &params, &Patch::Apply(&value)).await?;
            }
            _ => {}
        }
    }
    info!("placement gate policy installed on workload cluster");
    Ok(())
}

// Ungate the outgoing plan's pods, matched by name or by deployment.
// Gated pods that were never planned stay gated: they are planning demand,
// not leftovers. Controller-down safety is deleting the policy binding.
async fn ungate_plan_pods(rc: &ReconcileContext<'_>, planned: &[PlannedPlacement]) -> Result<()> {
    let pod_api = Api::<Pod>::all(rc.ctx.remote_client.clone());
    let pods = pod_api.list(&ListParams::default()).await.map_err(Error::PodError)?;
    for pod in &pods.items {
        if !has_placement_gate(pod) { continue; }
        if !in_plan(planned, pod) && !in_planned_deployment(planned, pod) { continue; }
        let (Some(ns), Some(name)) = (pod.metadata.namespace.as_deref(), pod.metadata.name.as_deref()) else { continue };
        remove_placement_gate(rc, ns, name).await?;
    }
    Ok(())
}

// Same owner-RS match as find_gated_replacement: "<deployment>-<pod-template-hash>"
fn in_planned_deployment(planned: &[PlannedPlacement], pod: &Pod) -> bool {
    let Some(hash) = pod.labels().get("pod-template-hash") else { return false };
    let Some(rs) = pod.metadata.owner_references.as_ref()
        .and_then(|refs| refs.iter().find(|o| o.kind == "ReplicaSet")) else { return false };
    planned.iter().any(|p| {
        Some(p.pod_namespace.as_str()) == pod.metadata.namespace.as_deref()
            && p.deployment.as_deref().is_some_and(|d| rs.name == format!("{d}-{hash}"))
    })
}

// First hard (DoNotSchedule) spread constraint on a non-hostname key
fn hard_spread_key(pod: &Pod) -> Option<String> {
    pod.spec.as_ref()?.topology_spread_constraints.as_ref()?
        .iter()
        .find(|c| c.when_unsatisfiable == "DoNotSchedule" && c.topology_key != optimiser::HOSTNAME_TOP_KEY)
        .map(|c| c.topology_key.clone())
}

// Deployments whose pods carry a hard zone-style spread constraint; their
// release is held for the round-robin pass. Keyed (namespace, deployment),
// value is the constraint's topology_key. Classified from the planned pod
// itself, or any deployment sibling if it was deleted.
fn deferred_deployments<P: std::borrow::Borrow<Pod>>(
    planned: &[PlannedPlacement],
    pods: &[P],
) -> HashMap<(String, String), String> {
    let mut out = HashMap::new();
    for p in planned {
        let Some(deployment) = p.deployment.as_deref() else { continue };
        let key = (p.pod_namespace.clone(), deployment.to_string());
        if out.contains_key(&key) { continue; }
        let pod = pods.iter().map(|pod| pod.borrow())
            .find(|pod| {
                pod.metadata.namespace.as_deref() == Some(p.pod_namespace.as_str())
                    && pod.metadata.name.as_deref() == Some(p.pod_name.as_str())
            })
            .or_else(|| pods.iter().map(|pod| pod.borrow()).find(|pod| {
                // Owner RS is "<deployment>-<pod-template-hash>"
                pod.metadata.namespace.as_deref() == Some(p.pod_namespace.as_str())
                    && pod.labels().get("pod-template-hash").is_some_and(|hash| {
                        pod.metadata.owner_references.as_ref().is_some_and(|refs| {
                            refs.iter().any(|o| o.kind == "ReplicaSet" && o.name == format!("{deployment}-{hash}"))
                        })
                    })
            }));
        if let Some(topology_key) = pod.and_then(hard_spread_key) {
            out.insert(key, topology_key);
        }
    }
    out
}

// Point the scheduler at the planned node. A pod deleted mid-cycle returns
// false so the caller can hand its place to a replacement; not an error.
async fn nominate_pod(rc: &ReconcileContext<'_>, ns: &str, name: &str, node: &str) -> Result<bool> {
    let pod_api = Api::<Pod>::namespaced(rc.ctx.remote_client.clone(), ns);
    let patch = serde_json::json!({"status": {"nominatedNodeName": node}});
    match pod_api.patch_status(name, &PatchParams::default(), &Patch::Merge(patch)).await {
        Ok(_) => {
            rc.ctx.placed.lock().unwrap().nominated_at.insert(format!("{ns}/{name}"), rc.time);
            Ok(true)
        }
        Err(kube::Error::Api(ae)) if ae.code == 404 => {
            warn!("pod {ns}/{name} gone before nomination, skipping");
            Ok(false)
        }
        // 422: the scheduler bound the pod before our hint landed; the
        // nomination is moot, not an error
        Err(kube::Error::Api(ae)) if ae.code == 422 => {
            info!("pod {ns}/{name} already bound, skipping nomination");
            Ok(true)
        }
        Err(e) => Err(e.into()),
    }
}

// A deleted planned pod's stand-in: a gated pod from the same deployment,
// not itself planned, not already handed a slot this pass
fn find_gated_replacement(
    pods: &[Arc<Pod>],
    planned: &[PlannedPlacement],
    p: &PlannedPlacement,
    used: &std::collections::HashSet<String>,
) -> Option<(String, String)> {
    let deployment = p.deployment.as_deref()?;
    pods.iter().find_map(|pod| {
        let ns = pod.metadata.namespace.as_deref()?;
        let name = pod.metadata.name.as_deref()?;
        if ns != p.pod_namespace || has_nominated_node(pod) { return None; }
        if !has_placement_gate(pod) || in_plan(planned, pod) || used.contains(&format!("{ns}/{name}")) { return None; }
        // Owner RS is "<deployment>-<pod-template-hash>"
        let hash = pod.labels().get("pod-template-hash")?;
        let rs = pod.metadata.owner_references.as_ref()?
            .iter().find(|o| o.kind == "ReplicaSet")?;
        (rs.name == format!("{deployment}-{hash}"))
            .then(|| (ns.to_string(), name.to_string()))
    })
}

// Gates can only be removed, so a merge patch with the filtered list is legal
async fn remove_placement_gate(rc: &ReconcileContext<'_>, ns: &str, name: &str) -> Result<()> {
    let pod_api = Api::<Pod>::namespaced(rc.ctx.remote_client.clone(), ns);
    let pod = match pod_api.get(name).await {
        Ok(p) => p,
        Err(kube::Error::Api(ae)) if ae.code == 404 => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    let Some(gates) = pod.spec.as_ref().and_then(|s| s.scheduling_gates.clone()) else { return Ok(()) };
    if !gates.iter().any(|g| g.name == PLACEMENT_GATE) {
        return Ok(());
    }
    let remaining: Vec<_> = gates.into_iter().filter(|g| g.name != PLACEMENT_GATE).collect();
    let patch = serde_json::json!({"spec": {"schedulingGates": remaining}});
    pod_api.patch(name, &PatchParams::default(), &Patch::Merge(patch)).await?;
    Ok(())
}

// Placement pass: each Ready node created after the plan committed claims the
// lowest unclaimed solver slot for its MD, then that slot's pods are nominated
// to it and ungated. Claims live in ctx.placed; reconciles are serialised for
// the single Autoscale, so snapshot-then-insert cannot race.
async fn place_pods_on_ready_nodes(
    rc: &ReconcileContext<'_>,
    planned: &[PlannedPlacement],
    nodes: &kube_core::ObjectList<Node>,
    node_to_md_map: &HashMap<String, String>,
) -> Result<()> {
    // Per-MD open slots with their wave's cutoff: only nodes created after a
    // slot's earliest committed_time may claim it. None means no cutoff.
    let mut open_slots: HashMap<&str, Vec<(i32, Option<Timestamp>)>> = HashMap::new();
    {
        let placed = rc.ctx.placed.lock().unwrap();
        for p in planned {
            let Some(slot) = p.new_node_slot else { continue };
            if placed.claimed_slots.contains_key(&(p.md.clone(), slot)) { continue; }
            let slots = open_slots.entry(p.md.as_str()).or_default();
            match slots.iter_mut().find(|(s, _)| *s == slot) {
                Some((_, cutoff)) => {
                    *cutoff = match (*cutoff, p.committed_time) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        _ => None,
                    };
                }
                None => slots.push((slot, p.committed_time)),
            }
        }
    }
    for slots in open_slots.values_mut() {
        slots.sort_by_key(|(s, _)| *s);
    }

    let mut eligible: Vec<&Node> = nodes.iter()
        .filter(|n| is_node_ready(n))
        .filter(|n| n.metadata.creation_timestamp.is_some())
        .filter(|n| !rc.ctx.placed.lock().unwrap().nodes.contains(&n.name_any()))
        .collect();
    eligible.sort_by_key(|n| (n.metadata.creation_timestamp.as_ref().map(|t| t.0), n.name_any()));

    let all_pods = rc.ctx.pod_store.as_ref().map(|s| s.state()).unwrap_or_default();
    let deferred = deferred_deployments(planned, &all_pods);
    let mut used_replacements: std::collections::HashSet<String> = std::collections::HashSet::new();

    for node in eligible {
        let node_name = node.name_any();
        let Some(created) = node.metadata.creation_timestamp.as_ref().map(|t| t.0) else { continue };
        let Some(md) = node_to_md_map.get(&node_name) else { continue };
        let Some(slots) = open_slots.get_mut(md.as_str()) else { continue };
        // Lowest open slot whose wave predates this node
        let Some(idx) = slots.iter().position(|(_, cutoff)| cutoff.is_none_or(|t| created > t)) else { continue };
        let (slot, _) = slots.remove(idx);

        let mut all_placed = true;
        for p in planned.iter().filter(|p| p.md == *md && p.new_node_slot == Some(slot)) {
            // Spread pods wait for the whole deployment's nodes, released below
            if p.deployment.as_deref().is_some_and(|d| deferred.contains_key(&(p.pod_namespace.clone(), d.to_string()))) {
                continue;
            }
            if nominate_pod(rc, &p.pod_namespace, &p.pod_name, &node_name).await? {
                remove_placement_gate(rc, &p.pod_namespace, &p.pod_name).await?;
            } else if let Some((rns, rname)) = find_gated_replacement(&all_pods, planned, p, &used_replacements) {
                // Planned pod deleted: its gated replacement takes the slot
                nominate_pod(rc, &rns, &rname, &node_name).await?;
                remove_placement_gate(rc, &rns, &rname).await?;
                info!("placement: {rns}/{rname} replaces deleted {}/{} on {node_name}", p.pod_namespace, p.pod_name);
                used_replacements.insert(format!("{rns}/{rname}"));
            } else {
                // No replacement yet; leave the slot unclaimed for a later pass
                all_placed = false;
            }
        }
        if !all_placed { continue; }
        info!("placement: node {node_name} (md {md}) claimed slot {slot}");

        let mut placed = rc.ctx.placed.lock().unwrap();
        placed.claimed_slots.insert((md.clone(), slot), node_name.clone());
        placed.nodes.insert(node_name);
    }

    release_deferred_deployments(rc, planned, nodes, &deferred, &all_pods, &mut used_replacements).await?;

    Ok(())
}

// Barrier: every (md, slot) the deployment's pods occupy has a claimed node.
// Returns the slot -> node map, or None while any slot is still open.
fn deferred_barrier_met(
    pods: &[&PlannedPlacement],
    claimed: &HashMap<(String, i32), String>,
) -> Option<HashMap<(String, i32), String>> {
    pods.iter()
        .filter_map(|p| p.new_node_slot.map(|s| (p.md.clone(), s)))
        .map(|key| claimed.get(&key).map(|n| (key, n.clone())))
        .collect()
}

// Zones sorted by name, pods within a zone already sorted by the caller,
// one entry per zone cycling until every queue is drained
fn round_robin_zones<T>(zones: std::collections::BTreeMap<String, Vec<T>>) -> Vec<T> {
    let mut queues: Vec<_> = zones.into_values().map(|v| v.into_iter()).collect();
    let mut out = Vec::new();
    loop {
        let mut emitted = false;
        for q in queues.iter_mut() {
            if let Some(item) = q.next() {
                out.push(item);
                emitted = true;
            }
        }
        if !emitted { break out; }
    }
}

// Deferred release: a spread deployment's pods are held until every slot
// hosting them has a Ready node, then released round-robin across zones so
// bind order never front-loads one zone past maxSkew. Runs every pass so a
// deleted pod's replacement is picked up on a later reconcile.
async fn release_deferred_deployments(
    rc: &ReconcileContext<'_>,
    planned: &[PlannedPlacement],
    nodes: &kube_core::ObjectList<Node>,
    deferred: &HashMap<(String, String), String>,
    all_pods: &[Arc<Pod>],
    used_replacements: &mut std::collections::HashSet<String>,
) -> Result<()> {
    // nominated_at alone is not enough: clear_stale_nominations prunes it,
    // so a pod showing a nomination in the store also counts as released
    let nominated_keys: std::collections::HashSet<String> = all_pods.iter()
        .filter(|p| has_nominated_node(p))
        .filter_map(|p| pod_key(p))
        .collect();

    for ((ns, deployment), topology_key) in deferred {
        let pods: Vec<&PlannedPlacement> = planned.iter()
            .filter(|p| p.pod_namespace == *ns && p.deployment.as_deref() == Some(deployment.as_str()))
            .filter(|p| p.new_node_slot.is_some())
            .collect();
        if pods.is_empty() { continue; }

        let slot_nodes = {
            let placed = rc.ctx.placed.lock().unwrap();
            deferred_barrier_met(&pods, &placed.claimed_slots)
        };
        let Some(slot_nodes) = slot_nodes else { continue };

        // Group unreleased pods by the claimed node's zone; a node gone from
        // the list groups by name and the stale-nomination pass cleans up
        let mut zones: std::collections::BTreeMap<String, Vec<(&PlannedPlacement, String)>> = Default::default();
        for p in &pods {
            let key = format!("{}/{}", p.pod_namespace, p.pod_name);
            if nominated_keys.contains(&key)
                || rc.ctx.placed.lock().unwrap().nominated_at.contains_key(&key) {
                continue;
            }
            let node_name = slot_nodes[&(p.md.clone(), p.new_node_slot.unwrap())].clone();
            let zone = nodes.iter()
                .find(|n| n.name_any() == node_name)
                .and_then(|n| n.labels().get(topology_key).cloned())
                .unwrap_or_else(|| node_name.clone());
            zones.entry(zone).or_default().push((p, node_name));
        }
        if zones.is_empty() { continue; }
        for group in zones.values_mut() {
            group.sort_by(|(a, _), (b, _)| (&a.pod_namespace, &a.pod_name).cmp(&(&b.pod_namespace, &b.pod_name)));
        }

        info!("placement: releasing deferred deployment {ns}/{deployment} round-robin over {topology_key}");
        for (p, node_name) in round_robin_zones(zones) {
            if nominate_pod(rc, &p.pod_namespace, &p.pod_name, &node_name).await? {
                remove_placement_gate(rc, &p.pod_namespace, &p.pod_name).await?;
            } else if let Some((rns, rname)) = find_gated_replacement(all_pods, planned, p, used_replacements) {
                nominate_pod(rc, &rns, &rname, &node_name).await?;
                remove_placement_gate(rc, &rns, &rname).await?;
                info!("placement: {rns}/{rname} replaces deleted {}/{} on {node_name}", p.pod_namespace, p.pod_name);
                used_replacements.insert(format!("{rns}/{rname}"));
            }
        }
    }

    Ok(())
}

// Backstop for the Idle-at-desired transition: any deferred plan pod still
// gated is nominated to its claimed node where the slot claimed, or plainly
// ungated where not. In the common case place_pods_on_ready_nodes already
// released everything this reconcile and this is a no-op.
async fn force_release_deferred(rc: &ReconcileContext<'_>, planned: &[PlannedPlacement]) -> Result<()> {
    let all_pods = rc.ctx.pod_store.as_ref().map(|s| s.state()).unwrap_or_default();
    let deferred = deferred_deployments(planned, &all_pods);
    if deferred.is_empty() { return Ok(()) }

    for p in planned {
        let Some(slot) = p.new_node_slot else { continue };
        let Some(d) = p.deployment.as_deref() else { continue };
        if !deferred.contains_key(&(p.pod_namespace.clone(), d.to_string())) { continue; }

        let key = format!("{}/{}", p.pod_namespace, p.pod_name);
        let gated = all_pods.iter()
            .find(|pod| pod_key(pod).as_deref() == Some(key.as_str()))
            .is_some_and(|pod| has_placement_gate(pod));
        if !gated { continue; }

        let node = rc.ctx.placed.lock().unwrap().claimed_slots.get(&(p.md.clone(), slot)).cloned();
        if let Some(node) = node {
            warn!("placement: force releasing {key} to {node} at cycle end");
            nominate_pod(rc, &p.pod_namespace, &p.pod_name, &node).await?;
        } else {
            warn!("placement: ungating {key} without a nomination, slot never claimed");
        }
        remove_placement_gate(rc, &p.pod_namespace, &p.pod_name).await?;
    }

    Ok(())
}

// Crash recovery: rebuild ctx.placed from live pods. A slot whose pods carry
// a nominatedNodeName already claimed that node; finish its stragglers (NNN
// then ungate is idempotent) so a crash between the two writes heals here.
async fn recover_placed_state(rc: &ReconcileContext<'_>, planned: &[PlannedPlacement]) -> Result<()> {
    let pod_api = Api::<Pod>::all(rc.ctx.remote_client.clone());
    let pods = pod_api.list(&ListParams::default()).await.map_err(Error::PodError)?;
    let nominated: HashMap<(String, String), String> = pods.items.iter()
        .filter_map(|p| {
            let ns = p.metadata.namespace.clone()?;
            let name = p.metadata.name.clone()?;
            let node = p.status.as_ref()?.nominated_node_name.clone()?;
            Some(((ns, name), node))
        })
        .collect();

    let deferred = deferred_deployments(planned, &pods.items);

    let mut groups: HashMap<(String, i32), Vec<&PlannedPlacement>> = HashMap::new();
    for p in planned {
        if let Some(slot) = p.new_node_slot {
            groups.entry((p.md.clone(), slot)).or_default().push(p);
        }
    }

    for ((md, slot), group) in groups {
        let Some(node) = group.iter()
            .find_map(|p| nominated.get(&(p.pod_namespace.clone(), p.pod_name.clone())))
            .cloned() else { continue };

        for p in &group {
            // Unreleased spread pods stay held; the deferred pass re-fires
            // once claimed_slots is rebuilt. Crash between NNN and ungate
            // still heals here because the pod already shows a nomination.
            let held = p.deployment.as_deref()
                .is_some_and(|d| deferred.contains_key(&(p.pod_namespace.clone(), d.to_string())));
            if held && !nominated.contains_key(&(p.pod_namespace.clone(), p.pod_name.clone())) {
                continue;
            }
            nominate_pod(rc, &p.pod_namespace, &p.pod_name, &node).await?;
            remove_placement_gate(rc, &p.pod_namespace, &p.pod_name).await?;
        }
        info!("placement recovery: node {node} (md {md}) had claimed slot {slot}");

        let mut placed = rc.ctx.placed.lock().unwrap();
        placed.claimed_slots.insert((md, slot), node.clone());
        placed.nodes.insert(node);
    }

    Ok(())
}

#[instrument(skip_all, fields(autoscale_name))]
pub async fn reconcile(autoscale: Arc<Autoscale>, ctx: Arc<Context>) -> Result<Action> {
    let autoscale_name = autoscale.metadata.name.as_deref()
        .ok_or(Error::MissingAutoscaleName)?;
    tracing::Span::current().record("autoscale_name", autoscale_name);

    let scan_interval = autoscale.spec.scan_interval;
    let observed_generation = autoscale.metadata.generation;
    let mds = get_machine_deployments(ctx.clone()).await?;
    let time = Timestamp::now();
    let store = ctx.pod_store.clone();
    let autoscale_api = Api::<Autoscale>::namespaced(ctx.client.clone(), &ctx.namespace);

    let rc = ReconcileContext{
        autoscale_name,
        autoscale_api: &autoscale_api,
        time,
        mds: &mds,
        ctx: ctx.clone(),
        observed_generation,
	node_selector: autoscale.spec.node_selector.clone(),
    };

    // If we have pods in the store, then we have unschedulable pods
    let Some(pod_store) = store else {
        return Ok(Action::requeue(scan_interval.into()));
    };
    let Some(ref status) = autoscale.status else {
        handle_init_status(&rc, None).await?;
        return Ok(Action::requeue(scan_interval.into()));
    };

    // One-shot after restart: rebuild in-memory placement claims from pods
    if !ctx.placed.lock().unwrap().recovered {
        if let Some(planned) = status.planned_placements.as_deref() {
            recover_placed_state(&rc, planned).await?;
        }
        ctx.placed.lock().unwrap().recovered = true;
    }

    let mut cleared = clear_stale_nominations(
        &rc,
        &pod_store,
        status.planned_placements.as_deref().unwrap_or(&[]),
    ).await?;

    // Check status to see if we're already waiting for more unschedulable pods
    let conds = status.conditions.clone();
    let scaling_up_idx = conds.iter().position(|c| {
        c.type_ == ConditionType::ScalingUp.name()
    });

    let Some(cond_idx) = scaling_up_idx else {
        handle_init_status(&rc, autoscale.status.as_ref()).await?;
        return Ok(Action::requeue(scan_interval.into()));
    };

    // State machine keyed on the ScalingUp condition reason:
    // Idle -> WaitingForPendingPods -> WaitingForNodes -> Idle
    match conds[cond_idx].reason.as_str() {
        ScalingUpReason::WAITING_FOR_PENDING_PODS => {
	    cleared |= handle_waiting_for_pods(&rc, &pod_store, autoscale.clone(), cond_idx, conds.clone()).await?;
        }
        ScalingUpReason::WAITING_FOR_NODES => {
            cleared |= handle_waiting_for_nodes(&rc, cond_idx, conds.clone(), autoscale.clone()).await?;
        }
        ScalingUpReason::IDLE => {
	    let planned = status.planned_placements.as_deref().unwrap_or(&[]);
	    let has_unschedulable = pod_store.state().iter().any(|pod| is_unschedulable(pod) && !has_non_placement_gate(pod) && !has_nominated_node(pod) && !gated_plan_pod(planned, pod));
	    if has_unschedulable {
		handle_init_status(&rc, autoscale.status.as_ref()).await?;
	    } else if autoscale.status.as_ref().is_some_and(|s| s.planned_placements.is_some()) {
		// Scaling settled: report plan-vs-actual and clear the plan
		cleared |= handle_transition_idle(&rc, autoscale.status.as_ref()).await?;
	    }
        }
        _ => {
            return Ok(Action::requeue(scan_interval.into()));
        }
    }

    if cleared {
        // The pod predicate hashes only the uid, so the post-clear watch event
        // dedups against the pre-nomination one; requeue promptly instead
        return Ok(Action::requeue(Duration::from_secs(5)));
    }

    Ok(Action::requeue(scan_interval.into()))
}

// error_policy is the function called when a reconciliation error happens
fn error_policy(autoscale: Arc<Autoscale>, error: &Error, _: Arc<Context>) -> Action {
    error!(
        "error happened during reconciliation of service svc {:?}",
        error
    );
    let scan_interval = autoscale.spec.scan_interval;
    Action::requeue(scan_interval.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{PodSchedulingGate, PodSpec, TopologySpreadConstraint};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

    const NS: &str = "test-ns";
    const ZONE_KEY: &str = "topology.kubernetes.io/zone";

    fn named_pod(ns: &str, n: usize) -> Pod {
        let mut pod = Pod::default();
        pod.metadata.namespace = Some(ns.to_string());
        pod.metadata.name = Some(format!("test-pod-{n}"));
        pod.spec = Some(PodSpec::default());
        pod
    }

    fn gated(mut pod: Pod, gate: &str) -> Pod {
        pod.spec.as_mut().unwrap().scheduling_gates = Some(vec![PodSchedulingGate { name: gate.to_string() }]);
        pod
    }

    fn spread(mut pod: Pod, key: &str, when: &str) -> Pod {
        pod.spec.as_mut().unwrap().topology_spread_constraints = Some(vec![TopologySpreadConstraint {
            max_skew: 1,
            topology_key: key.to_string(),
            when_unsatisfiable: when.to_string(),
            ..Default::default()
        }]);
        pod
    }

    /// test-pod-N planned onto new-node slot `slot` of test-instance-`md`.
    fn planned(n: usize, md: usize, slot: i32, deployment: Option<usize>) -> PlannedPlacement {
        PlannedPlacement {
            pod_namespace: NS.to_string(),
            pod_name: format!("test-pod-{n}"),
            deployment: deployment.map(|d| format!("test-deployment-{d}")),
            md: format!("test-instance-{md}"),
            node: None,
            new_node_slot: Some(slot),
            committed_time: None,
        }
    }

    fn slot_key(md: usize, slot: i32) -> (String, i32) {
        (format!("test-instance-{md}"), slot)
    }

    #[test]
    fn only_gated_planned_pods_are_held_back_from_demand() {
        let plan = vec![planned(1, 1, 0, Some(1))];

        // Gated and in the plan: waiting for its node
        assert!(gated_plan_pod(&plan, &gated(named_pod(NS, 1), PLACEMENT_GATE)));
        // Same name in another namespace is a different pod
        assert!(!gated_plan_pod(&plan, &gated(named_pod("test-ns-other", 1), PLACEMENT_GATE)));
        // Held by someone else's gate, not ours
        assert!(!gated_plan_pod(&plan, &gated(named_pod(NS, 1), "example.com/other-gate")));
        // Ungated after its nomination was cleared: demand again
        assert!(!gated_plan_pod(&plan, &named_pod(NS, 1)));
    }

    #[test]
    fn hard_non_hostname_spreads_defer_their_deployment() {
        // test-deployment-1's planned pod is present. test-deployment-2's was
        // deleted, so it is classified from a sibling owned by the same RS.
        let plan = vec![planned(1, 1, 0, Some(1)), planned(2, 2, 0, Some(2))];

        let mut sibling = spread(named_pod(NS, 7), "topology.example.com/rack", "DoNotSchedule");
        sibling.metadata.labels = Some(std::collections::BTreeMap::from([("pod-template-hash".to_string(), "7f9c4".to_string())]));
        sibling.metadata.owner_references = Some(vec![OwnerReference {
            kind: "ReplicaSet".to_string(),
            name: "test-deployment-2-7f9c4".to_string(),
            ..Default::default()
        }]);
        let pods = vec![spread(named_pod(NS, 1), ZONE_KEY, "DoNotSchedule"), sibling];

        let deferred = deferred_deployments(&plan, &pods);

        let key = |d: usize| (NS.to_string(), format!("test-deployment-{d}"));
        assert_eq!(deferred.len(), 2, "{:?}", deferred);
        assert_eq!(deferred[&key(1)], ZONE_KEY);
        assert_eq!(deferred[&key(2)], "topology.example.com/rack");
    }

    #[test]
    fn soft_hostname_and_ownerless_spreads_are_released_normally() {
        let plan = vec![
            planned(1, 1, 0, Some(1)),
            planned(2, 1, 1, Some(2)),
            planned(3, 1, 2, None),
        ];
        let pods = vec![
            spread(named_pod(NS, 1), ZONE_KEY, "ScheduleAnyway"),
            spread(named_pod(NS, 2), optimiser::HOSTNAME_TOP_KEY, "DoNotSchedule"),
            // Hard zone spread, but no deployment to hold back
            spread(named_pod(NS, 3), ZONE_KEY, "DoNotSchedule"),
        ];

        assert!(deferred_deployments(&plan, &pods).is_empty());
    }

    #[test]
    fn barrier_opens_once_every_new_node_slot_is_claimed() {
        let first = planned(1, 1, 0, Some(1));
        let second = planned(2, 1, 1, Some(1));
        let third = planned(3, 2, 0, Some(1));
        // Already on an existing node, so the barrier has nothing to wait for
        let mut existing = planned(4, 2, 0, Some(1));
        existing.node = Some("test-node-9".to_string());
        existing.new_node_slot = None;
        let pods = vec![&first, &second, &third, &existing];

        let mut claimed = HashMap::from([
            (slot_key(1, 0), "test-node-1".to_string()),
            (slot_key(3, 0), "test-node-5".to_string()), // another deployment's slot
        ]);
        assert!(deferred_barrier_met(&pods, &claimed).is_none());

        claimed.insert(slot_key(2, 0), "test-node-3".to_string());
        assert!(deferred_barrier_met(&pods, &claimed).is_none(), "test-instance-1 slot 1 is still open");

        claimed.insert(slot_key(1, 1), "test-node-2".to_string());
        let slot_nodes = deferred_barrier_met(&pods, &claimed).expect("every slot is claimed");
        assert_eq!(slot_nodes.len(), 3);
        assert_eq!(slot_nodes[&slot_key(1, 1)], "test-node-2");
        assert_eq!(slot_nodes[&slot_key(2, 0)], "test-node-3");
    }

    #[test]
    fn round_robin_takes_one_per_zone_until_all_drain() {
        // Inserted out of order with an empty zone: zones go by name and the
        // empty one is skipped.
        let zones = std::collections::BTreeMap::from([
            ("test-zone-d".to_string(), vec!["d1"]),
            ("test-zone-a".to_string(), vec!["a1", "a2"]),
            ("test-zone-c".to_string(), vec![]),
            ("test-zone-b".to_string(), vec!["b1", "b2", "b3"]),
        ]);
        assert_eq!(round_robin_zones(zones), vec!["a1", "b1", "d1", "a2", "b2", "b3"]);
    }
}
