use std::collections::HashMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time, LabelSelector};
use k8s_openapi::jiff;
use k8s_openapi::{jiff::Timestamp};
use kube::CustomResource;
use kube_core::Duration;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Copy, Debug, Clone)]
pub enum ConditionType {
    Ready,
    ScalingUp,
    ScalingDown,
}

impl ConditionType {
    pub fn name(self) -> &'static str {
          match self {
              Self::Ready => "Ready",
              Self::ScalingUp => "ScalingUp",
              Self::ScalingDown => "ScalingDown",
          }
      }

      pub fn build(self, status: &str, reason: &str, message: &str) -> Condition {
          Condition {
              type_: self.name().to_string(),
              status: status.to_string(),
              reason: reason.to_string(),
              message: message.to_string(),
              last_transition_time: Time(Timestamp::now()),
              observed_generation: None,
          }
      }
}

#[derive(Copy, Debug, Clone)]
pub enum ScalingUpReason {
    WaitingForPendingPods,
    WaitingForNodes { ready: usize, desired: usize },
    Idle,
}

impl ScalingUpReason {
      pub const WAITING_FOR_PENDING_PODS: &str = "WaitingForPendingPods";
      pub const WAITING_FOR_NODES: &str = "WaitingForNodes";
      pub const IDLE: &str = "Idle";

      pub fn status(&self) -> &'static str {
          match self {
              Self::Idle => "False",
              _ => "True",
          }
      }

      pub fn reason(&self) -> &'static str {
          match self {
              Self::WaitingForPendingPods => Self::WAITING_FOR_PENDING_PODS,
              Self::WaitingForNodes { .. } => Self::WAITING_FOR_NODES,
              Self::Idle => Self::IDLE,
          }
      }

      pub fn message(&self) -> String {
          match self {
              Self::WaitingForPendingPods => "Collecting pending pods".to_string(),
              Self::WaitingForNodes { ready, desired } => {
                  format!("Nodes provisioning, {ready}/{desired} ready")
              }
              Self::Idle => "No scale up needed".to_string(),
          }
      }

      pub fn build(self) -> Condition {
          ConditionType::ScalingUp.build(
              self.status(),
              self.reason(),
              &self.message()
          )
      } 
}

fn pod_scale_up_delay_default() -> Duration {
    std::time::Duration::from_secs(30).into()
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct ScalingTarget {
    pub original: i32,
    pub current: i32,
    pub target: i32,
}

/// Where the solver planned to put one pending pod.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlannedPlacement {
    pub pod_namespace: String,
    pub pod_name: String,
    /// Owner deployment, used for gating scope and startup recovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment: Option<String>,
    pub md: String,
    /// Only set for existing-capacity placements; new nodes have no name yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Solver slot index for new-node placements; pods sharing it share a node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_node_slot: Option<i32>,
    /// When this placement's wave committed; new-node eligibility cutoff for
    /// its slot in the placement pass.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<String>")]
    pub committed_time: Option<Timestamp>,
}

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(kind = "Autoscale", group = "wikoion.dev", version = "v1alpha1", namespaced)]
#[kube(status = "AutoscaleStatus")]
#[serde(rename_all ="camelCase")]
pub struct AutoscaleSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_selector: Option<LabelSelector>,
    pub scan_interval: Duration,
    #[serde(default = "pod_scale_up_delay_default")]
    pub pod_scale_up_delay: Duration,
    pub max_scale_out: i32,
    pub scaling_timeout: Duration,
    pub solver_timeout: Duration,
    #[serde(default)]
    pub unready_node_limit: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_improvement_percent: Option<i32>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct AutoscaleStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    #[schemars(with = "Option<String>")]
    pub last_update_time: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scaling_plan: Option<HashMap<String, ScalingTarget>>,
    // No skip_serializing_if: None must serialize as null so the merge patch clears it
    #[serde(default)]
    pub planned_placements: Option<Vec<PlannedPlacement>>,
}
