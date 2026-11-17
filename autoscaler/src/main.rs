use kube::Client;
use kube::Config;
use kube::config::{KubeConfigOptions, Kubeconfig};

use tracing::error;
use tracing_subscriber;
use capi_autoscaler::Context;
use capi_autoscaler::controller;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run().await;
    Ok(())
}

pub async fn run() {
    tracing_subscriber::fmt::init();

    let remote_config_path = std::env::var("REMOTE_CONFIG_PATH")
        .expect("REMOTE_CONFIG_PATH is required");
    let cluster_id = std::env::var("CLUSTER_ID")
        .expect("CLUSTER_ID is required");
    let namespace = std::env::var("NAMESPACE")
        .expect("NAMESPACE is required");

    let client = Client::try_default()
        .await
        .expect("error getting a Kubernetes client");

    let remote_kubeconfig = Kubeconfig::read_from(&remote_config_path)
        .expect("failed to read kubeconfig");                                               

    let config = Config::from_custom_kubeconfig(remote_kubeconfig, &KubeConfigOptions::default())
        .await
        .expect("failed to build config from kubeconfig");

    let remote_client = Client::try_from(config)
        .expect("failed to create client from config");

    let ctx = Context {
        client: client.clone(),
        remote_client: remote_client.clone(),
        cluster_id: cluster_id,
        namespace: namespace,
        pod_store: None,
        placed: Default::default(),
    };

    if let Err(error) = controller::controller(ctx).await {
        error!("failed to start Autoscale controller: {error}");
        std::process::exit(1);
    }
}
