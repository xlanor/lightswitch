use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams};
use kube::runtime::{watcher, WatchStreamExt};
use kube::Client;
use tracing::{debug, error, info, warn};

#[derive(Clone, Debug)]
pub struct PodMetadata {
    pub pod_name: String,
    pub namespace: String,
    pub owner_kind: Option<String>,
    pub owner_name: Option<String>,
}

pub struct K8sPodCache {
    container_to_pod: Arc<RwLock<HashMap<String, PodMetadata>>>,
}

impl K8sPodCache {
    pub fn new(node_name: String) -> Result<Self, kube::Error> {
        let container_to_pod: Arc<RwLock<HashMap<String, PodMetadata>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let cache = container_to_pod.clone();
        std::thread::Builder::new()
            .name("k8s-pod-informer".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to create tokio runtime");
                runtime.block_on(async move {
                    if let Err(e) = run_informer(node_name, cache).await {
                        error!("pod informer failed: {}", e);
                    }
                });
            })
            .expect("failed to spawn pod informer thread");

        Ok(Self {
            container_to_pod,
        })
    }

    pub fn get_pod_metadata(&self, container_id: &str) -> Option<PodMetadata> {
        self.container_to_pod
            .read()
            .ok()?
            .get(container_id)
            .cloned()
    }
}

async fn run_informer(
    node_name: String,
    cache: Arc<RwLock<HashMap<String, PodMetadata>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::try_default().await?;
    let pods: Api<Pod> = Api::all(client);

    let field_selector = format!("spec.nodeName={}", node_name);
    let watcher_config = watcher::Config::default().fields(&field_selector);

    info!("starting pod informer for node {}", node_name);

    let mut stream = watcher(pods, watcher_config)
        .default_backoff()
        .applied_objects()
        .boxed();

    while let Some(event) = stream.try_next().await? {
        handle_pod_event(&cache, event);
    }

    warn!("pod informer stream ended");
    Ok(())
}

fn handle_pod_event(cache: &Arc<RwLock<HashMap<String, PodMetadata>>>, pod: Pod) {
    let metadata = pod.metadata;
    let pod_name = match metadata.name {
        Some(ref name) => name.clone(),
        None => return,
    };
    let namespace = metadata.namespace.clone().unwrap_or_default();

    let (owner_kind, owner_name) = metadata
        .owner_references
        .as_ref()
        .and_then(|refs| refs.first())
        .map(|owner| (Some(owner.kind.clone()), Some(owner.name.clone())))
        .unwrap_or((None, None));

    let pod_meta = PodMetadata {
        pod_name,
        namespace,
        owner_kind,
        owner_name,
    };

    let status = match pod.status {
        Some(ref s) => s,
        None => return,
    };

    let mut cache = match cache.write() {
        Ok(c) => c,
        Err(_) => return,
    };

    let all_statuses = status
        .container_statuses
        .iter()
        .flatten()
        .chain(status.init_container_statuses.iter().flatten());

    for cs in all_statuses {
        if let Some(ref container_id) = cs.container_id {
            if let Some(id) = parse_container_id(container_id) {
                debug!("mapping container {} to pod {}", id, pod_meta.pod_name);
                cache.insert(id, pod_meta.clone());
            }
        }
    }
}

fn parse_container_id(raw: &str) -> Option<String> {
    raw.split("://").nth(1).map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_container_id_containerd() {
        let raw = "containerd://abcdef0123456789";
        assert_eq!(
            parse_container_id(raw),
            Some("abcdef0123456789".to_string())
        );
    }

    #[test]
    fn test_parse_container_id_docker() {
        let raw = "docker://abcdef0123456789";
        assert_eq!(
            parse_container_id(raw),
            Some("abcdef0123456789".to_string())
        );
    }

    #[test]
    fn test_parse_container_id_crio() {
        let raw = "cri-o://abcdef0123456789";
        assert_eq!(
            parse_container_id(raw),
            Some("abcdef0123456789".to_string())
        );
    }

    #[test]
    fn test_parse_container_id_no_scheme() {
        assert_eq!(parse_container_id("abcdef"), None);
    }
}
