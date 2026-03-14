use std::sync::Mutex;

use lightswitch_metadata::types::{
    MetadataLabel, TaskKey, TaskMetadataProvider, TaskMetadataProviderError,
};
use lru::LruCache;
use std::num::NonZeroUsize;
use tracing::debug;

use crate::cgroup::container_id_from_pid;
use crate::k8s_client::K8sPodCache;

pub struct K8sMetadataProvider {
    pod_cache: K8sPodCache,
    pid_container_cache: Mutex<LruCache<i32, Option<String>>>,
    node_name: String,
}

impl K8sMetadataProvider {
    pub fn new(node_name: String) -> Result<Self, kube::Error> {
        let pod_cache = K8sPodCache::new(node_name.clone())?;

        Ok(Self {
            pod_cache,
            pid_container_cache: Mutex::new(LruCache::new(NonZeroUsize::new(10000).unwrap())),
            node_name,
        })
    }
}

impl TaskMetadataProvider for K8sMetadataProvider {
    fn get_metadata(
        &self,
        task_key: TaskKey,
    ) -> Result<Vec<MetadataLabel>, TaskMetadataProviderError> {
        let container_id = {
            let mut cache = self.pid_container_cache.lock().unwrap();
            if let Some(cached) = cache.get(&task_key.pid) {
                cached.clone()
            } else {
                let id = container_id_from_pid(task_key.pid);
                cache.push(task_key.pid, id.clone());
                id
            }
        };

        let container_id = match container_id {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        let pod_meta = match self.pod_cache.get_pod_metadata(&container_id) {
            Some(meta) => meta,
            None => {
                debug!(
                    "no pod metadata found for container {} (pid {})",
                    container_id, task_key.pid
                );
                return Ok(vec![]);
            }
        };

        let mut labels = vec![
            MetadataLabel::from_string_value("k8s.pod.name".into(), pod_meta.pod_name),
            MetadataLabel::from_string_value("k8s.namespace.name".into(), pod_meta.namespace),
            MetadataLabel::from_string_value("k8s.node.name".into(), self.node_name.clone()),
        ];

        if let Some(ref kind) = pod_meta.owner_kind {
            labels.push(MetadataLabel::from_string_value(
                "k8s.owner.kind".into(),
                kind.clone(),
            ));
        }

        if let Some(ref name) = pod_meta.owner_name {
            labels.push(MetadataLabel::from_string_value(
                "k8s.owner.name".into(),
                name.clone(),
            ));
        }

        Ok(labels)
    }
}
