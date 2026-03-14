use std::any::{Any, TypeId};

use lightswitch_metadata::types::{
    MetadataLabel, TaskKey, TaskMetadataProvider, TaskMetadataProviderError,
};
use tracing::debug;

use crate::cgroup::container_id_from_pid;
use crate::k8s_client::{K8sPodCache, PodMetadata};

pub struct K8sMetadataProvider {
    pod_cache: K8sPodCache,
    node_name: String,
}

impl K8sMetadataProvider {
    pub fn new(node_name: String) -> Result<Self, kube::Error> {
        let pod_cache = K8sPodCache::new(node_name.clone())?;

        Ok(Self {
            pod_cache,
            node_name,
        })
    }

    fn resolve_pod_metadata(&self, pid: i32) -> Option<PodMetadata> {
        let container_id = container_id_from_pid(pid)?;

        let meta = self.pod_cache.get_pod_metadata(&container_id);
        if meta.is_none() {
            debug!(
                "no pod metadata found for container {} (pid {})",
                container_id, pid
            );
        }
        meta
    }
}

impl TaskMetadataProvider for K8sMetadataProvider {
    fn get_metadata(
        &self,
        task_key: TaskKey,
    ) -> Result<Vec<MetadataLabel>, TaskMetadataProviderError> {
        let pod_meta = match self.resolve_pod_metadata(task_key.pid) {
            Some(meta) => meta,
            None => return Ok(vec![]),
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

    fn get_typed_metadata(
        &self,
        task_key: TaskKey,
        type_id: TypeId,
    ) -> Option<Box<dyn Any + Send>> {
        if type_id == TypeId::of::<PodMetadata>() {
            self.resolve_pod_metadata(task_key.pid)
                .map(|m| Box::new(m) as Box<dyn Any + Send>)
        } else {
            None
        }
    }
}
