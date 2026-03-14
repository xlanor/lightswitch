use base64::Engine;
use flate2::Compression;
use flate2::write::GzEncoder;
use prost::Message;
use serde::Serialize;
use std::collections::HashMap;
use std::io::Write;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, span, Level};
use uuid::Uuid;

use crate::process::ObjectFileInfo;
use crate::process::ProcessInfo;
use crate::profile::raw_to_processed;
use crate::profile::AggregatedProfile;
use crate::profile::AggregatedSample;
use crate::profile::RawAggregatedProfile;
use crate::profile::{fold_profile, symbolize_profile, to_pprof};
use lightswitch_object::ExecutableId;

use lightswitch_metadata::metadata_provider::ThreadSafeGlobalMetadataProvider;
use lightswitch_metadata::types::TaskKey;
#[cfg(feature = "kubernetes")]
use lightswitch_metadata::types::{MetadataLabel, MetadataLabelValue};

pub trait Collector {
    fn collect(
        &mut self,
        profile: RawAggregatedProfile,
        procs: &HashMap<i32, ProcessInfo>,
        objs: &HashMap<ExecutableId, ObjectFileInfo>,
    );
    fn finish(
        &self,
    ) -> (
        AggregatedProfile,
        &HashMap<i32, ProcessInfo>,
        &HashMap<ExecutableId, ObjectFileInfo>,
    );
}

pub type ThreadSafeCollector = Arc<Mutex<Box<dyn Collector + Send>>>;

#[derive(Default)]
pub struct NullCollector {
    procs: HashMap<i32, ProcessInfo>,
    objs: HashMap<ExecutableId, ObjectFileInfo>,
}

impl NullCollector {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Discards the profile, useful for testing.
impl Collector for NullCollector {
    fn collect(
        &mut self,
        _profile: RawAggregatedProfile,
        _procs: &HashMap<i32, ProcessInfo>,
        _objs: &HashMap<ExecutableId, ObjectFileInfo>,
    ) {
    }

    fn finish(
        &self,
    ) -> (
        AggregatedProfile,
        &HashMap<i32, ProcessInfo>,
        &HashMap<ExecutableId, ObjectFileInfo>,
    ) {
        (AggregatedProfile::new(), &self.procs, &self.objs)
    }
}

#[derive(Default)]
pub struct StreamingCollector {
    token: Option<String>,
    local_symbolizer: bool,
    pprof_ingest_url: String,
    http_client_timeout: Duration,
    profile_duration: Duration,
    profile_frequency_hz: u64,
    procs: HashMap<i32, ProcessInfo>,
    objs: HashMap<ExecutableId, ObjectFileInfo>,
    metadata_provider: ThreadSafeGlobalMetadataProvider,
}

impl StreamingCollector {
    pub fn new(
        token: Option<String>,
        local_symbolizer: bool,
        pprof_ingest_url: &str,
        ingest_path: &str,
        profile_duration: Duration,
        profile_frequency_hz: u64,
        metadata_provider: ThreadSafeGlobalMetadataProvider,
    ) -> Self {
        Self {
            token,
            local_symbolizer,
            pprof_ingest_url: format!("{pprof_ingest_url}{ingest_path}"),
            http_client_timeout: Duration::from_secs(30),
            profile_duration,
            profile_frequency_hz,
            metadata_provider,
            ..Default::default()
        }
    }
}

/// POSTs the pprof formatted profiles to the given url.
impl Collector for StreamingCollector {
    fn collect(
        &mut self,
        profile: RawAggregatedProfile,
        procs: &HashMap<i32, ProcessInfo>,
        objs: &HashMap<ExecutableId, ObjectFileInfo>,
    ) {
        let _span = span!(Level::DEBUG, "StreamingCollector.finish").entered();

        let mut profile = raw_to_processed(&profile, procs, objs);
        if self.local_symbolizer {
            profile = symbolize_profile(&profile, procs, objs);
        }

        let pprof_profile = to_pprof(
            profile,
            procs,
            objs,
            &self.metadata_provider,
            self.profile_duration,
            self.profile_frequency_hz,
        );

        let client_builder = reqwest::blocking::Client::builder().timeout(self.http_client_timeout);
        let client = client_builder.build().unwrap();
        let mut request = client
            .post(self.pprof_ingest_url.clone())
            .body(pprof_profile.encode_to_vec());
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request.send();
        tracing::debug!("http response: {:?}", response);
    }

    fn finish(
        &self,
    ) -> (
        AggregatedProfile,
        &HashMap<i32, ProcessInfo>,
        &HashMap<ExecutableId, ObjectFileInfo>,
    ) {
        (AggregatedProfile::new(), &self.procs, &self.objs)
    }
}

#[derive(Default)]
pub struct PyroscopeCollector {
    local_symbolizer: bool,
    push_url: String,
    service_name: String,
    node_name: Option<String>,
    client: reqwest::blocking::Client,
    tenant_id: Option<String>,
    profile_duration: Duration,
    profile_frequency_hz: u64,
    procs: HashMap<i32, ProcessInfo>,
    objs: HashMap<ExecutableId, ObjectFileInfo>,
    metadata_provider: ThreadSafeGlobalMetadataProvider,
}

impl PyroscopeCollector {
    pub fn new(
        local_symbolizer: bool,
        server_url: &str,
        service_name: &str,
        node_name: Option<String>,
        tenant_id: Option<String>,
        profile_duration: Duration,
        profile_frequency_hz: u64,
        metadata_provider: ThreadSafeGlobalMetadataProvider,
    ) -> Self {
        let push_url = format!("{}/push.v1.PusherService/Push", server_url);
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap();
        Self {
            local_symbolizer,
            push_url,
            service_name: service_name.to_string(),
            node_name,
            client,
            tenant_id,
            profile_duration,
            profile_frequency_hz,
            metadata_provider,
            ..Default::default()
        }
    }
}

#[derive(Serialize)]
struct PyroscopePushRequest {
    series: Vec<PyroscopeSeries>,
}

#[derive(Serialize)]
struct PyroscopeSeries {
    labels: Vec<PyroscopeLabel>,
    samples: Vec<PyroscopeSample>,
}

#[derive(Serialize)]
struct PyroscopeLabel {
    name: String,
    value: String,
}

#[derive(Serialize)]
struct PyroscopeSample {
    #[serde(rename = "ID")]
    id: String,
    #[serde(rename = "rawProfile")]
    raw_profile: String,
}

impl Collector for PyroscopeCollector {
    fn collect(
        &mut self,
        profile: RawAggregatedProfile,
        procs: &HashMap<i32, ProcessInfo>,
        objs: &HashMap<ExecutableId, ObjectFileInfo>,
    ) {
        let _span = span!(Level::DEBUG, "PyroscopeCollector.collect").entered();

        let mut profile = raw_to_processed(&profile, procs, objs);
        if self.local_symbolizer {
            profile = symbolize_profile(&profile, procs, objs);
        }

        let pprof_profile = to_pprof(
            profile,
            procs,
            objs,
            &self.metadata_provider,
            self.profile_duration,
            self.profile_frequency_hz,
        );

        let mut gz_encoder = GzEncoder::new(Vec::new(), Compression::default());
        gz_encoder
            .write_all(&pprof_profile.encode_to_vec())
            .unwrap();
        let gzipped_profile = gz_encoder.finish().unwrap();
        let base64_profile = base64::engine::general_purpose::STANDARD.encode(gzipped_profile);

        let mut labels = vec![
            PyroscopeLabel {
                name: "__name__".to_string(),
                value: "process_cpu".to_string(),
            },
            PyroscopeLabel {
                name: "service_name".to_string(),
                value: self.service_name.clone(),
            },
        ];
        if let Some(node_name) = &self.node_name {
            labels.push(PyroscopeLabel {
                name: "node".to_string(),
                value: node_name.clone(),
            });
        }

        let payload = PyroscopePushRequest {
            series: vec![PyroscopeSeries {
                labels,
                samples: vec![PyroscopeSample {
                    id: Uuid::new_v4().to_string().to_uppercase(),
                    raw_profile: base64_profile,
                }],
            }],
        };

        let mut request = self.client.post(self.push_url.clone()).json(&payload);
        if let Some(tenant_id) = &self.tenant_id {
            request = request.header("X-Scope-OrgID", tenant_id.clone());
        }
        let response = request.send();
        tracing::info!("pyroscope http response: {:?}", response);
    }

    fn finish(
        &self,
    ) -> (
        AggregatedProfile,
        &HashMap<i32, ProcessInfo>,
        &HashMap<ExecutableId, ObjectFileInfo>,
    ) {
        (AggregatedProfile::new(), &self.procs, &self.objs)
    }
}

#[derive(Default)]
pub struct AggregatorCollector {
    profiles: Vec<AggregatedProfile>,
    procs: HashMap<i32, ProcessInfo>,
    objs: HashMap<ExecutableId, ObjectFileInfo>,
}

impl AggregatorCollector {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Aggregates the samples in memory, which might be acceptable when profiling
/// for short amounts of time.
impl Collector for AggregatorCollector {
    fn collect(
        &mut self,
        raw_profile: RawAggregatedProfile,
        procs: &HashMap<i32, ProcessInfo>,
        objs: &HashMap<ExecutableId, ObjectFileInfo>,
    ) {
        self.profiles
            .push(raw_to_processed(&raw_profile, procs, objs));

        for (k, v) in procs {
            self.procs.insert(*k, v.clone());
        }

        for (object_id, object_file_info) in objs {
            self.objs.insert(*object_id, object_file_info.clone());
        }
    }

    fn finish(
        &self,
    ) -> (
        AggregatedProfile,
        &HashMap<i32, ProcessInfo>,
        &HashMap<ExecutableId, ObjectFileInfo>,
    ) {
        let _span = span!(Level::DEBUG, "AggregatorCollector.finish").entered();

        let mut samples_count = HashMap::new();
        for profile in &self.profiles {
            for sample in profile {
                let sample_without_count = AggregatedSample {
                    count: 0,
                    ustack: sample.ustack.clone(),
                    kstack: sample.kstack.clone(),
                    ..*sample
                };
                *samples_count.entry(sample_without_count).or_insert(0) += sample.count
            }
        }

        debug!("found {} unique samples", samples_count.len());
        let profile = samples_count
            .iter()
            .map(|(sample, count)| AggregatedSample {
                count: *count,
                ustack: sample.ustack.clone(),
                kstack: sample.kstack.clone(),
                ..*sample
            })
            .collect();

        (profile, &self.procs, &self.objs)
    }
}

pub struct LiveCollector {
    tx: Sender<String>,
    procs: HashMap<i32, ProcessInfo>,
    objs: HashMap<ExecutableId, ObjectFileInfo>,
}

impl LiveCollector {
    pub fn new(tx: Sender<String>) -> Self {
        Self {
            tx,
            procs: HashMap::new(),
            objs: HashMap::new(),
        }
    }
}

impl Collector for LiveCollector {
    fn collect(
        &mut self,
        raw_profile: RawAggregatedProfile,
        procs: &HashMap<i32, ProcessInfo>,
        objs: &HashMap<ExecutableId, ObjectFileInfo>,
    ) {
        let _span = span!(Level::DEBUG, "LiveCollector.collect").entered();

        let profile = raw_to_processed(&raw_profile, procs, objs);
        let profile = symbolize_profile(&profile, procs, objs);
        let folded = fold_profile(profile, true);

        if !folded.trim().is_empty() {
            let _ = self.tx.send(folded);
        }
    }

    fn finish(
        &self,
    ) -> (
        AggregatedProfile,
        &HashMap<i32, ProcessInfo>,
        &HashMap<ExecutableId, ObjectFileInfo>,
    ) {
        (AggregatedProfile::new(), &self.procs, &self.objs)
    }
}

#[cfg(feature = "kubernetes")]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PodLabels {
    pub namespace: String,
    pub pod_name: String,
    pub node_name: Option<String>,
    pub owner_kind: Option<String>,
    pub owner_name: Option<String>,
}

#[cfg(feature = "kubernetes")]
pub fn extract_pod_labels(labels: &[MetadataLabel]) -> Option<PodLabels> {
    let mut namespace = None;
    let mut pod_name = None;
    let mut node_name = None;
    let mut owner_kind = None;
    let mut owner_name = None;

    for label in labels {
        let value = match &label.value {
            MetadataLabelValue::String(s) => s.clone(),
            _ => continue,
        };
        match label.key.as_str() {
            "k8s.namespace.name" => namespace = Some(value),
            "k8s.pod.name" => pod_name = Some(value),
            "k8s.node.name" => node_name = Some(value),
            "k8s.owner.kind" => owner_kind = Some(value),
            "k8s.owner.name" => owner_name = Some(value),
            _ => {}
        }
    }

    Some(PodLabels {
        namespace: namespace?,
        pod_name: pod_name?,
        node_name,
        owner_kind,
        owner_name,
    })
}

#[cfg(feature = "kubernetes")]
pub struct K8sCollector {
    metadata_provider: ThreadSafeGlobalMetadataProvider,
    collector_factory: Box<dyn Fn(Option<PodLabels>) -> Box<dyn Collector + Send> + Send>,
    pod_collectors: HashMap<(String, String), Box<dyn Collector + Send>>,
    host_collector: Box<dyn Collector + Send>,
    pid_cache: HashMap<i32, Option<PodLabels>>,
    procs: HashMap<i32, ProcessInfo>,
    objs: HashMap<ExecutableId, ObjectFileInfo>,
}

#[cfg(feature = "kubernetes")]
impl K8sCollector {
    pub fn new(
        metadata_provider: ThreadSafeGlobalMetadataProvider,
        collector_factory: Box<dyn Fn(Option<PodLabels>) -> Box<dyn Collector + Send> + Send>,
    ) -> Self {
        let host_collector = collector_factory(None);
        Self {
            metadata_provider,
            collector_factory,
            pod_collectors: HashMap::new(),
            host_collector,
            pid_cache: HashMap::new(),
            procs: HashMap::new(),
            objs: HashMap::new(),
        }
    }

    fn resolve_pid(&mut self, pid: i32, tid: i32) -> Option<PodLabels> {
        if let Some(cached) = self.pid_cache.get(&pid) {
            return cached.clone();
        }
        let labels = self
            .metadata_provider
            .lock()
            .unwrap()
            .get_metadata(TaskKey { pid, tid });
        let pod_labels = extract_pod_labels(&labels);
        self.pid_cache.insert(pid, pod_labels.clone());
        pod_labels
    }
}

#[cfg(feature = "kubernetes")]
impl Collector for K8sCollector {
    fn collect(
        &mut self,
        profile: RawAggregatedProfile,
        procs: &HashMap<i32, ProcessInfo>,
        objs: &HashMap<ExecutableId, ObjectFileInfo>,
    ) {
        let _span = span!(Level::DEBUG, "K8sCollector.collect").entered();

        let mut grouped: HashMap<Option<(String, String)>, RawAggregatedProfile> = HashMap::new();
        let mut unique_pids: HashMap<i32, Option<(String, String)>> = HashMap::new();
        for sample in &profile {
            unique_pids.entry(sample.sample.pid).or_insert(None);
        }
        let total_samples = profile.len();
        for sample in profile {
            let pod_labels = self.resolve_pid(sample.sample.pid, sample.sample.tid);
            let key = pod_labels.map(|pl| (pl.namespace.clone(), pl.pod_name.clone()));
            unique_pids.insert(sample.sample.pid, key.clone());
            grouped.entry(key).or_default().push(sample);
        }

        let pod_groups: Vec<_> = grouped
            .keys()
            .filter_map(|k| k.as_ref().map(|(ns, pod)| format!("{ns}/{pod}")))
            .collect();
        let host_count = grouped.get(&None).map_or(0, |s| s.len());
        let unique_pid_list: Vec<_> = unique_pids
            .iter()
            .map(|(pid, key)| {
                let label = match key {
                    Some((ns, pod)) => format!("{ns}/{pod}"),
                    None => "host".to_string(),
                };
                format!("{pid}={label}")
            })
            .collect();
        tracing::info!(
            "K8sCollector: {} total samples, {} unique pids, {} pod groups ({:?}), {} host samples, pids: {:?}",
            total_samples,
            unique_pids.len(),
            pod_groups.len(),
            pod_groups,
            host_count,
            unique_pid_list,
        );

        for (key, samples) in grouped {
            match key {
                None => {
                    self.host_collector.collect(samples, procs, objs);
                }
                Some(ref ns_pod) => {
                    let collector =
                        self.pod_collectors
                            .entry(ns_pod.clone())
                            .or_insert_with(|| {
                                let pid_labels = {
                                    let first_sample = &samples[0];
                                    self.pid_cache
                                        .get(&first_sample.sample.pid)
                                        .cloned()
                                        .flatten()
                                };
                                (self.collector_factory)(pid_labels)
                            });
                    collector.collect(samples, procs, objs);
                }
            }
        }
    }

    fn finish(
        &self,
    ) -> (
        AggregatedProfile,
        &HashMap<i32, ProcessInfo>,
        &HashMap<ExecutableId, ObjectFileInfo>,
    ) {
        (AggregatedProfile::new(), &self.procs, &self.objs)
    }
}
