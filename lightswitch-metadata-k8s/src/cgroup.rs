use procfs::process::Process;
use tracing::debug;

pub fn container_id_from_pid(pid: i32) -> Option<String> {
    let process = Process::new(pid).ok()?;
    let cgroups = process.cgroups().ok()?;

    for cgroup in &cgroups {
        if let Some(id) = extract_container_id(&cgroup.pathname) {
            return Some(id);
        }
    }

    None
}

fn extract_container_id(pathname: &str) -> Option<String> {
    if !pathname.contains("kubepods") {
        return None;
    }

    let last_segment = pathname.rsplit('/').next()?;
    if last_segment.is_empty() {
        return None;
    }

    let id = strip_runtime_prefix(last_segment);
    let id = strip_scope_suffix(id);

    if id.len() == 64 && id.chars().all(|c| c.is_ascii_hexdigit()) {
        debug!("extracted container id {} from cgroup path {}", id, pathname);
        Some(id.to_string())
    } else {
        None
    }
}

fn strip_runtime_prefix(s: &str) -> &str {
    for prefix in &["cri-containerd-", "crio-", "docker-"] {
        if let Some(stripped) = s.strip_prefix(prefix) {
            return stripped;
        }
    }
    s
}

fn strip_scope_suffix(s: &str) -> &str {
    s.strip_suffix(".scope").unwrap_or(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_containerd_cgroupv2_besteffort() {
        let path = "/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-podafb4928c_2488_4273_8dec_0489dfc01bd8.slice/cri-containerd-9b162322bb77c24f112c8043f5f81cc48a14fc515f2f2526bebf76b4939b62e3.scope";
        let id = extract_container_id(path).unwrap();
        assert_eq!(
            id,
            "9b162322bb77c24f112c8043f5f81cc48a14fc515f2f2526bebf76b4939b62e3"
        );
    }

    #[test]
    fn test_containerd_cgroupv2_burstable() {
        let path = "/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod778e2ad0_833b_4bbc_a41a_fc8b3d0071a1.slice/cri-containerd-d52635a1fea37c22490ecca2f728cd954a25dbea3709b79b4c022ffc16233eb5.scope";
        let id = extract_container_id(path).unwrap();
        assert_eq!(
            id,
            "d52635a1fea37c22490ecca2f728cd954a25dbea3709b79b4c022ffc16233eb5"
        );
    }

    #[test]
    fn test_containerd_cgroupv1() {
        let path = "/kubepods/besteffort/podafb4928c_2488_4273_8dec_0489dfc01bd8/cri-containerd-859af4d85f8260df636f56171d691c192abec4b10528b4e5d3f7a010e0de80e0.scope";
        let id = extract_container_id(path).unwrap();
        assert_eq!(
            id,
            "859af4d85f8260df636f56171d691c192abec4b10528b4e5d3f7a010e0de80e0"
        );
    }

    #[test]
    fn test_crio() {
        let path = "/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod778e2ad0_833b_4bbc_a41a_fc8b3d0071a1.slice/crio-d52635a1fea37c22490ecca2f728cd954a25dbea3709b79b4c022ffc16233eb5.scope";
        let id = extract_container_id(path).unwrap();
        assert_eq!(
            id,
            "d52635a1fea37c22490ecca2f728cd954a25dbea3709b79b4c022ffc16233eb5"
        );
    }

    #[test]
    fn test_docker_bare_id() {
        let path = "/kubepods/besteffort/podafb4928c_2488_4273_8dec_0489dfc01bd8/9b162322bb77c24f112c8043f5f81cc48a14fc515f2f2526bebf76b4939b62e3";
        let id = extract_container_id(path).unwrap();
        assert_eq!(
            id,
            "9b162322bb77c24f112c8043f5f81cc48a14fc515f2f2526bebf76b4939b62e3"
        );
    }

    #[test]
    fn test_init_scope() {
        let path = "/init.scope";
        assert!(extract_container_id(path).is_none());
    }

    #[test]
    fn test_non_k8s_path() {
        let path = "/user.slice/user-1000.slice/session-1.scope";
        assert!(extract_container_id(path).is_none());
    }

    #[test]
    fn test_empty_path() {
        assert!(extract_container_id("").is_none());
    }

    #[test]
    fn test_invalid_hex() {
        let path = "/kubepods/besteffort/podafb4928c_2488_4273_8dec_0489dfc01bd8/not-a-valid-container-id";
        assert!(extract_container_id(path).is_none());
    }
}
