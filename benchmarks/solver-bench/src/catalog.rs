//! Static mirror of the bench cluster and workload templates.
//! Pools: benchmarks/manifests/capa-workload-cluster.yaml (order matches
//! harness config.py MACHINE_DEPLOYMENTS). Components: the affinity-pods and
//! topology-pods templates, shapes and replica fractions from workload.py.

#[derive(Debug, Clone, Copy)]
pub struct PoolSpec {
    pub name: &'static str,
    pub instance_type: &'static str,
    pub cpu: f64,
    pub memory: f64,
    pub max_size: i32,
    pub cost: f64,
    pub zone: Option<&'static str>,
}

pub const POOLS: [PoolSpec; 15] = [
    PoolSpec { name: "small", instance_type: "t3.medium", cpu: 2.0, memory: 4.0, max_size: 10, cost: 0.0416, zone: None },
    PoolSpec { name: "medium", instance_type: "m5.large", cpu: 2.0, memory: 8.0, max_size: 10, cost: 0.096, zone: None },
    PoolSpec { name: "large", instance_type: "m5.xlarge", cpu: 4.0, memory: 16.0, max_size: 10, cost: 0.192, zone: None },
    PoolSpec { name: "general-xl", instance_type: "m5.2xlarge", cpu: 8.0, memory: 32.0, max_size: 8, cost: 0.384, zone: None },
    PoolSpec { name: "general-4xl", instance_type: "m5.4xlarge", cpu: 16.0, memory: 64.0, max_size: 8, cost: 0.768, zone: None },
    PoolSpec { name: "compute-l", instance_type: "c5.xlarge", cpu: 4.0, memory: 8.0, max_size: 10, cost: 0.17, zone: None },
    PoolSpec { name: "compute-xl", instance_type: "c5.2xlarge", cpu: 8.0, memory: 16.0, max_size: 8, cost: 0.34, zone: None },
    PoolSpec { name: "compute-4xl", instance_type: "c5.4xlarge", cpu: 16.0, memory: 32.0, max_size: 4, cost: 0.68, zone: None },
    PoolSpec { name: "memory-m", instance_type: "r5.large", cpu: 2.0, memory: 16.0, max_size: 10, cost: 0.126, zone: None },
    PoolSpec { name: "memory-l", instance_type: "r5.xlarge", cpu: 4.0, memory: 32.0, max_size: 8, cost: 0.252, zone: None },
    // r5.2xlarge billed at its 3yr reserved rate, same as the cluster manifest
    PoolSpec { name: "memory-xl", instance_type: "r5.2xlarge", cpu: 8.0, memory: 64.0, max_size: 15, cost: 0.277, zone: None },
    PoolSpec { name: "memory-4xl", instance_type: "r5.4xlarge", cpu: 16.0, memory: 128.0, max_size: 8, cost: 1.008, zone: None },
    PoolSpec { name: "zone-a", instance_type: "m6i.large", cpu: 2.0, memory: 8.0, max_size: 15, cost: 0.096, zone: Some("us-east-1a") },
    PoolSpec { name: "zone-b", instance_type: "m6i.large", cpu: 2.0, memory: 8.0, max_size: 15, cost: 0.096, zone: Some("us-east-1b") },
    PoolSpec { name: "zone-c", instance_type: "m6i.large", cpu: 2.0, memory: 8.0, max_size: 15, cost: 0.096, zone: Some("us-east-1c") },
];

// {3,8,15} MDs can't cover the zone pools plus the c5/r5 and 16Gi pins, so the
// MD-count axis is {5,10,15}. Subsets keep catalog order for stable domains.
const SUBSET_5: [&str; 5] = ["compute-xl", "memory-xl", "zone-a", "zone-b", "zone-c"];
const SUBSET_10: [&str; 10] = [
    "small", "medium", "large", "general-xl", "compute-xl",
    "memory-m", "memory-xl", "zone-a", "zone-b", "zone-c",
];

pub fn pool_subset(md_count: usize) -> Vec<PoolSpec> {
    let keep: Vec<&str> = match md_count {
        5 => SUBSET_5.to_vec(),
        10 => SUBSET_10.to_vec(),
        15 => POOLS.iter().map(|p| p.name).collect(),
        _ => panic!("unsupported md_count {md_count}, expected 5, 10 or 15"),
    };
    POOLS.iter().filter(|p| keep.contains(&p.name)).copied().collect()
}

/// Node pin, matching the templates' required nodeAffinity terms.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Pin {
    None,
    /// node.kubernetes.io/instance-type=m6i.large (the zoned pools)
    M6i,
    /// m6i.large restricted to us-east-1a/b (the batch component)
    M6iZoneAb,
    /// (c5.xlarge|c5.2xlarge) or (r5.xlarge|r5.2xlarge), from topology-pods'
    /// affinity component (the c5 term's NotIn c5.4xlarge collapses to this)
    LightAffinity,
}

impl Pin {
    pub fn allows(&self, pool: &PoolSpec) -> bool {
        match self {
            Pin::None => true,
            Pin::M6i => pool.instance_type == "m6i.large",
            Pin::M6iZoneAb => matches!(pool.zone, Some("us-east-1a") | Some("us-east-1b")),
            Pin::LightAffinity => matches!(
                pool.instance_type,
                "c5.xlarge" | "c5.2xlarge" | "r5.xlarge" | "r5.2xlarge"
            ),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Component {
    pub name: &'static str,
    pub frac: f64,
    pub cpu: f64,
    pub memory: f64,
    pub pin: Pin,
}

// Template order; filler is the remainder so counts hit the total exactly.
pub const HEAVY_COMPONENTS: [Component; 8] = [
    Component { name: "web", frac: 0.20, cpu: 0.5, memory: 0.5, pin: Pin::M6i },
    Component { name: "cache", frac: 0.12, cpu: 0.5, memory: 0.5, pin: Pin::M6i },
    Component { name: "pair", frac: 0.10, cpu: 0.25, memory: 0.25, pin: Pin::M6i },
    Component { name: "batch", frac: 0.10, cpu: 0.5, memory: 1.0, pin: Pin::M6iZoneAb },
    Component { name: "db", frac: 0.06, cpu: 1.0, memory: 2.0, pin: Pin::M6i },
    Component { name: "api", frac: 0.12, cpu: 1.0, memory: 4.0, pin: Pin::None },
    Component { name: "memcache", frac: 0.20, cpu: 0.5, memory: 8.0, pin: Pin::None },
    Component { name: "standalone", frac: 0.06, cpu: 2.0, memory: 16.0, pin: Pin::None },
];
pub const HEAVY_FILLER: Component =
    Component { name: "filler", frac: 0.0, cpu: 0.5, memory: 0.5, pin: Pin::None };

pub const LIGHT_COMPONENTS: [Component; 3] = [
    Component { name: "zonespread", frac: 0.25, cpu: 0.5, memory: 0.5, pin: Pin::M6i },
    Component { name: "hostspread", frac: 0.15, cpu: 0.5, memory: 0.5, pin: Pin::M6i },
    Component { name: "affinity", frac: 0.20, cpu: 1.2, memory: 2.0, pin: Pin::LightAffinity },
];
pub const LIGHT_FILLER: Component =
    Component { name: "filler", frac: 0.0, cpu: 0.5, memory: 0.5, pin: Pin::None };

/// Python round(): ties to even on the binary double, same as workload.py.
pub fn py_round(x: f64) -> i32 {
    x.round_ties_even() as i32
}

/// (component, replicas) with filler absorbing the remainder, matching
/// workload.py's render context exactly. Panics where workload.py raises:
/// when the components alone round to more than total.
pub fn replica_split(components: &[Component], filler: Component, total: i32) -> Vec<(Component, i32)> {
    let mut out: Vec<(Component, i32)> = components
        .iter()
        .map(|c| (*c, py_round(total as f64 * c.frac)))
        .collect();
    let used: i32 = out.iter().map(|(_, n)| n).sum();
    assert!(used <= total, "components round to {used} replicas, more than the {total} asked for");
    out.push((filler, total - used));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml::Value;

    const CLUSTER_MANIFEST: &str = include_str!("../../manifests/capa-workload-cluster.yaml");
    const HARNESS_CONFIG: &str = include_str!("../../harness/benchmark/config.py");
    const MD_PREFIX: &str = "bench-cluster-workers-";

    const TEST_FILLER: Component =
        Component { name: "test-filler", frac: 0.0, cpu: 1.0, memory: 1.0, pin: Pin::None };

    fn test_components(fracs: &[f64]) -> Vec<Component> {
        const NAMES: [&str; 3] = ["test-component-1", "test-component-2", "test-component-3"];
        fracs.iter().zip(NAMES)
            .map(|(&frac, name)| Component { name, frac, cpu: 1.0, memory: 1.0, pin: Pin::None })
            .collect()
    }

    fn split(fracs: &[f64], total: i32) -> Vec<i32> {
        replica_split(&test_components(fracs), TEST_FILLER, total).iter().map(|(_, n)| *n).collect()
    }

    fn names(pools: &[PoolSpec]) -> Vec<&'static str> {
        pools.iter().map(|p| p.name).collect()
    }

    fn text<'a>(doc: &'a Value, path: &[&str]) -> &'a str {
        path.iter().fold(doc, |v, key| &v[*key]).as_str()
            .unwrap_or_else(|| panic!("no string at {path:?}"))
    }

    fn manifest_docs(kind: &str) -> Vec<Value> {
        CLUSTER_MANIFEST.split("\n---\n")
            .map(|doc| serde_yaml::from_str::<Value>(doc).unwrap())
            .filter(|doc| doc["kind"].as_str() == Some(kind))
            .collect()
    }

    #[test]
    fn pools_mirror_the_bench_cluster_manifest() {
        let mds: Vec<Value> = manifest_docs("MachineDeployment").into_iter()
            .filter(|md| text(md, &["metadata", "name"]).starts_with(MD_PREFIX))
            .collect();
        let templates = manifest_docs("AWSMachineTemplate");

        let md_pools: Vec<&str> = mds.iter().map(|md| &text(md, &["metadata", "name"])[MD_PREFIX.len()..]).collect();
        assert_eq!(md_pools, names(&POOLS), "pools must follow the manifest's MD order");

        for (pool, md) in POOLS.iter().zip(&mds) {
            let annotation = |key: &str| text(md, &["metadata", "annotations", key]);
            let memory = annotation("capacity.cluster-autoscaler.kubernetes.io/memory").strip_suffix("Gi").unwrap();
            assert_eq!(annotation("capacity.cluster-autoscaler.kubernetes.io/cpu").parse::<f64>().unwrap(), pool.cpu, "{}", pool.name);
            assert_eq!(memory.parse::<f64>().unwrap(), pool.memory, "{}", pool.name);
            assert_eq!(annotation("cluster.x-k8s.io/cluster-api-autoscaler-node-group-max-size").parse::<i32>().unwrap(), pool.max_size, "{}", pool.name);
            assert_eq!(annotation("wikoion.dev/cost-per-hour").parse::<f64>().unwrap(), pool.cost, "{}", pool.name);
            assert!(
                annotation("capacity.cluster-autoscaler.kubernetes.io/labels").split(',')
                    .any(|label| label == format!("node.kubernetes.io/instance-type={}", pool.instance_type)),
                "{} must declare its instance type label", pool.name
            );
            assert_eq!(md["metadata"]["labels"]["topology.kubernetes.io/zone"].as_str(), pool.zone, "{}", pool.name);

            // The zoned pools share one template, so check the type it boots
            let template_name = text(md, &["spec", "template", "spec", "infrastructureRef", "name"]);
            let template = templates.iter().find(|t| text(t, &["metadata", "name"]) == template_name).unwrap();
            assert_eq!(text(template, &["spec", "template", "spec", "instanceType"]), pool.instance_type, "{}", pool.name);
        }
    }

    #[test]
    fn pool_order_matches_the_harness_reset_list() {
        let reset_list: Vec<&str> = HARNESS_CONFIG
            .split_once("MACHINE_DEPLOYMENTS = [").unwrap().1
            .split_once(']').unwrap().0
            .lines()
            .filter_map(|line| line.trim().strip_prefix('"')?.split_once('"').map(|(name, _)| name))
            .collect();
        let expected: Vec<String> = POOLS.iter().map(|p| format!("{MD_PREFIX}{}", p.name)).collect();
        assert_eq!(reset_list, expected);
    }

    #[test]
    fn replica_split_rounds_the_product_like_python() {
        // Exact ties go to the even count, the filler takes what is left
        assert_eq!(split(&[0.25, 0.35, 0.05], 10), vec![2, 4, 0, 4]);
        // 0.07 * 150 is 10.500000000000002 and 0.41 * 150 is 61.49999999999999
        // as doubles, so Python gives 11 and 61, not the decimal 10 and 62
        assert_eq!(split(&[0.07, 0.41], 150), vec![11, 61, 78]);
        assert_eq!(split(&[], 9), vec![9], "no components leaves everything to the filler");
    }

    #[test]
    #[should_panic(expected = "components round to 8 replicas, more than the 7 asked for")]
    fn replica_split_refuses_to_overshoot_the_total() {
        split(&[0.5, 0.5], 7);
    }

    #[test]
    #[should_panic(expected = "components round to 10 replicas, more than the 9 asked for")]
    fn heavy_mix_cannot_make_9_pods() {
        replica_split(&HEAVY_COMPONENTS, HEAVY_FILLER, 9);
    }

    #[test]
    fn pins_admit_exactly_their_pools() {
        let admitted = |pin: Pin| -> Vec<&str> { POOLS.iter().filter(|p| pin.allows(p)).map(|p| p.name).collect() };
        assert_eq!(admitted(Pin::None), names(&POOLS));
        assert_eq!(admitted(Pin::M6i), vec!["zone-a", "zone-b", "zone-c"]);
        assert_eq!(admitted(Pin::M6iZoneAb), vec!["zone-a", "zone-b"]);
        assert_eq!(admitted(Pin::LightAffinity), vec!["compute-l", "compute-xl", "memory-l", "memory-xl"]);
    }

    #[test]
    fn subsets_keep_catalogue_order() {
        for (md_count, expected) in [(5, SUBSET_5.to_vec()), (10, SUBSET_10.to_vec()), (15, names(&POOLS))] {
            let subset = pool_subset(md_count);
            assert_eq!(names(&subset), expected);
            let positions: Vec<usize> = subset.iter()
                .map(|p| POOLS.iter().position(|q| q.name == p.name).unwrap())
                .collect();
            assert!(positions.is_sorted(), "md_count {md_count} is out of catalogue order: {positions:?}");
        }
    }

    #[test]
    #[should_panic(expected = "unsupported md_count 8")]
    fn subsets_only_exist_for_the_bench_md_counts() {
        pool_subset(8);
    }

    #[test]
    fn smallest_subset_still_fits_every_component() {
        let pools = pool_subset(5);
        let fits = |c: &Component| -> Vec<&str> {
            pools.iter().filter(|p| c.pin.allows(p) && c.cpu <= p.cpu && c.memory <= p.memory).map(|p| p.name).collect()
        };
        let zoned = vec!["zone-a", "zone-b", "zone-c"];
        let all = vec!["compute-xl", "memory-xl", "zone-a", "zone-b", "zone-c"];
        let expected: [(&str, Vec<&str>); 13] = [
            ("web", zoned.clone()),
            ("cache", zoned.clone()),
            ("pair", zoned.clone()),
            ("batch", vec!["zone-a", "zone-b"]),
            ("db", zoned.clone()),
            ("api", all.clone()),
            ("memcache", all.clone()),
            ("standalone", vec!["compute-xl", "memory-xl"]),
            ("filler", all.clone()),
            ("zonespread", zoned.clone()),
            ("hostspread", zoned.clone()),
            ("affinity", vec!["compute-xl", "memory-xl"]),
            ("filler", all.clone()),
        ];
        let components = HEAVY_COMPONENTS.iter().chain([&HEAVY_FILLER])
            .chain(LIGHT_COMPONENTS.iter()).chain([&LIGHT_FILLER]);
        for (c, (name, pools)) in components.zip(expected) {
            assert_eq!(c.name, name);
            assert_eq!(fits(c), pools, "{name}");
        }
    }
}
