//! Builds solver instances mirroring the cluster benchmark's three profiles.
//! Constraint encoding follows autoscaler/src/optimiser.rs: hostname hard anti
//! becomes pairwise peer rules, every other hard term becomes an AffinityGroup,
//! spreads become TopologyGroups. Zone slots share a domain per zone label,
//! hostname gives every slot its own domain. Empty cluster: no existing slots,
//! baseline counts all zero.

use std::collections::HashMap;

use optimiser::{AffinityGroup, Domains, Enforcement, MachineDeployment, Pod, TopologyGroup, total_flat_slots};

use crate::catalog::{
    Component, HEAVY_COMPONENTS, HEAVY_FILLER, LIGHT_COMPONENTS, LIGHT_FILLER, PoolSpec,
    pool_subset, replica_split,
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Profile {
    Unconstrained,
    LightConstrained,
    HeavyConstrained,
}

impl Profile {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "unconstrained" => Some(Self::Unconstrained),
            "light-constrained" => Some(Self::LightConstrained),
            "heavy-constrained" => Some(Self::HeavyConstrained),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Unconstrained => "unconstrained",
            Self::LightConstrained => "light-constrained",
            Self::HeavyConstrained => "heavy-constrained",
        }
    }

    pub const ALL: [Profile; 3] = [Self::Unconstrained, Self::LightConstrained, Self::HeavyConstrained];
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SlotsLevel {
    Low,
    Med,
    High,
}

impl SlotsLevel {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "low" => Some(Self::Low),
            "med" => Some(Self::Med),
            "high" => Some(Self::High),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Med => "med",
            Self::High => "high",
        }
    }

    fn scale(&self, max_size: i32) -> i32 {
        match self {
            Self::High => max_size,
            Self::Med => (max_size + 1) / 2,
            Self::Low => (max_size + 3) / 4,
        }
    }

    pub const ALL: [SlotsLevel; 3] = [Self::Low, Self::Med, Self::High];
}

pub struct Instance {
    pub mds: Vec<MachineDeployment>,
    pub pods: Vec<Pod>,
    pub plugin_scores: Vec<f64>,
    pub allowed_matrix: Vec<Vec<Vec<bool>>>,
    pub topology_groups: Vec<TopologyGroup>,
    pub affinity_groups: Vec<AffinityGroup>,
    pub total_slots: usize,
    pub pools: Vec<PoolSpec>,
}

// Heavy components carrying hostname self anti-affinity (one per node)
const SELF_ANTI: [&str; 3] = ["web", "cache", "standalone"];

pub fn build(profile: Profile, pod_count: i32, md_count: usize, slots: SlotsLevel) -> Instance {
    let pools = pool_subset(md_count);

    let mds: Vec<MachineDeployment> = pools
        .iter()
        .map(|p| MachineDeployment {
            name: format!("bench-cluster-workers-{}", p.name),
            cpu: p.cpu,
            memory: p.memory,
            max_scale_out: slots.scale(p.max_size),
            existing_slots: Vec::new(),
            cost_per_node: Some(p.cost),
        })
        .collect();
    let total_slots = total_flat_slots(&mds);

    let split = match profile {
        Profile::LightConstrained => replica_split(&LIGHT_COMPONENTS, LIGHT_FILLER, pod_count),
        _ => replica_split(&HEAVY_COMPONENTS, HEAVY_FILLER, pod_count),
    };

    // One entry per pod, template order preserved
    let mut components: Vec<Component> = Vec::new();
    for (comp, n) in &split {
        for _ in 0..*n {
            components.push(*comp);
        }
    }

    let constrained = profile == Profile::HeavyConstrained;
    let mut pods: Vec<Pod> = components
        .iter()
        .map(|c| Pod {
            cpu: c.cpu,
            memory: c.memory,
            labels: HashMap::from([("bench-component".to_string(), c.name.to_string())]),
            ..Default::default()
        })
        .collect();

    if constrained {
        for comp in SELF_ANTI {
            let members = mask(&components, comp);
            for (i, in_group) in members.iter().enumerate() {
                if !in_group {
                    continue;
                }
                for (j, peer) in members.iter().enumerate() {
                    if *peer && j != i {
                        pods[i].hard_peers.push(j);
                        pods[i].hard_rules.push(-1);
                    }
                }
            }
        }
    }

    let zone = zone_domains(&mds, &pools);
    let hostname = Domains {
        slot_domains: (0..total_slots as i32).collect(),
        num_domains: total_slots as i32,
        baseline_counts: vec![0; total_slots],
    };

    let affinity_groups = if constrained {
        vec![
            // cache: zone affinity to web
            AffinityGroup {
                domains: zone.clone(),
                enforcement: Enforcement::Hard,
                colocate: true,
                pods_matching_term: mask(&components, "web"),
                pods_carrying_term: mask(&components, "cache"),
            },
            // pair: hostname affinity to web
            AffinityGroup {
                domains: hostname.clone(),
                enforcement: Enforcement::Hard,
                colocate: true,
                pods_matching_term: mask(&components, "web"),
                pods_carrying_term: mask(&components, "pair"),
            },
            // batch: zone anti-affinity to db
            AffinityGroup {
                domains: zone.clone(),
                enforcement: Enforcement::Hard,
                colocate: false,
                pods_matching_term: mask(&components, "db"),
                pods_carrying_term: mask(&components, "batch"),
            },
            // db: zone self affinity, all in one zone
            AffinityGroup {
                domains: zone.clone(),
                enforcement: Enforcement::Hard,
                colocate: true,
                pods_matching_term: mask(&components, "db"),
                pods_carrying_term: mask(&components, "db"),
            },
        ]
    } else {
        Vec::new()
    };

    let topology_groups = if profile == Profile::LightConstrained {
        vec![
            TopologyGroup {
                pods_in_group: mask(&components, "zonespread"),
                domains: zone.clone(),
                max_skew: 1,
                min_domains: 0,
                enforcement: Enforcement::Hard,
            },
            TopologyGroup {
                pods_in_group: mask(&components, "hostspread"),
                domains: hostname,
                max_skew: 1,
                min_domains: 0,
                enforcement: Enforcement::Hard,
            },
        ]
    } else {
        Vec::new()
    };

    // Uniform per (pod, md) on an empty cluster: template fit plus node pin
    let allowed_matrix: Vec<Vec<Vec<bool>>> = components
        .iter()
        .map(|c| {
            mds.iter()
                .zip(&pools)
                .map(|(md, pool)| {
                    let ok = c.cpu <= md.cpu && c.memory <= md.memory && c.pin.allows(pool);
                    vec![ok; md.total_slots()]
                })
                .collect()
        })
        .collect();

    let plugin_scores = vec![0.0; mds.len()];

    Instance {
        mds,
        pods,
        plugin_scores,
        allowed_matrix,
        topology_groups,
        affinity_groups,
        total_slots,
        pools,
    }
}

fn mask(components: &[Component], name: &str) -> Vec<bool> {
    components.iter().map(|c| c.name == name).collect()
}

// Zone label value order of first appearance, matching calculate_domain_indicies
fn zone_domains(mds: &[MachineDeployment], pools: &[PoolSpec]) -> Domains {
    let mut by_label: HashMap<&str, i32> = HashMap::new();
    let mut slot_domains: Vec<i32> = Vec::new();
    for (md, pool) in mds.iter().zip(pools) {
        let domain = match pool.zone {
            Some(zone) => {
                let next = by_label.len() as i32;
                *by_label.entry(zone).or_insert(next)
            }
            None => -1,
        };
        slot_domains.extend(std::iter::repeat_n(domain, md.total_slots()));
    }
    // Empty cluster, so nothing runs in any zone yet
    Domains { slot_domains, num_domains: by_label.len() as i32, baseline_counts: vec![0; by_label.len()] }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heavy_mid_size_instance_shapes_line_up() {
        // SUBSET_10 at Med: (max + 1) / 2 per pool, 5+5+5+4+4+5+8+8+8+8
        let inst = build(Profile::HeavyConstrained, 50, 10, SlotsLevel::Med);
        assert_eq!(inst.pods.len(), 50);
        assert_eq!(inst.total_slots, 60);
        assert_eq!(inst.allowed_matrix.len(), 50);
        for (i, row) in inst.allowed_matrix.iter().enumerate() {
            assert_eq!(row.iter().map(|s| s.len()).sum::<usize>(), 60, "pod {i}");
            assert!(row.iter().flatten().any(|b| *b), "pod {i} has no allowed slot");
        }

        assert_eq!(inst.affinity_groups.len(), 4);
        for g in &inst.affinity_groups {
            assert_eq!(g.domains.slot_domains.len(), 60);
            assert_eq!((g.pods_matching_term.len(), g.pods_carrying_term.len()), (50, 50));
        }
        // cache follows web by zone, pair by hostname
        assert_eq!(inst.affinity_groups[0].domains.num_domains, 3);
        assert_eq!(inst.affinity_groups[1].domains.num_domains, 60);

        // 50 pods split web 10, cache 6, pair 5, batch 5, db 3, api 6, ...
        let peers = |i: usize| inst.pods[i].hard_peers.len();
        assert_eq!(peers(0), 9, "web anti peers are the other 9 webs");
        assert_eq!(peers(10), 5, "cache anti peers are the other 5 caches");
        assert_eq!(peers(29), 0, "api carries no anti rule");
        assert!(inst.pods.iter().flat_map(|p| &p.hard_rules).all(|r| *r == -1));
    }

    #[test]
    fn unconstrained_has_no_terms_but_keeps_node_pins() {
        // SUBSET_5 at Low: (max + 3) / 4 per pool, 2+4+4+4+4
        let inst = build(Profile::Unconstrained, 25, 5, SlotsLevel::Low);
        assert_eq!(inst.total_slots, 18);
        assert!(inst.affinity_groups.is_empty());
        assert!(inst.topology_groups.is_empty());
        assert!(inst.pods.iter().all(|p| p.hard_peers.is_empty() && p.hard_rules.is_empty()));

        // Pod 0 is web, pinned to the m6i zone pools even with no constraints
        let allowed_pools: Vec<&str> = inst.allowed_matrix[0].iter()
            .zip(&inst.pools)
            .filter(|(slots, _)| slots.iter().any(|b| *b))
            .map(|(_, pool)| pool.name)
            .collect();
        assert_eq!(allowed_pools, vec!["zone-a", "zone-b", "zone-c"]);
    }

    #[test]
    fn light_zone_spread_only_counts_zoned_pools() {
        // All 15 pools at Low come to 42 slots, 4 for each zone pool
        let inst = build(Profile::LightConstrained, 100, 15, SlotsLevel::Low);
        assert_eq!(inst.total_slots, 42);

        let zone_group = &inst.topology_groups[0];
        assert_eq!(zone_group.domains.num_domains, 3);
        assert_eq!(zone_group.domains.slot_domains.len(), 42);
        for (domain, expected) in [(-1, 30), (0, 4), (1, 4), (2, 4)] {
            let n = zone_group.domains.slot_domains.iter().filter(|d| **d == domain).count();
            assert_eq!(n, expected, "slots in domain {domain}");
        }
        assert_eq!(zone_group.pods_in_group.iter().filter(|b| **b).count(), 25, "zonespread pods");

        let host_group = &inst.topology_groups[1];
        assert_eq!(host_group.domains.slot_domains, (0..42).collect::<Vec<i32>>());
        assert_eq!(host_group.pods_in_group.iter().filter(|b| **b).count(), 15, "hostspread pods");
    }
}
