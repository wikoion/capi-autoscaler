use std::collections::{HashSet};

use cp_sat::builder::{BoolVar, CpModelBuilder, LinearExpr};
use cp_sat::proto::CpSolverResponse;
use crate::{
    AffinityGroup,
    COST_SCALE,
    Enforcement,
    OBJECTIVE_SCALE,
    SOFT_AFFINITY_FACTOR,
    MachineDeployment,
    Placement,
    Pod,
    PodAssignment,
    TopologyGroup,
    flat_slot_index,
    plugin_slot_weight,
    scaled_capacity,
    scaled_request,
    unplaced_pod_penalty
};

pub struct Optimiser {
    pub mds: Vec<MachineDeployment>,
    pub pods: Vec<Pod>,
    pub allowed_matrix: Vec<Vec<Vec<bool>>>,
    pub model: CpModelBuilder,
    pub slot_counts: Vec<i32>,
    pub slots_used: Vec<Vec<BoolVar>>,
    pub pod_slot_assignment: Vec<Vec<Vec<BoolVar>>>,
    plugin_scores: Vec<f64>,
    new_slots_per_md: Vec<LinearExpr>,
    cpu_waste_weight: f64,
    memory_waste_weight: f64,
    allow_partial: bool,
}

impl Optimiser {
    #[allow(clippy::too_many_arguments)]
    pub fn init(
        mds: Vec<MachineDeployment>,
        pods: Vec<Pod>,
        plugin_scores: Vec<f64>,
        allowed_matrix: Vec<Vec<Vec<bool>>>,
        allocatable_fraction: f64,
        cpu_waste_weight: f64,
        memory_waste_weight: f64,
        allow_partial: bool,
    ) -> Self {
        let mut opt = Self::new(mds, pods, plugin_scores, allowed_matrix);
        opt.cpu_waste_weight = cpu_waste_weight;
        opt.memory_waste_weight = memory_waste_weight;
        opt.allow_partial = allow_partial;

        opt.pin_disallowed();
        opt.calculate_affinity();
        opt.add_pod_assignment_constraints();
        opt.add_resource_constraints(allocatable_fraction);
        opt
    }

    /// Warm-start the next solve from a placement, replacing any earlier hint.
    /// Hints aren't part of the model, so one built model serves every attempt.
    pub fn set_hint(&mut self, assignments: &[PodAssignment]) {
        self.model.del_hints();
        for (pod_idx, assignment) in assignments.iter().enumerate() {
            if assignment.is_placed() {
                let var = self.pod_slot_assignment[pod_idx][assignment.md as usize][assignment.slot as usize];
                self.model.add_hint(var, 1);
            }
        }
    }

    /// Read the placement back out of a solved response.
    pub fn read_solution(&self, response: &CpSolverResponse) -> Placement {
        let assignments = self.pod_slot_assignment
            .iter()
            .map(|pod_vars| {
                pod_vars.iter().enumerate()
                    .find_map(|(md_idx, md_vars)| {
                        md_vars.iter()
                            .position(|var| var.solution_value(response))
                            .map(|slot_idx| PodAssignment { md: md_idx as i32, slot: slot_idx as i32 })
                    })
                    .unwrap_or(PodAssignment::UNPLACED)
            })
            .collect();

        let slots_used = self.slots_used
            .iter()
            .map(|md_vars| md_vars.iter().map(|var| var.solution_value(response)).collect())
            .collect();

        Placement::new(assignments, slots_used)
    }

    // Two variable families drive the whole model: slots_used[md][slot] and
    // pod_slot_assignment[pod][md][slot]. Every constraint references these
    pub fn new(
        mds: Vec<MachineDeployment>,
        pods: Vec<Pod>,
        plugin_scores: Vec<f64>,
        allowed_matrix: Vec<Vec<Vec<bool>>>,
    ) -> Self {
        let mut model = CpModelBuilder::default();
        let num_pods = pods.len();
        let slot_counts: Vec<i32> = mds.iter().map(|md| md.total_slots() as i32).collect();

        let slots_used: Vec<Vec<BoolVar>> = mds
            .iter()
            .map(|md| {
                (0..md.total_slots())
                    .map(|_| model.new_bool_var())
                    .collect()
            })
            .collect();

        let pod_slot_assignment: Vec<Vec<Vec<BoolVar>>> = (0..num_pods)
            .map(|_| {
                mds.iter()
                    .map(|md| {
                        (0..md.total_slots())
                            .map(|_| model.new_bool_var())
                            .collect()
                    })
                    .collect()
            })
            .collect();

        let num_mds = mds.len();

        let new_slots_per_md: Vec<LinearExpr> = (0..num_mds)
            .map(|_| LinearExpr::from(0))
            .collect();

        Self {
            mds,
            pods,
            plugin_scores,
            allowed_matrix,
            model,
            slot_counts,
            slots_used,
            pod_slot_assignment,
            new_slots_per_md,
            cpu_waste_weight: 4.0,
            memory_waste_weight: 1.0,
            allow_partial: false,
        }
    }

    fn pin_disallowed(&mut self) {
        for (pod_idx, pod_assignments) in self.pod_slot_assignment.iter().enumerate() {
            for (md_idx, md_assignments) in pod_assignments.iter().enumerate() {
                for (slot_idx, var) in md_assignments.iter().enumerate() {
                    if !self.allowed_matrix[pod_idx][md_idx][slot_idx] {
                        self.model.add_eq(*var, 0);
                    }
                }
            }
        }
    }

    /// Provisioned cost over new slots only: new_slots_per_md only counts
    /// template slots (see add_resource_constraints), so filling existing
    /// nodes costs nothing. Per-node cost comes from MachineDeployment::node_cost.
    /// Must mirror solver::calculate_solution_cost, including COST_SCALE.
    pub fn calculate_provisioned_cost_objective(&self) -> LinearExpr {
        let mut total_cost = LinearExpr::from(0);

        for (md_idx, md) in self.mds.iter().enumerate() {
            let node_cost = (md.node_cost(self.cpu_waste_weight, self.memory_waste_weight) * COST_SCALE) as i64;
            total_cost += self.new_slots_per_md[md_idx].clone() * node_cost;
        }

        if self.allow_partial {
            // Charge every unplaced pod: penalty x (1 - sum of its assignment
            // vars). The per-pod constant offset is harmless to the argmin.
            let penalty = unplaced_pod_penalty(&self.mds, self.cpu_waste_weight, self.memory_waste_weight) as i64;
            for (pod_idx, pod_assignments) in self.pod_slot_assignment.iter().enumerate() {
                let mut placed = LinearExpr::from(0);
                for (md_idx, md_assignments) in pod_assignments.iter().enumerate() {
                    for (slot_idx, var) in md_assignments.iter().enumerate() {
                        if !self.allowed_matrix[pod_idx][md_idx][slot_idx] {
                            continue;
                        }
                        placed += *var;
                    }
                }
                total_cost += LinearExpr::from(1) * penalty - placed * penalty;
            }
        }

        total_cost
    }

    pub fn model_objective(
        &self, 
        soft_penalty: &LinearExpr, 
        topology_penalty: &LinearExpr, 
        affinity_penalty: &LinearExpr, 
        plugin_weight: f64
    ) -> LinearExpr {
        let provisioned_cost = self.calculate_provisioned_cost_objective();

        let mut plugin_penalty = LinearExpr::from(0);
        for md_idx in 0..self.mds.len() {
            let weight = plugin_slot_weight(self.plugin_scores[md_idx]);

            // Existing slots carry no plugin cost, using them must not be
            // penalised relative to leaving them empty.
            let existing = self.mds[md_idx].existing_slots.len();
            for &slot_used in &self.slots_used[md_idx][existing..] {
                plugin_penalty += slot_used * weight;
            }
        }

        let mut objective = LinearExpr::from(0);
        objective += provisioned_cost * OBJECTIVE_SCALE as i64;
        objective += plugin_penalty * (OBJECTIVE_SCALE * plugin_weight) as i64;
        objective += soft_penalty.clone() * SOFT_AFFINITY_FACTOR as i64;
        objective += topology_penalty.clone() * OBJECTIVE_SCALE as i64;
        objective += affinity_penalty.clone() * OBJECTIVE_SCALE as i64;

        objective
    }

    fn calculate_affinity(&mut self) {
        let mut affinity_seen: HashSet<(i32, i32)> = HashSet::new();

        for (pod_idx, pod) in self.pods.iter().enumerate() {
            for (peer, rule) in pod.hard_peers.iter().zip(pod.hard_rules.iter()) {
                // Only hard anti is pairwise; colocate goes through AffinityGroups.
                // Self-pairs are skipped, add_or([!v, !v]) would pin the pod to no
                // slot. affinity_seen dedups pairs, so one-directional declarations
                // (A lists B, B doesn't) still get modelled.
                if pod_idx == *peer || *rule != -1 {
                    continue;
                }

                let pair = (pod_idx.min(*peer) as i32, pod_idx.max(*peer) as i32);
                if !affinity_seen.insert(pair) { continue; }

                for md_idx in 0..self.mds.len() {
                    let pod_any = self.allowed_matrix[pod_idx][md_idx].iter().any(|&b| b);
                    let peer_any = self.allowed_matrix[*peer][md_idx].iter().any(|&b| b);
                    if !pod_any || !peer_any {
                        continue;
                    }

                    for slot_idx in 0..self.slot_counts[md_idx] {
                        // Anti-affinity: at most one of the pair per slot
                        self.model.add_or([!self.pod_slot_assignment[pod_idx][md_idx][slot_idx as usize], !self.pod_slot_assignment[*peer][md_idx][slot_idx as usize]]);
                    }
                }
            }
        }
    }

    // Each pod lands on exactly one allowed slot (at most one under allow_partial)
    fn add_pod_assignment_constraints(&mut self) {
      for (pod_idx, pod_assignments) in self.pod_slot_assignment.iter().enumerate() {
          let mut sum = LinearExpr::from(0);

          for (md_idx, md_assignments) in pod_assignments.iter().enumerate() {
              for (slot_idx, var) in md_assignments.iter().enumerate() {
                  if !self.allowed_matrix[pod_idx][md_idx][slot_idx] {
                      continue;
                  }
                  sum += *var;
              }
          }
          if self.allow_partial {
              // Best-effort: a pod may be left unplaced; the objective charges
              // unplaced_pod_penalty per dropped pod, so only genuinely
              // unplaceable pods end up unassigned.
              self.model.add_le(sum, 1);
          } else {
              self.model.add_eq(sum, 1);
          }
      }
    }
    
    // Per-slot cpu/memory limits, ties slots_used to whether any pod landed on
    // the slot, and caps new nodes per MD at max_scale_out
    fn add_resource_constraints(&mut self, allocatable_fraction: f64) {
        for (md_idx, md) in self.mds.iter().enumerate() {
            let existing = md.existing_slots.len();
            // Only new (template) slots: existing nodes are already provisioned,
            // so they don't count towards max_scale_out or the waste objective.
            let mut new_slots_used = LinearExpr::from(0);

            for (slot_idx, slot_used) in self.slots_used[md_idx].iter().enumerate() {
                // Symmetry breaking is only valid among interchangeable template
                // slots; existing slots have distinct capacities.
                if slot_idx >= existing && slot_idx + 1 < self.slots_used[md_idx].len() {
                    self.model.add_implication(
                        self.slots_used[md_idx][slot_idx + 1],
                        self.slots_used[md_idx][slot_idx]
                    );
                }

                let mut cpu_requested = LinearExpr::from(0);
                let mut memory_requested = LinearExpr::from(0);
                let mut pods_on_slot = LinearExpr::from(0);

                for (pod_idx, pod) in self.pods.iter().enumerate() {
                    if !self.allowed_matrix[pod_idx][md_idx][slot_idx] { continue; }
                    cpu_requested += self.pod_slot_assignment[pod_idx][md_idx][slot_idx] * scaled_request(pod.cpu);
                    memory_requested += self.pod_slot_assignment[pod_idx][md_idx][slot_idx] * scaled_request(pod.memory);
                    pods_on_slot += self.pod_slot_assignment[pod_idx][md_idx][slot_idx];
                }

                let len_pods = self.pods.len();
                let (slot_cpu, slot_memory) = md.slot_capacity(slot_idx, allocatable_fraction);
                self.model.add_le(cpu_requested.clone(), scaled_capacity(slot_cpu));
                self.model.add_le(memory_requested.clone(), scaled_capacity(slot_memory));
                self.model.add_ge(pods_on_slot.clone(), *slot_used);
                self.model.add_le(pods_on_slot.clone(), *slot_used * len_pods as i64);

                if slot_idx >= existing {
                    new_slots_used += *slot_used;
                }
            }
            self.model.add_le(new_slots_used.clone(), md.max_scale_out as i64);
            self.new_slots_per_md[md_idx] = new_slots_used.clone();
        }
    }

    /// Soft (preferred) affinity as objective penalties. Each pod pair is
    /// priced once with one shares_slot var, exactly as calculate_solution_cost
    /// prices it, and in the same check order so both pick the same weight.
    pub fn model_soft_weights(&mut self) -> LinearExpr {
        let mut priced_pairs: HashSet<(usize, usize)> = HashSet::new();
        let mut total_penalty = LinearExpr::from(0);

        for (pod_idx, pod) in self.pods.iter().enumerate() {
            for (&peer, &weight) in pod.soft_peers.iter().zip(&pod.soft_weights) {
                let penalty = (weight.abs() * 1000.0) as i64;
                // A zero penalty must not claim the pair, or it hides the peer's own weight
                if penalty == 0 { continue; }
                let pod_pair = (pod_idx.min(peer), pod_idx.max(peer));
                if !priced_pairs.insert(pod_pair) { continue; }

                // Only slots both pods may use can hold the pair together
                let shares_slot = self.model.new_bool_var();
                let mut pod_on_shared_slot = LinearExpr::from(0);
                for md_idx in 0..self.mds.len() {
                    for slot_idx in 0..self.slot_counts[md_idx] as usize {
                        if !self.allowed_matrix[pod_idx][md_idx][slot_idx]
                            || !self.allowed_matrix[peer][md_idx][slot_idx] { continue; }

                        let pod_here = self.pod_slot_assignment[pod_idx][md_idx][slot_idx];
                        let peer_here = self.pod_slot_assignment[peer][md_idx][slot_idx];

                        let cst1 = self.model.add_eq(pod_here, peer_here);
                        self.model.only_enforce_if(cst1, [shares_slot]);
                        let cst2 = self.model.add_or([!pod_here, !peer_here]);
                        self.model.only_enforce_if(cst2, [!shares_slot]);

                        pod_on_shared_slot += pod_here;
                    }
                }
                // Without this, a pod that's unplaced or on a slot the peer can't use
                // would count as sharing, since a == b holds on every shared slot
                let cst = self.model.add_ge(pod_on_shared_slot, 1);
                self.model.only_enforce_if(cst, [shares_slot]);

                let preference_broken = if weight > 0.0 { !shares_slot } else { shares_slot };
                total_penalty += preference_broken * penalty;
            }
        }
        total_penalty
    }

    // Models hard i.e. Required, non hostname topology keyed pod affinity and anti affinity for a given group
    // A group is one constraint applied to a pod over a domain i.e. topology
    fn model_hard_affinity_group(&mut self, group: &AffinityGroup) {
        let pods = &self.pods;
        let mds = &self.mds;
        let slots = &self.slot_counts;
        let model = &mut self.model;

	let domain_vars: Vec<Vec<(usize, BoolVar)>> = pods.iter().enumerate().flat_map(|(pod_idx, _)| {
	    mds.iter().enumerate().flat_map(move |(md_idx, _)| {
		(0..slots[md_idx]).map(move |slot_idx| {
		    (pod_idx, md_idx, slot_idx as usize)
		})
	    })
	})
	.fold(vec![Vec::new(); group.domains.num_domains as usize], |mut acc, (pod_idx, md_idx, slot_idx)|{
	    let flat_idx = flat_slot_index(&self.mds, md_idx, slot_idx);
	    let domain = group.domains.slot_domains[flat_idx];
	    let var = self.pod_slot_assignment[pod_idx][md_idx][slot_idx];

	    // Domain pre-filtering, 0 to prevent placement
	    if domain >= 0 {
		if group.pods_carrying_term[pod_idx] {
		    let has_peer = group.domains.baseline_counts[domain as usize] > 0;
		    if has_peer && !group.colocate {
			model.add_eq(var, 0);
		    }
		}

	      if group.pods_matching_term[pod_idx] {
		  acc[domain as usize].push((pod_idx, var));
	      }
	    } else if group.colocate && group.pods_carrying_term[pod_idx] {
		// Required affinity is unsatisfiable on a node without the
		// topology label, matching the kube-scheduler
		model.add_eq(var, 0);
	    }

	    acc
	});

	let total_baseline: i32 = group.domains.baseline_counts.iter().sum();
	let has_noncarrier_match = group.pods_matching_term.iter()
	    .zip(group.pods_carrying_term.iter())
	    .any(|(m, c)| *m && !*c);

	// Fresh self-affinity set, nothing running: kube binds one pod and the
	// rest join it, so the group converges on one domain. Model that directly.
	let self_affinity_set = group.colocate && total_baseline == 0 && !has_noncarrier_match;
	if self_affinity_set {
	    let colony: Vec<BoolVar> = (0..group.domains.num_domains).map(|_| model.new_bool_var()).collect();
	    for (d_idx, matches) in domain_vars.iter().enumerate() {
		for (_, var) in matches {
		    model.add_implication(*var, colony[d_idx]);
		}
	    }
	    let colony_sum: LinearExpr = colony.iter().copied().collect();
	    model.add_le(colony_sum, 1);
	}

	if group.colocate {
	    // As the kube-scheduler: a carrier's domain needs a running match or a
	    // pending match that binds independently of the term. Only a self-affinity
	    // set counts its own members, held to one domain by the colony vars above.
	    // One support flag per domain keeps the model small.
	    let domain_supported: Vec<BoolVar> = (0..group.domains.num_domains as usize)
		.map(|d_idx| {
		    let supported = model.new_bool_var();
		    let support_sum: LinearExpr = domain_vars[d_idx].iter()
			.filter(|(match_idx, _)| self_affinity_set || !group.pods_carrying_term[*match_idx])
			.map(|(_, match_in_domain)| *match_in_domain)
			.collect();
		    let cst = model.add_ge(support_sum, 1);
		    model.only_enforce_if(cst, [supported]);
		    supported
		})
		.collect();

	    pods.iter().enumerate()
		.filter(|(pod_idx, _)| group.pods_carrying_term[*pod_idx])
		.for_each(|(pod_idx, _)| {
		    mds.iter().enumerate().for_each(|(md_idx, _)| {
			(0..slots[md_idx]).for_each(|slot_idx| {
			    let flat_slot_idx = flat_slot_index(mds, md_idx, slot_idx as usize);
			    let domain = group.domains.slot_domains[flat_slot_idx];
			    if domain >= 0 && group.domains.baseline_counts[domain as usize] == 0 {
				let slot_var = self.pod_slot_assignment[pod_idx][md_idx][slot_idx as usize];
				model.add_implication(slot_var, domain_supported[domain as usize]);
			    }
			});
		    });
		});
	    return;
	}

	pods.iter().enumerate()
	    .filter(|(pod_idx, _)| group.pods_carrying_term[*pod_idx])
	    .for_each(|(pod_idx, _)| {
		(0..group.domains.num_domains).for_each(|d_idx| {
		    let pod_in_domain = model.new_bool_var();

		    mds.iter().enumerate().for_each(|(md_idx, _)| {
			    (0..slots[md_idx])
				.filter(|slot_idx| {
				    let flat_slot_idx = flat_slot_index(mds, md_idx, *slot_idx as usize);
				    group.domains.slot_domains[flat_slot_idx] == d_idx
				})
				.for_each(|slot_idx| {
				    let slot_var = self.pod_slot_assignment[pod_idx][md_idx][slot_idx as usize];
				    model.add_implication(slot_var, pod_in_domain);
				});
		    });

		    // Anti: no other matching pod may share the domain
		    let anti_sum: LinearExpr = domain_vars[d_idx as usize].iter()
			.filter(|(match_idx, _)| *match_idx != pod_idx)
			.map(|(_, match_in_domain)| *match_in_domain)
			.collect();

		    let cst = model.add_eq(anti_sum, 0);
		    model.only_enforce_if(cst, [pod_in_domain]);
		});
	    });
    }

    pub fn model_affinity_group_constraints(&mut self, affinity_groups: &[AffinityGroup]) -> LinearExpr {
        // Placeholder for soft affinity
        let total_penalty = LinearExpr::from(0);

	for group in affinity_groups {
            if group.enforcement == Enforcement::Hard {
		self.model_hard_affinity_group(group);
            }
        } 	
	total_penalty

    }

    /// Models topology spread constraints.
    /// A slot's flat index is: sum of slot_counts[0..md_idx] + slot_idx.
    /// `topology_group.domains.slot_domains[flat_idx]` gives the domain index for that slot.
    pub fn model_topology_constraints(&mut self, topology_groups: &[TopologyGroup]) -> LinearExpr {
        let mut total_penalty = LinearExpr::from(0);

        for group in topology_groups {
            let num_domains = group.domains.num_domains as usize;

            // Create a linear expr for each domain, starting at baseline_count (the number of currently
            // running pods in each domain)
            let mut domain_exprs: Vec<LinearExpr> = (0..num_domains)
                .map(|d| LinearExpr::from(group.domains.baseline_counts[d] as i64))
                .collect();

            // For each pod belonging to the topology group, iterate through every slot and add the
            // pod_slot_assignment value to domain_exprs constructed above. This means that when the
            // solver assigns a slot to a pod, the baseline counts are incremented.
            for (pod_idx, _pod) in self.pods.iter().enumerate() {
                if !group.pods_in_group[pod_idx] {
                    continue;
                }

                let mut flat_idx = 0;
                for (md_idx, _md) in self.mds.iter().enumerate() {
                    for slot_idx in 0..self.slot_counts[md_idx] as usize {
                        let domain = group.domains.slot_domains[flat_idx];
                        // Make sure to exclude -1 (MD slots that aren't participating)
                        if domain >= 0 {
                            domain_exprs[domain as usize] += self.pod_slot_assignment[pod_idx][md_idx][slot_idx];
                        }
                        flat_idx += 1;
                    }
                }
            }

            // Create int vars bounded by domain expressions
            let mut domain_vars = Vec::with_capacity(num_domains);
            let total_pods_in_group: i64 = self.pods.iter().enumerate()
                .filter(|(i, _)| group.pods_in_group[*i])
                .count() as i64;
            let max_count = total_pods_in_group + group.domains.baseline_counts.iter().max().copied().unwrap_or(0) as i64;

            // For each domain, create int var: v, add rule that v must = this domain's total
            // (domain_exprs[d] is incremented each time the solver places a pod in that zone)
            for d in 0..num_domains {
                let v = self.model.new_int_var([(0, max_count)]);
                self.model.add_eq(v, domain_exprs[d].clone());
                domain_vars.push(v);
            }

            // Same as above, we'll need min and max vals in the model for later
            let max_val = self.model.new_int_var([(0, max_count)]);
            let min_val = self.model.new_int_var([(0, max_count)]);
            // Max val holds the largest domain in domain_vars
            self.model.add_max_eq(max_val, domain_vars.iter().copied());
            // Min val holds the smallest domain in domain_vars
            self.model.add_min_eq(min_val, domain_vars.iter().copied());

            // Count domains with at least one pod (domain_used[d] = domain_vars[d] > 0)
            // The solver will set domain used provided it staisfies it's constaints
            let mut domains_used_expr = LinearExpr::from(0);
            for d in 0..num_domains {
                let domain_used = self.model.new_bool_var();
                // domain_used = 1 iff domain_vars[d] >= 1
                // If domain_used=1 then domain_vars[d] >= 1
                let cst1 = self.model.add_ge(domain_vars[d], 1);
                self.model.only_enforce_if(cst1, [domain_used]);
                // If domain_used=0 then domain_vars[d] == 0
                let cst2 = self.model.add_eq(domain_vars[d], 0);
                self.model.only_enforce_if(cst2, [!domain_used]);
                domains_used_expr += domain_used;
            }

            if group.enforcement == Enforcement::Hard {
		// Add the constraint: Max - min vars, created above, must be less than or = to max skew
                self.model.add_le(
                    LinearExpr::from(max_val) - LinearExpr::from(min_val),
                    group.max_skew as i64,
                );
		// If min_domains is set then enforce domains_used >= min_domains
                if group.min_domains > 0 {
                    self.model.add_ge(domains_used_expr, group.min_domains as i64);
                }
            } else {
                // Soft: add slack for skew violation
                // skew_slack >= (max - min) - max_skew, skew_slack >= 0
                let skew_slack = self.model.new_int_var([(0, max_count)]);
                self.model.add_ge(
                    LinearExpr::from(skew_slack),
                    LinearExpr::from(max_val) - LinearExpr::from(min_val) - group.max_skew as i64,
                );

                // domain_slack >= min_domains - domains_used, domain_slack >= 0
                let domain_slack = self.model.new_int_var([(0, num_domains as i64)]);
                if group.min_domains > 0 {
                    self.model.add_ge(
                        LinearExpr::from(domain_slack),
                        LinearExpr::from(group.min_domains as i64) - domains_used_expr,
                    );
                }

                // Penalty weight: high enough to matter but below waste scale
                let topology_penalty_weight: i64 = 5000;
                total_penalty += LinearExpr::from(skew_slack) * topology_penalty_weight;
                total_penalty += LinearExpr::from(domain_slack) * topology_penalty_weight;
            }
        }

        total_penalty
    }
}

