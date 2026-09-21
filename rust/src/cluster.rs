//! A tiny in-memory cluster that wires schedulers together and pumps envelopes.
//!
//! Used by the tests and the benchmark to show a fleet of groups converging on
//! their leaders while the RPC count stays proportional to the number of peer
//! pairs, not the number of groups.

use std::collections::{HashMap, HashSet};

use crate::scheduler::{GroupMessage, GroupReplica, Scheduler};

/// Wires one scheduler per node together and pumps envelopes between them.
pub struct Cluster {
    /// Maps node id to that node's scheduler.
    pub schedulers: HashMap<String, Scheduler>,
    order: Vec<String>,
}

impl Cluster {
    /// Creates a cluster with one scheduler per node id, preserving order.
    pub fn new(node_ids: Vec<String>) -> Self {
        let mut schedulers = HashMap::with_capacity(node_ids.len());
        let mut order = Vec::with_capacity(node_ids.len());
        for n in node_ids {
            schedulers.insert(n.clone(), Scheduler::new(n.clone()));
            order.push(n);
        }
        Cluster { schedulers, order }
    }

    /// Adds a replica of `group_id` to every member node.
    pub fn add_group(&mut self, group_id: &str, members: Vec<String>) {
        for n in &members {
            self.schedulers
                .get_mut(n)
                .expect("member node must exist in the cluster")
                .add(GroupReplica::new(group_id, n.clone(), members.clone()));
        }
    }

    /// Bootstraps every node, then delivers the resulting envelopes.
    pub fn run_once(&mut self) {
        let mut pending: HashMap<String, Vec<GroupMessage>> = HashMap::new();
        for n in &self.order {
            let envelopes = self
                .schedulers
                .get_mut(n)
                .expect("scheduler for node must exist")
                .bootstrap();
            for (dst, envelope) in envelopes {
                pending.entry(dst).or_default().extend(envelope);
            }
        }
        for (dst, envelope) in pending {
            if let Some(sched) = self.schedulers.get_mut(&dst) {
                sched.deliver(envelope);
            }
        }
    }

    /// Total logical messages sent across every scheduler.
    pub fn total_messages(&self) -> i64 {
        self.schedulers.values().map(|s| s.messages_sent).sum()
    }

    /// Total envelopes sent across every scheduler.
    pub fn total_envelopes(&self) -> i64 {
        self.schedulers.values().map(|s| s.envelopes_sent).sum()
    }

    /// Maps each group id to the set of leaders its replicas believe in, which
    /// should have exactly one member once the cluster has converged.
    pub fn leaders(&self) -> HashMap<String, HashSet<String>> {
        let mut seen: HashMap<String, HashSet<String>> = HashMap::new();
        for sched in self.schedulers.values() {
            for (gid, r) in &sched.replicas {
                if let Some(leader) = &r.leader {
                    seen.entry(gid.clone()).or_default().insert(leader.clone());
                }
            }
        }
        seen
    }
}
