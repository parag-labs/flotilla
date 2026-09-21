//! The multiplexing layer: many consensus groups sharing one loop and one wire.
//!
//! The expensive parts of running thousands of Raft groups on a box aren't the
//! Raft state machines - those are cheap. It's (1) a thread per group, and (2) a
//! separate RPC for every group even when a hundred of them are talking to the
//! same physical peer. This module is about avoiding both: one scheduler drives
//! every local group replica in a single tick, and outbound messages to the same
//! peer are coalesced into one envelope.

use std::collections::HashMap;

/// One inner message routed to one group on one node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupMessage {
    /// Message type; only `"leader"` announcements are modelled here.
    pub kind: String,
    /// Id of the consensus group the message belongs to.
    pub group: String,
    /// Leadership term the message was emitted in.
    pub term: i64,
    /// Id of the node claiming leadership.
    pub leader: String,
}

/// One node's replica of one consensus group.
///
/// Election here is intentionally trivial - the node with the smallest id in the
/// group is the leader - because the point of this repo is the scheduling and
/// batching layer, not re-deriving Raft. Swap this state machine for a real
/// `coracle` node and the scheduler/batcher are unchanged.
#[derive(Clone, Debug)]
pub struct GroupReplica {
    /// Id of the group this replica belongs to.
    pub group_id: String,
    /// Id of the node hosting this replica.
    pub local_id: String,
    /// Every member id of the group, including `local_id`.
    pub peers: Vec<String>,
    /// `"follower"` or `"leader"`.
    pub role: String,
    /// Believed leader, or `None` when none is known yet.
    pub leader: Option<String>,
    /// Highest leadership term this replica has observed.
    pub term: i64,
}

impl GroupReplica {
    /// Creates a follower replica of `group_id` on node `local_id`.
    pub fn new(
        group_id: impl Into<String>,
        local_id: impl Into<String>,
        peers: Vec<String>,
    ) -> Self {
        GroupReplica {
            group_id: group_id.into(),
            local_id: local_id.into(),
            peers,
            role: "follower".to_string(),
            leader: None,
            term: 0,
        }
    }

    /// Called once. If this replica is the designated leader (the smallest-id
    /// member) it announces itself to every peer and returns the outbound
    /// messages; otherwise it returns an empty vector.
    pub fn bootstrap(&mut self) -> Vec<(String, GroupMessage)> {
        if self.peers.iter().min() == Some(&self.local_id) {
            self.role = "leader".to_string();
            self.leader = Some(self.local_id.clone());
            self.term = 1;
            self.peers
                .iter()
                .filter(|p| **p != self.local_id)
                .map(|p| {
                    (
                        p.clone(),
                        GroupMessage {
                            kind: "leader".to_string(),
                            group: self.group_id.clone(),
                            term: 1,
                            leader: self.local_id.clone(),
                        },
                    )
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Applies an inbound message, adopting any leader whose term is at least as
    /// high as the one already seen. It never emits replies.
    pub fn handle(&mut self, msg: &GroupMessage) -> Vec<(String, GroupMessage)> {
        if msg.kind == "leader" && msg.term >= self.term {
            self.term = msg.term;
            self.leader = Some(msg.leader.clone());
            self.role = "follower".to_string();
        }
        Vec::new()
    }
}

/// One physical node's executor. Owns all of that node's group replicas and
/// drives them without a thread per group.
#[derive(Clone, Debug)]
pub struct Scheduler {
    /// Id of the physical node this scheduler runs on.
    pub node_id: String,
    /// Maps group id to the local replica of that group.
    pub replicas: HashMap<String, GroupReplica>,
    /// Count of coalesced envelopes emitted so far.
    pub envelopes_sent: i64,
    /// Count of logical inner messages emitted so far.
    pub messages_sent: i64,
}

impl Scheduler {
    /// Creates an empty scheduler for node `node_id`.
    pub fn new(node_id: impl Into<String>) -> Self {
        Scheduler {
            node_id: node_id.into(),
            replicas: HashMap::new(),
            envelopes_sent: 0,
            messages_sent: 0,
        }
    }

    /// Registers a replica under its group id.
    pub fn add(&mut self, replica: GroupReplica) {
        self.replicas.insert(replica.group_id.clone(), replica);
    }

    /// Bootstraps every local replica and returns the batched outbound envelopes
    /// keyed by destination peer.
    pub fn bootstrap(&mut self) -> HashMap<String, Vec<GroupMessage>> {
        let mut outbound: Vec<(String, GroupMessage)> = Vec::new();
        for r in self.replicas.values_mut() {
            outbound.extend(r.bootstrap());
        }
        self.batch(outbound)
    }

    /// Coalesces per-group messages headed to the same peer into one envelope.
    /// 1000 groups each sending a heartbeat to peer X become a single envelope
    /// carrying 1000 inner messages, not 1000 RPCs.
    fn batch(
        &mut self,
        outbound: Vec<(String, GroupMessage)>,
    ) -> HashMap<String, Vec<GroupMessage>> {
        let count = outbound.len();
        let mut by_dst: HashMap<String, Vec<GroupMessage>> = HashMap::new();
        for (dst, msg) in outbound {
            by_dst.entry(dst).or_default().push(msg);
        }
        self.messages_sent += count as i64;
        self.envelopes_sent += by_dst.len() as i64;
        by_dst
    }

    /// Unpacks an envelope and fans its inner messages out to the right group
    /// replicas, returning any replies (already re-batched).
    pub fn deliver(&mut self, envelope: Vec<GroupMessage>) -> HashMap<String, Vec<GroupMessage>> {
        let mut replies: Vec<(String, GroupMessage)> = Vec::new();
        for msg in &envelope {
            if let Some(replica) = self.replicas.get_mut(&msg.group) {
                replies.extend(replica.handle(msg));
            }
        }
        self.batch(replies)
    }
}
