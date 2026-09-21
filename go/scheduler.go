// Package flotilla runs thousands of independent Raft consensus groups on one
// set of physical nodes: a shared scheduler instead of a thread per group, and
// cross-group RPC batching instead of an RPC per group.
//
// The expensive parts of running thousands of Raft groups on a box aren't the
// Raft state machines - those are cheap. It's (1) a thread per group, and (2) a
// separate RPC for every group even when a hundred of them are talking to the
// same physical peer. This package is about avoiding both: one scheduler drives
// every local group replica in a single tick, and outbound messages to the same
// peer are coalesced into one envelope.
//
// The actual consensus (real elections, log replication, the current-term commit
// rule) is deliberately not reimplemented here - that lives in the sibling
// coracle project. flotilla is the layer that lets you run a fleet of them.
package flotilla

import "slices"

// GroupMessage is one inner message routed to one group on one node.
type GroupMessage struct {
	// Kind is the message type; only "leader" announcements are modelled here.
	Kind string
	// Group is the id of the consensus group the message belongs to.
	Group string
	// Term is the leadership term the message was emitted in.
	Term int
	// Leader is the id of the node claiming leadership.
	Leader string
}

// addressed is a destination-tagged message emitted by a replica.
type addressed struct {
	dst string
	msg GroupMessage
}

// GroupReplica is one node's replica of one consensus group.
//
// Election here is intentionally trivial - the node with the smallest id in the
// group is the leader - because the point of this repo is the scheduling and
// batching layer, not re-deriving Raft. Swap this state machine for a real
// coracle node and the scheduler/batcher are unchanged.
type GroupReplica struct {
	// GroupID is the id of the group this replica belongs to.
	GroupID string
	// LocalID is the id of the node hosting this replica.
	LocalID string
	// Peers holds every member id of the group, including LocalID.
	Peers []string
	// Role is "follower" or "leader".
	Role string
	// Leader is the id of the believed leader, or "" when none is known yet.
	Leader string
	// Term is the highest leadership term this replica has observed.
	Term int
}

// NewGroupReplica creates a follower replica of group groupID on node localID.
// The peers slice is copied so later mutation by the caller cannot affect it.
func NewGroupReplica(groupID, localID string, peers []string) *GroupReplica {
	cp := make([]string, len(peers))
	copy(cp, peers)
	return &GroupReplica{
		GroupID: groupID,
		LocalID: localID,
		Peers:   cp,
		Role:    "follower",
	}
}

// Bootstrap is called once. If this replica is the designated leader (the
// smallest-id member) it announces itself to every peer and returns the outbound
// messages; otherwise it returns nil.
func (r *GroupReplica) Bootstrap() []addressed {
	if r.LocalID == slices.Min(r.Peers) {
		r.Role = "leader"
		r.Leader = r.LocalID
		r.Term = 1
		out := make([]addressed, 0, len(r.Peers))
		for _, p := range r.Peers {
			if p != r.LocalID {
				out = append(out, addressed{
					dst: p,
					msg: GroupMessage{Kind: "leader", Group: r.GroupID, Term: 1, Leader: r.LocalID},
				})
			}
		}
		return out
	}
	return nil
}

// Handle applies an inbound message, adopting any leader whose term is at least
// as high as the one already seen. It never emits replies.
func (r *GroupReplica) Handle(msg GroupMessage) []addressed {
	if msg.Kind == "leader" && msg.Term >= r.Term {
		r.Term = msg.Term
		r.Leader = msg.Leader
		r.Role = "follower"
	}
	return nil
}

// Scheduler is one physical node's executor. It owns all of that node's group
// replicas and drives them without a thread per group.
type Scheduler struct {
	// NodeID is the id of the physical node this scheduler runs on.
	NodeID string
	// Replicas maps group id to the local replica of that group.
	Replicas map[string]*GroupReplica
	// EnvelopesSent counts the coalesced envelopes emitted so far.
	EnvelopesSent int
	// MessagesSent counts the logical inner messages emitted so far.
	MessagesSent int
}

// NewScheduler creates an empty scheduler for node nodeID.
func NewScheduler(nodeID string) *Scheduler {
	return &Scheduler{NodeID: nodeID, Replicas: make(map[string]*GroupReplica)}
}

// Add registers a replica under its group id.
func (s *Scheduler) Add(replica *GroupReplica) {
	s.Replicas[replica.GroupID] = replica
}

// Bootstrap bootstraps every local replica and returns the batched outbound
// envelopes keyed by destination peer.
func (s *Scheduler) Bootstrap() map[string][]GroupMessage {
	var outbound []addressed
	for _, r := range s.Replicas {
		outbound = append(outbound, r.Bootstrap()...)
	}
	return s.batch(outbound)
}

// batch coalesces per-group messages headed to the same peer into one envelope.
// 1000 groups each sending a heartbeat to peer X become a single envelope
// carrying 1000 inner messages, not 1000 RPCs.
func (s *Scheduler) batch(outbound []addressed) map[string][]GroupMessage {
	byDst := make(map[string][]GroupMessage)
	for _, a := range outbound {
		byDst[a.dst] = append(byDst[a.dst], a.msg)
	}
	s.MessagesSent += len(outbound)
	s.EnvelopesSent += len(byDst)
	return byDst
}

// Deliver unpacks an envelope and fans its inner messages out to the right group
// replicas, returning any replies (already re-batched).
func (s *Scheduler) Deliver(envelope []GroupMessage) map[string][]GroupMessage {
	var replies []addressed
	for _, msg := range envelope {
		if replica, ok := s.Replicas[msg.Group]; ok {
			replies = append(replies, replica.Handle(msg)...)
		}
	}
	return s.batch(replies)
}
