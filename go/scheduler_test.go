package flotilla

import "testing"

func TestNewGroupReplicaStartsAsFollower(t *testing.T) {
	r := NewGroupReplica("g1", "n2", []string{"n1", "n2", "n3"})
	if r.Role != "follower" {
		t.Fatalf("role = %q, want follower", r.Role)
	}
	if r.Leader != "" {
		t.Fatalf("leader = %q, want empty", r.Leader)
	}
	if r.Term != 0 {
		t.Fatalf("term = %d, want 0", r.Term)
	}
}

func TestNewGroupReplicaCopiesPeers(t *testing.T) {
	peers := []string{"n1", "n2"}
	r := NewGroupReplica("g1", "n1", peers)
	peers[0] = "mutated"
	if r.Peers[0] != "n1" {
		t.Fatalf("peers[0] = %q, want n1 (should be an independent copy)", r.Peers[0])
	}
}

func TestBootstrapLeaderAnnouncesToEveryPeer(t *testing.T) {
	r := NewGroupReplica("g1", "n1", []string{"n1", "n2", "n3"})
	out := r.Bootstrap()
	if len(out) != 2 {
		t.Fatalf("outbound = %d messages, want 2", len(out))
	}
	if r.Role != "leader" || r.Leader != "n1" || r.Term != 1 {
		t.Fatalf("after bootstrap got role=%q leader=%q term=%d", r.Role, r.Leader, r.Term)
	}
	dsts := map[string]bool{}
	for _, a := range out {
		dsts[a.dst] = true
		if a.msg.Kind != "leader" || a.msg.Group != "g1" || a.msg.Term != 1 || a.msg.Leader != "n1" {
			t.Fatalf("unexpected inner message %+v", a.msg)
		}
	}
	if !dsts["n2"] || !dsts["n3"] || dsts["n1"] {
		t.Fatalf("destinations = %v, want {n2,n3} and never self", dsts)
	}
}

func TestBootstrapNonLeaderStaysSilent(t *testing.T) {
	r := NewGroupReplica("g1", "n2", []string{"n1", "n2", "n3"})
	out := r.Bootstrap()
	if len(out) != 0 {
		t.Fatalf("outbound = %d, want 0 for a non-leader", len(out))
	}
	if r.Role != "follower" || r.Leader != "" {
		t.Fatalf("non-leader mutated state: role=%q leader=%q", r.Role, r.Leader)
	}
}

func TestBootstrapLeaderIsLexicographicMin(t *testing.T) {
	// "n10" sorts before "n2" lexicographically, mirroring Python's min() on
	// strings, so the smallest-*string* id wins rather than the smallest number.
	r := NewGroupReplica("g1", "n10", []string{"n2", "n10", "n3"})
	out := r.Bootstrap()
	if r.Role != "leader" {
		t.Fatalf("expected n10 to win as lexicographic min, role=%q", r.Role)
	}
	if len(out) != 2 {
		t.Fatalf("outbound = %d, want 2", len(out))
	}
}

func TestHandleAdoptsLeaderAtEqualOrHigherTerm(t *testing.T) {
	r := NewGroupReplica("g1", "n2", []string{"n1", "n2", "n3"})
	replies := r.Handle(GroupMessage{Kind: "leader", Group: "g1", Term: 1, Leader: "n1"})
	if replies != nil {
		t.Fatalf("handle should never reply, got %v", replies)
	}
	if r.Leader != "n1" || r.Term != 1 || r.Role != "follower" {
		t.Fatalf("did not adopt leader: leader=%q term=%d role=%q", r.Leader, r.Term, r.Role)
	}
	// Equal term but a different leader is still adopted (>= comparison).
	r.Handle(GroupMessage{Kind: "leader", Group: "g1", Term: 1, Leader: "nX"})
	if r.Leader != "nX" {
		t.Fatalf("equal-term leader not adopted: leader=%q", r.Leader)
	}
}

func TestHandleIgnoresStaleTermAndNonLeaderKind(t *testing.T) {
	r := NewGroupReplica("g1", "n2", []string{"n1", "n2", "n3"})
	r.Handle(GroupMessage{Kind: "leader", Group: "g1", Term: 5, Leader: "n1"})
	r.Handle(GroupMessage{Kind: "leader", Group: "g1", Term: 4, Leader: "old"})
	if r.Leader != "n1" || r.Term != 5 {
		t.Fatalf("stale term should be ignored: leader=%q term=%d", r.Leader, r.Term)
	}
	r.Handle(GroupMessage{Kind: "heartbeat", Group: "g1", Term: 9, Leader: "other"})
	if r.Leader != "n1" || r.Term != 5 {
		t.Fatalf("non-leader kind should be ignored: leader=%q term=%d", r.Leader, r.Term)
	}
}

func TestSchedulerBatchCoalescesByDestination(t *testing.T) {
	s := NewScheduler("n1")
	s.Add(NewGroupReplica("g0", "n1", []string{"n1", "n2", "n3"}))
	s.Add(NewGroupReplica("g1", "n1", []string{"n1", "n2", "n3"}))
	env := s.Bootstrap()
	if len(env) != 2 {
		t.Fatalf("envelopes = %d, want 2 (one per peer)", len(env))
	}
	if len(env["n2"]) != 2 || len(env["n3"]) != 2 {
		t.Fatalf("each envelope should carry 2 inner messages, got n2=%d n3=%d", len(env["n2"]), len(env["n3"]))
	}
	if s.MessagesSent != 4 {
		t.Fatalf("messagesSent = %d, want 4", s.MessagesSent)
	}
	if s.EnvelopesSent != 2 {
		t.Fatalf("envelopesSent = %d, want 2", s.EnvelopesSent)
	}
}

func TestSchedulerDeliverIgnoresUnknownGroup(t *testing.T) {
	s := NewScheduler("n2")
	s.Add(NewGroupReplica("g0", "n2", []string{"n1", "n2"}))
	replies := s.Deliver([]GroupMessage{
		{Kind: "leader", Group: "g0", Term: 1, Leader: "n1"},
		{Kind: "leader", Group: "ghost", Term: 1, Leader: "n1"},
	})
	if len(replies) != 0 {
		t.Fatalf("replies = %d, want 0", len(replies))
	}
	if s.Replicas["g0"].Leader != "n1" {
		t.Fatalf("known group should have adopted the leader")
	}
	// The unknown group contributed nothing to the batch metrics.
	if s.EnvelopesSent != 0 {
		t.Fatalf("envelopesSent = %d, want 0 (no replies)", s.EnvelopesSent)
	}
}
