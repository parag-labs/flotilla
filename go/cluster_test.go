package flotilla

import (
	"strconv"
	"testing"
)

var defaultNodes = []string{"n1", "n2", "n3"}

func swarm(nGroups int, nodes []string) *Cluster {
	c := NewCluster(nodes)
	for g := 0; g < nGroups; g++ {
		c.AddGroup("g"+strconv.Itoa(g), nodes)
	}
	c.RunOnce()
	return c
}

func TestEveryGroupAgreesOnOneLeader(t *testing.T) {
	c := swarm(50, defaultNodes)
	leaders := c.Leaders()
	if len(leaders) != 50 {
		t.Fatalf("groups with a leader = %d, want 50", len(leaders))
	}
	for gid, who := range leaders {
		if len(who) != 1 {
			t.Fatalf("group %s disagreed on its leader: %v", gid, who)
		}
	}
}

func TestLeaderIsTheMinIDMember(t *testing.T) {
	c := swarm(10, defaultNodes)
	for gid, who := range c.Leaders() {
		if len(who) != 1 || !who["n1"] {
			t.Fatalf("group %s leader set = %v, want {n1}", gid, who)
		}
	}
}

func TestBatchingBeatsOneRPCPerGroup(t *testing.T) {
	// 200 groups across 3 nodes. Without batching the leader would emit
	// 200 * 2 = 400 separate RPCs; batching collapses each node's outbound to at
	// most (peers) envelopes, so envelopes << messages.
	c := swarm(200, defaultNodes)
	if c.TotalMessages() < 400 {
		t.Fatalf("totalMessages = %d, want >= 400", c.TotalMessages())
	}
	if c.TotalEnvelopes() > 6 {
		t.Fatalf("totalEnvelopes = %d, want <= 6", c.TotalEnvelopes())
	}
	if c.TotalEnvelopes() >= c.TotalMessages()/10 {
		t.Fatalf("envelopes %d not << messages %d", c.TotalEnvelopes(), c.TotalMessages())
	}
}

func TestScalesToThousandsOfGroups(t *testing.T) {
	c := swarm(2000, defaultNodes)
	leaders := c.Leaders()
	if len(leaders) != 2000 {
		t.Fatalf("groups with a leader = %d, want 2000", len(leaders))
	}
	for _, who := range leaders {
		if len(who) != 1 {
			t.Fatalf("a group disagreed on its leader: %v", who)
		}
	}
	if c.TotalEnvelopes() > 6 {
		t.Fatalf("totalEnvelopes = %d, want <= 6 even at 2000 groups", c.TotalEnvelopes())
	}
}

func TestEmptyClusterHasNoLeaders(t *testing.T) {
	c := swarm(0, defaultNodes)
	if len(c.Leaders()) != 0 {
		t.Fatalf("empty cluster should have no leaders, got %v", c.Leaders())
	}
	if c.TotalMessages() != 0 || c.TotalEnvelopes() != 0 {
		t.Fatalf("empty cluster sent traffic: messages=%d envelopes=%d", c.TotalMessages(), c.TotalEnvelopes())
	}
}

func TestSingleGroupSingleNodeSelfLeads(t *testing.T) {
	c := NewCluster([]string{"solo"})
	c.AddGroup("g0", []string{"solo"})
	c.RunOnce()
	leaders := c.Leaders()
	if len(leaders) != 1 || !leaders["g0"]["solo"] {
		t.Fatalf("solo node should lead its own group, got %v", leaders)
	}
	// A single-member group has no peers to notify, so nothing goes on the wire.
	if c.TotalMessages() != 0 || c.TotalEnvelopes() != 0 {
		t.Fatalf("solo group sent traffic: messages=%d envelopes=%d", c.TotalMessages(), c.TotalEnvelopes())
	}
}

func TestTwoNodesConvergeAfterDelivery(t *testing.T) {
	c := NewCluster([]string{"a", "b"})
	c.AddGroup("g0", []string{"a", "b"})
	c.RunOnce()
	// "a" is the min id, so both replicas should believe in "a".
	for _, sched := range c.Schedulers {
		if got := sched.Replicas["g0"].Leader; got != "a" {
			t.Fatalf("node %s believes leader is %q, want a", sched.NodeID, got)
		}
	}
	if c.TotalMessages() != 1 || c.TotalEnvelopes() != 1 {
		t.Fatalf("two-node group: messages=%d envelopes=%d, want 1 and 1", c.TotalMessages(), c.TotalEnvelopes())
	}
}
