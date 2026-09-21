// A tiny in-memory cluster that wires schedulers together and pumps envelopes.
//
// Used by the tests and the benchmark to show a fleet of groups converging on
// their leaders while the RPC count stays proportional to the number of peer
// pairs, not the number of groups.

package flotilla

// Cluster wires one scheduler per node together and pumps envelopes between them.
type Cluster struct {
	// Schedulers maps node id to that node's scheduler.
	Schedulers map[string]*Scheduler
	// order preserves node insertion order so bootstrap is deterministic.
	order []string
}

// NewCluster creates a cluster with one scheduler per node id, preserving order.
func NewCluster(nodeIDs []string) *Cluster {
	c := &Cluster{Schedulers: make(map[string]*Scheduler, len(nodeIDs))}
	for _, n := range nodeIDs {
		c.Schedulers[n] = NewScheduler(n)
		c.order = append(c.order, n)
	}
	return c
}

// AddGroup adds a replica of groupID to every member node.
func (c *Cluster) AddGroup(groupID string, members []string) {
	for _, n := range members {
		c.Schedulers[n].Add(NewGroupReplica(groupID, n, members))
	}
}

// RunOnce bootstraps every node, then delivers the resulting envelopes.
func (c *Cluster) RunOnce() {
	pending := make(map[string][]GroupMessage)
	for _, n := range c.order {
		for dst, envelope := range c.Schedulers[n].Bootstrap() {
			pending[dst] = append(pending[dst], envelope...)
		}
	}
	for dst, envelope := range pending {
		c.Schedulers[dst].Deliver(envelope)
	}
}

// TotalMessages returns the logical messages sent across every scheduler.
func (c *Cluster) TotalMessages() int {
	total := 0
	for _, s := range c.Schedulers {
		total += s.MessagesSent
	}
	return total
}

// TotalEnvelopes returns the envelopes sent across every scheduler.
func (c *Cluster) TotalEnvelopes() int {
	total := 0
	for _, s := range c.Schedulers {
		total += s.EnvelopesSent
	}
	return total
}

// Leaders maps each group id to the set of leaders its replicas believe in,
// which should have exactly one member once the cluster has converged.
func (c *Cluster) Leaders() map[string]map[string]bool {
	seen := make(map[string]map[string]bool)
	for _, sched := range c.Schedulers {
		for gid, r := range sched.Replicas {
			if r.Leader != "" {
				if seen[gid] == nil {
					seen[gid] = make(map[string]bool)
				}
				seen[gid][r.Leader] = true
			}
		}
	}
	return seen
}
