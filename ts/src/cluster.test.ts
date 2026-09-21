import { describe, expect, it } from "vitest";

import { Cluster } from "./cluster";

const NODES = ["n1", "n2", "n3"];

function swarm(nGroups: number, nodes: string[] = NODES): Cluster {
  const c = new Cluster(nodes);
  for (let g = 0; g < nGroups; g++) c.addGroup(`g${g}`, nodes);
  c.runOnce();
  return c;
}

describe("Cluster", () => {
  it("has every group agree on exactly one leader", () => {
    const c = swarm(50);
    const leaders = c.leaders();
    expect(leaders.size).toBe(50);
    for (const [gid, who] of leaders) {
      expect(who.size, `group ${gid} disagreed on its leader`).toBe(1);
    }
  });

  it("elects the min-id member as leader", () => {
    const c = swarm(10);
    for (const who of c.leaders().values()) {
      expect(who.size).toBe(1);
      expect(who.has("n1")).toBe(true); // min of n1,n2,n3
    }
  });

  it("beats one RPC per group through batching", () => {
    // 200 groups across 3 nodes. Without batching the leader would emit
    // 200 * 2 = 400 separate RPCs; batching collapses each node's outbound to at
    // most (peers) envelopes, so envelopes << messages.
    const c = swarm(200);
    expect(c.totalMessages()).toBeGreaterThanOrEqual(400);
    expect(c.totalEnvelopes()).toBeLessThanOrEqual(6); // <= 3 nodes * 2 peers
    expect(c.totalEnvelopes()).toBeLessThan(c.totalMessages() / 10);
  });

  it("scales to thousands of groups with bounded envelopes", () => {
    const c = swarm(2000);
    const leaders = c.leaders();
    expect(leaders.size).toBe(2000);
    expect([...leaders.values()].every((w) => w.size === 1)).toBe(true);
    // Envelope count stays bounded by peer pairs even at 2000 groups.
    expect(c.totalEnvelopes()).toBeLessThanOrEqual(6);
  });

  it("has no leaders and no traffic when empty", () => {
    const c = swarm(0);
    expect(c.leaders().size).toBe(0);
    expect(c.totalMessages()).toBe(0);
    expect(c.totalEnvelopes()).toBe(0);
  });

  it("lets a single node lead its own group without traffic", () => {
    const c = new Cluster(["solo"]);
    c.addGroup("g0", ["solo"]);
    c.runOnce();
    const leaders = c.leaders();
    expect(leaders.size).toBe(1);
    expect(leaders.get("g0")?.has("solo")).toBe(true);
    expect(c.totalMessages()).toBe(0);
    expect(c.totalEnvelopes()).toBe(0);
  });

  it("converges two nodes after a single delivery", () => {
    const c = new Cluster(["a", "b"]);
    c.addGroup("g0", ["a", "b"]);
    c.runOnce();
    for (const sched of c.schedulers.values()) {
      expect(sched.replicas.get("g0")?.leader).toBe("a");
    }
    expect(c.totalMessages()).toBe(1);
    expect(c.totalEnvelopes()).toBe(1);
  });
});
