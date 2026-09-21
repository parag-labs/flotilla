import { describe, expect, it } from "vitest";

import { GroupMessage, GroupReplica, Scheduler } from "./scheduler";

describe("GroupReplica", () => {
  it("starts as a follower with no leader", () => {
    const r = new GroupReplica("g1", "n2", ["n1", "n2", "n3"]);
    expect(r.role).toBe("follower");
    expect(r.leader).toBeNull();
    expect(r.term).toBe(0);
  });

  it("copies the peers array so later mutation cannot affect it", () => {
    const peers = ["n1", "n2"];
    const r = new GroupReplica("g1", "n1", peers);
    peers[0] = "mutated";
    expect(r.peers[0]).toBe("n1");
  });

  it("announces to every peer when it is the leader", () => {
    const r = new GroupReplica("g1", "n1", ["n1", "n2", "n3"]);
    const out = r.bootstrap();
    expect(out).toHaveLength(2);
    expect(r.role).toBe("leader");
    expect(r.leader).toBe("n1");
    expect(r.term).toBe(1);
    const dsts = out.map(([d]) => d).sort();
    expect(dsts).toEqual(["n2", "n3"]);
    for (const [, msg] of out) {
      expect(msg).toEqual({ kind: "leader", group: "g1", term: 1, leader: "n1" });
    }
  });

  it("stays silent when it is not the leader", () => {
    const r = new GroupReplica("g1", "n2", ["n1", "n2", "n3"]);
    expect(r.bootstrap()).toHaveLength(0);
    expect(r.role).toBe("follower");
    expect(r.leader).toBeNull();
  });

  it("selects the lexicographic-min id, not the numeric-min", () => {
    // "n10" sorts before "n2" lexicographically, mirroring Python's min() on
    // strings, so the smallest-*string* id wins rather than the smallest number.
    const r = new GroupReplica("g1", "n10", ["n2", "n10", "n3"]);
    const out = r.bootstrap();
    expect(r.role).toBe("leader");
    expect(out).toHaveLength(2);
  });

  it("leads a single-member group without emitting traffic", () => {
    const r = new GroupReplica("g1", "solo", ["solo"]);
    expect(r.bootstrap()).toHaveLength(0);
    expect(r.role).toBe("leader");
    expect(r.leader).toBe("solo");
  });

  it("adopts a leader at an equal or higher term", () => {
    const r = new GroupReplica("g1", "n2", ["n1", "n2", "n3"]);
    const replies = r.handle({ kind: "leader", group: "g1", term: 1, leader: "n1" });
    expect(replies).toHaveLength(0);
    expect(r.leader).toBe("n1");
    expect(r.term).toBe(1);
    // Equal term but a different leader is still adopted (>= comparison).
    r.handle({ kind: "leader", group: "g1", term: 1, leader: "nX" });
    expect(r.leader).toBe("nX");
  });

  it("ignores a stale term and non-leader message kinds", () => {
    const r = new GroupReplica("g1", "n2", ["n1", "n2", "n3"]);
    r.handle({ kind: "leader", group: "g1", term: 5, leader: "n1" });
    r.handle({ kind: "leader", group: "g1", term: 4, leader: "old" });
    expect(r.leader).toBe("n1");
    expect(r.term).toBe(5);
    r.handle({ kind: "heartbeat", group: "g1", term: 9, leader: "other" });
    expect(r.leader).toBe("n1");
    expect(r.term).toBe(5);
  });
});

describe("Scheduler", () => {
  it("coalesces per-group messages by destination", () => {
    const s = new Scheduler("n1");
    s.add(new GroupReplica("g0", "n1", ["n1", "n2", "n3"]));
    s.add(new GroupReplica("g1", "n1", ["n1", "n2", "n3"]));
    const env = s.bootstrap();
    expect(env.size).toBe(2);
    expect(env.get("n2")).toHaveLength(2);
    expect(env.get("n3")).toHaveLength(2);
    expect(s.messagesSent).toBe(4);
    expect(s.envelopesSent).toBe(2);
  });

  it("ignores messages for unknown groups on delivery", () => {
    const s = new Scheduler("n2");
    s.add(new GroupReplica("g0", "n2", ["n1", "n2"]));
    const envelope: GroupMessage[] = [
      { kind: "leader", group: "g0", term: 1, leader: "n1" },
      { kind: "leader", group: "ghost", term: 1, leader: "n1" },
    ];
    const replies = s.deliver(envelope);
    expect(replies.size).toBe(0);
    expect(s.replicas.get("g0")?.leader).toBe("n1");
    expect(s.envelopesSent).toBe(0);
  });
});
