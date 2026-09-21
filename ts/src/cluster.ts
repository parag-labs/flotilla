// A tiny in-memory cluster that wires schedulers together and pumps envelopes.
//
// Used by the tests and the benchmark to show a fleet of groups converging on
// their leaders while the RPC count stays proportional to the number of peer
// pairs, not the number of groups.

import { GroupMessage, GroupReplica, Scheduler } from "./scheduler";

/** Wires one scheduler per node together and pumps envelopes between them. */
export class Cluster {
  /** Maps node id to that node's scheduler (insertion order preserved). */
  readonly schedulers = new Map<string, Scheduler>();

  constructor(nodeIds: string[]) {
    for (const n of nodeIds) this.schedulers.set(n, new Scheduler(n));
  }

  /** Adds a replica of `groupId` to every member node. */
  addGroup(groupId: string, members: string[]): void {
    for (const n of members) {
      this.schedulers.get(n)?.add(new GroupReplica(groupId, n, members));
    }
  }

  /** Bootstraps every node, then delivers the resulting envelopes. */
  runOnce(): void {
    const pending = new Map<string, GroupMessage[]>();
    for (const sched of this.schedulers.values()) {
      for (const [dst, envelope] of sched.bootstrap()) {
        const list = pending.get(dst);
        if (list) list.push(...envelope);
        else pending.set(dst, [...envelope]);
      }
    }
    for (const [dst, envelope] of pending) {
      this.schedulers.get(dst)?.deliver(envelope);
    }
  }

  /** Total logical messages sent across every scheduler. */
  totalMessages(): number {
    let total = 0;
    for (const s of this.schedulers.values()) total += s.messagesSent;
    return total;
  }

  /** Total envelopes sent across every scheduler. */
  totalEnvelopes(): number {
    let total = 0;
    for (const s of this.schedulers.values()) total += s.envelopesSent;
    return total;
  }

  /**
   * Maps each group id to the set of leaders its replicas believe in, which
   * should have exactly one member once the cluster has converged.
   */
  leaders(): Map<string, Set<string>> {
    const seen = new Map<string, Set<string>>();
    for (const sched of this.schedulers.values()) {
      for (const [gid, r] of sched.replicas) {
        if (r.leader !== null) {
          let set = seen.get(gid);
          if (!set) {
            set = new Set<string>();
            seen.set(gid, set);
          }
          set.add(r.leader);
        }
      }
    }
    return seen;
  }
}
