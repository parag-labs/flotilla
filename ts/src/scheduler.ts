// The multiplexing layer: many consensus groups sharing one loop and one wire.
//
// The expensive parts of running thousands of Raft groups on a box aren't the
// Raft state machines - those are cheap. It's (1) a thread per group, and (2) a
// separate RPC for every group even when a hundred of them are talking to the
// same physical peer. This module is about avoiding both: one scheduler drives
// every local group replica in a single tick, and outbound messages to the same
// peer are coalesced into one envelope.

/** One inner message routed to one group on one node. */
export interface GroupMessage {
  /** Message type; only `"leader"` announcements are modelled here. */
  kind: string;
  /** Id of the consensus group the message belongs to. */
  group: string;
  /** Leadership term the message was emitted in. */
  term: number;
  /** Id of the node claiming leadership. */
  leader: string;
}

/** A destination-tagged message emitted by a replica: `[dst, msg]`. */
export type Addressed = [dst: string, msg: GroupMessage];

/** Returns the lexicographically smallest string, matching Python's `min()`. */
function minOf(values: string[]): string {
  let smallest = values[0];
  for (const v of values) {
    if (v < smallest) smallest = v;
  }
  return smallest;
}

/**
 * One node's replica of one consensus group.
 *
 * Election here is intentionally trivial - the node with the smallest id in the
 * group is the leader - because the point of this repo is the scheduling and
 * batching layer, not re-deriving Raft. Swap this state machine for a real
 * coracle node and the scheduler/batcher are unchanged.
 */
export class GroupReplica {
  /** Every member id of the group, including `localId`. */
  readonly peers: string[];
  /** `"follower"` or `"leader"`. */
  role = "follower";
  /** Believed leader, or `null` when none is known yet. */
  leader: string | null = null;
  /** Highest leadership term this replica has observed. */
  term = 0;

  constructor(
    readonly groupId: string,
    readonly localId: string,
    peers: string[],
  ) {
    this.peers = [...peers];
  }

  /**
   * Called once. If this replica is the designated leader (the smallest-id
   * member) it announces itself to every peer and returns the outbound
   * messages; otherwise it returns an empty array.
   */
  bootstrap(): Addressed[] {
    if (this.localId === minOf(this.peers)) {
      this.role = "leader";
      this.leader = this.localId;
      this.term = 1;
      return this.peers
        .filter((p) => p !== this.localId)
        .map(
          (p): Addressed => [
            p,
            { kind: "leader", group: this.groupId, term: 1, leader: this.localId },
          ],
        );
    }
    return [];
  }

  /**
   * Applies an inbound message, adopting any leader whose term is at least as
   * high as the one already seen. It never emits replies.
   */
  handle(msg: GroupMessage): Addressed[] {
    if (msg.kind === "leader" && msg.term >= this.term) {
      this.term = msg.term;
      this.leader = msg.leader;
      this.role = "follower";
    }
    return [];
  }
}

/**
 * One physical node's executor. Owns all of that node's group replicas and
 * drives them without a thread per group.
 */
export class Scheduler {
  /** Maps group id to the local replica of that group. */
  readonly replicas = new Map<string, GroupReplica>();
  /** Count of coalesced envelopes emitted so far. */
  envelopesSent = 0;
  /** Count of logical inner messages emitted so far. */
  messagesSent = 0;

  constructor(readonly nodeId: string) {}

  /** Registers a replica under its group id. */
  add(replica: GroupReplica): void {
    this.replicas.set(replica.groupId, replica);
  }

  /**
   * Bootstraps every local replica and returns the batched outbound envelopes
   * keyed by destination peer.
   */
  bootstrap(): Map<string, GroupMessage[]> {
    const outbound: Addressed[] = [];
    for (const r of this.replicas.values()) {
      outbound.push(...r.bootstrap());
    }
    return this.batch(outbound);
  }

  /**
   * Coalesces per-group messages headed to the same peer into one envelope.
   * 1000 groups each sending a heartbeat to peer X become a single envelope
   * carrying 1000 inner messages, not 1000 RPCs.
   */
  private batch(outbound: Addressed[]): Map<string, GroupMessage[]> {
    const byDst = new Map<string, GroupMessage[]>();
    for (const [dst, msg] of outbound) {
      const list = byDst.get(dst);
      if (list) list.push(msg);
      else byDst.set(dst, [msg]);
    }
    this.messagesSent += outbound.length;
    this.envelopesSent += byDst.size;
    return byDst;
  }

  /**
   * Unpacks an envelope and fans its inner messages out to the right group
   * replicas, returning any replies (already re-batched).
   */
  deliver(envelope: GroupMessage[]): Map<string, GroupMessage[]> {
    const replies: Addressed[] = [];
    for (const msg of envelope) {
      const replica = this.replicas.get(msg.group);
      if (replica) replies.push(...replica.handle(msg));
    }
    return this.batch(replies);
  }
}
