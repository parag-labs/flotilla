use flotilla::{GroupMessage, GroupReplica, Scheduler};

#[test]
fn new_replica_starts_as_follower() {
    let r = GroupReplica::new("g1", "n2", vec!["n1".into(), "n2".into(), "n3".into()]);
    assert_eq!(r.role, "follower");
    assert_eq!(r.leader, None);
    assert_eq!(r.term, 0);
}

#[test]
fn bootstrap_leader_announces_to_every_peer() {
    let mut r = GroupReplica::new("g1", "n1", vec!["n1".into(), "n2".into(), "n3".into()]);
    let out = r.bootstrap();
    assert_eq!(out.len(), 2);
    assert_eq!(r.role, "leader");
    assert_eq!(r.leader.as_deref(), Some("n1"));
    assert_eq!(r.term, 1);
    let mut dsts: Vec<String> = out.iter().map(|(d, _)| d.clone()).collect();
    dsts.sort();
    assert_eq!(dsts, vec!["n2".to_string(), "n3".to_string()]);
    for (_, msg) in &out {
        assert_eq!(msg.kind, "leader");
        assert_eq!(msg.group, "g1");
        assert_eq!(msg.term, 1);
        assert_eq!(msg.leader, "n1");
    }
}

#[test]
fn bootstrap_non_leader_stays_silent() {
    let mut r = GroupReplica::new("g1", "n2", vec!["n1".into(), "n2".into(), "n3".into()]);
    let out = r.bootstrap();
    assert!(out.is_empty());
    assert_eq!(r.role, "follower");
    assert_eq!(r.leader, None);
}

#[test]
fn bootstrap_leader_is_lexicographic_min() {
    // "n10" sorts before "n2" lexicographically, mirroring Python's min() on
    // strings, so the smallest-*string* id wins, not the smallest number.
    let mut r = GroupReplica::new("g1", "n10", vec!["n2".into(), "n10".into(), "n3".into()]);
    let out = r.bootstrap();
    assert_eq!(r.role, "leader");
    assert_eq!(out.len(), 2);
}

#[test]
fn single_member_group_leads_itself_without_traffic() {
    let mut r = GroupReplica::new("g1", "solo", vec!["solo".into()]);
    let out = r.bootstrap();
    assert!(out.is_empty());
    assert_eq!(r.role, "leader");
    assert_eq!(r.leader.as_deref(), Some("solo"));
}

#[test]
fn handle_adopts_leader_at_equal_or_higher_term() {
    let mut r = GroupReplica::new("g1", "n2", vec!["n1".into(), "n2".into(), "n3".into()]);
    let replies = r.handle(&GroupMessage {
        kind: "leader".into(),
        group: "g1".into(),
        term: 1,
        leader: "n1".into(),
    });
    assert!(replies.is_empty());
    assert_eq!(r.leader.as_deref(), Some("n1"));
    assert_eq!(r.term, 1);
    // Equal term but a different leader is still adopted (>= comparison).
    r.handle(&GroupMessage {
        kind: "leader".into(),
        group: "g1".into(),
        term: 1,
        leader: "nX".into(),
    });
    assert_eq!(r.leader.as_deref(), Some("nX"));
}

#[test]
fn handle_ignores_stale_term_and_non_leader_kind() {
    let mut r = GroupReplica::new("g1", "n2", vec!["n1".into(), "n2".into(), "n3".into()]);
    r.handle(&GroupMessage {
        kind: "leader".into(),
        group: "g1".into(),
        term: 5,
        leader: "n1".into(),
    });
    r.handle(&GroupMessage {
        kind: "leader".into(),
        group: "g1".into(),
        term: 4,
        leader: "old".into(),
    });
    assert_eq!(r.leader.as_deref(), Some("n1"));
    assert_eq!(r.term, 5);
    r.handle(&GroupMessage {
        kind: "heartbeat".into(),
        group: "g1".into(),
        term: 9,
        leader: "other".into(),
    });
    assert_eq!(r.leader.as_deref(), Some("n1"));
    assert_eq!(r.term, 5);
}

#[test]
fn scheduler_batch_coalesces_by_destination() {
    let mut s = Scheduler::new("n1");
    s.add(GroupReplica::new(
        "g0",
        "n1",
        vec!["n1".into(), "n2".into(), "n3".into()],
    ));
    s.add(GroupReplica::new(
        "g1",
        "n1",
        vec!["n1".into(), "n2".into(), "n3".into()],
    ));
    let env = s.bootstrap();
    assert_eq!(env.len(), 2);
    assert_eq!(env["n2"].len(), 2);
    assert_eq!(env["n3"].len(), 2);
    assert_eq!(s.messages_sent, 4);
    assert_eq!(s.envelopes_sent, 2);
}

#[test]
fn scheduler_deliver_ignores_unknown_group() {
    let mut s = Scheduler::new("n2");
    s.add(GroupReplica::new(
        "g0",
        "n2",
        vec!["n1".into(), "n2".into()],
    ));
    let replies = s.deliver(vec![
        GroupMessage {
            kind: "leader".into(),
            group: "g0".into(),
            term: 1,
            leader: "n1".into(),
        },
        GroupMessage {
            kind: "leader".into(),
            group: "ghost".into(),
            term: 1,
            leader: "n1".into(),
        },
    ]);
    assert!(replies.is_empty());
    assert_eq!(s.replicas["g0"].leader.as_deref(), Some("n1"));
    assert_eq!(s.envelopes_sent, 0);
}
