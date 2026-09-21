use flotilla::Cluster;

const NODES: [&str; 3] = ["n1", "n2", "n3"];

fn swarm(n_groups: usize) -> Cluster {
    let nodes: Vec<String> = NODES.iter().map(|s| s.to_string()).collect();
    let mut c = Cluster::new(nodes.clone());
    for g in 0..n_groups {
        c.add_group(&format!("g{g}"), nodes.clone());
    }
    c.run_once();
    c
}

#[test]
fn every_group_agrees_on_one_leader() {
    let c = swarm(50);
    let leaders = c.leaders();
    assert_eq!(leaders.len(), 50);
    for (gid, who) in &leaders {
        assert_eq!(who.len(), 1, "group {gid} disagreed on its leader: {who:?}");
    }
}

#[test]
fn leader_is_the_min_id_member() {
    let c = swarm(10);
    for who in c.leaders().values() {
        assert_eq!(who.len(), 1);
        assert!(who.contains("n1")); // min of n1,n2,n3
    }
}

#[test]
fn batching_beats_one_rpc_per_group() {
    // 200 groups across 3 nodes. Without batching the leader would emit
    // 200 * 2 = 400 separate RPCs; batching collapses each node's outbound to at
    // most (peers) envelopes, so envelopes << messages.
    let c = swarm(200);
    assert!(c.total_messages() >= 400);
    assert!(c.total_envelopes() <= 6); // <= 3 nodes * 2 peers
    assert!(c.total_envelopes() < c.total_messages() / 10);
}

#[test]
fn scales_to_thousands_of_groups() {
    let c = swarm(2000);
    let leaders = c.leaders();
    assert_eq!(leaders.len(), 2000);
    assert!(leaders.values().all(|w| w.len() == 1));
    // Envelope count stays bounded by peer pairs even at 2000 groups.
    assert!(c.total_envelopes() <= 6);
}

#[test]
fn empty_cluster_has_no_leaders_and_no_traffic() {
    let c = swarm(0);
    assert!(c.leaders().is_empty());
    assert_eq!(c.total_messages(), 0);
    assert_eq!(c.total_envelopes(), 0);
}

#[test]
fn single_node_single_group_self_leads() {
    let mut c = Cluster::new(vec!["solo".to_string()]);
    c.add_group("g0", vec!["solo".to_string()]);
    c.run_once();
    let leaders = c.leaders();
    assert_eq!(leaders.len(), 1);
    assert!(leaders["g0"].contains("solo"));
    assert_eq!(c.total_messages(), 0);
    assert_eq!(c.total_envelopes(), 0);
}

#[test]
fn two_nodes_converge_after_delivery() {
    let mut c = Cluster::new(vec!["a".to_string(), "b".to_string()]);
    c.add_group("g0", vec!["a".to_string(), "b".to_string()]);
    c.run_once();
    for sched in c.schedulers.values() {
        assert_eq!(sched.replicas["g0"].leader.as_deref(), Some("a"));
    }
    assert_eq!(c.total_messages(), 1);
    assert_eq!(c.total_envelopes(), 1);
}
