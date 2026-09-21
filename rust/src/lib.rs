//! flotilla: run thousands of Raft groups on one set of nodes as a single fleet.
//!
//! The consensus logic is deliberately left to the sibling `coracle` project;
//! this crate is the multiplexing layer that lets you run a swarm of state
//! machines efficiently - one scheduler per node instead of a thread per group,
//! and cross-group RPC batching instead of an RPC per group.

pub mod cluster;
pub mod scheduler;

pub use cluster::Cluster;
pub use scheduler::{GroupMessage, GroupReplica, Scheduler};
