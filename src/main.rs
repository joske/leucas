use arc_swap::ArcSwap;
use config::{
    AnemoParameters, Authority, Committee, Import, NetworkAdminServerParameters, Parameters,
    PrometheusMetricsParameters, WorkerCache, WorkerId, WorkerIndex, WorkerInfo,
};
use crypto::traits::KeyPair;
use crypto::PublicKey;
use eyre::Context;
use fastcrypto::{
    bls12381::min_sig::BLS12381KeyPair, ed25519::Ed25519KeyPair, traits::ToFromBytes,
};
use multiaddr::Multiaddr;
use mysten_metrics::RegistryService;
use node::{
    execution_state::SimpleExecutionState,
    metrics::{primary_metrics_registry, start_prometheus_server, worker_metrics_registry},
    primary_node::PrimaryNode,
    worker_node::WorkerNode,
    NodeStorage,
};
use prometheus::Registry;
use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Duration};
use sui_keys::keypair_file::{read_authority_keypair_from_file, read_network_keypair_from_file};
use sui_types::{
    committee,
    crypto::{get_key_pair_from_rng, AuthorityKeyPair, NetworkKeyPair},
};
use tokio::{sync::mpsc::channel, task::JoinHandle};
use tracing::debug;
use worker::TrivialTransactionValidator;

#[tokio::main]
async fn main() -> Result<(), eyre::Report> {
    tracing_subscriber::fmt::init();
    let args: Vec<String> = std::env::args().collect();
    let id: u32 = args.get(1).unwrap().parse().unwrap();
    let primary_key_file = format!(".primary-{id}-key.json");
    let primary_keypair = read_authority_keypair_from_file(primary_key_file)
        .expect("Failed to load the node's primary keypair");
    let primary_network_key_file = format!(".primary-{id}-network-key.json");
    let primary_network_keypair = read_network_keypair_from_file(primary_network_key_file)
        .expect("Failed to load the node's primary network keypair");
    let worker_key_file = format!(".worker-{id}-key.json");
    let worker_keypair = read_network_keypair_from_file(worker_key_file)
        .expect("Failed to load the node's worker keypair");
    debug!("creating task {}", id);
    // Read the committee, workers and node's keypair from file.
    let committee_file = ".committee.json";
    let committee = Arc::new(ArcSwap::from_pointee(
        Committee::import(committee_file).context("Failed to load the committee information")?,
    ));
    let workers_file = ".workers.json";
    let worker_cache = Arc::new(ArcSwap::from_pointee(
        WorkerCache::import(workers_file).context("Failed to load the worker information")?,
    ));

    // Load default parameters if none are specified.
    let filename = ".parameters.json";
    let parameters =
        Parameters::import(filename).context("Failed to load the node's parameters")?;

    // Make the data store.
    let store_path = format!(".db-{id}-key.json");
    let store = NodeStorage::reopen(store_path);

    // The channel returning the result for each transaction's execution.
    let (_tx_transaction_confirmation, _rx_transaction_confirmation) = channel(100);

    let registry_service = RegistryService::new(Registry::new());

    let primary = PrimaryNode::new(parameters.clone(), true, registry_service.clone());
    let worker = WorkerNode::new(id, parameters, registry_service);
    let primary_keypair_clone: AuthorityKeyPair =
        AuthorityKeyPair::from_bytes(primary_keypair.as_bytes()).unwrap();
    let committee_clone = committee.clone();
    let worker_cache_clone = worker_cache.clone();
    let store_clone = store.clone();
    tokio::spawn(async move {
        primary
            .start(
                primary_keypair,
                primary_network_keypair,
                committee,
                worker_cache,
                &store,
                Arc::new(SimpleExecutionState::new(_tx_transaction_confirmation)),
            )
            .await
            .unwrap();
        primary.wait().await;
    })
    .await
    .ok();

    tokio::spawn(async move {
        worker
            .start(
                primary_keypair_clone.public().clone(),
                worker_keypair,
                committee_clone,
                worker_cache_clone,
                &store_clone,
                TrivialTransactionValidator::default(),
                None,
            )
            .await
            .unwrap();
        worker.wait().await;
    })
    .await
    .ok();

    Ok(())
}
