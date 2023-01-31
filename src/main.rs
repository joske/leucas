use arc_swap::ArcSwap;
use config::{
    AnemoParameters, Authority, Committee, NetworkAdminServerParameters, Parameters,
    PrometheusMetricsParameters, WorkerCache, WorkerId, WorkerIndex, WorkerInfo,
};
use crypto::traits::KeyPair;
use crypto::PublicKey;
use multiaddr::Multiaddr;
use mysten_metrics::RegistryService;
use node::{
    execution_state::SimpleExecutionState, primary_node::PrimaryNode, worker_node::WorkerNode,
    NodeStorage,
};
use prometheus::Registry;
use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Duration};
use sui_types::crypto::{get_key_pair_from_rng, AuthorityKeyPair, NetworkKeyPair};
use tokio::sync::mpsc::channel;
use worker::TrivialTransactionValidator;

#[tokio::main]
async fn main() -> Result<(), eyre::Report> {
    let primary_keypair: AuthorityKeyPair = get_key_pair_from_rng(&mut rand::rngs::OsRng).1;
    let network_keypair: NetworkKeyPair = get_key_pair_from_rng(&mut rand::rngs::OsRng).1;
    let block_sync_params = config::BlockSynchronizerParameters::default();
    let mut consensus_api_grpc = config::ConsensusAPIGrpcParameters::default();
    consensus_api_grpc.socket_addr = Multiaddr::from_str("/ip4/0.0.0.0/tcp/3001").unwrap();
    println!("grpc:{:?}", consensus_api_grpc);
    let prometheus_metrics = PrometheusMetricsParameters::default();
    let network_admin_server = NetworkAdminServerParameters::default();
    let anemo = AnemoParameters::default();
    let parameters = Parameters {
        header_num_of_batches_threshold: 32,
        max_header_num_of_batches: 1000,
        max_header_delay: Duration::from_secs(2),
        gc_depth: 10,
        sync_retry_delay: Duration::from_secs(10),
        sync_retry_nodes: 3,
        batch_size: 5,
        max_batch_delay: Duration::from_millis(200),
        block_synchronizer: block_sync_params,
        consensus_api_grpc,
        max_concurrent_requests: 5,
        prometheus_metrics,
        network_admin_server,
        anemo,
    };
    let registry_service = RegistryService::new(Registry::new());

    let store_path = "store";

    let mut authorities = BTreeMap::<PublicKey, Authority>::new();
    authorities.insert(
        primary_keypair.public().clone(),
        Authority {
            stake: 1,
            primary_address: Multiaddr::from_str("/ip4/127.0.0.1/udp/3001").unwrap(),
            network_key: network_keypair.public().clone(),
        },
    );
    let epoch = 1;
    let c = Committee { authorities, epoch };
    let committee = Arc::new(ArcSwap::from_pointee(c));
    let mut workers = BTreeMap::<PublicKey, WorkerIndex>::new();


    let store = NodeStorage::reopen(store_path);
    let (_tx_transaction_confirmation, _rx_transaction_confirmation) = channel(100);

    let id = 0;
    let worker_keypair: NetworkKeyPair = get_key_pair_from_rng(&mut rand::rngs::OsRng).1;
    let worker = WorkerNode::new(id, parameters.clone(), registry_service.clone());

    let mut worker_info = BTreeMap::<WorkerId, WorkerInfo>::new();
    worker_info.insert(0, WorkerInfo {name: worker_keypair.public().clone(), transactions: Multiaddr::from_str("/ip4/127.0.0.1/tcp/3011").unwrap(), worker_address: Multiaddr::from_str("/ip4/127.0.0.1/udp/3010").unwrap()});
    let worker_index = WorkerIndex(worker_info);
    workers.insert(primary_keypair.public().clone(), worker_index);

    let worker_cache = Arc::new(ArcSwap::from_pointee(WorkerCache { workers, epoch }));

    worker
        .start(
            primary_keypair.public().clone(),
            worker_keypair,
            committee.clone(),
            worker_cache.clone(),
            &store,
            TrivialTransactionValidator::default(),
            None,
        )
        .await?;

    let primary = PrimaryNode::new(parameters.clone(), false, registry_service.clone());

    primary
        .start(
            primary_keypair,
            network_keypair,
            committee.clone(),
            worker_cache.clone(),
            &store,
            Arc::new(SimpleExecutionState::new(_tx_transaction_confirmation)),
        )
        .await?;

    primary.wait().await;
    worker.wait().await;

    Ok(())
}
