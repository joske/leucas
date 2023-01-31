use arc_swap::ArcSwap;
use config::{
    AnemoParameters, Authority, Committee, NetworkAdminServerParameters, Parameters,
    PrometheusMetricsParameters, WorkerCache, WorkerId, WorkerIndex, WorkerInfo,
};
use crypto::traits::KeyPair;
use crypto::PublicKey;
use fastcrypto::{bls12381::min_sig::BLS12381KeyPair, ed25519::Ed25519KeyPair};
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

const NODES: usize = 4;

#[tokio::main]
async fn main() -> Result<(), eyre::Report> {
    let block_sync_params = config::BlockSynchronizerParameters::default();
    let consensus_api_grpc = config::ConsensusAPIGrpcParameters {
        socket_addr : Multiaddr::from_str("/ip4/0.0.0.0/tcp/0/http").unwrap(),
        get_collections_timeout: Duration::from_secs(5000),
        remove_collections_timeout: Duration::from_secs(5000),
    };
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
    let store = NodeStorage::reopen(store_path);

    let mut primary_keys = Vec::new();
    for _ in 0..NODES {
        let primary_keypair: AuthorityKeyPair = get_key_pair_from_rng(&mut rand::rngs::OsRng).1;
        primary_keys.push(primary_keypair);
    };
    let mut network_keys = Vec::new();
    for _ in 0..NODES {
        let network_keypair: NetworkKeyPair = get_key_pair_from_rng(&mut rand::rngs::OsRng).1;
        network_keys.push(network_keypair);
    };
    let mut worker_keys = Vec::new();
    for _ in 0..NODES {
        let worker_keypair: NetworkKeyPair = get_key_pair_from_rng(&mut rand::rngs::OsRng).1;
        worker_keys.push(worker_keypair);
    };

    // create the committee
    let c = create_committee(&primary_keys, &network_keys);
    let committee = Arc::new(ArcSwap::from_pointee(c));

    // create the worker cache
    let w = create_worker_cache(&primary_keys, &worker_keys);
    let worker_cache = Arc::new(ArcSwap::from_pointee(w));

    for id in 0..NODES {
        let (primary, worker) = start_node(id as u32, primary_keys.remove(id), network_keys.remove(id), worker_keys.remove(id), parameters.clone(), store.clone(), registry_service.clone(), committee.clone(), worker_cache.clone()).await?;
        primary.wait().await;
        worker.wait().await;
    }

    Ok(())
}

async fn start_node(id: u32, primary_keypair: AuthorityKeyPair, network_keypair: NetworkKeyPair, worker_keypair: NetworkKeyPair, parameters: Parameters, store: NodeStorage, registry_service: RegistryService, committee: Arc<ArcSwap<Committee>>, worker_cache: Arc<ArcSwap<WorkerCache>>) -> Result<(PrimaryNode, WorkerNode), eyre::Report> {
    let (_tx_transaction_confirmation, _rx_transaction_confirmation) = channel(100);

    let worker = WorkerNode::new(id, parameters.clone(), registry_service.clone());
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
    Ok((primary, worker))
}

fn create_committee(primary_keys: &Vec<BLS12381KeyPair>, network_keys: &Vec<Ed25519KeyPair>) -> Committee {
    let mut authorities = BTreeMap::<PublicKey, Authority>::new();
    let start_port = 3000usize;
    for i in 0..NODES {
        let addr = format!("/ip4/127.0.0.1/udp/{}", start_port + i);
        authorities.insert(
            primary_keys.get(i).unwrap().public().clone(),
            Authority {
                stake: 1,
                primary_address: Multiaddr::from_str(addr.as_str()).unwrap(),
                network_key: network_keys.get(i).unwrap().public().clone(),
            },
        );
    }
    Committee { authorities, epoch: 0 }
}

fn create_worker_cache(primary_keys: &Vec<BLS12381KeyPair>, worker_keys: &Vec<Ed25519KeyPair>) -> WorkerCache {
    let mut workers = BTreeMap::<PublicKey, WorkerIndex>::new();
    let start_port = 3008usize;
    for i in 0..NODES {
        let transactions = format!("/ip4/127.0.0.1/tcp/{}/http", start_port + i + 1);
        let worker = format!("/ip4/127.0.0.1/udp/{}", start_port + i);
        let mut worker_info = BTreeMap::<WorkerId, WorkerInfo>::new();
        worker_info.insert(0, WorkerInfo {name: worker_keys.get(i).unwrap().public().clone(), transactions: Multiaddr::from_str(transactions.as_str()).unwrap(), worker_address: Multiaddr::from_str(worker.as_str()).unwrap()});
        let worker_index = WorkerIndex(worker_info);
        workers.insert(primary_keys.get(i).unwrap().public().clone(), worker_index);
    }
    WorkerCache { workers, epoch: 0 }
}
