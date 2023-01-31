use crypto::traits::KeyPair;
use arc_swap::ArcSwap;
use config::{
    AnemoParameters, Committee, NetworkAdminServerParameters, Parameters,
    PrometheusMetricsParameters, WorkerCache, Authority, WorkerIndex, WorkerId, WorkerInfo,
};
use crypto::PublicKey;
use multiaddr::Multiaddr;
use mysten_metrics::RegistryService;
use node::{execution_state::SimpleExecutionState, primary_node::PrimaryNode, NodeStorage};
use prometheus::Registry;
use std::{sync::Arc, time::Duration, collections::BTreeMap, str::FromStr};
use sui_types::crypto::{get_key_pair_from_rng, AuthorityKeyPair, NetworkKeyPair};
use tokio::sync::mpsc::channel;

#[tokio::main]
async fn main() -> Result<(), eyre::Report> {
    let primary_keypair: AuthorityKeyPair = get_key_pair_from_rng(&mut rand::rngs::OsRng).1;
    let network_keypair: NetworkKeyPair = get_key_pair_from_rng(&mut rand::rngs::OsRng).1;
    let block_sync_params = config::BlockSynchronizerParameters::default();
    let consensus_api_grpc = config::ConsensusAPIGrpcParameters::default();
    let prometheus_metrics = PrometheusMetricsParameters::default();
    let network_admin_server = NetworkAdminServerParameters::default();
    let anemo = AnemoParameters::default();
    let parameters = Parameters {
        header_num_of_batches_threshold: 1,
        max_header_num_of_batches: 5,
        max_header_delay: Duration::from_secs(2),
        gc_depth: 10,
        sync_retry_delay: Duration::from_secs(2),
        sync_retry_nodes: 1,
        batch_size: 5,
        max_batch_delay: Duration::from_secs(2),
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
    authorities.insert(primary_keypair.public().clone(), Authority { stake: 2, primary_address: Multiaddr::from_str("/ip4/127.0.0.1/udp/10000").unwrap(), network_key: network_keypair.public().clone() });
    let epoch = 1;
    let c = Committee {
        authorities,
        epoch,
    };
    let committee = Arc::new(ArcSwap::from_pointee(c));
    let mut workers = BTreeMap::<PublicKey, WorkerIndex>::new();
    let worker_index = WorkerIndex(BTreeMap::<WorkerId, WorkerInfo>::new());
    workers.insert(primary_keypair.public().clone(), worker_index);

    let worker_cache = Arc::new(ArcSwap::from_pointee(
        WorkerCache{
            workers,
            epoch,
        }
    ));

    let store = NodeStorage::reopen(store_path);
    let (_tx_transaction_confirmation, _rx_transaction_confirmation) = channel(100);

    let primary = PrimaryNode::new(parameters, false, registry_service);

    primary
        .start(
            primary_keypair,
            network_keypair,
            committee,
            worker_cache,
            &store,
            Arc::new(SimpleExecutionState::new(_tx_transaction_confirmation)),
        )
        .await?;
    Ok(())
}
