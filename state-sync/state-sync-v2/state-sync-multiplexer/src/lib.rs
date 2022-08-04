// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]

use aptos_config::{config::NodeConfig, network_id::NetworkId};
use aptos_data_client::aptosnet::AptosNetDataClient;
use aptos_types::{move_resource::MoveStorage, waypoint::Waypoint};
use consensus_notifications::ConsensusNotificationListener;
use data_streaming_service::streaming_client::StreamingServiceClient;
use event_notifications::{EventNotificationSender, EventSubscriptionService};
use executor_types::ChunkExecutorTrait;
use futures::executor::block_on;
use mempool_notifications::MempoolNotificationSender;
use network::protocols::network::AppConfig;
use state_sync_driver::{
    driver_factory::DriverFactory, metadata_storage::PersistentMetadataStorage,
};
use state_sync_v1::{
    bootstrapper::StateSyncBootstrapper,
    network::{StateSyncEvents, StateSyncSender},
};
use std::sync::Arc;
use storage_interface::DbReaderWriter;
use tokio::runtime::Runtime;

/// A struct for holding the various runtimes required by state sync v2.
/// Note: it's useful to maintain separate runtimes because the logger
/// can prepend all logs with the runtime thread name.
pub struct StateSyncRuntimes {
    _aptos_data_client: Runtime,
    state_sync: StateSyncMultiplexer,
    _storage_service: Runtime,
    _streaming_service: Runtime,
}

impl StateSyncRuntimes {
    pub fn new(
        aptos_data_client: Runtime,
        state_sync: StateSyncMultiplexer,
        storage_service: Runtime,
        streaming_service: Runtime,
    ) -> Self {
        Self {
            _aptos_data_client: aptos_data_client,
            state_sync,
            _storage_service: storage_service,
            _streaming_service: streaming_service,
        }
    }

    pub fn block_until_initialized(&self) {
        self.state_sync.block_until_initialized()
    }
}

/// A multiplexer allowing multiple versions of state sync to operate
/// concurrently (i.e., state sync v1 and state sync v2).
pub struct StateSyncMultiplexer {
    activate_state_sync_v2: bool,
    state_sync_v1: Option<StateSyncBootstrapper>,
    state_sync_v2: Option<DriverFactory>,
}

impl StateSyncMultiplexer {
    pub fn new<
        ChunkExecutor: ChunkExecutorTrait + 'static,
        MempoolNotifier: MempoolNotificationSender + 'static,
    >(
        network: Vec<(NetworkId, StateSyncSender, StateSyncEvents)>,
        mempool_notifier: MempoolNotifier,
        consensus_listener: ConsensusNotificationListener,
        storage: DbReaderWriter,
        chunk_executor: Arc<ChunkExecutor>,
        node_config: &NodeConfig,
        waypoint: Waypoint,
        mut event_subscription_service: EventSubscriptionService,
        aptos_data_client: AptosNetDataClient,
        streaming_service_client: StreamingServiceClient,
    ) -> Self {
        // Notify subscribers of the initial on-chain config values
        match (&*storage.reader).fetch_latest_state_checkpoint_version() {
            Ok(synced_version) => {
                if let Err(error) =
                    event_subscription_service.notify_initial_configs(synced_version)
                {
                    panic!(
                        "Failed to notify subscribers of initial on-chain configs: {:?}",
                        error
                    )
                }
            }
            Err(error) => panic!("Failed to fetch the initial synced version: {:?}", error),
        }

        // TODO(joshlind): update this to support also running v1 in read-only mode!
        // Start state sync (with the version depending on the config)
        let mut state_sync_v1 = None;
        let mut state_sync_v2 = None;
        let activate_state_sync_v2 = node_config
            .state_sync
            .state_sync_driver
            .enable_state_sync_v2;
        if activate_state_sync_v2 {
            // Start the state sync v2 driver
            let metadata_storage = PersistentMetadataStorage::new(node_config.storage.dir());
            state_sync_v2 = Some(DriverFactory::create_and_spawn_driver(
                true,
                node_config,
                waypoint,
                storage,
                chunk_executor,
                mempool_notifier,
                metadata_storage,
                consensus_listener,
                event_subscription_service,
                aptos_data_client,
                streaming_service_client,
            ));
        } else {
            // Start state sync v1
            state_sync_v1 = Some(StateSyncBootstrapper::bootstrap(
                network,
                mempool_notifier,
                consensus_listener,
                storage.reader,
                chunk_executor,
                node_config,
                waypoint,
                event_subscription_service,
                false,
            ));
        }

        Self {
            activate_state_sync_v2,
            state_sync_v1,
            state_sync_v2,
        }
    }

    pub fn block_until_initialized(&self) {
        if self.activate_state_sync_v2 {
            let state_sync_v2_client = self
                .state_sync_v2
                .as_ref()
                .expect("State sync v2 is not running!")
                .create_driver_client();
            block_on(state_sync_v2_client.notify_once_bootstrapped())
                .expect("State sync v2 initialization failure");
        } else {
            let state_sync_v1_client = self
                .state_sync_v1
                .as_ref()
                .expect("State sync v1 is not running!")
                .create_client();
            block_on(state_sync_v1_client.wait_until_initialized())
                .expect("State sync v1 initialization failure");
        }
    }
}

/// Configuration for the network endpoints to support state sync.
pub fn state_sync_v1_network_config() -> AppConfig {
    state_sync_v1::network::network_endpoint_config()
}

#[cfg(any(test, feature = "fuzzing"))]
mod tests {
    use crate::StateSyncMultiplexer;
    use aptos_config::config::TARGET_SNAPSHOT_SIZE;
    use aptos_config::{
        config::{RocksdbConfigs, NO_OP_STORAGE_PRUNER_CONFIG},
        utils::get_genesis_txn,
    };
    use aptos_crypto::HashValue;
    use aptos_data_client::aptosnet::AptosNetDataClient;
    use aptos_genesis::test_utils::test_config;
    use aptos_infallible::RwLock;
    use aptos_temppath::TempPath;
    use aptos_time_service::TimeService;
    use aptos_types::{
        block_info::BlockInfo, ledger_info::LedgerInfo, on_chain_config::ON_CHAIN_CONFIG_REGISTRY,
        waypoint::Waypoint,
    };
    use aptos_vm::AptosVM;
    use aptosdb::AptosDB;
    use consensus_notifications::new_consensus_notifier_listener_pair;
    use data_streaming_service::streaming_client::new_streaming_service_client_listener_pair;
    use event_notifications::EventSubscriptionService;
    use executor::chunk_executor::ChunkExecutor;
    use executor_test_helpers::bootstrap_genesis;
    use futures::{FutureExt, StreamExt};
    use mempool_notifications::new_mempool_notifier_listener_pair;
    use network::application::{interface::MultiNetworkSender, storage::PeerMetadataStorage};
    use std::{collections::HashMap, sync::Arc};
    use storage_interface::DbReaderWriter;
    use storage_service_client::StorageServiceClient;

    #[test]
    fn test_new_initialized_configs() {
        // Create a test database
        let tmp_dir = TempPath::new();
        let db = AptosDB::open(
            &tmp_dir,
            false,
            NO_OP_STORAGE_PRUNER_CONFIG,
            RocksdbConfigs::default(),
            false,
            TARGET_SNAPSHOT_SIZE,
        )
        .unwrap();
        let (_, db_rw) = DbReaderWriter::wrap(db);

        // Bootstrap the database
        let (node_config, _) = test_config();
        bootstrap_genesis::<AptosVM>(&db_rw, get_genesis_txn(&node_config).unwrap()).unwrap();

        // Create mempool and consensus notifiers
        let (mempool_notifier, _) = new_mempool_notifier_listener_pair();
        let (_, consensus_listener) = new_consensus_notifier_listener_pair(0);

        // Create the event subscription service and a reconfig subscriber
        let mut event_subscription_service = EventSubscriptionService::new(
            ON_CHAIN_CONFIG_REGISTRY,
            Arc::new(RwLock::new(db_rw.clone())),
        );
        let mut reconfiguration_subscriber = event_subscription_service
            .subscribe_to_reconfigurations()
            .unwrap();

        // Create a test streaming service client
        let (streaming_service_client, _) = new_streaming_service_client_listener_pair();

        // Create a test aptos data client
        let network_client = StorageServiceClient::new(
            MultiNetworkSender::new(HashMap::new()),
            PeerMetadataStorage::new(&[]),
        );
        let (aptos_data_client, _) = AptosNetDataClient::new(
            node_config.state_sync.aptos_data_client,
            node_config.base.clone(),
            node_config.state_sync.storage_service,
            TimeService::mock(),
            network_client,
            None,
        );

        // Create the multiplexer
        let _ = StateSyncMultiplexer::new(
            vec![],
            mempool_notifier,
            consensus_listener,
            db_rw.clone(),
            Arc::new(ChunkExecutor::<AptosVM>::new(db_rw)),
            &node_config,
            Waypoint::new_any(&LedgerInfo::new(BlockInfo::empty(), HashValue::random())),
            event_subscription_service,
            aptos_data_client,
            streaming_service_client,
        );

        // Verify the initial configs were notified
        assert!(reconfiguration_subscriber
            .select_next_some()
            .now_or_never()
            .is_some());
    }
}
