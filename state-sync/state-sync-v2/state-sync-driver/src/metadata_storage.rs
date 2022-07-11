// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::{
    error::Error,
    metadata_storage::database_schema::{MetadataKey, MetadataSchema, MetadataValue},
};
use anyhow::{anyhow, Result};
use aptos_infallible::Mutex;
use aptos_logger::prelude::*;
use aptos_types::transaction::Version;
use schemadb::{
    define_schema,
    schema::{KeyCodec, ValueCodec},
    ColumnFamilyName, Options, SchemaBatch, DB,
};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc, time::Instant};

/// The metadata storage interface required by state sync to handle
/// failures and reboots during critical parts of the syncing process.
pub trait MetadataStorageInterface {
    /// Returns true iff a state snapshot was successfully committed for the
    /// specified version. If no snapshot progress is found, false is returned.
    fn is_snapshot_sync_complete(&self, version: Version) -> Result<bool, Error>;

    /// Gets the last persisted state value index for the snapshot sync at the
    /// specified version. If no snapshot progress is found, an error is returned.
    fn get_last_persisted_state_value_index(&self, version: Version) -> Result<u64, Error>;

    /// Updates the last persisted state value index for the state snapshot
    /// sync at the specified version.
    fn update_last_persisted_state_value_index(
        &self,
        version: Version,
        last_persisted_state_value_index: u64,
        snapshot_sync_completed: bool,
    ) -> Result<(), Error>;
}

/// The name of the state sync db file
const STATE_SYNC_DB_NAME: &str = "state_sync_db";

/// The name of the metadata column family
const METADATA_CF_NAME: ColumnFamilyName = "metadata";

/// A metadata storage implementation that uses a RocksDB backend to persist data
#[derive(Clone)]
pub struct PersistentMetadataStorage {
    database: Arc<Mutex<DB>>,
}

impl PersistentMetadataStorage {
    pub fn new<P: AsRef<Path> + Clone>(db_root_path: P) -> Self {
        // Set the options to create the database if it's missing
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // Open the database
        let state_sync_db_path = db_root_path.as_ref().join(STATE_SYNC_DB_NAME);
        let instant = Instant::now();
        let database = DB::open(
            state_sync_db_path.clone(),
            "state_sync",
            vec![METADATA_CF_NAME],
            &options,
        )
        .unwrap_or_else(|_| {
            panic!(
                "Failed to open/create the state sync database at: {:?}",
                state_sync_db_path
            )
        });
        info!(
            "Opened the state sync database at: {:?}, in {:?} ms",
            state_sync_db_path,
            instant.elapsed().as_millis()
        );

        let database = Arc::new(Mutex::new(database));
        Self { database }
    }

    /// Returns the snapshot sync progress recorded for the specified version
    /// if it exists.
    fn get_snapshot_sync_progress_at_version(
        &self,
        version: Version,
    ) -> Result<Option<StateSnapshotSyncProgress>, Error> {
        let maybe_metadata_value = self
            .database
            .lock()
            .get::<MetadataSchema>(&MetadataKey::StateSnapshotSync(version))
            .map_err(|error| {
                Error::StorageError(format!(
                    "Failed to read metadata value at version: {:?}. Error: {:?}",
                    version, error
                ))
            })?;
        Ok(maybe_metadata_value.map(|metadata_value| {
            let MetadataValue::StateSnapshotSync(state_snapshot_sync_progress) = metadata_value;
            state_snapshot_sync_progress
        }))
    }

    /// Write the schema batch to the database
    fn commit(&self, batch: SchemaBatch) -> Result<(), Error> {
        self.database.lock().write_schemas(batch).map_err(|error| {
            Error::StorageError(format!(
                "Failed to write the metadata schema. Error: {:?}",
                error
            ))
        })
    }
}

impl MetadataStorageInterface for PersistentMetadataStorage {
    fn is_snapshot_sync_complete(&self, version: Version) -> Result<bool, Error> {
        let snapshot_sync_progress = self.get_snapshot_sync_progress_at_version(version)?;
        let result = match snapshot_sync_progress {
            Some(snapshot_sync_progress) => snapshot_sync_progress.snapshot_sync_completed,
            None => false, // No snapshot sync progress was found
        };
        Ok(result)
    }

    fn get_last_persisted_state_value_index(&self, version: Version) -> Result<u64, Error> {
        let snapshot_sync_progress = self.get_snapshot_sync_progress_at_version(version)?;
        match snapshot_sync_progress {
            Some(snapshot_sync_progress) => {
                Ok(snapshot_sync_progress.last_persisted_state_value_index)
            }
            None => Err(Error::StorageError(format!(
                "No state snapshot progress was found for version: {:?}",
                version
            ))),
        }
    }

    fn update_last_persisted_state_value_index(
        &self,
        version: Version,
        last_persisted_state_value_index: u64,
        snapshot_sync_completed: bool,
    ) -> Result<(), Error> {
        // Create the key/value pair
        let metadata_key = MetadataKey::StateSnapshotSync(version);
        let metadata_value = MetadataValue::StateSnapshotSync(StateSnapshotSyncProgress {
            last_persisted_state_value_index,
            snapshot_sync_completed,
        });

        // Insert the new key/value pair
        let batch = SchemaBatch::new();
        batch.put::<MetadataSchema>(
            &metadata_key,
            &metadata_value,
        ).map_err(|error| {
            Error::StorageError(format!(
                "Failed to batch put the metadata key and value at version: {:?}. Key: {:?}, Value: {:?}, Error: {:?}",
                version, metadata_key, metadata_value, error
            ))
        })?;
        self.commit(batch)
    }
}

/// A simple struct for recording the progress of a state snapshot sync
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StateSnapshotSyncProgress {
    pub last_persisted_state_value_index: u64,
    pub snapshot_sync_completed: bool,
}

/// The raw schema format used by the database
mod database_schema {
    use super::*;

    // This defines a physical storage schema for any metadata.
    //
    // The key will be a bcs serialized MetadataKey type.
    // The value will be a bcs serialized MetadataValue type.
    //
    // |<-------key------->|<-----value----->|
    // |   metadata key    | metadata value  |
    define_schema!(MetadataSchema, MetadataKey, MetadataValue, METADATA_CF_NAME);

    /// A metadata key that can be inserted into the database
    #[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
    #[repr(u8)]
    pub enum MetadataKey {
        StateSnapshotSync(Version), // A state snapshot sync that was executed at the specified version
    }

    /// A metadata value that can be inserted into the database
    #[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
    #[repr(u8)]
    pub enum MetadataValue {
        StateSnapshotSync(StateSnapshotSyncProgress), // A state snapshot sync progress marker
    }

    impl KeyCodec<MetadataSchema> for MetadataKey {
        fn encode_key(&self) -> Result<Vec<u8>> {
            bcs::to_bytes(self).map_err(|error| {
                anyhow!(
                    "Failed to encode metadata key: {:?}. Error: {:?}",
                    self,
                    error
                )
            })
        }

        fn decode_key(data: &[u8]) -> Result<Self> {
            bcs::from_bytes::<MetadataKey>(data).map_err(|error| {
                anyhow!(
                    "Failed to decode metadata key: {:?}. Error: {:?}",
                    data,
                    error
                )
            })
        }
    }

    impl ValueCodec<MetadataSchema> for MetadataValue {
        fn encode_value(&self) -> Result<Vec<u8>> {
            bcs::to_bytes(self).map_err(|error| {
                anyhow!(
                    "Failed to encode metadata value: {:?}. Error: {:?}",
                    self,
                    error
                )
            })
        }

        fn decode_value(data: &[u8]) -> Result<Self> {
            bcs::from_bytes::<MetadataValue>(data).map_err(|error| {
                anyhow!(
                    "Failed to decode metadata value: {:?}. Error: {:?}",
                    data,
                    error
                )
            })
        }
    }
}

/*
/// Simple unit tests to ensure encoding and decoding works
#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_metadata_schema_encode_decode() {
        assert_encode_decode::<MetadataSchema>(
            &MetadataKey::StateSnapshotSync(123456789),
            &vec![1u8, 2u8, 3u8],
        );
    }

    test_no_panic_decoding!(MetadataSchema);

    #[test]
    fn test_put_get() {
        let tmp_dir = TempPath::new();
        let db = ConsensusDB::new(&tmp_dir);

        let block = Block::make_genesis_block();
        let blocks = vec![block];

        assert_eq!(db.get_blocks().unwrap().len(), 0);
        assert_eq!(db.get_quorum_certificates().unwrap().len(), 0);

        let qcs = vec![certificate_for_genesis()];
        db.save_blocks_and_quorum_certificates(blocks.clone(), qcs.clone())
            .unwrap();

        assert_eq!(db.get_blocks().unwrap().len(), 1);
        assert_eq!(db.get_quorum_certificates().unwrap().len(), 1);

        let tc = vec![0u8, 1, 2];
        db.save_highest_2chain_timeout_certificate(tc.clone())
            .unwrap();

        let vote = vec![2u8, 1, 0];
        db.save_vote(vote.clone()).unwrap();

        let (vote_1, tc_1, blocks_1, qc_1) = db.get_data().unwrap();
        assert_eq!(blocks, blocks_1);
        assert_eq!(qcs, qc_1);
        assert_eq!(Some(tc), tc_1);
        assert_eq!(Some(vote), vote_1);

        db.delete_highest_2chain_timeout_certificate().unwrap();
        db.delete_last_vote_msg().unwrap();
        assert!(db
            .get_highest_2chain_timeout_certificate()
            .unwrap()
            .is_none());
        assert!(db.get_last_vote().unwrap().is_none());
    }

    #[test]
    fn test_delete_block_and_qc() {
        let tmp_dir = TempPath::new();
        let db = ConsensusDB::new(&tmp_dir);

        assert_eq!(db.get_blocks().unwrap().len(), 0);
        assert_eq!(db.get_quorum_certificates().unwrap().len(), 0);

        let blocks = vec![Block::make_genesis_block()];
        let block_id = blocks[0].id();

        let qcs = vec![certificate_for_genesis()];
        let qc_id = qcs[0].certified_block().id();

        db.save_blocks_and_quorum_certificates(blocks, qcs).unwrap();
        assert_eq!(db.get_blocks().unwrap().len(), 1);
        assert_eq!(db.get_quorum_certificates().unwrap().len(), 1);

        // Start to delete
        db.delete_blocks_and_quorum_certificates(vec![block_id, qc_id])
            .unwrap();
        assert_eq!(db.get_blocks().unwrap().len(), 0);
        assert_eq!(db.get_quorum_certificates().unwrap().len(), 0);
    }
}
*/
