// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

extern crate core;

// Defines Forge Tests
pub mod aptos;
pub mod fullnode;
pub mod indexer;
pub mod nft_transaction;
pub mod rest_api;
pub mod transaction;

// Converted to local Forge backend
#[cfg(test)]
mod aptos_cli;
#[cfg(test)]
mod client;
#[cfg(test)]
mod full_nodes;
#[cfg(test)]
mod network;
#[cfg(test)]
mod rosetta;
#[cfg(test)]
mod state_sync;
#[cfg(test)]
mod storage;
#[cfg(test)]
mod txn_broadcast;

#[cfg(test)]
mod smoke_test_environment;

#[cfg(test)]
mod test_utils;

#[cfg(test)]
mod workspace_builder;
