// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use std::thread;

use aptos_logger::info;
use forge::{NetworkContext, NetworkTest, Result, Test};
use rand::{
    rngs::{OsRng, StdRng},
    seq::IteratorRandom,
    Rng, SeedableRng,
};
use tokio::runtime::Runtime;

const STATE_SYNC_COMMITTED_COUNTER_NAME: &str = "aptos_state_sync_version{type=\"synced\"}";

pub struct ForgeSetupTest;

impl Test for ForgeSetupTest {
    fn name(&self) -> &'static str {
        "verify_forge_setup"
    }
}

impl NetworkTest for ForgeSetupTest {
    fn run<'t>(&self, ctx: &mut NetworkContext<'t>) -> Result<()> {
        let mut rng = StdRng::from_seed(OsRng.gen());
        let runtime = Runtime::new().unwrap();

        let all_fullnodes = ctx
            .swarm()
            .full_nodes()
            .map(|v| v.peer_id())
            .collect::<Vec<_>>();

        info!("Pick one fullnode to stop");
        let fullnode_id = all_fullnodes.iter().choose(&mut rng).unwrap();
        let fullnode = ctx.swarm().full_node_mut(*fullnode_id).unwrap();
        runtime.block_on(fullnode.stop())?;
        info!("Clear its storage");
        fullnode.clear_storage()?;
        info!("Start it up again");
        if let Err(e) = runtime.block_on(fullnode.start()) {
            info!("Error on fullnode startup: {}", e);
        } else {
            info!("Fullnode started successfully");
        }

        info!("Try port-forwarding the fullnode metrics");
        let fullnode_metric_port = fullnode.expose_metric()?;
        info!("Fullnode metrics at {}", fullnode_metric_port);
        for i in 0..10 {
            let version = fullnode
                .counter(STATE_SYNC_COMMITTED_COUNTER_NAME, fullnode_metric_port)
                .unwrap_or(0.0);
            info!("Attempt {}, Fullnode synced version: {}", i, version);
            thread::sleep(std::time::Duration::from_secs(1));
        }

        Ok(())
    }
}
