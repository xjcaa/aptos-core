// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::generate_traffic;
use forge::{NetworkContext, NetworkTest, Result, Test};
use rand::{
    prelude::{IteratorRandom, StdRng},
    rngs::OsRng,
    Rng, SeedableRng,
};

pub struct PerformanceBenchmark;

impl Test for PerformanceBenchmark {
    fn name(&self) -> &'static str {
        "all up"
    }
}

impl NetworkTest for PerformanceBenchmark {
    fn run<'t>(&self, ctx: &mut NetworkContext<'t>) -> Result<()> {
        let duration = ctx.global_job.duration;
        let all_validators = ctx
            .swarm()
            .validators()
            .map(|v| v.peer_id())
            .collect::<Vec<_>>();

        let all_fullnodes = ctx
            .swarm()
            .full_nodes()
            .map(|v| v.peer_id())
            .collect::<Vec<_>>();

        let all_nodes = [&all_validators[..], &all_fullnodes[..]].concat();

        // check metrics of a random validator
        let mut rng = StdRng::from_seed(OsRng.gen());
        let validator_id = all_validators.iter().choose(&mut rng).unwrap();
        let validator = ctx.swarm().validator(*validator_id).unwrap();
        let _validator_metric_port = validator.expose_metric()?;

        // Generate some traffic
        let txn_stat = generate_traffic(ctx, &all_nodes, duration, 1)?;
        ctx.report
            .report_txn_stats(self.name().to_string(), &txn_stat, duration);
        // ensure we meet the success criteria
        ctx.success_criteria()
            .check_for_success(&txn_stat, &duration)?;

        Ok(())
    }
}
