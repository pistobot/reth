//! Unwinding a certain block range

use crate::{
    common::{AccessRights, CliNodeTypes, Environment, EnvironmentArgs},
    stage::CliNodeComponents,
};
use alloy_eips::BlockHashOrNumber;
use alloy_primitives::B256;
use clap::{Parser, Subcommand};
use reth_chainspec::{ChainSpecProvider, EthChainSpec, EthereumHardforks};
use reth_cli::chainspec::ChainSpecParser;
use reth_config::Config;
use reth_consensus::noop::NoopConsensus;
use reth_db::DatabaseEnv;
use reth_downloaders::{bodies::noop::NoopBodiesDownloader, headers::noop::NoopHeaderDownloader};
use reth_evm::ConfigureEvm;
use reth_exex::ExExManagerHandle;
use reth_provider::{providers::ProviderNodeTypes, BlockNumReader, ProviderFactory};
use reth_stages::{
    sets::{DefaultStages, OfflineStages},
    stages::ExecutionStage,
    ExecutionStageThresholds, Pipeline, StageSet,
};
use reth_static_file::StaticFileProducer;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::info;

/// `reth stage unwind` command
#[derive(Debug, Parser)]
pub struct Command<C: ChainSpecParser> {
    #[command(flatten)]
    env: EnvironmentArgs<C>,

    #[command(subcommand)]
    command: Subcommands,

    /// If this is enabled, then all stages except headers, bodies, and sender recovery will be
    /// unwound.
    #[arg(long)]
    offline: bool,

    /// Maximum number of blocks unwound per Execution-stage iteration before committing the MDBX
    /// write transaction.
    ///
    /// When set, caps `ExecutionStageThresholds::max_blocks` and the IndexHistory / Hashing
    /// `commit_threshold`s used in the unwind pipeline so each stage flushes intermediate
    /// progress rather than buffering the entire unwind range as one `BundleState` in memory.
    /// Useful when unwinding very large ranges (>50k blocks) where the default (unbounded) path
    /// OOMs on 32-64 GiB hosts.
    ///
    /// If unset, retains historical behavior of one transaction per stage (unbounded memory).
    #[arg(long, env = "RETH_UNWIND_CHUNK_SIZE")]
    unwind_chunk_size: Option<u64>,
}

impl<C: ChainSpecParser<ChainSpec: EthChainSpec + EthereumHardforks>> Command<C> {
    /// Execute `db stage unwind` command
    pub async fn execute<N: CliNodeTypes<ChainSpec = C::ChainSpec>, F, Comp>(
        self,
        components: F,
        runtime: reth_tasks::Runtime,
    ) -> eyre::Result<()>
    where
        Comp: CliNodeComponents<N>,
        F: FnOnce(Arc<C::ChainSpec>) -> Comp,
    {
        let Environment { provider_factory, config, data_dir: _ } =
            self.env.init::<N>(AccessRights::RW, runtime)?;

        let target = self.command.unwind_target(provider_factory.clone())?;

        let components = components(provider_factory.chain_spec());

        if self.offline {
            info!(target: "reth::cli", "Performing an unwind for offline-only data!");
        }

        let highest_static_file_block = provider_factory.provider()?.last_block_number()?;
        info!(target: "reth::cli", ?target, ?highest_static_file_block, prune_config=?config.prune,  "Executing a pipeline unwind.");

        // This will build an offline-only pipeline if the `offline` flag is enabled
        let mut pipeline =
            self.build_pipeline(config, provider_factory, components.evm_config().clone())?;


        // Move all applicable data from database to static files.
        pipeline.move_to_static_files()?;

        pipeline.unwind(target, None)?;

        info!(target: "reth::cli", ?target, "Unwound blocks");

        Ok(())
    }

    fn build_pipeline<N: ProviderNodeTypes<ChainSpec = C::ChainSpec>>(
        self,
        mut config: Config,
        provider_factory: ProviderFactory<N>,
        evm_config: impl ConfigureEvm<Primitives = N::Primitives> + 'static,
    ) -> Result<Pipeline<N>, eyre::Error> {
        // Apply --unwind-chunk-size to every stage that already supports a per-iteration cap.
        // This forces the pipeline loop at `Pipeline::unwind` (which commits the MDBX write tx
        // after each stage iteration) to flush progress to disk every `chunk` blocks instead of
        // buffering the entire unwind range as one `BundleState` in memory.
        if let Some(chunk) = self.unwind_chunk_size {
            info!(
                target: "reth::cli",
                chunk_size = chunk,
                "Capping per-iteration unwind range to bound peak memory"
            );
            config.stages.execution.max_blocks = Some(chunk);
            // IndexHistory / Hashing default to 100_000 — clamp to the requested chunk so the
            // unwind loop commits at the same cadence as Execution.
            if config.stages.index_account_history.commit_threshold > chunk {
                config.stages.index_account_history.commit_threshold = chunk;
            }
            if config.stages.index_storage_history.commit_threshold > chunk {
                config.stages.index_storage_history.commit_threshold = chunk;
            }
            if config.stages.account_hashing.commit_threshold > chunk {
                config.stages.account_hashing.commit_threshold = chunk;
            }
            if config.stages.storage_hashing.commit_threshold > chunk {
                config.stages.storage_hashing.commit_threshold = chunk;
            }
            if config.stages.transaction_lookup.chunk_size > chunk {
                config.stages.transaction_lookup.chunk_size = chunk;
            }
        }

        let stage_conf = &config.stages;
        let prune_modes = config.prune.segments.clone();

        let (tip_tx, tip_rx) = watch::channel(B256::ZERO);

        let builder = if self.offline {
            Pipeline::<N>::builder().add_stages(
                OfflineStages::new(
                    evm_config,
                    NoopConsensus::arc(),
                    config.stages,
                    prune_modes.clone(),
                )
                .builder()
                .disable(reth_stages::StageId::SenderRecovery),
            )
        } else {
            Pipeline::<N>::builder().with_tip_sender(tip_tx).add_stages(
                DefaultStages::new(
                    provider_factory.clone(),
                    tip_rx,
                    Arc::new(NoopConsensus::default()),
                    NoopHeaderDownloader::default(),
                    NoopBodiesDownloader::default(),
                    evm_config.clone(),
                    stage_conf.clone(),
                    prune_modes.clone(),
                    None,
                )
                .set(ExecutionStage::new(
                    evm_config,
                    Arc::new(NoopConsensus::default()),
                    ExecutionStageThresholds {
                        max_blocks: self.unwind_chunk_size,
                        max_changes: None,
                        max_cumulative_gas: None,
                        max_duration: None,
                    },
                    stage_conf.execution_external_clean_threshold(),
                    ExExManagerHandle::empty(),
                )),
            )
        };

        let pipeline = builder.build(
            provider_factory.clone(),
            StaticFileProducer::new(provider_factory, prune_modes),
        );
        Ok(pipeline)
    }
}

impl<C: ChainSpecParser> Command<C> {
    /// Return the underlying chain being used to run this command
    pub fn chain_spec(&self) -> Option<&Arc<C::ChainSpec>> {
        Some(&self.env.chain)
    }
}

/// `reth stage unwind` subcommand
#[derive(Subcommand, Debug, Eq, PartialEq)]
enum Subcommands {
    /// Unwinds the database from the latest block, until the given block number or hash has been
    /// reached, that block is not included.
    #[command(name = "to-block")]
    ToBlock { target: BlockHashOrNumber },
    /// Unwinds the database from the latest block, until the given number of blocks have been
    /// reached.
    #[command(name = "num-blocks")]
    NumBlocks { amount: u64 },
}

impl Subcommands {
    /// Returns the block to unwind to. The returned block will stay in database.
    fn unwind_target<N: ProviderNodeTypes<DB = DatabaseEnv>>(
        &self,
        factory: ProviderFactory<N>,
    ) -> eyre::Result<u64> {
        let provider = factory.provider()?;
        let last = provider.last_block_number()?;
        let target = match self {
            Self::ToBlock { target } => match target {
                BlockHashOrNumber::Hash(hash) => provider
                    .block_number(*hash)?
                    .ok_or_else(|| eyre::eyre!("Block hash not found in database: {hash:?}"))?,
                BlockHashOrNumber::Number(num) => *num,
            },
            Self::NumBlocks { amount } => last.saturating_sub(*amount),
        };
        if target > last {
            eyre::bail!(
                "Target block number {target} is higher than the latest block number {last}"
            )
        }
        Ok(target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_chainspec::SEPOLIA;
    use reth_ethereum_cli::chainspec::EthereumChainSpecParser;

    #[test]
    fn parse_unwind() {
        let cmd = Command::<EthereumChainSpecParser>::parse_from([
            "reth",
            "--datadir",
            "dir",
            "to-block",
            "100",
        ]);
        assert_eq!(cmd.command, Subcommands::ToBlock { target: BlockHashOrNumber::Number(100) });

        let cmd = Command::<EthereumChainSpecParser>::parse_from([
            "reth",
            "--datadir",
            "dir",
            "num-blocks",
            "100",
        ]);
        assert_eq!(cmd.command, Subcommands::NumBlocks { amount: 100 });
    }

    #[test]
    fn parse_unwind_chain() {
        let cmd = Command::<EthereumChainSpecParser>::parse_from([
            "reth", "--chain", "sepolia", "to-block", "100",
        ]);
        assert_eq!(cmd.command, Subcommands::ToBlock { target: BlockHashOrNumber::Number(100) });
        assert_eq!(cmd.env.chain.chain_id(), SEPOLIA.chain_id());
    }
}
