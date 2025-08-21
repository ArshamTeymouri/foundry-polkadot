// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![warn(missing_docs)]

use jsonrpsee::RpcModule;
use polkadot_sdk::{
    sc_consensus_manual_seal::rpc::ManualSeal,
    sc_transaction_pool_api::TransactionPool,
    sp_blockchain::{Error as BlockChainError, HeaderBackend, HeaderMetadata},
    *,
};
use std::sync::Arc;
use crate::rpc::sp_block_builder::BlockBuilder;
// use substrate_runtime::Runtime;

mod interface {
    use polkadot_sdk::{polkadot_sdk_frame as frame, *};
    use substrate_runtime::Runtime;

    pub type Block = substrate_runtime::Block;
    pub use frame::runtime::types_common::OpaqueBlock;
    pub type AccountId = <Runtime as frame_system::Config>::AccountId;
    pub type Nonce = <Runtime as frame_system::Config>::Nonce;
    pub type Hash = <Runtime as frame_system::Config>::Hash;
    pub type Balance = <Runtime as pallet_balances::Config>::Balance;
    // pub type MinimumBalance = <Runtime as pallet_balances::Config>::ExistentialDeposit;
}

use interface::{AccountId, Nonce, OpaqueBlock};
/// Full client dependencies.
pub struct FullDeps<C, P> {
    /// The client instance to use.
    pub client: Arc<C>,
    /// Transaction pool instance.
    pub pool: Arc<P>,
    /// Used by RPC to forward commands to the block engine.
    pub command_sink: Option<
        futures::channel::mpsc::Sender<
            sc_consensus_manual_seal::rpc::EngineCommand<interface::Hash>,
        >,
    >,
}

/// Instantiate all full RPC extensions.
pub fn create_full<C, P>(
    deps: FullDeps<C, P>,
) -> Result<RpcModule<()>, Box<dyn std::error::Error + Send + Sync>>
where
    C: Send
        + Sync
        + 'static
        + sp_api::ProvideRuntimeApi<OpaqueBlock>
        + HeaderBackend<OpaqueBlock>
        + HeaderMetadata<OpaqueBlock, Error = BlockChainError>
        + 'static,
    C::Api: sp_block_builder::BlockBuilder<OpaqueBlock>,
    C::Api: substrate_frame_rpc_system::AccountNonceApi<OpaqueBlock, AccountId, Nonce>,
    P: TransactionPool + 'static,
{
    use polkadot_sdk::{
        sc_consensus_manual_seal::rpc::ManualSealApiServer,
        substrate_frame_rpc_system::{System, SystemApiServer},
    };

    let mut module = RpcModule::new(());
    let FullDeps { client, pool, command_sink } = deps;

    module.merge(System::new(client.clone(), pool.clone()).into_rpc())?;

    if let Some(sink) = command_sink {
        module.merge(ManualSeal::new(sink).into_rpc())?;
    }
    Ok(module)
}
