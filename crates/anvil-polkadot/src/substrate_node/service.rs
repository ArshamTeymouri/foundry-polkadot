
use polkadot_sdk::{
    sc_basic_authorship, sc_consensus, sc_consensus_manual_seal::{self, seal_block, InstantSealParams, SealBlockParams},
    sc_executor::WasmExecutor,
    sc_network,
    sc_service::{self, error::Error as ServiceError, Configuration, RpcHandlers, TaskManager},
    sc_transaction_pool, sp_io,
    sp_runtime::{impl_tx_ext_default, traits::Block as BlockT},
    sp_timestamp,
    substrate_frame_rpc_system::SystemApiServer,
};
use serde::ser;
use std::sync::Arc;
use substrate_runtime::{OpaqueBlock as Block, RuntimeApi};

use crate::{
    rpc::{create_full, FullDeps},
    AnvilNodeConfig,
};

pub type FullClient =
    sc_service::TFullClient<Block, RuntimeApi, WasmExecutor<sp_io::SubstrateHostFunctions>>;

pub type Backend = sc_service::TFullBackend<Block>;

pub type TransactionPoolHandle = sc_transaction_pool::TransactionPoolHandle<Block, FullClient>;

type SelectChain = sc_consensus::LongestChain<Backend, Block>;

pub struct Service {
    pub task_manager: TaskManager,
    pub client: Arc<FullClient>,
    pub backend: Arc<Backend>,
    pub tx_pool: Arc<TransactionPoolHandle>,
    pub rpc_handlers: RpcHandlers,
}

/// Builds a new service for a full client.
pub async fn new<Network: sc_network::NetworkBackend<Block, <Block as BlockT>::Hash>>(
    _anvil_config: &AnvilNodeConfig,
    config: Configuration,
) -> Result<Service, ServiceError> {
    use polkadot_sdk::sc_service::TransactionPool;
    let (client, backend, keystore_container, mut task_manager) =
        sc_service::new_full_parts::<Block, RuntimeApi, _>(
            &config,
            None,
            sc_service::new_wasm_executor(&config.executor),
        )?;
    let client = Arc::new(client);

    let transaction_pool = Arc::from(
        sc_transaction_pool::Builder::new(
            task_manager.spawn_essential_handle(),
            client.clone(),
            config.role.is_authority().into(),
        )
        .with_options(config.transaction_pool.clone())
        .build(),
    );

    let import_queue = sc_consensus_manual_seal::import_queue(
        Box::new(client.clone()),
        &task_manager.spawn_essential_handle(),
        None,
    );

    let net_config = sc_network::config::FullNetworkConfiguration::<
        Block,
        <Block as BlockT>::Hash,
        Network,
    >::new(&config.network, None);
    let metrics = Network::register_notification_metrics(None);

    let (network, system_rpc_tx, tx_handler_controller, sync_service) =
        sc_service::build_network(sc_service::BuildNetworkParams {
            config: &config,
            net_config,
            client: client.clone(),
            transaction_pool: transaction_pool.clone(),
            spawn_handle: task_manager.spawn_handle(),
            import_queue,
            block_announce_validator_builder: None,
            warp_sync_config: None,
            block_relay: None,
            metrics,
        })?;

    let (mut sink, commands_stream) = futures::channel::mpsc::channel(1024);
    let rpc_extensions_builder = {
        let client = client.clone();
        let pool = transaction_pool.clone();
        let command_sink = sink.clone();
        Box::new(move |_| {
            let deps = FullDeps {
                client: client.clone(),
                pool: pool.clone(),
                command_sink: Some(command_sink.clone()),
                // command_sink: None,
            };
            create_full(deps).map_err(Into::into)
        })
    };

    let rpc_handlers = sc_service::spawn_tasks(sc_service::SpawnTasksParams {
        network,
        client: client.clone(),
        keystore: keystore_container.keystore(),
        task_manager: &mut task_manager,
        transaction_pool: transaction_pool.clone(),
        rpc_builder: rpc_extensions_builder,
        backend: backend.clone(),
        system_rpc_tx,
        tx_handler_controller,
        sync_service,
        config,
        telemetry: None,
    })?;

    let mut proposer = sc_basic_authorship::ProposerFactory::new(
        task_manager.spawn_handle(),
        client.clone(),
        transaction_pool.clone(),
        None,
        None,
    );


    let select_chain = SelectChain::new(backend.clone());

			let mut client_mut = client.clone();
            let create_inherent_data_providers =
				|_, ()| async move { Ok(sp_timestamp::InherentDataProvider::from_system_time()) };
        let seal_params = SealBlockParams{
        sender: None,
        parent_hash: None,
        finalize: true,
        create_empty: true,
        env: &mut proposer,
        select_chain: &select_chain,
        block_import: &mut client_mut,
        consensus_data_provider: None,
        pool: transaction_pool.clone(),
        client: client.clone(),
        create_inherent_data_providers:  &create_inherent_data_providers,
    };
    seal_block(seal_params).await;
    // Implement a dummy block production mechanism for now, just build an instantly finalized block
    // every 6 seconds. This will have to change.
    let default_block_time = 6000;
    //task_manager.spawn_handle().spawn("block_authoring", "anvil-polkadot", async move {
    //    loop {
    //        futures_timer::Delay::new(std::time::Duration::from_millis(default_block_time)).await;
    //        sink.try_send(sc_consensus_manual_seal::EngineCommand::SealNewBlock {
    //            create_empty: true,
    //            finalize: true,
    //            parent_hash: None,
    //            sender: None,
    //        })
    //        .unwrap();
    //    }
    //});
    let tx_cl = transaction_pool.clone();
    task_manager.spawn_handle().spawn("transaction-monitoring", "anvil-polkadot", async move {
        loop {
            futures_timer::Delay::new(std::time::Duration::from_millis(default_block_time/6)).await;
            info!("0--->{:?}", tx_cl.futures());
            let ready_transactions: Vec<_> = tx_cl.ready().collect();
            info!("1---> Ready transactions: {:?}", ready_transactions);
        }
    });

    //let manual_seal_params = sc_consensus_manual_seal::ManualSealParams {
    //    block_import: client.clone(),
    //    env: proposer,
    //    client: client.clone(),
    //    pool: transaction_pool.clone(),
    //    select_chain: SelectChain::new(backend.clone()),
    //    commands_stream: Box::pin(commands_stream),
    //    consensus_data_provider: None,
    //    create_inherent_data_providers: move |_, ()| async move {
    //        Ok(sp_timestamp::InherentDataProvider::from_system_time())
    //    },
    //};

    /////////////// Instant Seal
    let create_inherent_data_providers = | _, ()| async move {
        Ok(sp_timestamp::InherentDataProvider::from_system_time())
    };

    let tx_clone = transaction_pool.clone();
    let instant_seal_params = InstantSealParams{
        block_import: client.clone(),
        env: proposer,
        client: client.clone(),
        pool: transaction_pool.clone(),
        select_chain,
        consensus_data_provider: None,
        create_inherent_data_providers,
    };
    let authorship_future = sc_consensus_manual_seal::run_instant_seal(instant_seal_params);
    //let authorship_future = sc_consensus_manual_seal::run_manual_seal(manual_seal_params);

    task_manager.spawn_essential_handle().spawn_blocking(
        "manual-seal",
        "substrate",
        authorship_future
    );

    Ok( Service { task_manager, client: client.clone(), backend: backend.clone(), tx_pool: transaction_pool, rpc_handlers })
}
