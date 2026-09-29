// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bulk loads a local Parquet file with several indexing pipelines, then runs merges to a
//! fixpoint. See `docs/internals/parquet-bulk-load.md`.

use std::collections::HashSet;
use std::io::stdout;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use bytesize::ByteSize;
use colored::Colorize;
use humantime::format_duration;
use quickwit_actors::{ActorHandle, Universe};
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_common::uri::Uri;
use quickwit_config::service::QuickwitService;
use quickwit_config::{
    CLI_SOURCE_ID, IndexConfig, IndexerConfig, SourceConfig, SourceParams, TransformConfig,
};
use quickwit_index_management::{IndexService, clear_cache_directory};
use quickwit_indexing::actors::{IndexingService, MergePipeline};
use quickwit_indexing::docs_clustering::Fingerprinter;
use quickwit_indexing::models::{
    DetachIndexingPipeline, DetachMergePipeline, IndexingStatistics, MergeStatistics, SpawnPipeline,
};
use quickwit_indexing::source::parquet_file::{
    DEFAULT_PARQUET_BATCH_NUM_ROWS, ParquetLoadPlan, ParquetLoadPlanGuard,
    register_parquet_load_plan,
};
use quickwit_indexing::{DrainMergesAndShutdownPipeline, IndexingPipeline, IndexingSplitCache};
use quickwit_ingest::IngesterPool;
use quickwit_metastore::{
    IndexMetadata, IndexMetadataResponseExt, ListSplitsQuery, ListSplitsRequestExt,
    MetastoreServiceStreamSplitsExt, SplitMetadata, SplitState,
};
use quickwit_proto::metastore::{
    IndexMetadataRequest, ListSplitsRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::PipelineUid;
use thousands::Separable;

use super::parquet_report::{LoadReport, is_database_metastore};
use super::{LocalIngestDocsArgs, ThroughputCalculator, create_empty_cluster, display_statistics};
use crate::checklist::GREEN_COLOR;
use crate::{get_resolvers, load_node_config, run_index_checklist, start_actor_runtimes};

/// Above this number of row groups, publishing on a file-backed metastore gets slow, because the
/// whole index metadata, including one checkpoint partition per row group, is rewritten on every
/// publish.
const NUM_ROW_GROUPS_WARNING_THRESHOLD: usize = 100_000;

/// The CLI fails if the indexing pipelines may use more than this fraction of the total memory.
const MAX_HEAP_MEMORY_RATIO: f64 = 0.8;

const REPORT_INTERVAL: Duration = Duration::from_secs(1);

pub(super) async fn local_ingest_parquet_cli(args: LocalIngestDocsArgs) -> anyhow::Result<()> {
    let input_uri = args
        .input_path_opt
        .clone()
        .context("`--input-format parquet` requires `--input-path`")?;
    let num_pipelines = args.num_pipelines.get();
    let batch_num_rows = args
        .batch_num_rows_opt
        .map(|batch_num_rows| batch_num_rows.get())
        .unwrap_or(DEFAULT_PARQUET_BATCH_NUM_ROWS);
    println!("❯ Ingesting Parquet file locally with {num_pipelines} pipeline(s)...");

    let config = load_node_config(&args.config_uri, None).await?;
    let (storage_resolver, metastore_resolver) =
        get_resolvers(&config.storage_configs, &config.metastore_configs);
    let mut metastore = metastore_resolver.resolve(&config.metastore_uri).await?;

    let source_config = SourceConfig {
        source_id: CLI_SOURCE_ID.to_string(),
        num_pipelines: args.num_pipelines,
        enabled: true,
        source_params: SourceParams::file_from_uri(input_uri.clone()),
        transform_config: args
            .vrl_script
            .clone()
            .map(|vrl_script| TransformConfig::new(vrl_script, None)),
        input_format: args.input_format,
    };
    run_index_checklist(
        &mut metastore,
        &storage_resolver,
        &args.index_id,
        Some(&source_config),
    )
    .await?;

    if args.overwrite {
        let mut index_service = IndexService::new(metastore.clone(), storage_resolver.clone());
        index_service.clear_index(&args.index_id).await?;
    }
    // Fetched after the optional overwrite, which resets the checkpoint.
    let index_metadata = fetch_index_metadata(&metastore, &args.index_id).await?;
    check_memory(&index_metadata.index_config, num_pipelines)?;

    let plan_guard = build_parquet_load_plan(&index_metadata, input_uri, batch_num_rows).await?;
    let plan = plan_guard.plan().clone();
    let num_rows_to_index = plan.num_rows_to_index();
    let progress = plan.progress();
    println!(
        "Parquet file: {} rows, {} row groups ({} already indexed), {} uncompressed.",
        plan.num_rows().separate_with_commas(),
        plan.num_row_groups().separate_with_commas(),
        progress.num_row_groups_skipped.separate_with_commas(),
        ByteSize(plan.num_uncompressed_bytes()),
    );
    if plan.num_row_groups() > NUM_ROW_GROUPS_WARNING_THRESHOLD
        && !is_database_metastore(&config.metastore_uri)
    {
        println!(
            "{} the file has more than {NUM_ROW_GROUPS_WARNING_THRESHOLD} row groups: publishing \
             on a file-backed metastore will be slow",
            "Warning:".yellow()
        );
    }
    let cluster = create_empty_cluster(&config).await?;
    let indexer_config = IndexerConfig::default();
    let merge_concurrency = indexer_config.merge_concurrency;
    let runtimes_config = RuntimesConfig::default();
    start_actor_runtimes(
        runtimes_config,
        &HashSet::from_iter([QuickwitService::Indexer]),
    )?;
    let universe = Universe::new();
    let merge_scheduler_service_mailbox = universe.get_or_spawn_one();
    let split_cache =
        Arc::new(IndexingSplitCache::from_config(&indexer_config, &config.data_dir_path).await?);
    let fingerprinter_opt = config
        .docs_clustering_config
        .as_ref()
        .map(Fingerprinter::new);
    let indexing_server = IndexingService::new(
        config.node_id.clone(),
        config.data_dir_path.clone(),
        indexer_config,
        runtimes_config.num_threads_blocking,
        cluster,
        metastore.clone(),
        None,
        Some(merge_scheduler_service_mailbox),
        IngesterPool::default(),
        storage_resolver,
        Default::default(),
        split_cache,
        fingerprinter_opt,
    )
    .await?;
    let (indexing_server_mailbox, indexing_server_handle) =
        universe.spawn_builder().spawn(indexing_server);

    let start_time = Instant::now();
    let mut pipeline_ids = Vec::with_capacity(num_pipelines);
    for _ in 0..num_pipelines {
        let pipeline_id = indexing_server_mailbox
            .ask_for_res(SpawnPipeline {
                index_id: args.index_id.clone(),
                source_config: source_config.clone(),
                pipeline_uid: PipelineUid::random(),
            })
            .await?;
        pipeline_ids.push(pipeline_id);
    }
    // All the pipelines share the same merge pipeline. It must be detached after all the pipelines
    // are spawned, otherwise the indexing service would spawn a new one.
    let merge_pipeline_handle = indexing_server_mailbox
        .ask_for_res(DetachMergePipeline {
            pipeline_id: pipeline_ids[0].merge_pipeline_id(),
        })
        .await?;
    let mut pipeline_handles = Vec::with_capacity(num_pipelines);
    for pipeline_id in pipeline_ids {
        let pipeline_handle = indexing_server_mailbox
            .ask_for_res(DetachIndexingPipeline { pipeline_id })
            .await?;
        pipeline_handles.push(pipeline_handle);
    }
    let indexing_outcome = wait_for_indexing_pipelines(pipeline_handles, &plan).await?;
    let indexing_elapsed = start_time.elapsed();
    let num_splits_after_indexing = count_published_splits(&metastore, &index_metadata).await?;
    println!(
        "Indexed {} documents in {}. Draining merges...",
        indexing_outcome.statistics.num_docs.separate_with_commas(),
        format_duration(Duration::from_secs(indexing_elapsed.as_secs())),
    );
    merge_pipeline_handle
        .mailbox()
        .ask(DrainMergesAndShutdownPipeline)
        .await?;
    let merge_statistics = wait_for_merge_pipeline(merge_pipeline_handle).await?;
    let total_elapsed = start_time.elapsed();

    universe
        .send_exit_with_success(&indexing_server_mailbox)
        .await?;
    indexing_server_handle.join().await;
    universe.quit().await;
    drop(plan_guard);

    let final_splits = list_published_splits(&metastore, &index_metadata).await?;
    let report = LoadReport {
        num_pipelines,
        batch_num_rows,
        num_rows_to_index,
        parquet_num_bytes: plan.num_uncompressed_bytes(),
        indexing_statistics: indexing_outcome.statistics,
        num_pipeline_restarts: indexing_outcome.num_pipeline_restarts,
        indexing_elapsed,
        total_elapsed,
        num_splits_after_indexing,
        merge_statistics,
        final_splits,
        index_config: &index_metadata.index_config,
        merge_concurrency: merge_concurrency.get(),
        metastore_uri: &config.metastore_uri,
    };
    report.print();

    if args.clear_cache {
        println!("Clearing local cache directory...");
        clear_cache_directory(&config.data_dir_path).await?;
        println!("{} Local cache directory cleared.", "✔".color(GREEN_COLOR));
    }
    report.check()
}

async fn fetch_index_metadata(
    metastore: &MetastoreServiceClient,
    index_id: &str,
) -> anyhow::Result<IndexMetadata> {
    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await?
        .deserialize_index_metadata()?;
    Ok(index_metadata)
}

async fn build_parquet_load_plan(
    index_metadata: &IndexMetadata,
    input_uri: Uri,
    batch_num_rows: usize,
) -> anyhow::Result<ParquetLoadPlanGuard> {
    let checkpoint = index_metadata
        .checkpoint
        .source_checkpoint(CLI_SOURCE_ID)
        .cloned()
        .unwrap_or_default();
    let plan = tokio::task::spawn_blocking(move || {
        ParquetLoadPlan::try_new(input_uri, &checkpoint, batch_num_rows)
    })
    .await??;
    register_parquet_load_plan(
        index_metadata.index_uid.clone(),
        CLI_SOURCE_ID.to_string(),
        plan,
    )
}

/// Fails if the indexing pipelines may use too much memory. Each pipeline can buffer up to
/// `heap_size` bytes in its indexer.
fn check_memory(index_config: &IndexConfig, num_pipelines: usize) -> anyhow::Result<()> {
    let heap_size = index_config.indexing_settings.resources.heap_size;
    let system = sysinfo::System::new_with_specifics(
        sysinfo::RefreshKind::nothing().with_memory(sysinfo::MemoryRefreshKind::everything()),
    );
    let total_memory = system.total_memory();
    let required_memory = heap_size.as_u64().saturating_mul(num_pipelines as u64);
    if required_memory as f64 > MAX_HEAP_MEMORY_RATIO * total_memory as f64 {
        bail!(
            "{num_pipelines} pipelines with a heap size of {heap_size} require {} but the total \
             memory is {}: lower `indexing_settings.resources.heap_size` in the index config or \
             `--num-pipelines`",
            ByteSize(required_memory),
            ByteSize(total_memory),
        );
    }
    Ok(())
}

struct IndexingOutcome {
    statistics: IndexingStatistics,
    num_pipeline_restarts: usize,
}

/// Displays the progress of the indexing pipelines until they all exit, then returns their
/// summed statistics.
async fn wait_for_indexing_pipelines(
    pipeline_handles: Vec<ActorHandle<IndexingPipeline>>,
    plan: &ParquetLoadPlan,
) -> anyhow::Result<IndexingOutcome> {
    let mut stdout_handle = stdout();
    let mut throughput_calculator = ThroughputCalculator::new(Instant::now());
    let mut report_interval = tokio::time::interval(REPORT_INTERVAL);

    loop {
        report_interval.tick().await;
        let mut statistics = IndexingStatistics::default();
        let mut num_running_pipelines = 0;

        for pipeline_handle in &pipeline_handles {
            pipeline_handle.refresh_observe();
            add_indexing_statistics(&mut statistics, &pipeline_handle.last_observation());
            if !pipeline_handle.state().is_exit() {
                num_running_pipelines += 1;
            }
        }
        if num_running_pipelines == 0 {
            break;
        }
        let progress = plan.progress();
        print!(
            " {} {}/{} {} {num_running_pipelines}",
            "Row groups".bright_blue(),
            // Published row groups are only tracked while the sources run: a source exits after
            // emitting its last batch, before this batch is published.
            progress.num_row_groups_skipped
                + progress.num_row_groups_emitted
                + progress.num_row_groups_published,
            progress.num_row_groups,
            "Pipelines".bright_blue(),
        );
        display_statistics(&mut stdout_handle, &mut throughput_calculator, &statistics)?;
    }
    let mut statistics = IndexingStatistics::default();
    let mut num_pipeline_restarts = 0;

    for pipeline_handle in pipeline_handles {
        let (exit_status, pipeline_statistics) = pipeline_handle.join().await;
        if !exit_status.is_success() {
            bail!("indexing pipeline failed: {exit_status:?}");
        }
        add_indexing_statistics(&mut statistics, &pipeline_statistics);
        num_pipeline_restarts += pipeline_statistics.generation.saturating_sub(1);
    }
    Ok(IndexingOutcome {
        statistics,
        num_pipeline_restarts,
    })
}

fn add_indexing_statistics(total: &mut IndexingStatistics, statistics: &IndexingStatistics) {
    total.num_docs += statistics.num_docs;
    total.num_invalid_docs += statistics.num_invalid_docs;
    total.num_local_splits += statistics.num_local_splits;
    total.num_staged_splits += statistics.num_staged_splits;
    total.num_uploaded_splits += statistics.num_uploaded_splits;
    total.num_published_splits += statistics.num_published_splits;
    total.num_empty_splits += statistics.num_empty_splits;
    total.total_bytes_processed += statistics.total_bytes_processed;
    total.total_size_splits += statistics.total_size_splits;
}

/// Displays the progress of the merge pipeline until it exits.
async fn wait_for_merge_pipeline(
    merge_pipeline_handle: ActorHandle<MergePipeline>,
) -> anyhow::Result<MergeStatistics> {
    let mut report_interval = tokio::time::interval(REPORT_INTERVAL);
    loop {
        report_interval.tick().await;
        if merge_pipeline_handle.state().is_exit() {
            break;
        }
        merge_pipeline_handle.refresh_observe();
        let statistics = merge_pipeline_handle.last_observation();
        println!(
            " {} {:>5} {} {:>5}",
            "Ongoing merges".bright_blue(),
            statistics.num_ongoing_merges,
            "Merges".bright_blue(),
            statistics.num_uploaded_splits,
        );
    }
    let (exit_status, statistics) = merge_pipeline_handle.join().await;
    if !exit_status.is_success() {
        bail!("merge pipeline failed: {exit_status:?}");
    }
    Ok(statistics)
}

async fn list_published_splits(
    metastore: &MetastoreServiceClient,
    index_metadata: &IndexMetadata,
) -> anyhow::Result<Vec<SplitMetadata>> {
    let query = ListSplitsQuery::for_index(index_metadata.index_uid.clone())
        .with_split_state(SplitState::Published);
    let request = ListSplitsRequest::try_from_list_splits_query(&query)?;
    let splits = metastore
        .list_splits(request)
        .await?
        .collect_splits_metadata()
        .await?;
    Ok(splits)
}

async fn count_published_splits(
    metastore: &MetastoreServiceClient,
    index_metadata: &IndexMetadata,
) -> anyhow::Result<usize> {
    Ok(list_published_splits(metastore, index_metadata)
        .await?
        .len())
}
