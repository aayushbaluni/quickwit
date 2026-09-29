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

use std::num::NonZeroUsize;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use quickwit_actors::{Command, Universe};
use quickwit_common::uri::Uri;
use quickwit_config::{FileSourceParams, SourceConfig, SourceInputFormat, SourceParams};
use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpoint, SourceCheckpointDelta};
use quickwit_proto::types::{IndexUid, Position};

use super::plan::row_group_partition_id;
use super::*;
use crate::actors::DocProcessor;
use crate::models::RawDocBatch;
use crate::source::tests::SourceRuntimeBuilder;
use crate::source::{FileSourceFactory, SourceActor, TypedSourceFactory};

/// Writes `num_rows` rows `{"id": i, "message": "message-{i}"}` in row groups of
/// `row_group_num_rows` rows.
fn write_parquet_file(path: &Path, num_rows: usize, row_group_num_rows: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("message", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (0..num_rows as i64).collect();
    let messages: Vec<String> = ids.iter().map(|id| format!("message-{id}")).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(messages)),
    ];
    let record_batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_num_rows))
        .build();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, Some(properties)).unwrap();
    writer.write(&record_batch).unwrap();
    writer.close().unwrap();
}

fn parquet_source_config(file_uri: &Uri) -> SourceConfig {
    SourceConfig {
        source_id: "test-parquet-source".to_string(),
        num_pipelines: NonZeroUsize::MIN,
        enabled: true,
        source_params: SourceParams::File(FileSourceParams::Filepath(file_uri.clone())),
        transform_config: None,
        input_format: SourceInputFormat::Parquet,
    }
}

/// The mock metastore registers the source config, which rejects the Parquet input format, so we
/// build the runtime with a JSON config and switch the input format afterwards.
fn parquet_source_runtime(
    index_uid: IndexUid,
    file_uri: &Uri,
    checkpoint_delta_opt: Option<SourceCheckpointDelta>,
) -> crate::source::SourceRuntime {
    let mut source_config = parquet_source_config(file_uri);
    source_config.input_format = SourceInputFormat::Json;
    let mut source_runtime = SourceRuntimeBuilder::new(index_uid, source_config)
        .with_mock_metastore(checkpoint_delta_opt)
        .build();
    source_runtime.source_config.input_format = SourceInputFormat::Parquet;
    source_runtime
}

fn collect_ids(raw_doc_batches: &[&RawDocBatch]) -> Vec<i64> {
    raw_doc_batches
        .iter()
        .flat_map(|batch| batch.docs.iter())
        .map(|doc| {
            let json_doc: serde_json::Value = serde_json::from_slice(doc).unwrap();
            json_doc["id"].as_i64().unwrap()
        })
        .collect()
}

#[tokio::test]
async fn test_parquet_file_source() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filepath = temp_dir.path().join("test.parquet");
    write_parquet_file(&filepath, 10, 4);
    let file_uri = Uri::from_str(filepath.to_str().unwrap()).unwrap();
    let index_uid = IndexUid::new_with_random_ulid("test-index");

    let plan = ParquetLoadPlan::try_new(file_uri.clone(), &SourceCheckpoint::default(), 3).unwrap();
    assert_eq!(plan.num_rows(), 10);
    assert_eq!(plan.num_rows_to_index(), 10);
    assert_eq!(plan.num_row_groups(), 3);
    let plan_guard =
        register_parquet_load_plan(index_uid.clone(), "test-parquet-source".to_string(), plan)
            .unwrap();

    let universe = Universe::with_accelerated_time();
    let (doc_processor_mailbox, doc_processor_inbox) =
        universe.create_test_mailbox::<DocProcessor>();
    let source_runtime = parquet_source_runtime(index_uid, &file_uri, None);
    let params = FileSourceParams::Filepath(file_uri.clone());
    let file_source = FileSourceFactory::typed_create_source(source_runtime, params)
        .await
        .unwrap();
    let source_actor = SourceActor::new(Box::new(file_source), doc_processor_mailbox);
    let (_source_mailbox, source_handle) = universe.spawn_builder().spawn(source_actor);
    let (exit_status, observable_state) = source_handle.join().await;
    assert!(exit_status.is_success());
    assert_eq!(observable_state["num_rows_emitted"], 10);

    let messages = doc_processor_inbox.drain_for_test();
    let raw_doc_batches: Vec<&RawDocBatch> = messages
        .iter()
        .filter_map(|message| message.downcast_ref::<RawDocBatch>())
        .collect();
    assert!(matches!(
        messages.last().unwrap().downcast_ref::<Command>().unwrap(),
        Command::ExitWithSuccess
    ));
    // Row groups of 4, 4 and 2 rows, read in batches of 3 rows.
    assert_eq!(raw_doc_batches.len(), 5);
    assert_eq!(collect_ids(&raw_doc_batches), (0..10).collect::<Vec<i64>>());

    let mut checkpoint_delta = SourceCheckpointDelta::default();
    for raw_doc_batch in &raw_doc_batches {
        checkpoint_delta
            .extend(raw_doc_batch.checkpoint_delta.clone())
            .unwrap();
    }
    let checkpoint = checkpoint_delta.get_source_checkpoint();
    for (row_group_idx, num_rows) in [(0, 4u64), (1, 4), (2, 2)] {
        let partition_id = row_group_partition_id(&file_uri, row_group_idx);
        assert_eq!(
            checkpoint.position_for_partition(&partition_id).unwrap(),
            &Position::eof(num_rows)
        );
    }
    let first_delta: Vec<_> = raw_doc_batches[0].checkpoint_delta.iter().collect();
    assert_eq!(first_delta.len(), 1);
    assert_eq!(first_delta[0].1.from, Position::Beginning);
    assert_eq!(first_delta[0].1.to, Position::offset(3u64));

    let progress = plan_guard.plan().progress();
    assert_eq!(progress.num_row_groups_emitted, 3);
    assert_eq!(progress.num_row_groups_pending, 0);
}

#[tokio::test]
async fn test_parquet_file_source_resumes_from_checkpoint() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filepath = temp_dir.path().join("test.parquet");
    write_parquet_file(&filepath, 10, 4);
    let file_uri = Uri::from_str(filepath.to_str().unwrap()).unwrap();
    let index_uid = IndexUid::new_with_random_ulid("test-index");

    // Row group 0 is fully indexed, row group 1 is indexed up to its 3rd row.
    let mut checkpoint_delta = SourceCheckpointDelta::default();
    checkpoint_delta
        .record_partition_delta(
            row_group_partition_id(&file_uri, 0),
            Position::Beginning,
            Position::eof(4u64),
        )
        .unwrap();
    checkpoint_delta
        .record_partition_delta(
            row_group_partition_id(&file_uri, 1),
            Position::Beginning,
            Position::offset(3u64),
        )
        .unwrap();
    let checkpoint = checkpoint_delta.get_source_checkpoint();
    let plan = ParquetLoadPlan::try_new(file_uri.clone(), &checkpoint, 100).unwrap();
    assert_eq!(plan.progress().num_row_groups_skipped, 1);
    assert_eq!(plan.num_rows_to_index(), 1 + 2);
    let _plan_guard =
        register_parquet_load_plan(index_uid.clone(), "test-parquet-source".to_string(), plan)
            .unwrap();

    let universe = Universe::with_accelerated_time();
    let (doc_processor_mailbox, doc_processor_inbox) =
        universe.create_test_mailbox::<DocProcessor>();
    let source_runtime = parquet_source_runtime(index_uid, &file_uri, Some(checkpoint_delta));
    let params = FileSourceParams::Filepath(file_uri.clone());
    let file_source = FileSourceFactory::typed_create_source(source_runtime, params)
        .await
        .unwrap();
    let source_actor = SourceActor::new(Box::new(file_source), doc_processor_mailbox);
    let (_source_mailbox, source_handle) = universe.spawn_builder().spawn(source_actor);
    let (exit_status, _) = source_handle.join().await;
    assert!(exit_status.is_success());

    let messages = doc_processor_inbox.drain_for_test();
    let raw_doc_batches: Vec<&RawDocBatch> = messages
        .iter()
        .filter_map(|message| message.downcast_ref::<RawDocBatch>())
        .collect();
    assert_eq!(collect_ids(&raw_doc_batches), vec![7, 8, 9]);
    let resumed_delta: Vec<(PartitionId, _)> = raw_doc_batches[0].checkpoint_delta.iter().collect();
    assert_eq!(resumed_delta[0].1.from, Position::offset(3u64));
    assert_eq!(resumed_delta[0].1.to, Position::eof(4u64));
}

#[tokio::test]
async fn test_parquet_file_source_requires_a_plan() {
    let file_uri = Uri::for_test("file:///does-not-exist.parquet");
    let index_uid = IndexUid::new_with_random_ulid("test-index");
    let source_runtime = parquet_source_runtime(index_uid, &file_uri, None);
    let params = FileSourceParams::Filepath(file_uri);
    let error = FileSourceFactory::typed_create_source(source_runtime, params)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("quickwit tool local-ingest"));
}

#[test]
fn test_parquet_load_plan_rejects_remote_files() {
    let file_uri = Uri::for_test("s3://bucket/test.parquet");
    let error = ParquetLoadPlan::try_new(file_uri, &SourceCheckpoint::default(), 10).unwrap_err();
    assert!(error.to_string().contains("only supports local files"));
}

#[test]
fn test_register_parquet_load_plan_twice_fails() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filepath = temp_dir.path().join("test.parquet");
    write_parquet_file(&filepath, 3, 2);
    let file_uri = Uri::from_str(filepath.to_str().unwrap()).unwrap();
    let index_uid = IndexUid::new_with_random_ulid("test-index");
    let source_id = "test-parquet-source".to_string();

    let new_plan =
        || ParquetLoadPlan::try_new(file_uri.clone(), &SourceCheckpoint::default(), 10).unwrap();
    let plan_guard =
        register_parquet_load_plan(index_uid.clone(), source_id.clone(), new_plan()).unwrap();
    register_parquet_load_plan(index_uid.clone(), source_id.clone(), new_plan()).unwrap_err();
    drop(plan_guard);
    let _plan_guard = register_parquet_load_plan(index_uid, source_id, new_plan()).unwrap();
}
