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

#![cfg(feature = "parquet")]

use std::num::NonZeroUsize;

use quickwit_cli::tool::{LocalIngestDocsArgs, local_ingest_docs_cli};
use quickwit_common::rand::append_random_suffix;
use quickwit_common::uri::Uri;
use quickwit_config::{
    CLI_SOURCE_ID, ConfigFormat, SourceInputFormat, load_index_config_from_user_config,
};
use quickwit_index_management::IndexService;
use quickwit_indexing::merge_policy::merge_policy_from_settings;
use quickwit_indexing::source::parquet_file::write_json_docs_as_parquet_file;
use quickwit_metastore::{
    ListSplitsQuery, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitMetadata,
    SplitState,
};
use quickwit_proto::metastore::{ListSplitsRequest, MetastoreService};
use serde_json::json;

use crate::helpers::{TestStorageType, create_test_env, uri_from_path};

#[allow(dead_code)]
mod helpers;

const NUM_ROWS: usize = 1_000;
const ROW_GROUP_NUM_ROWS: usize = 50;

/// Merges splits two by two until a single split is left: the fixpoint is reached after several
/// merge levels.
const INDEX_CONFIG: &str = r#"
    version: 0.8
    index_id: #index_id
    index_uri: #index_uri
    doc_mapping:
      field_mappings:
        - name: ts
          type: datetime
          input_formats: [unix_timestamp]
          fast: true
        - name: event
          type: text
        - name: city
          type: text
          tokenizer: raw
      timestamp_field: ts
    indexing_settings:
      commit_timeout_secs: 1
      split_num_docs_target: 10000
      merge_policy:
        type: stable_log
        merge_factor: 2
        max_merge_factor: 2
        min_level_num_docs: 100000
        maturation_period: 1h
      resources:
        heap_size: 50MB
"#;

#[tokio::test]
async fn test_local_ingest_parquet_with_multiple_pipelines() {
    quickwit_common::setup_logging_for_tests();
    let index_id = append_random_suffix("test-parquet-local-ingest");
    let test_env = create_test_env(index_id.clone(), TestStorageType::LocalFileSystem)
        .await
        .unwrap();
    let index_config_yaml = INDEX_CONFIG
        .replace("#index_id", &index_id)
        .replace("#index_uri", test_env.index_uri.as_str());
    let index_config = load_index_config_from_user_config(
        ConfigFormat::Yaml,
        index_config_yaml.as_bytes(),
        &test_env.index_uri,
    )
    .unwrap();
    let metastore = test_env.metastore().await;
    let mut index_service = IndexService::new(metastore.clone(), test_env.storage_resolver.clone());
    let index_metadata = index_service
        .create_index(index_config.clone(), false)
        .await
        .unwrap();

    let json_docs: Vec<serde_json::Value> = (0..NUM_ROWS)
        .map(|row_idx| {
            let city = ["paris", "tokyo", "london"][row_idx % 3];
            json!({
                "ts": 1_700_000_000 + row_idx as i64,
                "event": format!("event-{row_idx}"),
                "city": city,
            })
        })
        .collect();
    let parquet_path = test_env.data_dir_path.join("docs.parquet");
    write_json_docs_as_parquet_file(&parquet_path, &json_docs, ROW_GROUP_NUM_ROWS).unwrap();
    let parquet_uri: Uri = uri_from_path(&parquet_path);

    let args = LocalIngestDocsArgs {
        config_uri: test_env.resource_files.config.clone(),
        index_id: index_id.clone(),
        input_path_opt: Some(parquet_uri.clone()),
        input_format: SourceInputFormat::Parquet,
        overwrite: false,
        vrl_script: None,
        clear_cache: true,
        num_pipelines: NonZeroUsize::new(4).unwrap(),
        batch_num_rows_opt: NonZeroUsize::new(16),
    };
    local_ingest_docs_cli(args).await.unwrap();

    let query = ListSplitsQuery::for_index(index_metadata.index_uid.clone())
        .with_split_state(SplitState::Published);
    let mut splits: Vec<SplitMetadata> = metastore
        .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
        .await
        .unwrap()
        .collect_splits_metadata()
        .await
        .unwrap();
    let num_docs: usize = splits.iter().map(|split| split.num_docs).sum();
    assert_eq!(num_docs, NUM_ROWS);
    assert_eq!(splits.len(), 1);

    // The merges reached a fixpoint: the merge policy proposes no more merge.
    let merge_policy = merge_policy_from_settings(&index_config.indexing_settings);
    assert!(merge_policy.operations(&mut splits).is_empty());

    // Every row group is fully indexed.
    let index_metadata = test_env.index_metadata().await.unwrap();
    let checkpoint = index_metadata
        .checkpoint
        .source_checkpoint(CLI_SOURCE_ID)
        .unwrap();
    let partitions: Vec<_> = checkpoint.iter().collect();
    assert_eq!(partitions.len(), NUM_ROWS / ROW_GROUP_NUM_ROWS);
    assert!(partitions.iter().all(|(_, position)| position.is_eof()));

    // Ingesting the same file again is a no-op.
    let args = LocalIngestDocsArgs {
        config_uri: test_env.resource_files.config.clone(),
        index_id,
        input_path_opt: Some(parquet_uri),
        input_format: SourceInputFormat::Parquet,
        overwrite: false,
        vrl_script: None,
        clear_cache: true,
        num_pipelines: NonZeroUsize::new(2).unwrap(),
        batch_num_rows_opt: None,
    };
    local_ingest_docs_cli(args).await.unwrap();
    let num_splits = metastore
        .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
        .await
        .unwrap()
        .collect_splits_metadata()
        .await
        .unwrap()
        .len();
    assert_eq!(num_splits, 1);
}
