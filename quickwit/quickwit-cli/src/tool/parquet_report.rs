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

//! Report of a Parquet bulk load.

use std::time::Duration;

use anyhow::bail;
use bytesize::ByteSize;
use colored::Colorize;
use humantime::format_duration;
use quickwit_common::uri::Uri;
use quickwit_config::IndexConfig;
use quickwit_indexing::models::{IndexingStatistics, MergeStatistics};
use quickwit_metastore::SplitMetadata;
use thousands::Separable;

pub(super) fn is_database_metastore(metastore_uri: &Uri) -> bool {
    metastore_uri.protocol().is_database()
}

pub(super) struct LoadReport<'a> {
    pub num_pipelines: usize,
    pub batch_num_rows: usize,
    pub num_rows_to_index: u64,
    pub parquet_num_bytes: u64,
    pub indexing_statistics: IndexingStatistics,
    pub num_pipeline_restarts: usize,
    pub indexing_elapsed: Duration,
    pub total_elapsed: Duration,
    pub num_splits_after_indexing: usize,
    pub merge_statistics: MergeStatistics,
    pub final_splits: Vec<SplitMetadata>,
    pub index_config: &'a IndexConfig,
    pub merge_concurrency: usize,
    pub metastore_uri: &'a Uri,
}

impl LoadReport<'_> {
    pub fn print(&self) {
        let statistics = &self.indexing_statistics;
        let num_docs = statistics.num_docs;
        let ndjson_num_bytes = statistics.total_bytes_processed;
        let num_final_docs: usize = self.final_splits.iter().map(|split| split.num_docs).sum();
        let final_splits_num_bytes: u64 = self
            .final_splits
            .iter()
            .map(|split| split.footer_offsets.end)
            .sum();
        let indexing_settings = &self.index_config.indexing_settings;
        let metastore_type = if is_database_metastore(self.metastore_uri) {
            "PostgreSQL"
        } else {
            "file-backed"
        };
        println!();
        println!("{}", "Parquet bulk load report".bold());
        println!(
            "  Rows to index:        {}",
            self.num_rows_to_index.separate_with_commas()
        );
        println!(
            "  Parquet bytes:        {} (uncompressed)",
            ByteSize(self.parquet_num_bytes)
        );
        println!("  NDJSON bytes:         {}", ByteSize(ndjson_num_bytes));
        println!(
            "  Docs:                 {} processed, {} invalid",
            num_docs.separate_with_commas(),
            statistics.num_invalid_docs.separate_with_commas()
        );
        println!(
            "  Indexing:             {}, {}",
            format_elapsed(self.indexing_elapsed),
            format_throughput(num_docs, ndjson_num_bytes, self.indexing_elapsed)
        );
        println!(
            "  Indexing + merges:    {}, {}",
            format_elapsed(self.total_elapsed),
            format_throughput(num_docs, ndjson_num_bytes, self.total_elapsed)
        );
        println!(
            "  Splits:               {} after indexing, {} merge operations, {} final ({} docs, \
             {})",
            self.num_splits_after_indexing,
            // The merge uploader only uploads merged splits: one per merge operation. Published
            // merges are counted as replace operations, not as published splits.
            self.merge_statistics.num_uploaded_splits,
            self.final_splits.len(),
            num_final_docs.separate_with_commas(),
            ByteSize(final_splits_num_bytes),
        );
        println!("  Pipeline restarts:    {}", self.num_pipeline_restarts);
        println!(
            "  Settings:             num_pipelines={}, batch_num_rows={}, heap_size={}, \
             split_num_docs_target={}, commit_timeout_secs={}, merge_concurrency={}, \
             metastore={metastore_type}",
            self.num_pipelines,
            self.batch_num_rows,
            indexing_settings.resources.heap_size,
            indexing_settings.split_num_docs_target,
            indexing_settings.commit_timeout_secs,
            self.merge_concurrency,
        );
        println!(
            "  Merge policy:         {:?}",
            indexing_settings.merge_policy
        );
    }

    /// Fails if the load is not a valid benchmark run.
    pub fn check(&self) -> anyhow::Result<()> {
        let statistics = &self.indexing_statistics;
        if statistics.num_invalid_docs > 0 {
            bail!(
                "failed to index {} document(s)",
                statistics.num_invalid_docs.separate_with_commas()
            );
        }
        if self.num_pipeline_restarts > 0 {
            bail!(
                "indexing pipelines restarted {} time(s)",
                self.num_pipeline_restarts
            );
        }
        if statistics.num_docs != self.num_rows_to_index {
            bail!(
                "indexed {} documents but the Parquet file has {} rows to index",
                statistics.num_docs.separate_with_commas(),
                self.num_rows_to_index.separate_with_commas()
            );
        }
        Ok(())
    }
}

fn format_elapsed(elapsed: Duration) -> String {
    format_duration(Duration::from_millis(elapsed.as_millis() as u64)).to_string()
}

fn format_throughput(num_docs: u64, num_bytes: u64, elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64().max(f64::EPSILON);
    format!(
        "{:.0} docs/s, {:.2} MB/s",
        num_docs as f64 / secs,
        num_bytes as f64 / 1_000_000.0 / secs
    )
}
