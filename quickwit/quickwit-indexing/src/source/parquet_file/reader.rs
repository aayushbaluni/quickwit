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

use std::fs::File;
use std::sync::Arc;

use anyhow::{Context, bail};
use bytes::Bytes;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_common::runtimes::RuntimeType;
use quickwit_metastore::checkpoint::SourceCheckpoint;
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::{PipelineUid, Position};
use serde_json::json;

use super::ndjson::record_batch_to_ndjson_docs;
use super::plan::{ParquetLoadPlan, get_parquet_load_plan};
use crate::actors::DocProcessor;
use crate::source::{BATCH_NUM_BYTES_LIMIT, BatchBuilder, SourceContext, SourceRuntime};

/// Reads the row group of the current Parquet file.
struct RowGroupCursor {
    row_group_idx: usize,
    /// Number of rows of the row group already emitted (or indexed by a previous run).
    num_rows_emitted: u64,
    /// `None` while the reader is lent to the decoding task.
    reader_opt: Option<ParquetRecordBatchReader>,
}

/// Reads the rows of the row groups leased by one pipeline from a shared [`ParquetLoadPlan`] and
/// emits them as NDJSON documents.
pub(crate) struct ParquetFileReader {
    plan: Arc<ParquetLoadPlan>,
    pipeline_uid: PipelineUid,
    /// Published checkpoint when the source was created. Since a row group stays attached to the
    /// same pipeline until it is published, this checkpoint is up to date for all the row groups
    /// owned by this pipeline when they are opened.
    checkpoint: SourceCheckpoint,
    cursor_opt: Option<RowGroupCursor>,
    num_rows_emitted: u64,
    num_bytes_emitted: u64,
}

impl ParquetFileReader {
    pub async fn try_new(source_runtime: &SourceRuntime) -> anyhow::Result<Self> {
        let plan = get_parquet_load_plan(source_runtime.index_uid(), source_runtime.source_id())?;
        let checkpoint = source_runtime.fetch_checkpoint().await?;
        let pipeline_uid = source_runtime.pipeline_uid();
        plan.queue.lock().unwrap().attach_pipeline(pipeline_uid);
        Ok(Self {
            plan,
            pipeline_uid,
            checkpoint,
            cursor_opt: None,
            num_rows_emitted: 0,
            num_bytes_emitted: 0,
        })
    }

    /// Opens the next row group to read. Returns `None` when no row group is left.
    fn open_next_row_group(&mut self) -> anyhow::Result<Option<RowGroupCursor>> {
        loop {
            let Some(row_group_idx) = self
                .plan
                .queue
                .lock()
                .unwrap()
                .current_or_lease(self.pipeline_uid)
            else {
                return Ok(None);
            };
            let row_group = &self.plan.row_groups[row_group_idx];
            let position = self
                .checkpoint
                .position_for_partition(&row_group.partition_id)
                .cloned()
                .unwrap_or_default();
            let num_rows_indexed = match &position {
                Position::Beginning => 0,
                Position::Offset(_) => position
                    .as_u64()
                    .context("invalid Parquet checkpoint position")?,
                Position::Eof(_) => {
                    self.plan
                        .queue
                        .lock()
                        .unwrap()
                        .mark_current_published(self.pipeline_uid, row_group_idx);
                    continue;
                }
            };
            if num_rows_indexed >= row_group.num_rows {
                bail!(
                    "checkpoint position {num_rows_indexed} of partition `{}` is past the end of \
                     the row group ({} rows)",
                    row_group.partition_id,
                    row_group.num_rows
                );
            }
            let file = File::open(&self.plan.filepath).with_context(|| {
                format!("failed to open file `{}`", self.plan.filepath.display())
            })?;
            let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
                file,
                self.plan.arrow_metadata.clone(),
            )
            .with_row_groups(vec![row_group_idx])
            .with_offset(num_rows_indexed as usize)
            .with_batch_size(self.plan.batch_num_rows)
            .build()
            .with_context(|| {
                format!(
                    "failed to read Parquet row group `{}`",
                    row_group.partition_id
                )
            })?;
            return Ok(Some(RowGroupCursor {
                row_group_idx,
                num_rows_emitted: num_rows_indexed,
                reader_opt: Some(reader),
            }));
        }
    }

    /// Decodes one Arrow record batch and sends it to the doc processor, split into batches of at
    /// most `BATCH_NUM_BYTES_LIMIT` bytes.
    ///
    /// Returns `false` once all the row groups have been emitted.
    pub async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<bool, ActorExitStatus> {
        if self.cursor_opt.is_none() {
            self.cursor_opt = self.open_next_row_group()?;
        }
        let Some(cursor) = &mut self.cursor_opt else {
            return Ok(false);
        };
        let row_group = &self.plan.row_groups[cursor.row_group_idx];
        let reader = cursor
            .reader_opt
            .take()
            .expect("the reader should be returned by the decoding task");
        let (reader, docs_res) = ctx.protect_future(decode_next_batch(reader)).await?;
        cursor.reader_opt = Some(reader);

        let Some(docs) = docs_res? else {
            return Err(anyhow::anyhow!(
                "Parquet row group `{}` ended after {} rows, expected {}",
                row_group.partition_id,
                cursor.num_rows_emitted,
                row_group.num_rows
            )
            .into());
        };
        let num_rows_after_batch = cursor.num_rows_emitted + docs.len() as u64;
        if num_rows_after_batch > row_group.num_rows {
            return Err(anyhow::anyhow!(
                "Parquet row group `{}` has more rows than the {} declared in the footer",
                row_group.partition_id,
                row_group.num_rows
            )
            .into());
        }
        let mut batch_builder = BatchBuilder::new(SourceType::File);
        let num_docs = docs.len();

        for (doc_idx, doc) in docs.into_iter().enumerate() {
            batch_builder.add_doc(doc);
            let is_last_doc = doc_idx + 1 == num_docs;

            if !is_last_doc && batch_builder.num_bytes < BATCH_NUM_BYTES_LIMIT {
                continue;
            }
            let from_position = if cursor.num_rows_emitted == 0 {
                Position::Beginning
            } else {
                Position::offset(cursor.num_rows_emitted)
            };
            let num_rows_emitted = cursor.num_rows_emitted + batch_builder.docs.len() as u64;
            let to_position = if num_rows_emitted == row_group.num_rows {
                Position::eof(num_rows_emitted)
            } else {
                Position::offset(num_rows_emitted)
            };
            batch_builder
                .checkpoint_delta
                .record_partition_delta(row_group.partition_id.clone(), from_position, to_position)
                .context("failed to record Parquet checkpoint delta")?;
            self.num_rows_emitted += batch_builder.docs.len() as u64;
            self.num_bytes_emitted += batch_builder.num_bytes;
            cursor.num_rows_emitted = num_rows_emitted;

            let raw_doc_batch =
                std::mem::replace(&mut batch_builder, BatchBuilder::new(SourceType::File)).build();
            ctx.send_message(doc_processor_mailbox, raw_doc_batch)
                .await?;
        }
        if cursor.num_rows_emitted == row_group.num_rows {
            self.plan
                .queue
                .lock()
                .unwrap()
                .mark_emitted(self.pipeline_uid, cursor.row_group_idx);
            self.cursor_opt = None;
        }
        Ok(true)
    }

    /// Records the row groups whose rows have all been published.
    pub fn suggest_truncate(&mut self, checkpoint: &SourceCheckpoint) {
        let mut queue = self.plan.queue.lock().unwrap();
        for (partition_id, position) in checkpoint.iter() {
            if !position.is_eof() {
                continue;
            }
            if let Some(row_group_idx) = self.plan.row_group_idx_by_partition.get(&partition_id) {
                queue.mark_emitted_published(self.pipeline_uid, *row_group_idx);
            }
        }
    }

    pub fn observable_state(&self) -> serde_json::Value {
        json!({
            "num_rows_emitted": self.num_rows_emitted,
            "num_bytes_emitted": self.num_bytes_emitted,
            "current_row_group": self.cursor_opt.as_ref().map(|cursor| cursor.row_group_idx),
        })
    }
}

type DecodeResult = anyhow::Result<Option<Vec<Bytes>>>;

/// Decodes the next record batch on the blocking runtime: Parquet decoding and JSON encoding are
/// CPU-intensive, and sources run on the non-blocking runtime.
async fn decode_next_batch(
    mut reader: ParquetRecordBatchReader,
) -> anyhow::Result<(ParquetRecordBatchReader, DecodeResult)> {
    let join_handle = RuntimeType::Blocking
        .get_runtime_handle()
        .spawn(async move {
            let docs_res = match reader.next() {
                Some(Ok(record_batch)) => record_batch_to_ndjson_docs(&record_batch).map(Some),
                Some(Err(error)) => {
                    Err(anyhow::Error::from(error).context("failed to decode Parquet"))
                }
                None => Ok(None),
            };
            (reader, docs_res)
        });
    join_handle
        .await
        .context("Parquet decoding task panicked or was cancelled")
}
