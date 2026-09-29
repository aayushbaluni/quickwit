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

//! Load plan shared by the indexing pipelines of a Parquet bulk load.
//!
//! A load plan splits one local Parquet file into work units (row groups). The row groups are
//! distributed dynamically to the pipelines through a [`RowGroupQueue`]. Each row group is a
//! checkpoint partition, identified by `{file_uri}#rg={row_group_idx}`, whose position is the
//! number of rows indexed so far.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex};

use anyhow::{Context, bail};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use quickwit_common::uri::{Protocol, Uri};
use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpoint};
use quickwit_proto::types::{IndexUid, PipelineUid, Position, SourceId};
use serde::Serialize;

use super::ndjson::record_batch_to_ndjson_docs;

/// Default number of rows decoded per Arrow record batch.
pub const DEFAULT_PARQUET_BATCH_NUM_ROWS: usize = 8192;

pub(super) fn row_group_partition_id(file_uri: &Uri, row_group_idx: usize) -> PartitionId {
    PartitionId::from(format!("{file_uri}#rg={row_group_idx}"))
}

#[derive(Debug)]
pub(super) struct ParquetRowGroup {
    pub partition_id: PartitionId,
    pub num_rows: u64,
}

/// Progress of a Parquet load, aggregated over all the pipelines.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ParquetLoadProgress {
    /// Number of row groups in the file.
    pub num_row_groups: usize,
    /// Number of row groups that were already indexed when the plan was created.
    pub num_row_groups_skipped: usize,
    /// Number of row groups that have not been handed to any pipeline yet.
    pub num_row_groups_pending: usize,
    /// Number of row groups whose rows have all been emitted, but not all published yet.
    pub num_row_groups_emitted: usize,
    /// Number of row groups whose rows have all been published during this load, as reported to
    /// the sources. Best effort: a source exits after emitting its last batch, so the publication
    /// of its last row groups is not reported.
    pub num_row_groups_published: usize,
}

/// Distributes the row groups of a load plan to the pipelines.
///
/// A row group is in exactly one of the following states:
/// - pending: not handed to any pipeline yet;
/// - owned by a pipeline: handed to the pipeline, some of its rows may have been emitted;
/// - emitted by a pipeline: all its rows have been emitted by the pipeline, but not all published;
/// - published: all its rows have been published.
///
/// Once handed to a pipeline, a row group stays attached to it until it is published. This
/// guarantees that a single pipeline at a time writes to the corresponding checkpoint partition.
/// When a pipeline restarts, all the row groups it owned or emitted are handed back to it and
/// resumed from the published checkpoint.
#[derive(Debug, Default)]
pub(super) struct RowGroupQueue {
    pending: VecDeque<usize>,
    owned: HashMap<PipelineUid, VecDeque<usize>>,
    emitted: HashMap<PipelineUid, BTreeSet<usize>>,
    num_row_groups: usize,
    num_row_groups_skipped: usize,
    num_row_groups_published: usize,
}

impl RowGroupQueue {
    fn new(num_row_groups: usize, pending: VecDeque<usize>) -> Self {
        let num_row_groups_skipped = num_row_groups - pending.len();
        Self {
            pending,
            num_row_groups,
            num_row_groups_skipped,
            ..Default::default()
        }
    }

    /// Called when the source of a pipeline is (re)created: every row group owned or emitted by a
    /// previous incarnation of the pipeline must be processed again, starting from the published
    /// checkpoint.
    pub fn attach_pipeline(&mut self, pipeline_uid: PipelineUid) {
        let emitted = self.emitted.remove(&pipeline_uid).unwrap_or_default();
        let owned = self.owned.entry(pipeline_uid).or_default();
        let mut row_group_idxs: BTreeSet<usize> = owned.drain(..).collect();
        row_group_idxs.extend(emitted);
        owned.extend(row_group_idxs);
    }

    /// Returns the row group the pipeline should work on, leasing a new one if necessary.
    ///
    /// The same row group is returned until it is marked as emitted or published.
    pub fn current_or_lease(&mut self, pipeline_uid: PipelineUid) -> Option<usize> {
        let owned = self.owned.entry(pipeline_uid).or_default();
        if let Some(row_group_idx) = owned.front() {
            return Some(*row_group_idx);
        }
        let row_group_idx = self.pending.pop_front()?;
        owned.push_back(row_group_idx);
        Some(row_group_idx)
    }

    /// Records that all the rows of the current row group of the pipeline have been emitted.
    pub fn mark_emitted(&mut self, pipeline_uid: PipelineUid, row_group_idx: usize) {
        let owned = self.owned.entry(pipeline_uid).or_default();
        assert_eq!(
            owned.pop_front(),
            Some(row_group_idx),
            "row group must be the current row group of the pipeline"
        );
        self.emitted
            .entry(pipeline_uid)
            .or_default()
            .insert(row_group_idx);
    }

    /// Records that the current row group of the pipeline was already fully published by a
    /// previous incarnation of the pipeline.
    pub fn mark_current_published(&mut self, pipeline_uid: PipelineUid, row_group_idx: usize) {
        let owned = self.owned.entry(pipeline_uid).or_default();
        assert_eq!(
            owned.pop_front(),
            Some(row_group_idx),
            "row group must be the current row group of the pipeline"
        );
        self.num_row_groups_published += 1;
    }

    /// Records that all the rows of a row group emitted by the pipeline have been published.
    pub fn mark_emitted_published(&mut self, pipeline_uid: PipelineUid, row_group_idx: usize) {
        let Some(emitted) = self.emitted.get_mut(&pipeline_uid) else {
            return;
        };
        if emitted.remove(&row_group_idx) {
            self.num_row_groups_published += 1;
        }
    }

    pub fn progress(&self) -> ParquetLoadProgress {
        ParquetLoadProgress {
            num_row_groups: self.num_row_groups,
            num_row_groups_skipped: self.num_row_groups_skipped,
            num_row_groups_pending: self.pending.len(),
            num_row_groups_emitted: self.emitted.values().map(BTreeSet::len).sum(),
            num_row_groups_published: self.num_row_groups_published,
        }
    }
}

/// A plan to load one local Parquet file with several indexing pipelines.
///
/// A plan must be registered with [`register_parquet_load_plan`] before the pipelines are
/// spawned. The file source of each pipeline looks up the plan registered for its index and
/// source.
pub struct ParquetLoadPlan {
    pub(super) file_uri: Uri,
    pub(super) filepath: PathBuf,
    pub(super) arrow_metadata: ArrowReaderMetadata,
    pub(super) row_groups: Vec<ParquetRowGroup>,
    pub(super) row_group_idx_by_partition: HashMap<PartitionId, usize>,
    pub(super) batch_num_rows: usize,
    pub(super) queue: Mutex<RowGroupQueue>,
    /// Number of rows that were not indexed yet according to the checkpoint used to create the
    /// plan.
    num_rows_to_index: u64,
}

impl std::fmt::Debug for ParquetLoadPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("ParquetLoadPlan")
            .field("file_uri", &self.file_uri)
            .field("num_row_groups", &self.row_groups.len())
            .field("batch_num_rows", &self.batch_num_rows)
            .finish()
    }
}

impl ParquetLoadPlan {
    /// Reads the footer of the Parquet file and creates a plan with all the row groups that are
    /// not fully indexed according to `checkpoint`.
    ///
    /// This function performs blocking IO.
    pub fn try_new(
        file_uri: Uri,
        checkpoint: &SourceCheckpoint,
        batch_num_rows: usize,
    ) -> anyhow::Result<Self> {
        if file_uri.protocol() != Protocol::File {
            bail!("Parquet input only supports local files, got `{file_uri}`");
        }
        if batch_num_rows == 0 {
            bail!("Parquet batch number of rows must be strictly positive");
        }
        let filepath = file_uri
            .filepath()
            .context("failed to extract file path from URI")?
            .to_path_buf();
        let file = File::open(&filepath)
            .with_context(|| format!("failed to open file `{}`", filepath.display()))?;
        let arrow_metadata = ArrowReaderMetadata::load(&file, Default::default())
            .with_context(|| format!("failed to read Parquet footer of `{file_uri}`"))?;

        let mut row_groups = Vec::new();
        let mut row_group_idx_by_partition = HashMap::new();
        let mut pending = VecDeque::new();
        let mut num_rows_to_index = 0;

        for (row_group_idx, row_group_metadata) in
            arrow_metadata.metadata().row_groups().iter().enumerate()
        {
            let num_rows = u64::try_from(row_group_metadata.num_rows())
                .context("Parquet row group has a negative number of rows")?;
            let partition_id = row_group_partition_id(&file_uri, row_group_idx);
            let position = checkpoint
                .position_for_partition(&partition_id)
                .cloned()
                .unwrap_or_default();
            let num_rows_indexed = match &position {
                Position::Beginning => 0,
                Position::Offset(_) => position
                    .as_u64()
                    .context("invalid Parquet checkpoint position")?,
                Position::Eof(_) => num_rows,
            };
            if num_rows_indexed > num_rows {
                bail!(
                    "checkpoint position {num_rows_indexed} of partition `{partition_id}` is past \
                     the end of the row group ({num_rows} rows)"
                );
            }
            if num_rows > 0 && !position.is_eof() {
                pending.push_back(row_group_idx);
                num_rows_to_index += num_rows - num_rows_indexed;
            }
            row_group_idx_by_partition.insert(partition_id.clone(), row_group_idx);
            row_groups.push(ParquetRowGroup {
                partition_id,
                num_rows,
            });
        }
        let queue = RowGroupQueue::new(row_groups.len(), pending);
        let plan = Self {
            file_uri,
            filepath,
            arrow_metadata,
            row_groups,
            row_group_idx_by_partition,
            batch_num_rows,
            queue: Mutex::new(queue),
            num_rows_to_index,
        };
        plan.check_json_conversion()?;
        Ok(plan)
    }

    /// Fails early if the rows of the file cannot be converted to JSON.
    fn check_json_conversion(&self) -> anyhow::Result<()> {
        let Some(row_group_idx) = self
            .row_groups
            .iter()
            .position(|row_group| row_group.num_rows > 0)
        else {
            return Ok(());
        };
        let file = File::open(&self.filepath)?;
        let mut reader =
            ParquetRecordBatchReaderBuilder::new_with_metadata(file, self.arrow_metadata.clone())
                .with_row_groups(vec![row_group_idx])
                .with_batch_size(1)
                .with_limit(1)
                .build()?;
        if let Some(record_batch) = reader.next() {
            record_batch_to_ndjson_docs(&record_batch?)
                .with_context(|| format!("unsupported Parquet schema in `{}`", self.file_uri))?;
        }
        Ok(())
    }

    pub fn file_uri(&self) -> &Uri {
        &self.file_uri
    }

    /// Total number of rows in the file, including the rows already indexed.
    pub fn num_rows(&self) -> u64 {
        self.row_groups
            .iter()
            .map(|row_group| row_group.num_rows)
            .sum()
    }

    /// Number of rows that were not indexed yet when the plan was created.
    pub fn num_rows_to_index(&self) -> u64 {
        self.num_rows_to_index
    }

    pub fn num_row_groups(&self) -> usize {
        self.row_groups.len()
    }

    /// Uncompressed size of the file, as reported by the footer.
    pub fn num_uncompressed_bytes(&self) -> u64 {
        self.arrow_metadata
            .metadata()
            .row_groups()
            .iter()
            .map(|row_group| row_group.total_byte_size().max(0) as u64)
            .sum()
    }

    pub fn progress(&self) -> ParquetLoadProgress {
        self.queue.lock().unwrap().progress()
    }
}

type PlanKey = (IndexUid, SourceId);

static PARQUET_LOAD_PLANS: LazyLock<Mutex<HashMap<PlanKey, Arc<ParquetLoadPlan>>>> =
    LazyLock::new(Default::default);

/// Keeps a load plan registered. The plan is unregistered when the guard is dropped.
#[derive(Debug)]
#[must_use = "the plan is unregistered when the guard is dropped"]
pub struct ParquetLoadPlanGuard {
    key: PlanKey,
    plan: Arc<ParquetLoadPlan>,
}

impl ParquetLoadPlanGuard {
    pub fn plan(&self) -> &Arc<ParquetLoadPlan> {
        &self.plan
    }
}

impl Drop for ParquetLoadPlanGuard {
    fn drop(&mut self) {
        PARQUET_LOAD_PLANS.lock().unwrap().remove(&self.key);
    }
}

/// Registers the plan used by the file sources of `(index_uid, source_id)`. Fails if a plan is
/// already registered for that source.
pub fn register_parquet_load_plan(
    index_uid: IndexUid,
    source_id: SourceId,
    plan: ParquetLoadPlan,
) -> anyhow::Result<ParquetLoadPlanGuard> {
    let key = (index_uid, source_id);
    let plan = Arc::new(plan);
    let mut plans = PARQUET_LOAD_PLANS.lock().unwrap();
    if plans.contains_key(&key) {
        bail!(
            "a Parquet load plan is already registered for index `{}` and source `{}`",
            key.0,
            key.1
        );
    }
    plans.insert(key.clone(), plan.clone());
    Ok(ParquetLoadPlanGuard { key, plan })
}

pub(super) fn get_parquet_load_plan(
    index_uid: &IndexUid,
    source_id: &str,
) -> anyhow::Result<Arc<ParquetLoadPlan>> {
    let key = (index_uid.clone(), source_id.to_string());
    PARQUET_LOAD_PLANS
        .lock()
        .unwrap()
        .get(&key)
        .cloned()
        .context("the `parquet` input format is only supported by `quickwit tool local-ingest`")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_group_queue_distributes_row_groups() {
        let mut queue = RowGroupQueue::new(4, VecDeque::from([0, 2, 3]));
        let pipeline_a = PipelineUid::for_test(1);
        let pipeline_b = PipelineUid::for_test(2);
        queue.attach_pipeline(pipeline_a);
        queue.attach_pipeline(pipeline_b);

        assert_eq!(queue.current_or_lease(pipeline_a), Some(0));
        // The current row group is returned until it is emitted.
        assert_eq!(queue.current_or_lease(pipeline_a), Some(0));
        assert_eq!(queue.current_or_lease(pipeline_b), Some(2));
        queue.mark_emitted(pipeline_a, 0);
        assert_eq!(queue.current_or_lease(pipeline_a), Some(3));
        queue.mark_emitted(pipeline_a, 3);
        assert_eq!(queue.current_or_lease(pipeline_a), None);

        let progress = queue.progress();
        assert_eq!(progress.num_row_groups, 4);
        assert_eq!(progress.num_row_groups_skipped, 1);
        assert_eq!(progress.num_row_groups_pending, 0);
        assert_eq!(progress.num_row_groups_emitted, 2);
        assert_eq!(progress.num_row_groups_published, 0);

        queue.mark_emitted_published(pipeline_a, 0);
        // Publishing twice is idempotent.
        queue.mark_emitted_published(pipeline_a, 0);
        // Pipeline B does not own row group 3.
        queue.mark_emitted_published(pipeline_b, 3);
        assert_eq!(queue.progress().num_row_groups_published, 1);
    }

    #[test]
    fn test_row_group_queue_restarted_pipeline_resumes_its_row_groups() {
        let mut queue = RowGroupQueue::new(4, VecDeque::from([0, 1, 2, 3]));
        let pipeline_a = PipelineUid::for_test(1);
        let pipeline_b = PipelineUid::for_test(2);
        queue.attach_pipeline(pipeline_a);
        queue.attach_pipeline(pipeline_b);

        assert_eq!(queue.current_or_lease(pipeline_a), Some(0));
        queue.mark_emitted(pipeline_a, 0);
        assert_eq!(queue.current_or_lease(pipeline_a), Some(1));

        // Pipeline A restarts: its emitted and current row groups are handed back to it, in order.
        queue.attach_pipeline(pipeline_a);
        assert_eq!(queue.current_or_lease(pipeline_b), Some(2));
        assert_eq!(queue.current_or_lease(pipeline_a), Some(0));
        // Row group 0 was published before the restart.
        queue.mark_current_published(pipeline_a, 0);
        assert_eq!(queue.current_or_lease(pipeline_a), Some(1));
        queue.mark_emitted(pipeline_a, 1);
        assert_eq!(queue.current_or_lease(pipeline_a), Some(3));

        let progress = queue.progress();
        assert_eq!(progress.num_row_groups_pending, 0);
        assert_eq!(progress.num_row_groups_emitted, 1);
        assert_eq!(progress.num_row_groups_published, 1);
    }
}
