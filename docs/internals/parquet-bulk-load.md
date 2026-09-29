# Design: multi-pipeline Parquet bulk load in `quickwit tool local-ingest`

Status: proposal (rev. 2)

## Goal

For single-machine benchmarks: load a Parquet file into a Quickwit index **as
fast as the machine allows**, by:

- bypassing the ingest API, ingesters, and the WAL;
- running **N indexing pipelines in parallel** in one process, all fed from the
  same Parquet file;
- going through the real indexing pipeline
  (`DocProcessor → Indexer → IndexSerializer → Packager → Uploader → Sequencer → Publisher`)
  and the real merge pipeline;
- running merges **until the merge policy proposes no more merges** (fixpoint),
  so the final split layout is the steady-state layout.

## Non-goals / constraints

- **No new `SourceType`.** No proto change, and no source that users can add to
  an index through the API.
- **Multi-pipeline is a `local-ingest` feature only.** It's not available to
  sources running under `quickwit run` or the control plane.
- **No multi-pipeline NDJSON.** `--num-pipelines > 1` requires
  `--input-format parquet`.
- Parquet support stays behind a Cargo feature that is off by default. #6809
  removed Parquet/DataFusion from the default build on purpose.
- Phase 1 reads local `file://` inputs only.

## Current state

- `local_ingest_docs_cli` (`quickwit-cli/src/tool.rs`) starts an
  `IndexingService` in-process with a fake cluster. It builds a `SourceConfig`
  with `source_id = CLI_SOURCE_ID` (`_ingest-cli-source`), `SourceParams::File`
  or `Stdin`, and `num_pipelines = 1`, and passes it **directly** to
  `SpawnPipeline`. This `SourceConfig` is never stored in the metastore.
  Checkpoints are written under `_ingest-cli-source`, which is registered when
  the index is created.
- `FileSource` reads NDJSON as one partition (`PartitionId = file uri`), so N
  pipelines on the same file would each read every document.
- `IndexingService` accepts several `SpawnPipeline` calls for the same
  `(index, source_id)` with distinct `PipelineUid`s. They share one merge
  pipeline (`MergePipelineId = (node_id, index_uid, source_id)`), the split
  cache, and the upload semaphore.
- **Merge shutdown doesn't reach a fixpoint.**
  `FinishPendingMergesAndShutdownPipeline` first sends
  `DisconnectMergePlanner` to the merge publisher, then
  `RunFinalizeMergePolicyAndQuit` to the planner. Splits produced by in-flight
  merges are never fed back to the planner. So the multi-level merges of
  `StableLogMergePolicy` and `ConstWriteAmplificationMergePolicy` stop after the
  current level, and `finalize_operations` runs once on top of that.

## Overview

```
quickwit tool local-ingest --index idx --input-path file:///data/x.parquet \
    --input-format parquet --num-pipelines N
        │
        ├─ builds ParquetLoadPlan (all row groups), registers it in-process
        │
        ├─ SpawnPipeline × N   (same SourceConfig: File + input_format=parquet, CLI_SOURCE_ID)
        │     FileSource(Parquet state) ─┐   pulls row groups from the shared queue
        │     FileSource(Parquet state) ─┼─► DocProcessor → Indexer → … → Publisher
        │     FileSource(Parquet state) ─┘
        │                                 └──► one shared MergePipeline
        ├─ wait for the N indexing pipelines          → t_indexed
        └─ DrainMergesAndShutdownPipeline (fixpoint)  → t_merged
```

## Detailed design

### 1. Configuration: `SourceInputFormat::Parquet`, no new source type

- Add `SourceInputFormat::Parquet` in `quickwit-config`. It only exists in
  config and isn't a proto enum.
- `SourceConfig` validation (`validate_and_build`) rejects `Parquet` for every
  source: "the `parquet` input format can only be used through the CLI command
  `quickwit tool local-ingest`". The CLI builds its `SourceConfig` in code,
  which skips this validation. In the file source, `Parquet` with SQS
  notifications is rejected as well.
- `FileSourceFactory::typed_create_source` chooses its reader from
  `input_format`:
  - `Parquet` creates a new `FileSourceState::Parquet { .. }`, see §3.
    Without the `parquet` feature, it fails with
    `bail!("Quickwit was compiled without the `parquet` feature")`, the same way
    `sqs` is handled today.
  - Any other format keeps the existing `FileSourceState::Filepath`.
- The Parquet reader emits NDJSON bytes (§4). The `DocProcessor` handles
  `Parquet` in the same match arms as `Json`, so `parse_raw_doc` is otherwise
  unchanged.

**Guard against server usage.** `FileSourceState::Parquet` needs a
`ParquetLoadPlan` registered for `(index_uid, source_id)` (§3). Only
`local-ingest` registers one. If a user adds a `file` source with
`input_format: parquet` to an index served by `quickwit run`, pipeline creation
fails with an explicit error: "the `parquet` input format is only supported by
`quickwit tool local-ingest`". Nothing falls back silently.

### 2. Dependencies

- Workspace deps with minimal features:
  `parquet = { workspace = true, default-features = false, features = ["arrow", "snap", "zstd", "lz4"] }`
  and `arrow-array` (with `chrono-tz`, to format timestamps with a named time
  zone), `arrow-json`, `arrow-schema` (matching major version).
- New feature `parquet` on `quickwit-indexing`, forwarded by `quickwit-cli`
  (which also enables `sysinfo` for the memory check). It's not in
  `release-feature-set`.
- Code layout: `quickwit-indexing/src/source/parquet_file/` (`plan.rs`,
  `reader.rs`, `ndjson.rs`, plus a `testsuite` helper that writes JSON docs as
  a Parquet file), and `quickwit-cli/src/tool/parquet_ingest.rs`.
- Run `make update-licenses` and `cargo check`.

### 3. Work distribution: `ParquetLoadPlan` / `RowGroupQueue`

The unit of work is a **Parquet row group**.

```rust
struct RowGroupQueue {
    /// Row groups not handed to any pipeline yet.
    pending: VecDeque<usize>,
    /// Row groups owned by each pipeline and not fully emitted yet. The front one
    /// is being read.
    owned: HashMap<PipelineUid, VecDeque<usize>>,
    /// Row groups fully emitted by each pipeline, but not known to be published.
    emitted: HashMap<PipelineUid, BTreeSet<usize>>,
    ..
}

/// Process-global. Only `local-ingest` registers plans in it, through a guard
/// that unregisters the plan on drop.
static PARQUET_LOAD_PLANS: LazyLock<Mutex<HashMap<(IndexUid, SourceId), Arc<ParquetLoadPlan>>>>;
```

`ParquetLoadPlan` holds the file path, the parsed footer
(`ArrowReaderMetadata`), the row groups, the batch size, and the
`Mutex<RowGroupQueue>`.

- `local-ingest` builds the plan **before** spawning pipelines. It reads the
  footer, lists the row groups, and removes the ones the checkpoint marks done
  (§5). It also checks the Arrow schema for unsupported types. No pipeline ever
  has to create the plan, so there's no race.
- Each `FileSource` (Parquet state) leases one row group at a time and streams
  it as RecordBatches. When it sends the last batch, it marks the row group
  complete and leases the next one.
- When the queue is empty and the current lease is done, the source sends
  `exit_with_success` to its `DocProcessor`, like `FileSource` does at EOF.
- **Invariant (documented in code):** a row group stays attached to the
  pipeline that leased it until it is published. Durability comes from the
  checkpoint; `SuggestTruncate` is only used to prune `emitted` and to report
  progress.
- **Pipeline restart:** `IndexingPipeline` re-creates the source with the same
  `PipelineUid`. The new source fetches the checkpoint and calls
  `attach_pipeline`, which moves its emitted row groups back into its owned
  ones, in order. Each is resumed from the *published* checkpoint position, and
  the ones already at EOF are skipped. For benchmark validity, `local-ingest`
  reports the number of restarts and exits non-zero if it's above 0.

Why the queue is dynamic: `SourceRuntime` has no pipeline ordinal, and row
groups vary in size. With static round-robin assignment, the slowest pipeline
would set the wall time.

We rejected N separate `source_id`s: merge pipelines are scoped per
`source_id`, so splits from different pipelines would never merge.

### 4. Reading and converting Parquet

- `ParquetRecordBatchReaderBuilder` on a local `std::fs::File`, built from the
  footer parsed once by the plan, configured with `.with_row_groups(vec![idx])`,
  `.with_batch_size(batch_num_rows)` (default 8192), and `.with_offset(n)` to
  skip the rows already checkpointed when resuming.
- `SourceActor` runs on the small `NonBlocking` runtime. So decoding and
  conversion run on the `Blocking` runtime (`spawn` on its handle, wrapped in
  `protect_future`). `run_cpu_intensive` is not used: its pool is small and
  meant for short tasks. The sync reader moves into the task and back.
- The plan trial-decodes one row of the first non-empty row group, so an
  unsupported schema fails before any pipeline is spawned.
- **Phase 1:** `arrow_json::LineDelimitedWriter`. Each line becomes one `Bytes` in
  a `BatchBuilder`, sent as a regular `RawDocBatch`. The `DocProcessor`, VRL,
  doc mapping, timestamp extraction, and error counters are unchanged, and the
  documents are exactly what the ingest API would get. The cost is serializing
  and then parsing JSON.
- **Phase 2 (only if profiling shows the JSON round trip matters):** a new
  `ArrowDocBatch` message that `DocProcessor` turns directly into
  `serde_json::Map` rows, reusing `process_json_doc`. It needs an explicit
  definition of `num_bytes`, and the report must state which mode was used.
- Batches are capped at `BATCH_NUM_BYTES_LIMIT` (5 MiB) of NDJSON: the docs of
  one decoded Arrow batch are split into several `RawDocBatch`es, each with its
  own checkpoint delta. Backpressure comes from the bounded `DocProcessor`
  mailbox (capacity 10).

Type mapping caveats, checked at plan time where possible:

| Arrow | JSON produced | Doc mapping requirement |
|---|---|---|
| `Timestamp` / `Date*` | RFC 3339 / date string; timestamps without a time zone get a `Z` suffix (read as UTC) | matching `input_formats` |
| `Binary` | hex string | `bytes` field with `input_format: hex` |
| `Struct` / `List` / `Map` | object / array | `object`, array, or `json` field |
| `Decimal` | number | map explicitly |
| null | key omitted | none |
| `Interval`, others | unsupported | error at plan time |

### 5. Checkpointing

- Source id: the existing `CLI_SOURCE_ID`. The checkpoint already exists in the
  metastore, so no new source is stored.
- `PartitionId = "{uri}#rg={row_group_idx}"`. `Position` is the number of rows
  already emitted, and the last batch of a row group ends at
  `Position::eof(num_rows)`.
- Only the pipeline holding the lease writes to a partition, so deltas from
  different pipelines never conflict in `try_apply_delta`.
- `--overwrite` clears the index and the checkpoint. Without it, a new run skips
  row groups at EOF and resumes partial ones.
- One partition per row group is about 1k for 1B rows with 1M-row groups. The
  CLI warns above 100k row groups, because the file-backed metastore rewrites the
  whole index metadata on every publish.

### 6. Merge fixpoint: `DrainMergesAndShutdownPipeline`

**Target semantics:** after the last indexing pipeline exits, the merge
pipeline keeps running. It re-plans every split that merges produce, and stops
once there is no ongoing merge and `MergePolicy::operations` returns nothing
for every partition. `finalize_operations` is **not** called. It's a
shutdown-time relaxation that would change the final layout, and we want to
measure the steady-state layout.

**Mechanism.** A new message `DrainMergesAndShutdownPipeline` for
`MergePipeline`. It marks the shutdown as initiated (actors are no longer
respawned) and starts a `MergeDrainer` (`actors/merge_drainer.rs`). It doesn't
send `DisconnectMergePlanner`: merged splits must keep flowing back to the
planner.

The planner gets two new messages:

- `MergeDrainProbe`: runs `compute_merge_ops(is_finalize = false)`, schedules
  the operations, and replies with the number of operations it scheduled and
  the number of ongoing operations (`ongoing_merge_operations_inventory`). The
  planner is idle if both are 0.
- `QuitMergePlanner`: exits with `Success` without running the finalize merge
  policy. The rest of the pipeline shuts down in cascade, as when the planner
  exits after `RunFinalizeMergePolicyAndQuit`.

The pipeline supervise loop (1 s tick) drives the drainer after the health
check:

```
drive():
    if waiting for a reply:
        match reply_rx.try_recv():
            empty  → return Pending
            closed → error
            reply  → idle_count = reply.is_idle() ? idle_count + 1 : 0
        if idle_count == 2: return FixpointReached
    try_send_message(MergeDrainProbe)   // low priority, FIFO with NewSplits
        full → retry on the next tick
    return Pending

on FixpointReached: send QuitMergePlanner
```

The planner never sends probes to itself. Self-scheduled messages go through
the high-priority queue and could overtake a pending `NewSplits`, and the
planner mailbox has a capacity of 1, so a blocking self-send could deadlock.

**Why two consecutive idle replies.** The merge `Publisher` sends `NewSplits`
to the planner, and only then drops the `MergeTask`, which removes the
operation from `ongoing_merge_operations_inventory`. So when a probe sees an
empty inventory, the `NewSplits` of every completed merge has already been
queued, but possibly after that probe. The next probe is sent only after the
first reply is received, so it is queued after all of them. If it is idle as
well, the merge policy proposes nothing over all the published splits, and the
fixpoint is real.

**Failures.** After the shutdown is initiated, a failing actor used to leave the
merge pipeline supervising nothing, forever. It now exits with a failure,
which `local-ingest` reports.

Merges run **concurrently with indexing** (default `merge_concurrency`), as in
production. Only the drain phase comes after indexing.

Scope: `local-ingest` uses `DrainMergesAndShutdownPipeline` with
`--input-format parquet`. The NDJSON path keeps
`FinishPendingMergesAndShutdownPipeline`, so its behavior doesn't change.

Tests (`merge_drainer.rs`):

- The drainer needs two consecutive idle replies, a busy reply resets the
  count, and there is at most one probe in flight.
- A `MergePipeline` with `StableLogMergePolicy` (`merge_factor: 2`) over 4
  single-doc splits: the drain merges them over 2 levels into 1 split with
  `num_merge_ops == 2`, then exits with `Success`.

### 7. CLI changes to `local-ingest`

New flags:

- `--num-pipelines <N>`: default `max(1, num_cpus / 2)` for Parquet, and 1
  otherwise. It is **rejected if > 1 unless `--input-format parquet`**.
- `--batch-num-rows <R>`: Parquet only, default 8192.

`--input-format parquet` requires `--input-path` (no stdin).

Flow (`quickwit-cli/src/tool/parquet_ingest.rs`):

1. Load config, resolve the metastore, and run `run_index_checklist`.
2. Optionally clear the index (`--overwrite`), then fetch the index metadata,
   so the checkpoint reflects the overwrite.
3. Run the memory check from §8.
4. Build the `ParquetLoadPlan` from the `CLI_SOURCE_ID` checkpoint (in
   `spawn_blocking`) and register it. The guard lives until the end of the load.
5. Start the runtimes and the `IndexingService`, then send N
   `SpawnPipeline { pipeline_uid: PipelineUid::random(), .. }` with the same
   `SourceConfig`. Detach the N indexing handles, then the one merge handle
   (after all the spawns, otherwise the next spawn would create a new merge
   pipeline).
6. A reporting loop over the N handles sums `IndexingStatistics`. The live
   display adds row groups done/total and the number of running pipelines.
7. When all indexing pipelines have exited, record `t_indexed` and the number of
   splits published so far. Then send `DrainMergesAndShutdownPipeline`, join
   the merge pipeline, and record `t_merged`.
8. Print a report:
   - rows, Parquet bytes, NDJSON bytes;
   - docs indexed and invalid;
   - `t_indexed` and `t_merged`;
   - docs/s and MB/s, for indexing only and end to end;
   - splits published by indexing, number of merge operations, final number of
     splits;
   - pipeline restarts;
   - effective settings: N, `heap_size`, `split_num_docs_target`,
     `commit_timeout_secs`, `merge_concurrency`, merge policy, metastore type.
9. Exit non-zero if there are invalid docs, pipeline restarts, or if the number
   of indexed docs differs from the number of rows left to index according to
   the checkpoint.

The number of merge operations is the number of splits uploaded by the merge
pipeline: the merge publisher counts published merges as replace operations,
not as published splits.

### 8. Sizing N

- Each pipeline's `DocProcessor` and `Indexer` are mostly single-threaded, so one
  pipeline uses about 1.5 to 2 cores. All pipelines and merges share the
  `Blocking` runtime. The default N is `num_cpus / 2`, and benchmarks should
  sweep N.
- Memory: if `N × heap_size > 0.8 × total memory` (`heap_size` defaults to 2 GiB),
  the CLI **fails before spawning** and suggests lowering `heap_size` in the
  index config or lowering N. It doesn't adjust anything on its own.
- There are about N× more initial splits than with one pipeline, because each
  pipeline commits on `split_num_docs_target` or `commit_timeout_secs`. The
  fixpoint merge folds them back. The report shows both counts.
- `enable_cooperative_indexing` stays off (the default).
- File-backed metastore: publishes are serialized per index, and their cost
  grows with the number of splits. Use a local PostgreSQL metastore for large
  runs. The report says which one was used.

## Testing summary

- `RowGroupQueue`: every row group is leased exactly once, completion, re-lease
  after restart, and plan filtering from a checkpoint.
- Parquet `FileSource`: batches, per-row-group checkpoint deltas, resume from the
  middle of a row group, exit when drained, and a failure when no plan is
  registered.
- Config validation: `parquet` with anything other than `File(Filepath)` is
  rejected. `--num-pipelines > 1` without parquet is rejected.
- Merge drain: the fixpoint tests from §6.
- End-to-end (`quickwit-cli/tests/parquet.rs`, `--features parquet`): a
  generated Parquet file with 20 row groups, N=4, a `merge_factor: 2` policy.
  Checks that the docs count equals the row count, that a single split is left
  and the merge policy proposes nothing, that every row group partition is at
  EOF, and that a second run is a no-op.
- CI: a job that builds and tests with `--features parquet`.

## Alternatives considered

| Alternative | Why not |
|---|---|
| New `SourceType::Parquet` | Proto change, and it exposes a benchmark-only source to users. |
| N `local-ingest` processes on pre-split NDJSON | Concurrent processes on a file-backed metastore can lose updates, and splits never merge across processes. |
| Ingest API v2 with many shards | Goes through the WAL and ingesters. |
| Writing splits directly, without the actor pipeline | Measures a path production doesn't use. |
| N `source_id`s with static row-group ranges | Merges are per source, and balancing has a long tail. |
| Keep `FinishPendingMergesAndShutdownPipeline` | Doesn't reach the merge-policy fixpoint (see "Current state"). |

## Phasing

1. **Phase 1:**
   - `SourceInputFormat::Parquet`, the Parquet state in `FileSource`, and the
     `RowGroupQueue`;
   - multi-pipeline `local-ingest`;
   - `DrainMergesAndShutdownPipeline`;
   - the report.
2. **Phase 2 (driven by profiling):**
   - Arrow to JSON objects without serializing to bytes;
   - object-storage input through a `parquet` `AsyncFileReader` built on
     `Storage::get_slice`;
   - column projection;
   - several input files or a glob.
