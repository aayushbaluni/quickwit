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

//! Detects when the merges of a merge pipeline have reached a fixpoint.
//!
//! The merge pipeline drives the drainer from its supervise loop. The drainer sends
//! [`MergeDrainProbe`] messages to the merge planner, one at a time, through the low priority
//! queue of its mailbox: the probes are processed in FIFO order with the `NewSplits` messages
//! sent by the merge publisher. The planner never sends probes to itself (its mailbox has a
//! capacity of 1, and self-scheduled messages go through the high priority queue, which would let
//! a probe overtake a pending `NewSplits`).
//!
//! The fixpoint is reached after two consecutive idle replies: no merge scheduled and no merge
//! ongoing. A single idle reply is not enough. A merge operation leaves the planner inventory when
//! the merge publisher drops the merge task, right *after* sending the `NewSplits` message for the
//! merged split to the planner. When the first idle reply is observed, all the `NewSplits` messages
//! of the merges completed so far have therefore been queued, but some may have been queued after
//! the first probe. The second probe is sent after the first reply was received, so it is queued
//! after all of them: if it is idle as well, the merge policy proposes no merge over all the
//! published splits.

use quickwit_actors::{Mailbox, TrySendError};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tracing::{debug, info};

use super::MergePlanner;
use super::merge_planner::{MergeDrainProbe, MergeDrainProbeReply};

/// Number of consecutive idle probe replies required to declare the fixpoint reached.
const NUM_IDLE_PROBES_FOR_FIXPOINT: usize = 2;

#[derive(Debug)]
enum DrainState {
    /// A probe must be sent.
    SendProbe,
    /// A probe was sent, we are waiting for its reply.
    AwaitReply(oneshot::Receiver<MergeDrainProbeReply>),
}

#[derive(Debug)]
pub(super) struct MergeDrainer {
    state: DrainState,
    num_consecutive_idle_replies: usize,
    num_probes: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum DrainProgress {
    /// Merges are still being planned or executed.
    Pending,
    /// The merge policy proposes no merge and no merge is ongoing.
    FixpointReached,
}

impl MergeDrainer {
    pub fn new() -> Self {
        Self {
            state: DrainState::SendProbe,
            num_consecutive_idle_replies: 0,
            num_probes: 0,
        }
    }

    /// Advances the drain without blocking. Must be called periodically.
    ///
    /// Returns an error if the merge planner is gone, which should be caught by the health check
    /// of the pipeline first.
    pub fn drive(
        &mut self,
        merge_planner_mailbox: &Mailbox<MergePlanner>,
    ) -> anyhow::Result<DrainProgress> {
        if let DrainState::AwaitReply(reply_rx) = &mut self.state {
            let reply = match reply_rx.try_recv() {
                Ok(reply) => reply,
                Err(TryRecvError::Empty) => return Ok(DrainProgress::Pending),
                Err(TryRecvError::Closed) => {
                    anyhow::bail!("merge planner dropped a drain probe");
                }
            };
            debug!(?reply, "merge drain probe reply");

            if reply.is_idle() {
                self.num_consecutive_idle_replies += 1;
            } else {
                self.num_consecutive_idle_replies = 0;
            }
            self.state = DrainState::SendProbe;

            if self.num_consecutive_idle_replies >= NUM_IDLE_PROBES_FOR_FIXPOINT {
                info!(num_probes = self.num_probes, "merges reached a fixpoint");
                return Ok(DrainProgress::FixpointReached);
            }
        }
        match merge_planner_mailbox.try_send_message(MergeDrainProbe) {
            Ok(reply_rx) => {
                self.num_probes += 1;
                self.state = DrainState::AwaitReply(reply_rx);
            }
            // The planner mailbox is full, most likely with a `NewSplits` message. We will retry
            // on the next call.
            Err(TrySendError::Full(_)) => {}
            Err(TrySendError::Disconnected) => {
                anyhow::bail!("merge planner mailbox is disconnected");
            }
        }
        Ok(DrainProgress::Pending)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use quickwit_actors::{ActorExitStatus, Universe};
    use quickwit_common::temp_dir::TempDirectory;
    use quickwit_config::merge_policy_config::StableLogMergePolicyConfig;
    use quickwit_metastore::{
        ListSplitsQuery, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitState,
    };
    use quickwit_proto::indexing::MergePipelineId;
    use quickwit_proto::metastore::{ListSplitsRequest, MetastoreService};

    use super::*;
    use crate::actors::{DrainMergesAndShutdownPipeline, MergePipeline, MergePipelineParams};
    use crate::merge_policy::StableLogMergePolicy;
    use crate::{IndexingSplitStore, TestSandbox};

    /// Runs merges to a fixpoint over several levels: 4 splits are merged into 2 splits, which are
    /// merged into 1 split.
    #[tokio::test]
    async fn test_merge_pipeline_drains_merges_to_fixpoint() {
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: body
                type: text
              - name: ts
                type: datetime
                input_formats: [unix_timestamp]
                fast: true
            timestamp_field: ts
        "#;
        let test_sandbox = TestSandbox::create("test-index", doc_mapping_yaml, "", &["body"])
            .await
            .unwrap();
        for split_idx in 0..4u64 {
            let doc = serde_json::json!({"body": format!("split{split_idx}"), "ts": 1631072713u64 + split_idx});
            test_sandbox
                .add_documents(std::iter::once(doc))
                .await
                .unwrap();
        }
        let metastore = test_sandbox.metastore();
        let index_uid = test_sandbox.index_uid();
        let list_published_splits = || async {
            let query = ListSplitsQuery::for_index(index_uid.clone())
                .with_split_state(SplitState::Published);
            let request = ListSplitsRequest::try_from_list_splits_query(&query).unwrap();
            metastore
                .list_splits(request)
                .await
                .unwrap()
                .collect_splits_metadata()
                .await
                .unwrap()
        };
        let initial_splits = list_published_splits().await;
        assert_eq!(initial_splits.len(), 4);

        let merge_policy_config = StableLogMergePolicyConfig {
            min_level_num_docs: 100,
            merge_factor: 2,
            max_merge_factor: 2,
            maturation_period: Duration::from_secs(3600),
        };
        let merge_policy = Arc::new(StableLogMergePolicy::new(merge_policy_config, 1_000));
        let universe = test_sandbox.universe();
        let pipeline_params = MergePipelineParams {
            pipeline_id: MergePipelineId {
                node_id: test_sandbox.node_id(),
                index_uid: index_uid.clone(),
                source_id: test_sandbox.source_id(),
            },
            doc_mapper: test_sandbox.doc_mapper(),
            indexing_directory: TempDirectory::for_test(),
            metastore: metastore.clone(),
            merge_scheduler_service: universe.get_or_spawn_one(),
            split_store: IndexingSplitStore::create_without_local_store_for_test(
                test_sandbox.storage(),
            ),
            merge_policy,
            retention_policy: None,
            max_concurrent_split_uploads: 2,
            merge_io_throughput_limiter_opt: None,
            event_broker: Default::default(),
        };
        let pipeline =
            MergePipeline::new(pipeline_params, Some(initial_splits), universe.spawn_ctx());
        let (pipeline_mailbox, pipeline_handle) = universe.spawn_builder().spawn(pipeline);
        pipeline_mailbox
            .ask(DrainMergesAndShutdownPipeline)
            .await
            .unwrap();
        let (exit_status, statistics) = pipeline_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));
        assert_eq!(statistics.generation, 1);

        let final_splits = list_published_splits().await;
        assert_eq!(final_splits.len(), 1);
        assert_eq!(final_splits[0].num_docs, 4);
        assert_eq!(final_splits[0].num_merge_ops, 2);
        test_sandbox.assert_quit().await;
    }

    fn idle_reply() -> MergeDrainProbeReply {
        MergeDrainProbeReply {
            num_scheduled_merge_ops: 0,
            num_ongoing_merge_ops: 0,
        }
    }

    fn busy_reply() -> MergeDrainProbeReply {
        MergeDrainProbeReply {
            num_scheduled_merge_ops: 1,
            num_ongoing_merge_ops: 1,
        }
    }

    /// Drives the drainer until it sends a probe, then answers it with `reply`.
    fn answer_probe(
        drainer: &mut MergeDrainer,
        merge_planner_mailbox: &Mailbox<MergePlanner>,
        reply: MergeDrainProbeReply,
    ) -> DrainProgress {
        let progress = drainer.drive(merge_planner_mailbox).unwrap();
        assert_eq!(progress, DrainProgress::Pending);
        let DrainState::AwaitReply(_) = &drainer.state else {
            panic!("a probe should have been sent");
        };
        // Replace the pending receiver with one whose sender we control.
        let (reply_tx, reply_rx) = oneshot::channel();
        reply_tx.send(reply).unwrap();
        drainer.state = DrainState::AwaitReply(reply_rx);
        drainer.drive(merge_planner_mailbox).unwrap()
    }

    #[tokio::test]
    async fn test_merge_drainer_requires_two_consecutive_idle_replies() {
        let universe = Universe::new();
        let (merge_planner_mailbox, merge_planner_inbox) =
            universe.create_test_mailbox::<MergePlanner>();
        let mut drainer = MergeDrainer::new();

        assert_eq!(
            answer_probe(&mut drainer, &merge_planner_mailbox, idle_reply()),
            DrainProgress::Pending
        );
        // A busy reply resets the count.
        assert_eq!(
            answer_probe(&mut drainer, &merge_planner_mailbox, busy_reply()),
            DrainProgress::Pending
        );
        assert_eq!(
            answer_probe(&mut drainer, &merge_planner_mailbox, idle_reply()),
            DrainProgress::Pending
        );
        let DrainState::AwaitReply(_) = &drainer.state else {
            panic!("a probe should have been sent");
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        reply_tx.send(idle_reply()).unwrap();
        drainer.state = DrainState::AwaitReply(reply_rx);
        assert_eq!(
            drainer.drive(&merge_planner_mailbox).unwrap(),
            DrainProgress::FixpointReached
        );
        // One probe is sent per reply, never more.
        let probes = merge_planner_inbox.drain_for_test_typed::<MergeDrainProbe>();
        assert_eq!(probes.len(), 4);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_merge_drainer_waits_for_reply() {
        let universe = Universe::new();
        let (merge_planner_mailbox, merge_planner_inbox) =
            universe.create_test_mailbox::<MergePlanner>();
        let mut drainer = MergeDrainer::new();
        for _ in 0..3 {
            assert_eq!(
                drainer.drive(&merge_planner_mailbox).unwrap(),
                DrainProgress::Pending
            );
        }
        let probes = merge_planner_inbox.drain_for_test_typed::<MergeDrainProbe>();
        assert_eq!(probes.len(), 1);
        universe.assert_quit().await;
    }
}
