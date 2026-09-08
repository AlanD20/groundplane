package etcd

import (
	"bytes"
	"context"
	"testing"
	"time"

	domain "github.com/AlanD20/groundplane/internal/core/release"
)

// Rationale: lost proof witnesses must prevent intermediate checkpoint, summary
// and serving-projection writes, not merely the eventual Task terminal write.
func TestRecoveryProofRaceCannotPartiallyTerminalizeRelease(t *testing.T) {
	for _, source := range []string{"render", "manifest"} {
		for _, prune := range []bool{false, true} {
			t.Run(source+map[bool]string{false: "/replace", true: "/prune"}[prune], func(t *testing.T) {
				ctx := context.Background()
				f, assignment, result, _, renderKey := recoveryProofFixture(t, false)
				task := f.claim.Task.Record
				fence, err := encodeReleaseRecord("release-fence-set", ReleaseFenceSet{
					EnvironmentID: task.Owner.EnvironmentID, Generation: 1, OperationID: task.OperationID, AttemptTaskID: task.ID,
					Members: []ReleaseFenceMember{{ServiceID: f.serviceID, CandidateReleaseID: f.releaseID}},
				})
				if err != nil {
					t.Fatal(err)
				}
				checkpoint, err := encodeReleaseRecord(
					"release-checkpoint",
					domain.Checkpoint{ReleaseID: f.releaseID, State: domain.StatePending, UpdatedAt: f.now},
				)
				if err != nil {
					t.Fatal(err)
				}
				checkpointKey := releaseCheckpointStagingKey(task.Params[TaskReleasePublicationParam], f.releaseID)
				seed, err := f.repository.store.Transact(ctx, nil, []Mutation{
					{Type: MutationPut, Key: releaseFenceSetKey(task.Owner.EnvironmentID), Value: fence},
					{Type: MutationPut, Key: checkpointKey, Value: checkpoint},
				})
				if err != nil {
					t.Fatal(err)
				}
				ack, err := f.repository.releaseRecoveryAcknowledgementAtRevision(
					ctx,
					task,
					assignment,
					TaskStatusCompleted,
					result,
					seed.Revision,
				)
				if err != nil || !ack.final {
					t.Fatalf("proof rejected: %v", err)
				}
				key := renderKey
				if source == "manifest" {
					key = releaseManifestStagingKey(task.Params[TaskReleasePublicationParam])
				}
				read, err := f.repository.store.GetMany(
					ctx,
					GetManyRequest{Keys: []string{key}, Revision: seed.Revision},
				)
				if err != nil {
					t.Fatal(err)
				}
				mutation := Mutation{Type: MutationPut, Key: key, Value: read.Values[0].Value}
				if prune {
					mutation.Type = MutationDelete
				}
				if _, err := f.repository.store.Transact(ctx, nil, []Mutation{mutation}); err != nil {
					t.Fatal(err)
				}
				result.FailedStepID = f.forwardStepID
				processed, err := f.repository.finalizeReleaseTaskBatch(
					ctx,
					task,
					assignment,
					TaskStatusFailed,
					result,
					f.agentID,
					f.now.Add(time.Minute),
					seed.Revision,
					ack.conditions...)
				if err == nil || processed {
					t.Fatal("changed proof source committed a partial release terminal batch")
				}
				after, err := f.repository.store.GetMany(
					ctx,
					GetManyRequest{
						Keys: []string{
							checkpointKey,
							releaseTerminalKey(f.releaseID),
							releaseProjectionKey(f.serviceID),
							releaseRetentionKey(f.releaseID),
						},
					},
				)
				if err != nil || !bytes.Equal(after.Values[0].Value, checkpoint) || after.Values[1] != nil ||
					after.Values[2] != nil ||
					after.Values[3] != nil {
					t.Fatalf("partial release state changed: %v", err)
				}
			})
		}
	}
}
