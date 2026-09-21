package etcd

import (
	"bytes"
	"context"
	"testing"
	"time"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
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
				fence, err := testreleases.EncodeReleaseRecord("release-fence-set", testreleases.ReleaseFenceSet{
					EnvironmentID: task.Owner.EnvironmentID, Generation: 1, OperationID: task.OperationID, AttemptTaskID: task.ID,
					Members: []testreleases.ReleaseFenceMember{
						{ServiceID: f.serviceID, CandidateReleaseID: f.releaseID},
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				checkpoint, err := testreleases.EncodeReleaseRecord(
					"release-checkpoint",
					domain.Checkpoint{ReleaseID: f.releaseID, State: domain.StatePending, UpdatedAt: f.now},
				)
				if err != nil {
					t.Fatal(err)
				}
				checkpointKey := testreleases.ReleaseCheckpointStagingKey(
					task.Params[testreleaserender.TaskReleasePublicationParam],
					f.releaseID,
				)
				seed, err := f.repository.store.Transact(ctx, nil, []testkeyvalue.Mutation{
					{
						Type:  testkeyvalue.MutationPut,
						Key:   testreleases.ReleaseFenceSetKey(task.Owner.EnvironmentID),
						Value: fence,
					},
					{Type: testkeyvalue.MutationPut, Key: checkpointKey, Value: checkpoint},
				})
				if err != nil {
					t.Fatal(err)
				}
				ack, err := f.repository.releaseRecoveryAcknowledgementAtRevision(
					ctx,
					task,
					assignment, testtaskjournal.TaskStatusCompleted, result,
					seed.Revision,
				)
				if err != nil || !ack.final {
					t.Fatalf("proof rejected: %v", err)
				}
				key := renderKey
				if source == "manifest" {
					key = testreleases.ReleaseManifestStagingKey(
						task.Params[testreleaserender.TaskReleasePublicationParam],
					)
				}
				read, err := f.repository.store.GetMany(
					ctx, testkeyvalue.GetManyRequest{Keys: []string{key}, Revision: seed.Revision},
				)
				if err != nil {
					t.Fatal(err)
				}
				mutation := testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: key, Value: read.Values[0].Value}
				if prune {
					mutation.Type = testkeyvalue.MutationDelete
				}
				if _, err := f.repository.store.Transact(ctx, nil, []testkeyvalue.Mutation{mutation}); err != nil {
					t.Fatal(err)
				}
				result.FailedStepID = f.forwardStepID
				processed, err := f.repository.finalizeReleaseTaskBatch(
					ctx,
					task,
					assignment, testtaskjournal.TaskStatusFailed, result,
					f.agentID,
					f.now.Add(time.Minute),
					seed.Revision,
					ack.conditions...)
				if err == nil || processed {
					t.Fatal("changed proof source committed a partial release terminal batch")
				}
				after, err := f.repository.store.GetMany(
					ctx, testkeyvalue.GetManyRequest{
						Keys: []string{
							checkpointKey, testreleases.ReleaseTerminalKey(f.releaseID), testreleases.ReleaseProjectionKey(f.serviceID), testreleases.ReleaseRetentionKey(f.releaseID),
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
