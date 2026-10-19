package releasework

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Recovery hooks share Script transport checkpoints. Persisting only the
// logical recovery sequence loses acknowledged Script results or rejects the
// next recovery action. Exercise the actual storage publisher and reader.
func TestRecoveryRetainsAllHooksAcrossInterleavedCheckpoints(t *testing.T) {
	input, view, store := nativeRecoveryFixture(t, 16)
	progress, err := releaseexecution.NewRecoveryProgress(input, *input.Input.Attempt.StartedAt)
	if err != nil {
		t.Fatal(err)
	}
	at := progress.UpdatedAt
	var previous RecoverySnapshot
	var sequence uint64
	for action := range 22 {
		at = at.Add(time.Millisecond)
		progress, err = progress.Begin(input, at)
		if err != nil {
			t.Fatal(err)
		}
		stage, err := progress.StageID(input)
		if err != nil {
			t.Fatal(err)
		}
		sequence++
		begin := releaseexecution.RecoveryReport{Sequence: sequence, StageID: stage, Progress: progress}
		publish := func(report releaseexecution.RecoveryReport) {
			t.Helper()
			change, err := PrepareRecovery(view, input, previous, report)
			if err != nil {
				t.Fatalf("action %d sequence %d: %v", action, report.Sequence, err)
			}
			store.publish(change)
			view.Work.ReadRevision = store.revision
			checkpoint, err := report.Checkpoint(input)
			if err != nil {
				t.Fatal(err)
			}
			view.Checkpoint = workqueue.CheckpointSnapshot{Record: &checkpoint, Revision: store.revision}
			previous, err = LoadRecovery(t.Context(), store, input, store.revision)
			if err != nil {
				t.Fatal(err)
			}
		}
		publish(begin)
		if action >= 6 {
			hook, err := progress.ActiveHook(input)
			if err != nil || hook.ExecutionId != input.Input.Release.Hooks[21-action].ExecutionId {
				t.Fatalf("failure hooks did not execute in numeric order: %v", err)
			}
			// The Script owner publishes three transport checkpoints before
			// the enclosing recovery action can consume its terminal result.
			for range 3 {
				sequence++
				at = at.Add(time.Millisecond)
				checkpoint := workexecution.Checkpoint{
					AttemptID:  input.Input.Attempt.ID,
					Sequence:   sequence,
					StageID:    ids.New(ids.KindStep),
					Evidence:   input.Input.Attempt.Input,
					RecordedAt: at,
				}
				change, err := workqueue.PrepareCheckpoint(view.Work.Item, view.Attempt, view.Checkpoint, checkpoint)
				if err != nil {
					t.Fatal(err)
				}
				store.publish(change)
				view.Work.ReadRevision = store.revision
				view.Checkpoint = workqueue.CheckpointSnapshot{Record: &checkpoint, Revision: store.revision}
			}
		}
		at = at.Add(time.Millisecond)
		progress, err = progress.Confirm(input, input.Input.Attempt.Input, at)
		if err != nil {
			t.Fatal(err)
		}
		sequence++
		publish(releaseexecution.RecoveryReport{Sequence: sequence, StageID: stage, Progress: progress})
	}
	if progress.Phase != releaseexecution.RecoveryComplete || sequence != 92 ||
		previous.Record.Report.Progress.Sequence != 44 {
		t.Fatal("bounded hook recovery did not complete with separate logical and transport cursors")
	}
	// Checkpoint progression alone is not cleanup proof. The operation must
	// still supply and atomically consume every action's independent evidence.
	if _, err := LoadRecoveryCompletionProof(t.Context(), store, view, input); err == nil {
		t.Fatal("synthetic checkpoint references were accepted as actual effect evidence")
	}
}

// A recovery decision and its file selection are immutable assignment data:
// removing the explicit disposition must not silently select a default path.
func TestRecoveryDeliveryPinsPreservationAndConfiguration(t *testing.T) {
	input, _, _ := nativeRecoveryFixture(t, 1)
	input.Input.Release.FailurePolicy = agentpb.WorkReleaseFailurePolicy_WORK_RELEASE_FAILURE_POLICY_LEAVE_ACTIVE
	input.Disposition = agentpb.WorkReleaseRecoveryDisposition_WORK_RELEASE_RECOVERY_DISPOSITION_PRESERVE_SERVING
	assignment, err := releaseexecution.EncodeRecoveryAssignment(input)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := releaseexecution.DecodeRecoveryAssignment(assignment, input.Input.Attempt.Executor)
	if err != nil || !decoded.PreservesServing() ||
		decoded.RecoveryConfigurationReference().ConfigurationId != input.Input.Release.Configuration.ConfigurationId {
		t.Fatalf("preserved assignment lost its exact file selection: %v", err)
	}
	assignment.Disposition = 0
	if _, err := releaseexecution.DecodeRecoveryAssignment(assignment, input.Input.Attempt.Executor); err == nil {
		t.Fatal("missing recovery decision became executable default behavior")
	}
}
