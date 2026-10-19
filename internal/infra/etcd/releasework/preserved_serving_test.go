package releasework

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A failed deployment can keep a candidate only after its actual serving
// evidence was durably consumed. Candidate startup or a hook checkpoint is not
// that proof. This also exercises storage across the pre-readiness post hook.
func TestPreservationRequiresConsumedServingEvidence(t *testing.T) {
	input, view, store := nativeRecoveryFixture(t, 1)
	input.Input.Release.FailurePolicy = agentpb.WorkReleaseFailurePolicy_WORK_RELEASE_FAILURE_POLICY_LEAVE_ACTIVE
	input.Input.Release.Hooks[0].Phase = "post-deploy"
	forward := input.Input
	forward.Attempt = input.Forward
	forward.Attempt.FinishedAt, forward.Attempt.Result, forward.Attempt.Outcome = nil, nil, ""
	view.Attempt = workqueue.AttemptSnapshot{Attempt: forward.Attempt, Revision: 1}
	view.Work.Item.Item.AttemptNumber, view.Work.Item.Item.ActiveAttemptID = 1, forward.Attempt.ID
	view.Work.Item.Item.Outcome, view.Work.Item.Item.Cleanup, view.Work.Item.Item.Result = "", workexecution.CleanupNone, nil
	progress, err := releaseexecution.NewProgress(forward, *forward.Attempt.StartedAt)
	if err != nil {
		t.Fatal(err)
	}
	var saved Snapshot
	var sequence uint64
	at := progress.UpdatedAt
	var servingKey string
	publish := func(report releaseexecution.CheckpointReport) {
		t.Helper()
		change, err := Prepare(view, forward, saved, report)
		if err != nil {
			t.Fatal(err)
		}
		store.publish(change)
		view.Work.ReadRevision = store.revision
		checkpoint, err := report.Checkpoint(forward.Attempt)
		if err != nil {
			t.Fatal(err)
		}
		view.Checkpoint = workqueue.CheckpointSnapshot{Record: &checkpoint, Revision: store.revision}
		saved, err = Load(t.Context(), store, forward, store.revision)
		if err != nil {
			t.Fatal(err)
		}
	}
	for progress.Phase != releaseexecution.Complete {
		at = at.Add(time.Millisecond)
		progress, err = progress.Begin(forward, at)
		if err != nil {
			t.Fatal(err)
		}
		stage, err := progress.StageID(forward)
		if err != nil {
			t.Fatal(err)
		}
		sequence++
		report := releaseexecution.CheckpointReport{Sequence: sequence, StageID: stage, Progress: progress}
		publish(report)
		unproved, err := LoadPreservedServing(t.Context(), store, input, store.revision)
		if err != nil || unproved.Proof != nil {
			t.Fatalf("incomplete rollout acquired serving authority: %v", err)
		}
		reference := forward.Attempt.Input
		if progress.Action == releaseexecution.VerifyServing {
			checkpoint, err := report.Checkpoint(forward.Attempt)
			if err != nil {
				t.Fatal(err)
			}
			report.Ack = &checkpoint
			result, err := releaseexecution.HelperResult(
				&agentpb.ReleaseHelperResponse{Succeeded: true, Inspection: true},
			)
			if err != nil {
				t.Fatal(err)
			}
			raw, proof, err := releaseexecution.EncodeEffectEvidence(forward, report, result, at)
			if err != nil {
				t.Fatal(err)
			}
			servingKey = EffectEvidenceKey(forward.Attempt.ID, sequence)
			store.put(servingKey, raw)
			view.Work.ReadRevision = store.revision
			reference = proof
		}
		at = at.Add(time.Millisecond)
		progress, err = progress.Confirm(forward, reference, at)
		if err != nil {
			t.Fatal(err)
		}
		sequence++
		publish(releaseexecution.CheckpointReport{Sequence: sequence, StageID: stage, Progress: progress})
	}
	input.Failure.Checkpoint = view.Checkpoint.Record
	_, failureReference, err := releaseexecution.EncodeFailure(input.Forward, input.Failure)
	if err != nil {
		t.Fatal(err)
	}
	input.Forward.Result = &failureReference
	preserved, err := LoadPreservedServing(t.Context(), store, input, store.revision)
	if err != nil || preserved.Proof == nil || preserved.Proof.Runtime.ReleaseID != input.Input.Release.ReleaseId {
		t.Fatalf("proved candidate was lost after the failed forward attempt: %v", err)
	}
	input.Input.Release.FailurePolicy = agentpb.WorkReleaseFailurePolicy_WORK_RELEASE_FAILURE_POLICY_SWITCH_BACK
	if rejected, err := LoadPreservedServing(t.Context(), store, input, store.revision); err != nil ||
		rejected.Proof != nil {
		t.Fatalf("switch_back acquired preserve authority: %v", err)
	}
	input.Input.Release.FailurePolicy = agentpb.WorkReleaseFailurePolicy_WORK_RELEASE_FAILURE_POLICY_LEAVE_ACTIVE
	store.values[servingKey].Value[0] ^= 1
	if _, err := LoadPreservedServing(t.Context(), store, input, store.revision); err == nil {
		t.Fatal("corrupted serving evidence was accepted as preservation authority")
	}
}
