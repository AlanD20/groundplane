package app

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// TASK-56: operator acceptance, immutable input, authenticated checkpoints,
// resource settlement and replay must agree without the retired Task engine.
func TestNativeVolumeCreateRemoveAndReceiptReplay(t *testing.T) {
	f := newNativeVolumeFixture(t)
	view := f.start(t, f.created.TaskID)
	if applied, err := f.inputs.ReadApplied(t.Context(), f.created.Volume.ID, 0); err != nil || applied != nil {
		t.Fatalf("acceptance fabricated applied storage: %v", err)
	}
	progress := f.progress(t, view)
	for progress.Stage != volumeexecution.Complete {
		progress = f.advance(t, view, progress)
	}
	f.complete(t, view, progress)
	applied, err := f.inputs.ReadApplied(t.Context(), f.created.Volume.ID, 0)
	if err != nil || applied == nil || applied.EnvironmentID != f.environmentID || applied.ComposeKey != "data" {
		t.Fatalf("completed create has no independently retained storage record: %v", err)
	}
	for _, claim := range view.Work.Item.Item.Claims {
		value, err := f.store.Get(t.Context(), workqueue.ClaimKey(claim))
		if err != nil || value.Entry != nil {
			t.Fatalf("completed create retained resource ownership: %v", err)
		}
	}
	view, response, impact := f.remove(t)
	if applied, err := f.inputs.ReadApplied(t.Context(), f.created.Volume.ID, 0); err != nil || applied == nil {
		t.Fatalf("accepted removal discarded storage proof before host completion: %v", err)
	}
	progress = f.progress(t, view)
	first := progress
	first, err = first.Begin(
		view.Attempt.Attempt,
		agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE,
		f.created.Volume.ID,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	request := f.checkpoint(t, view, first)
	ack, err := f.reports.CheckpointVolume(t.Context(), f.authorization, request)
	if err != nil || ack.Sequence != 1 {
		t.Fatalf("saved checkpoint replay: %v", err)
	}
	// Completion cannot use the detach-start receipt as proof of directory absence.
	if _, err := f.reports.CompleteVolume(t.Context(), f.authorization, &agentpb.VolumeWorkCompletionRequest{Attempt: request.Attempt, Evidence: ack.Evidence}); err == nil {
		t.Fatal("in-flight detach checkpoint settled persistent storage removal")
	}
	// A changed observation at an already accepted sequence is not a replay.
	changed := first
	changed.UpdatedAt = changed.UpdatedAt.Add(time.Nanosecond)
	input, err := f.inputs.LoadVolumeWorkInput(t.Context(), view.Work)
	if err != nil {
		t.Fatal(err)
	}
	changedRaw, err := volumeexecution.EncodeProgress(view.Attempt.Attempt, input.Action, input.VolumeId, changed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.reports.CheckpointVolume(t.Context(), f.authorization, &agentpb.VolumeWorkCheckpointRequest{Attempt: request.Attempt, Progress: changedRaw}); err == nil {
		t.Fatal("changed report replaced an accepted sequence")
	}
	progress = f.confirm(t, view, first)
	for progress.Stage != volumeexecution.Complete {
		progress = f.advance(t, view, progress)
	}
	f.complete(t, view, progress)
	if applied, err := f.inputs.ReadApplied(t.Context(), f.created.Volume.ID, 0); err != nil || applied != nil {
		t.Fatalf("completed removal retained applied storage: %v", err)
	}
	projection, found, err := f.blueprint.GetEnvironmentComposeProjection(t.Context(), f.environmentID)
	if err != nil || !found || len(projection.Record.Volumes) != 0 {
		t.Fatalf("removal retained its desired declaration: %v", err)
	}
	before := f.store.revision
	replayed, err := f.mutations.RemoveVolume(
		t.Context(),
		f.created.Volume.ID,
		impact,
		"data",
		"native-volume-remove-key",
	)
	if err != nil || !bytes.Equal(replayed.Body, response.Body) || replayed.Status != response.Status ||
		f.store.revision != before {
		t.Fatalf("deleted Volume did not replay the original accepted response read-only: %v", err)
	}
	if _, err := f.mutations.RemoveVolume(t.Context(), f.created.Volume.ID, impact, "different", "native-volume-remove-key"); !errors.Is(
		err,
		errs.New(errs.KindIdempotencyMismatch, ""),
	) {
		t.Fatalf("different delete intent replayed an accepted action: %v", err)
	}
}

// TASK-56: no new host-stage grant can cross a concurrent desired-head or
// authenticated registration/resource-claim change. The competing write must
// reach commit; the same guards protect destructive removal stages.
func TestNativeVolumeCheckpointFencesConcurrentAuthority(t *testing.T) {
	for _, action := range []string{"create", "remove"} {
		for _, source := range []string{"desired", "executor", "Volume claim", "host Environment claim", "Environment claim"} {
			t.Run(action+"/"+source, func(t *testing.T) {
				f := newNativeVolumeFixture(t)
				view := f.start(t, f.created.TaskID)
				if action == "remove" {
					progress := f.progress(t, view)
					for progress.Stage != volumeexecution.Complete {
						progress = f.advance(t, view, progress)
					}
					f.complete(t, view, progress)
					view, _, _ = f.remove(t)
				}
				input, err := f.inputs.LoadVolumeWorkInput(t.Context(), view.Work)
				if err != nil {
					t.Fatal(err)
				}
				progress := f.progress(t, view)
				progress, err = progress.Begin(view.Attempt.Attempt, input.Action, input.VolumeId, time.Now().UTC())
				if err != nil {
					t.Fatal(err)
				}
				raw, err := volumeexecution.EncodeProgress(view.Attempt.Attempt, input.Action, input.VolumeId, progress)
				if err != nil {
					t.Fatal(err)
				}
				identity, err := workexecution.EncodeAgentAttempt(view.Attempt.Attempt)
				if err != nil {
					t.Fatal(err)
				}
				key := blueprints.EnvironmentBlueprintHeadKey(f.environmentID)
				if source == "executor" {
					key = workqueue.ExecutorKey(view.Attempt.Attempt.Executor)
				}
				if source == "Volume claim" || source == "host Environment claim" || source == "Environment claim" {
					found := false
					for _, claim := range view.Work.Item.Item.Claims {
						matches := source == "Volume claim" && claim.ScopeID == f.created.Volume.ID ||
							source == "host Environment claim" && claim.ScopeID == f.environmentID &&
								claim.HostID != "" ||
							source == "Environment claim" && claim.ScopeID == f.environmentID && claim.HostID == ""
						if matches {
							key, found = workqueue.ClaimKey(claim), true
							break
						}
					}
					if !found {
						t.Fatal("native Volume work did not capture its required resource claim")
					}
				}
				old, err := f.store.Get(t.Context(), key)
				if err != nil || old.Entry == nil {
					t.Fatalf("authority fixture: %v", err)
				}
				replacement := old.Entry.Value
				if source == "Volume claim" || source == "host Environment claim" || source == "Environment claim" {
					replacement, err = recordcodec.Encode("execution_claim", workqueue.ClaimOwner{
						TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem),
					})
					if err != nil {
						t.Fatal(err)
					}
				}
				f.store.beforeCommit = func(mutations []keyvalue.Mutation) {
					for _, mutation := range mutations {
						if mutation.Key != volumework.ReportKey(view.Attempt.Attempt.ID, 1) {
							continue
						}
						f.store.beforeCommit = nil
						if _, err := f.store.Put(t.Context(), key, replacement); err != nil {
							t.Fatal(err)
						}
						return
					}
				}
				_, err = f.reports.CheckpointVolume(
					t.Context(),
					f.authorization,
					&agentpb.VolumeWorkCheckpointRequest{Attempt: identity, Progress: raw},
				)
				if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || f.store.beforeCommit != nil {
					t.Fatalf("concurrent %s did not fence the actual grant: %v", source, err)
				}
				report, err := f.store.Get(t.Context(), volumework.ReportKey(view.Attempt.Attempt.ID, 1))
				if err != nil || report.Entry != nil {
					t.Fatalf("rejected grant persisted a report: %v", err)
				}
				resume, err := f.queue.ReadResume(
					t.Context(),
					view.Work.Task.Task.ID,
					view.Work.Item.Item.ID,
					view.Attempt.Attempt.ID,
					view.Attempt.Attempt.Executor,
				)
				if err != nil || resume.Checkpoint.Record != nil {
					t.Fatalf("rejected grant advanced the execution cursor: %v", err)
				}
			})
		}
	}
}

func (f *nativeVolumeFixture) progress(t *testing.T, view workqueue.ResumeView) volumeexecution.Progress {
	t.Helper()
	input, err := f.inputs.LoadVolumeWorkInput(t.Context(), view.Work)
	if err != nil {
		t.Fatal(err)
	}
	progress, err := volumeexecution.NewProgress(view.Attempt.Attempt, input.Action, input.VolumeId, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return progress
}

func (f *nativeVolumeFixture) advance(
	t *testing.T,
	view workqueue.ResumeView,
	progress volumeexecution.Progress,
) volumeexecution.Progress {
	t.Helper()
	input, err := f.inputs.LoadVolumeWorkInput(t.Context(), view.Work)
	if err != nil {
		t.Fatal(err)
	}
	progress, err = progress.Begin(view.Attempt.Attempt, input.Action, input.VolumeId, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	f.checkpoint(t, view, progress)
	return f.confirm(t, view, progress)
}

func (f *nativeVolumeFixture) confirm(
	t *testing.T,
	view workqueue.ResumeView,
	progress volumeexecution.Progress,
) volumeexecution.Progress {
	t.Helper()
	input, err := f.inputs.LoadVolumeWorkInput(t.Context(), view.Work)
	if err != nil {
		t.Fatal(err)
	}
	observation := volumeexecution.Observation{
		VolumeID: input.VolumeId,
		Input:    view.Attempt.Attempt.Input,
		At:       time.Now().UTC(),
	}
	switch progress.Stage {
	case volumeexecution.PrepareDirectory:
		observation.DirectoryReady = true
	case volumeexecution.PrepareVolume, volumeexecution.VerifyVolume:
		observation.DirectoryReady, observation.VolumeReady = true, true
	case volumeexecution.DetachConsumers:
		observation.ConsumersDetached = true
	case volumeexecution.RemoveVolume:
		observation.ConsumersDetached, observation.VolumeAbsent = true, true
	case volumeexecution.RemoveDirectory:
		observation.ConsumersDetached, observation.VolumeAbsent, observation.DirectoryAbsent = true, true, true
	}
	progress, err = progress.Confirm(view.Attempt.Attempt, input.Action, input.VolumeId, observation, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	f.checkpoint(t, view, progress)
	return progress
}

func (f *nativeVolumeFixture) complete(t *testing.T, view workqueue.ResumeView, progress volumeexecution.Progress) {
	t.Helper()
	input, err := f.inputs.LoadVolumeWorkInput(t.Context(), view.Work)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := progress.Checkpoint(view.Attempt.Attempt, input.Action, input.VolumeId)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := workexecution.EncodeAgentAttempt(view.Attempt.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	request := &agentpb.VolumeWorkCompletionRequest{
		Attempt:  identity,
		Evidence: &agentpb.WorkInputReference{InputId: checkpoint.Evidence.ID, Sha256: digest},
	}
	f.authorize(t)
	ack, err := f.reports.CompleteVolume(t.Context(), f.authorization, request)
	if err != nil {
		t.Fatal(err)
	}
	before := f.store.revision
	again, err := f.reports.CompleteVolume(t.Context(), f.authorization, request)
	if err != nil || !proto.Equal(ack, again) || f.store.revision != before {
		t.Fatalf("terminal receipt replay changed state: %v", err)
	}
}

func (f *nativeVolumeFixture) remove(t *testing.T) (workqueue.ResumeView, idempotency.IdempotencyResponse, string) {
	t.Helper()
	impact, err := f.reads.GetVolumeDeletionImpact(t.Context(), f.created.Volume.ID, "", 40)
	if err != nil || !impact.Complete || impact.ImpactToken == "" {
		t.Fatalf("Volume impact: %v", err)
	}
	response, err := f.mutations.RemoveVolume(
		t.Context(),
		f.created.Volume.ID,
		impact.ImpactToken,
		"data",
		"native-volume-remove-key",
	)
	if err != nil {
		t.Fatal(err)
	}
	var accepted api.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil ||
		ids.Validate(ids.KindTask, accepted.TaskID) != nil {
		t.Fatalf("Volume removal response: %v", err)
	}
	return f.start(t, accepted.TaskID), response, impact.ImpactToken
}
