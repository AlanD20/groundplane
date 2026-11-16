package app

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/volume"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A completed directory and interrupted Docker Volume creation must not leave
// the host permanently busy after absence is proved. This crosses the actual
// publication, checkpoint, failure, inspection and Retry owners; filesystem
// observations are simulated, not a claim of host/Docker qualification.
func TestNativeVolumeUncreatedRuntimeRecoveryAndExplicitRetry(t *testing.T) {
	for _, absent := range []bool{true, false} {
		name := "unknown runtime"
		if absent {
			name = "absent runtime"
		}
		t.Run(name, func(t *testing.T) {
			f := newNativeVolumeFixture(t)
			view := f.start(t, f.created.TaskID)
			progress := f.advance(t, view, f.progress(t, view))
			if progress.Stage != volumeexecution.PrepareVolume {
				t.Fatal("directory preparation did not reach runtime creation")
			}
			input, err := f.inputs.LoadVolumeWorkInput(t.Context(), view.Work)
			if err != nil {
				t.Fatal(err)
			}
			progress, err = progress.Begin(view.Attempt.Attempt, input.Action, input.VolumeId, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			f.checkpoint(t, view, progress)
			wire, err := workexecution.EncodeAgentAttempt(view.Attempt.Attempt)
			if err != nil {
				t.Fatal(err)
			}
			failure, err := f.reports.FailVolume(t.Context(), f.authorization, &agentpb.VolumeWorkFailureRequest{
				Attempt: wire, Reason: string(volumeexecution.ExecutionInterrupted),
			})
			if err != nil {
				t.Fatal(err)
			}
			owner, err := volume.NewAgentWork(f.inputs, f.queue)
			if err != nil {
				t.Fatal(err)
			}
			retry := func(number uint64) {
				t.Helper()
				current, previous, err := f.queue.ReadAttempt(t.Context(), view.Work.Task.Task.ID,
					view.Work.Item.Item.ID, number, 0)
				if err != nil {
					t.Fatal(err)
				}
				change, err := owner.PrepareWorkRetry(t.Context(), current.Task, current.Item,
					previous, current.ReadRevision, time.Now().UTC())
				if err != nil {
					t.Fatal(err)
				}
				f.commit(t, change)
			}
			retry(1)
			recovery := f.start(t, f.created.TaskID)
			captured, err := f.inputs.LoadRecoveryInput(t.Context(), recovery.Work, recovery.Attempt.Attempt)
			if err != nil {
				t.Fatal(err)
			}
			report := volumeexecution.RecoveryReport{AttemptID: recovery.Attempt.Attempt.ID,
				ForwardID: view.Attempt.Attempt.ID, Failure: captured.Source.Evidence, HelperStopped: true,
				Observation: volumeexecution.Observation{VolumeID: input.VolumeId,
					Input: recovery.Attempt.Attempt.Input, DirectoryReady: true, VolumeAbsent: absent, At: time.Now().UTC()}}
			raw, _, err := volumeexecution.EncodeRecoveryReport(recovery.Attempt.Attempt,
				captured.Source, input.Action, input.VolumeId, captured.Boundary, report)
			if err != nil {
				t.Fatal(err)
			}
			identity, err := workexecution.EncodeAgentAttempt(recovery.Attempt.Attempt)
			if err != nil {
				t.Fatal(err)
			}
			ack, err := f.reports.RecoverVolume(t.Context(), f.authorization,
				&agentpb.VolumeWorkRecoveryReportRequest{Attempt: identity, Report: raw})
			if err != nil || ack.BoundaryCompleted {
				t.Fatalf("inspection fabricated successful creation: %v", err)
			}
			current, original, err := f.queue.ReadAttempt(t.Context(), f.created.TaskID, view.Work.Item.Item.ID, 1, 0)
			if err != nil || original.Attempt.Outcome != workexecution.Failed ||
				original.Attempt.Result == nil || original.Attempt.Result.ID != failure.Evidence.InputId {
				t.Fatalf("inspection changed original failure: %v", err)
			}
			idle, err := f.queue.HostIdle(t.Context(), view.Attempt.Attempt.Executor.HostID, current.ReadRevision)
			if err != nil || idle != absent {
				t.Fatalf("host idle=%t with proved absence=%t: %v", idle, absent, err)
			}
			applied, err := f.inputs.ReadApplied(t.Context(), input.VolumeId, 0)
			if err != nil || applied != nil {
				t.Fatalf("inspection fabricated applied storage: %v", err)
			}
			if !absent {
				if current.Item.Item.Phase != workexecution.Attention ||
					current.Item.Item.Cleanup != workexecution.CleanupStopped {
					t.Fatal("unknown runtime state lost its recovery ownership")
				}
				return
			}
			if current.Item.Item.Phase != workexecution.Settled ||
				current.Item.Item.Cleanup != workexecution.CleanupComplete || current.Item.Item.Outcome != workexecution.Failed {
				t.Fatal("proved absent creation did not settle its retained failure")
			}
			retry(2)
			forward := f.start(t, f.created.TaskID)
			if forward.Attempt.Attempt.Direction != workexecution.Forward {
				t.Fatal("explicit Retry did not admit a new captured creation")
			}
			progress = f.progress(t, forward)
			for progress.Stage != volumeexecution.Complete {
				progress = f.advance(t, forward, progress)
			}
			f.complete(t, forward, progress)
			if applied, err := f.inputs.ReadApplied(t.Context(), input.VolumeId, 0); err != nil || applied == nil {
				t.Fatalf("explicit creation Retry did not publish applied storage: %v", err)
			}
		})
	}
}
