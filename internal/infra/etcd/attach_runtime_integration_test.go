package etcd

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testattachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// QA: ATT-09, ATT-10, SVC-13; local storage proof, not live qualification.
// Rationale: a real sealed preparation must never create authority from a
// missing historical runtime; stopped consumers remain validation-only.
func TestAttachRuntimePreparationReadsExistingAcknowledgement(t *testing.T) {
	store := newAttachTestStore()
	scope := seedAttachScope(t, t.Context(), store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	attach, _ := testPendingAttach(t, scope, 381, "runtime", nil)
	task := validTaskRecord(testAttachTime)
	task.Type, task.Target = testtaskjournal.TaskAttach, attach.ID
	prior := nativeAttachRuntimeFixture(t, scope.Environment.Record.ID, attach.ServiceID)
	plan := nativeAttachFixturePlan(t, prior, task, ids.New(ids.KindConfig), "new-backing", true)
	if _, err := repository.PrepareAttachRuntime(t.Context(), plan); err == nil {
		t.Fatal("missing runtime accepted")
	}
	revision := putNativeAttachRuntime(t, store, prior)
	prepared, err := repository.PrepareAttachRuntime(t.Context(), plan)
	if err != nil || len(prepared.Updates) != 1 || prepared.Updates[0].PreviousRevision != revision {
		t.Fatalf("prepare = %#v, %v", prepared, err)
	}
	if !bytes.Contains(prepared.Updates[0].Runtime.CurrentArtifact, []byte("new-backing")) ||
		bytes.Contains(prepared.Updates[0].Runtime.CurrentArtifact, []byte("old-backing")) {
		t.Fatal("prepared runtime did not replace the backing network")
	}
	plan = nativeAttachFixturePlan(t, prior, task, ids.New(ids.KindConfig), "new-backing", false)
	prepared, err = repository.PrepareAttachRuntime(t.Context(), plan)
	if err != nil || len(prepared.Updates) != 0 {
		t.Fatalf("stopped prepare: %v", err)
	}
}

// QA: ATT-08, ATT-10, SVC-16, SVC-17; local storage proof.
// Rationale: successful Attach and later Detach must commit their selected
// runtime with the Attach/Task terminal state, replay without rewriting it,
// and retain the self-contained record after the earlier planning input is gone.
func TestAttachRuntimeAcknowledgementAndDetachAreAtomicAndReplayable(t *testing.T) {
	store := newAttachTestStore()
	scope := seedAttachScope(t, t.Context(), store)
	attaches, err := NewAttachRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	record, facts := testPendingAttach(t, scope, 382, "runtime", nil)
	prior := nativeAttachRuntimeFixture(t, scope.Environment.Record.ID, record.ServiceID)
	putNativeAttachRuntime(t, store, prior)
	current := createTestAttach(
		t,
		t.Context(),
		attaches,
		scope,
		record,
		&facts,
		func(input testattachrender.AttachTaskRenderInput, task TaskRecord, _ testidempotency.IdempotencyMarker) (testattachrender.AttachTaskRenderInput, TaskRecord) {
			return prepareNativeAttachEnvelope(t, attaches, prior, input, task, "new-backing")
		},
	)
	terminal, acknowledged := acknowledgeNativeAttach(
		t,
		store,
		current.Record.TaskID,
		record.CreatedAt.Add(time.Second),
	)
	if acknowledged.Runtime.ReleaseID != prior.Runtime.ReleaseID ||
		!bytes.Contains(acknowledged.Runtime.CurrentArtifact, []byte("new-backing")) {
		t.Fatal("Attach lost Release identity or newly applied network")
	}
	current, err = attaches.GetAttach(t.Context(), record.ID)
	if err != nil || current.Record.Status != core.AttachReady || current.Revision != terminal.Revision {
		t.Fatalf("Attach terminal is not atomic: %v", err)
	}
	detach := publishTestDetach(
		t,
		t.Context(),
		attaches,
		scope,
		current,
		record.CreatedAt.Add(10*time.Second),
		func(input testattachrender.AttachTaskRenderInput, task TaskRecord, _ testidempotency.IdempotencyMarker) (testattachrender.AttachTaskRenderInput, TaskRecord) {
			return prepareNativeAttachEnvelope(t, attaches, acknowledged, input, task, "remaining-backing")
		},
	)
	_, detachedRuntime := acknowledgeNativeAttach(t, store, detach.ID, detach.CreatedAt.Add(time.Second))
	if bytes.Contains(detachedRuntime.Runtime.CurrentArtifact, []byte("new-backing")) ||
		!bytes.Contains(detachedRuntime.Runtime.CurrentArtifact, []byte("remaining-backing")) {
		t.Fatal("Detach did not preserve the selected remaining network")
	}
	if _, err := attaches.GetAttach(t.Context(), record.ID); err == nil {
		t.Fatal("completed Detach retained Attach")
	}
	before, err := store.Get(t.Context(), serviceruntimerecord.Key(record.ServiceID))
	if err != nil {
		t.Fatal(err)
	}
	// Task pruning owns this input, not the independently retained runtime.
	if _, err := store.Transact(t.Context(), nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: testattachrender.AttachTaskRenderInputKey(terminal.Record.PlanID)}}); err != nil {
		t.Fatal(err)
	}
	after, err := store.Get(t.Context(), serviceruntimerecord.Key(record.ServiceID))
	if err != nil || !bytes.Equal(before.Entry.Value, after.Entry.Value) ||
		serviceruntimerecord.Validate(detachedRuntime) != nil {
		t.Fatal("removing earlier Task input damaged current runtime")
	}
}

func acknowledgeNativeAttach(
	t *testing.T,
	store *attachTestStore,
	taskID string,
	at time.Time,
) (testkeyvalue.Versioned[TaskRecord], serviceruntimerecord.Record) {
	t.Helper()
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	agentID := ids.New(ids.KindAgent)
	claim, found, err := tasks.ClaimNextTask(t.Context(), agentID, 1, at)
	if err != nil || !found || claim.Task.Record.ID != taskID {
		t.Fatalf("claim: %v", err)
	}
	result := completedComposeTaskResult()
	result.ExecutionEpoch = claim.Assignment.Record.ExecutionEpoch
	terminal, err := tasks.AcknowledgeTask(t.Context(), agentID, 1, taskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, result, at.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	input, err := (&AttachRepository{store: store}).GetAttachTaskRenderInput(t.Context(), terminal.Record.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	serviceID := input.Record.RuntimePreparation.Updates[0].Runtime.ServiceID
	value, err := store.Get(t.Context(), serviceruntimerecord.Key(serviceID))
	if err != nil || value.Entry == nil || value.Entry.ModRevision != terminal.Revision {
		t.Fatalf("runtime not atomic: %v", err)
	}
	record, err := decodeAcknowledgedServiceRuntime(value.Entry.Value, input.Record.EnvironmentID, serviceID)
	if err != nil || record.Source.TaskID != taskID || record.Source.ExecutionEpoch != result.ExecutionEpoch {
		t.Fatalf("runtime source differs: %v", err)
	}
	// Recreate the repository to prove the replay does not depend on process memory.
	restarted, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := restarted.AcknowledgeTask(t.Context(), agentID, 1, taskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, result, at.Add(2*time.Second))
	if err != nil || replay.Revision != terminal.Revision {
		t.Fatalf("terminal replay: %v", err)
	}
	unchanged, err := store.Get(t.Context(), serviceruntimerecord.Key(serviceID))
	if err != nil || !bytes.Equal(value.Entry.Value, unchanged.Entry.Value) {
		t.Fatal("replay rewrote runtime")
	}
	changed := result
	changed.ExecutionEpoch++
	if _, err := restarted.AcknowledgeTask(t.Context(), agentID, 1, taskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, changed, at.Add(3*time.Second)); err == nil {
		t.Fatal("changed replay accepted")
	}
	return terminal, record
}

// QA: ATT-10, ATT-11, SVC-17; local publication/claim/terminal proof.
// Rationale: neither publication nor Claim may proceed after its acknowledged
// source changes, and a failed operation must not promote prepared input.
func TestAttachRuntimeStaleSourceAndFailurePreserveAuthority(t *testing.T) {
	for _, phase := range []string{"publication", "claim", "failure", "acknowledgement"} {
		t.Run(phase, func(t *testing.T) {
			store := newAttachTestStore()
			scope := seedAttachScope(t, t.Context(), store)
			attaches, err := NewAttachRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			record, facts := testPendingAttach(t, scope, 383, "runtime", nil)
			prior := nativeAttachRuntimeFixture(t, scope.Environment.Record.ID, record.ServiceID)
			putNativeAttachRuntime(t, store, prior)
			created := createTestAttach(
				t,
				t.Context(),
				attaches,
				scope,
				record,
				&facts,
				func(input testattachrender.AttachTaskRenderInput, task TaskRecord, marker testidempotency.IdempotencyMarker) (testattachrender.AttachTaskRenderInput, TaskRecord) {
					input, task = prepareNativeAttachEnvelope(t, attaches, prior, input, task, "new-backing")
					if phase == "publication" {
						putNativeAttachRuntime(t, store, prior)
						result, err := attaches.CreateAttachWithTask(
							t.Context(),
							scope,
							record,
							&facts,
							input,
							task,
							marker,
						)
						if err != nil {
							t.Fatalf("stale publication failed before its transaction: %v", err)
						}
						outcome, _, conflict, classifyErr := result.Classify()
						if outcome == IdempotencyKnownApplied || classifyErr != nil ||
							!errors.Is(conflict, errs.New(errs.KindStateConflict, "")) {
							t.Fatalf("stale publication = %v, %v, %v", outcome, conflict, classifyErr)
						}
						if _, err := attaches.GetAttach(t.Context(), record.ID); !errs.IsNotFound(err) {
							t.Fatalf("stale publication retained Attach: %v", err)
						}
						input, task = prepareNativeAttachEnvelope(t, attaches, prior, input, task, "new-backing")
					}
					return input, task
				},
			)
			if phase == "publication" {
				return
			}
			if phase == "claim" {
				putNativeAttachRuntime(t, store, prior)
			}
			tasks, err := newTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			agentID := ids.New(ids.KindAgent)
			claim, found, err := tasks.ClaimNextTask(t.Context(), agentID, 1, record.CreatedAt.Add(time.Second))
			if phase == "claim" {
				if found || !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
					t.Fatalf("stale claim: %v, %v", found, err)
				}
				return
			}
			if err != nil || !found {
				t.Fatalf("claim: %v", err)
			}
			if phase == "acknowledgement" {
				putNativeAttachRuntime(t, store, prior)
			}
			status := testtaskjournal.TaskStatusCompleted
			result := completedComposeTaskResult()
			result.ExecutionEpoch = claim.Assignment.Record.ExecutionEpoch
			if phase == "failure" {
				status, result.ExitCode, result.Diagnostic = testtaskjournal.TaskStatusFailed, 1, testtaskjournal.TaskResultDiagnosticComposeFailed
			}
			before, err := store.Get(t.Context(), serviceruntimerecord.Key(record.ServiceID))
			if err != nil {
				t.Fatal(err)
			}
			taskBefore, err := tasks.GetTask(t.Context(), created.Record.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			attachBefore, err := attaches.GetAttach(t.Context(), record.ID)
			if err != nil {
				t.Fatal(err)
			}
			_, err = tasks.AcknowledgeTask(t.Context(), agentID, 1, created.Record.TaskID,
				claim.Assignment.Record.AssignmentID, status, result, record.CreatedAt.Add(2*time.Second))
			if phase == "failure" && err != nil || phase == "acknowledgement" && err == nil {
				t.Fatalf("acknowledge(%s): %v", phase, err)
			}
			if phase == "acknowledgement" {
				currentTask, taskErr := tasks.GetTask(t.Context(), created.Record.TaskID)
				currentAttach, attachErr := attaches.GetAttach(t.Context(), record.ID)
				if taskErr != nil || attachErr != nil || currentTask.Revision != taskBefore.Revision ||
					currentAttach.Revision != attachBefore.Revision {
					t.Fatalf("rejected acknowledgement partially published: %v, %v", taskErr, attachErr)
				}
			}
			after, err := store.Get(t.Context(), serviceruntimerecord.Key(record.ServiceID))
			if err != nil || !bytes.Equal(before.Entry.Value, after.Entry.Value) ||
				strings.Contains(string(after.Entry.Value), "new-backing") {
				t.Fatal("unacknowledged runtime replaced prior authority")
			}
		})
	}
}
