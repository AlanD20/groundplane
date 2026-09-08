package etcd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	ref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: pending Blueprint abort must retire every staged Script authority before the same Task becomes terminal.
func TestAbortPendingBlueprintReleasesScriptExecutionAuthorityBeforeTerminalization(t *testing.T) {
	t.Run("bounded release resumes and exact replay permits successor publication", func(t *testing.T) {
		published, err := publishEnvironmentBlueprintAtomicShape(t, environmentBlueprintAtomicShape{
			name: "pending abort", releases: 32, hooks: 16, physicalSources: 17,
		}, false)
		if err != nil {
			t.Fatal(err)
		}
		successorOperationID, successorMember := captureSuccessorScriptSource(t, published)
		failing := &scriptSourceReferenceFailureStore{memoryHierarchyStore: published.store, failAt: 3}
		tasks, err := newTaskRepository(failing)
		if err != nil {
			t.Fatal(err)
		}
		terminalAt := published.task.CreatedAt.Add(time.Second)
		if _, err = tasks.AbortPendingTask(
			context.Background(), published.task.ID, terminalAt,
		); !isKind(err, errs.KindInternal) {
			t.Fatalf("AbortPendingTask(interrupted) error = %v", err)
		}
		assertBlueprintPendingAbortReleaseStarted(t, published)
		tasks, _ = newTaskRepository(published.store)
		terminal, err := tasks.AbortPendingTask(context.Background(), published.task.ID, terminalAt)
		if err != nil || terminal.Record.Status != TaskStatusAborted {
			t.Fatalf("AbortPendingTask(resume) = %#v, %v", terminal, err)
		}
		assertBlueprintPendingAbortReleased(t, published)
		replay, err := tasks.AbortPendingTask(context.Background(), published.task.ID, terminalAt.Add(time.Second))
		if err != nil || replay.Revision != terminal.Revision {
			t.Fatalf("AbortPendingTask(replay) = %#v, %v", replay, err)
		}
		publishSuccessorScriptSourceRoot(t, published, successorOperationID, successorMember)
	})

	t.Run("assignment and abort have exactly one winner", func(t *testing.T) {
		for _, abortFirst := range []bool{true, false} {
			published, err := publishEnvironmentBlueprintAtomicShape(t, environmentBlueprintAtomicShape{
				name: "abort assignment ordering", releases: 11, hooks: 2, physicalSources: 3,
			}, false)
			if err != nil {
				t.Fatal(err)
			}
			tasks, _ := newTaskRepository(published.store)
			agentID := ids.NewAt(ids.KindAgent, published.task.CreatedAt, 201)
			abort := func() error {
				_, abortErr := tasks.AbortPendingTask(
					context.Background(), published.task.ID, published.task.CreatedAt.Add(2*time.Second),
				)
				return abortErr
			}
			claim := func() (bool, error) {
				_, claimed, claimErr := tasks.ClaimNextTask(
					context.Background(), agentID, 1, published.task.CreatedAt.Add(time.Second),
				)
				return claimed, claimErr
			}
			if abortFirst {
				if abortErr := abort(); abortErr != nil {
					t.Fatal(abortErr)
				}
				if claimed, claimErr := claim(); claimErr != nil || claimed {
					t.Fatalf("claim after abort = %t, %v", claimed, claimErr)
				}
				continue
			}
			if claimed, claimErr := claim(); claimErr != nil || !claimed {
				t.Fatalf("claim before abort = %t, %v", claimed, claimErr)
			}
			if abortErr := abort(); !isKind(abortErr, errs.KindStateConflict) {
				t.Fatalf("abort after claim error = %v", abortErr)
			}
		}
	})

	t.Run("tampered sealed source root blocks claim and abort", func(t *testing.T) {
		for _, digest := range []bool{false, true} {
			for _, claim := range []bool{false, true} {
				published, err := publishEnvironmentBlueprintAtomicShape(t, environmentBlueprintAtomicShape{
					name: "tampered source root", releases: 11, hooks: 2, physicalSources: 3,
				}, false)
				if err != nil {
					t.Fatal(err)
				}
				tamperBlueprintPendingAbortRoot(t, published, digest)
				tasks, _ := newTaskRepository(published.store)
				if claim {
					_, _, claimErr := tasks.ClaimNextTask(
						context.Background(), ids.NewAt(ids.KindAgent, published.task.CreatedAt, 220), 1,
						published.task.CreatedAt.Add(time.Second),
					)
					if !isKind(claimErr, errs.KindInternal) {
						t.Fatalf("ClaimNextTask(tampered root) error = %v", claimErr)
					}
					continue
				}
				_, abortErr := tasks.AbortPendingTask(
					context.Background(), published.task.ID, published.task.CreatedAt.Add(time.Second),
				)
				if !isKind(abortErr, errs.KindInternal) {
					t.Fatalf("AbortPendingTask(tampered root) error = %v", abortErr)
				}
			}
		}
	})

	t.Run("controller cleanup discriminator is exact and Blueprint-only", func(t *testing.T) {
		at := scriptSourceReferenceTestTime()
		record := scriptCheckpointTestRecord(at)
		outcome := ScriptOutcomeEvidence{Reason: ScriptOutcomeAbortBeforeStart, ObservedAt: at.Add(time.Second)}
		cleanup := ScriptCleanupEvidence{ContainerAbsent: true, BodyAbsent: true, ExecutionDirectoryAbsent: true}
		digest, err := scriptControllerCleanupSHA256(outcome, cleanup)
		if err != nil {
			t.Fatal(err)
		}
		record.State = ScriptExecutionCleanupProven
		record.Outcome = &outcome
		record.Cleanup = &cleanup
		record.LastCheckpointSHA256 = digest
		record.ActiveReference = false
		record.UpdatedAt = outcome.ObservedAt
		if validateScriptExecutionRecord(record) == nil {
			t.Fatal("assignmentless cleanup without Controller authority accepted")
		}
		for _, taskType := range []TaskType{TaskScript, TaskDeploy, TaskRollback} {
			manualRecord := scriptCheckpointTestRecord(at)
			nonBlueprint := TaskRecord{Type: taskType, Status: TaskStatusPending}
			if _, transitionErr := abortScriptExecutionBeforeStart(
				manualRecord,
				nonBlueprint,
				releaseHookExecutionStep{stepID: manualRecord.StepID, executionID: manualRecord.ID},
				outcome.ObservedAt,
			); !isKind(transitionErr, errs.KindStateConflict) {
				t.Fatalf("%s Controller cleanup transition error = %v", taskType, transitionErr)
			}
		}
		record.ControllerCleanup = ScriptControllerCleanupBlueprintPendingAbort
		record.StartAuthorized = true
		if validateScriptExecutionRecord(record) == nil {
			t.Fatal("Controller cleanup discriminator outside exact shape accepted")
		}
	})

	t.Run("completed replay rejects cleanup timestamp mismatch", func(t *testing.T) {
		published, err := publishEnvironmentBlueprintAtomicShape(t, environmentBlueprintAtomicShape{
			name: "replay timestamp mismatch", releases: 11, hooks: 2, physicalSources: 3,
		}, false)
		if err != nil {
			t.Fatal(err)
		}
		tasks, _ := newTaskRepository(published.store)
		terminal, err := tasks.AbortPendingTask(
			context.Background(), published.task.ID, published.task.CreatedAt.Add(time.Second),
		)
		if err != nil {
			t.Fatal(err)
		}
		steps, _ := releaseHookExecutionSteps(published.task)
		read, err := published.store.Get(context.Background(), scriptExecutionKey(steps[0].executionID))
		if err != nil || read.Entry == nil {
			t.Fatalf("Script execution = %#v, %v", read, err)
		}
		record, err := decodeEnvelope[ScriptExecutionRecord](read.Entry.Value, "script-execution")
		if err != nil {
			t.Fatal(err)
		}
		mismatchedAt := terminal.Record.FinishedAt.Add(time.Nanosecond)
		record.Outcome.ObservedAt = mismatchedAt
		record.UpdatedAt = mismatchedAt
		record.LastCheckpointSHA256, err = scriptControllerCleanupSHA256(*record.Outcome, *record.Cleanup)
		if err != nil {
			t.Fatal(err)
		}
		value, err := encodeEnvelope("script-execution", record)
		if err != nil {
			t.Fatal(err)
		}
		_, err = published.store.Transact(context.Background(), nil, []Mutation{{
			Type: MutationPut, Key: read.Entry.Key, Value: value,
		}})
		clear(value)
		if err != nil {
			t.Fatal(err)
		}
		if _, replayErr := tasks.AbortPendingTask(
			context.Background(), published.task.ID, terminal.Record.FinishedAt.Add(time.Second),
		); !isKind(replayErr, errs.KindInternal) {
			t.Fatalf("AbortPendingTask(timestamp mismatch replay) error = %v", replayErr)
		}
	})
}

func tamperBlueprintPendingAbortRoot(
	t *testing.T,
	published environmentBlueprintAtomicPublication,
	digest bool,
) {
	t.Helper()
	ctx := context.Background()
	read, err := published.store.Get(ctx, scriptSourceRootKey(published.task.OperationID))
	if err != nil || read.Entry == nil {
		t.Fatalf("source root = %#v, %v", read, err)
	}
	root, err := decodeScriptOperationSourceRoot(read.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	if digest {
		root.MembershipSHA256 = strings.Repeat("f", 64)
	} else {
		root.MembershipCount++
	}
	value, err := encodeEnvelope("script-operation-source-root", root)
	if err != nil {
		t.Fatal(err)
	}
	result, err := published.store.Transact(ctx, []Condition{{
		Key: read.Entry.Key, ModRevision: read.Entry.ModRevision,
	}}, []Mutation{{Type: MutationPut, Key: read.Entry.Key, Value: value}})
	clear(value)
	if err != nil || !result.Succeeded {
		t.Fatalf("tamper source root = %#v, %v", result, err)
	}
}

func assertBlueprintPendingAbortReleaseStarted(t *testing.T, published environmentBlueprintAtomicPublication) {
	t.Helper()
	ctx := context.Background()
	rootRead, err := published.store.Get(ctx, scriptSourceRootKey(published.task.OperationID))
	if err != nil || rootRead.Entry == nil {
		t.Fatalf("releasing source root = %#v, %v", rootRead, err)
	}
	root, err := decodeScriptOperationSourceRoot(rootRead.Entry.Value)
	if err != nil || root.Phase != ScriptOperationSourceReleasing ||
		root.ReleasePath != ScriptSourceReleaseNormal || root.RetryDisposition != ScriptRetryDispositionAbandoned ||
		root.ReleaseCursor == 0 || root.ReleaseCursor >= root.MembershipCount {
		t.Fatalf("releasing source root = %#v, %v", root, err)
	}
	pending, err := published.store.Get(ctx, taskKey(published.task.ID))
	if err != nil || pending.Entry == nil {
		t.Fatalf("pending Task = %#v, %v", pending, err)
	}
	record, err := decodeTaskRecord(pending.Entry.Value)
	if err != nil || record.Status != TaskStatusPending {
		t.Fatalf("Task during release = %#v, %v", record, err)
	}
}

func assertBlueprintPendingAbortReleased(t *testing.T, published environmentBlueprintAtomicPublication) {
	t.Helper()
	ctx := context.Background()
	root, err := published.store.Get(ctx, scriptSourceRootKey(published.task.OperationID))
	if err != nil || root.Entry != nil {
		t.Fatalf("released source root = %#v, %v", root, err)
	}
	steps, _ := releaseHookExecutionSteps(published.task)
	for _, step := range steps {
		read, getErr := published.store.Get(ctx, scriptExecutionKey(step.executionID))
		if getErr != nil || read.Entry == nil {
			t.Fatalf("Script execution %q = %#v, %v", step.executionID, read, getErr)
		}
		record, decodeErr := decodeEnvelope[ScriptExecutionRecord](read.Entry.Value, "script-execution")
		if decodeErr != nil || record.Outcome == nil || !pendingScriptAbortExecutionMatches(
			record, published.task, step, record.Outcome.ObservedAt,
		) {
			t.Fatalf("released Script execution %q = %#v, %v", step.executionID, record, decodeErr)
		}
	}
}

func captureSuccessorScriptSource(
	t *testing.T,
	published environmentBlueprintAtomicPublication,
) (string, ScriptSourcePreparationMember) {
	t.Helper()
	ctx := context.Background()
	page, err := published.store.Range(ctx, RangeRequest{
		Prefix: ref.ReversePrefix(published.task.OperationID), Limit: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	successorOperationID := ids.NewAt(ids.KindOperation, published.task.CreatedAt, 9100)
	for _, value := range page.Values {
		reference, decodeErr := decodeEnvelope[ScriptSourceReference](value.Value, "script-source-reference")
		if decodeErr != nil || reference.Source.Kind != ScriptSourceService {
			continue
		}
		sourceKey := serviceRuntimeKey(reference.Source.ServiceID)
		source, getErr := published.store.Get(ctx, sourceKey)
		if getErr != nil || source.Entry == nil {
			t.Fatalf("successor source = %#v, %v", source, getErr)
		}
		reference.OperationID = successorOperationID
		reference.ScriptExecutionID = scriptSourceReferenceExecutionID(published.task.CreatedAt, 201)
		reference.SourceModRevision = source.Entry.ModRevision
		return successorOperationID, ScriptSourcePreparationMember{
			Reference: reference,
			Evidence:  ScriptSourceEvidence{Existing: &ScriptExistingSourceEvidence{SourceKey: sourceKey}},
		}
	}
	t.Fatal("Blueprint source set has no reusable Service source")
	return "", ScriptSourcePreparationMember{}
}

func publishSuccessorScriptSourceRoot(
	t *testing.T,
	published environmentBlueprintAtomicPublication,
	operationID string,
	member ScriptSourcePreparationMember,
) {
	t.Helper()
	ctx := context.Background()
	authority, err := newScriptSourceReferenceAuthority(published.store)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := authority.Prepare(ctx, operationID, []ScriptSourcePreparationMember{member})
	if err != nil {
		t.Fatalf("Prepare(successor) error = %v", err)
	}
	fragment, err := authority.FinalPublicationFragment(ctx, prepared)
	if err != nil {
		t.Fatalf("FinalPublicationFragment(successor) error = %v", err)
	}
	defer fragment.Clear()
	result, err := published.store.Transact(ctx, fragment.conditions, fragment.mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("successor source-root publication = %#v, %v", result, err)
	}
}
