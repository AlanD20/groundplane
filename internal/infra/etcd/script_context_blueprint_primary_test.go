package etcd

import (
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: Blueprint source preparation changes counts, not the selected
// Script metadata. Both inherited and explicit captures need the final CAS.
func TestScriptContextBlueprintPrimaryFenceRejectsConcurrentEdits(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "inherited", true: "explicit"}[explicit], func(t *testing.T) {
			ctx := context.Background()
			store, _, execution, _, _ := manualScriptLifecycleFixture(t)
			hook := scriptContextBlueprintPrimaryFixture(t, store, execution, explicit)
			primary := hook.Sources.Script.Record
			primary.Desired.Body = hook.Sources.BodyGeneration.Record.Body
			primary.ActiveReferences++
			writeScriptContextPrimaryFixture(t, store, primary)
			ledger := &ReleaseLedger{store: &releasePlanningTestStore{memoryHierarchyStore: store}}
			conditions, err := ledger.blueprintScriptPrimaryConditions(
				ctx,
				[]ReleaseHookExecutionPublication{hook, hook},
			)
			if err != nil || len(conditions) != 1 {
				t.Fatalf("prepared primary must have one exact shared fence: %v, %v", conditions, err)
			}
			primary.ActiveReferences++
			writeScriptContextPrimaryFixture(t, store, primary)
			conditions, err = ledger.blueprintScriptPrimaryConditions(ctx, []ReleaseHookExecutionPublication{hook})
			if err != nil {
				t.Fatalf("reference-count-only change rejected: %v", err)
			}
			primary.Desired.Execution = &core.ScriptExecution{
				Mode: core.ScriptExecutionExplicit, Image: "example/setup@sha256:" + strings.Repeat("c", 64), User: "12:34",
			}
			writeScriptContextPrimaryFixture(t, store, primary)
			if _, err := ledger.blueprintScriptPrimaryConditions(ctx, []ReleaseHookExecutionPublication{hook}); !isKind(
				err,
				errs.KindStateConflict,
			) {
				t.Fatalf("metadata edit before the final read accepted: %v", err)
			}
			key := scriptExecutionKey(execution.ID)
			result, err := store.Transact(
				ctx,
				conditions,
				[]Mutation{{Type: MutationPut, Key: key, Value: []byte("published")}},
			)
			if err != nil || result.Succeeded || store.valueAt(key, store.revision) != nil {
				t.Fatalf("metadata edit after the final read escaped CAS: %v, %v", result.Succeeded, err)
			}
		})
	}
}

// Rationale: a genuine primary belonging to another Script cannot fence the
// selected execution, even if its desired context is identical.
func TestScriptContextBlueprintPrimaryFenceRejectsDifferentScript(t *testing.T) {
	store, _, execution, _, _ := manualScriptLifecycleFixture(t)
	hook := scriptContextBlueprintPrimaryFixture(t, store, execution, true)
	hook.Execution.ScriptID = "scr_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	ledger := &ReleaseLedger{store: &releasePlanningTestStore{memoryHierarchyStore: store}}
	if _, err := ledger.blueprintScriptPrimaryConditions(context.Background(), []ReleaseHookExecutionPublication{hook}); !isKind(
		err,
		errs.KindValidationFailed,
	) {
		t.Fatalf("unrelated primary accepted: %v", err)
	}
}

// Rationale: the actual final Blueprint transaction must consume the primary
// fences, so an edit after preparation cannot expose a Task or Release marker.
func TestScriptContextBlueprintFinalTransactionRejectsLatePrimaryEdit(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		shape := environmentBlueprintAtomicShape{
			releases: 1, hooks: 1, physicalSources: 1, explicitHooks: explicit, scriptEditAfterPreparation: true,
		}
		published, err := publishEnvironmentBlueprintAtomicShape(t, shape, false)
		if err != nil {
			t.Fatal(err)
		}
		outcome, _, conflict, err := published.result.Classify()
		if err != nil || outcome != IdempotencyKnownConflict || conflict == nil {
			t.Fatalf("late Script edit was not fenced (explicit=%t): %v, %v, %v", explicit, outcome, conflict, err)
		}
		for _, key := range []string{
			taskKey(published.task.ID), taskQueueKey(published.task.Executor, published.task.ID),
			releasePublicationKey(published.releasePublicationID), environmentBlueprintHeadKey(published.environmentID),
			scriptSourceRootPrefix + published.task.OperationID,
		} {
			if published.store.valueAt(key, published.store.revision) != nil {
				t.Fatalf("failed publication exposed authority at %s", key)
			}
		}
	}
}
