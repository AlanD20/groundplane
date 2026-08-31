package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

func TestReleaseHookFailureUsesOwningMemberOrdinal(t *testing.T) {
	task := TaskRecord{
		Params: map[string]string{
			ReleaseHookStepMemberParam("hook"):    "3",
			ReleaseHookStepExecutionParam("hook"): "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
		Steps: []TaskStepRecord{
			{ID: "one-apply"}, {ID: "one-health"}, {ID: "one-switch"}, {ID: "one-probe"}, {ID: "one-compensate"},
			{ID: "two-apply"}, {ID: "two-health"}, {ID: "two-switch"}, {ID: "two-probe"}, {ID: "two-compensate"},
			{ID: "three-apply"}, {ID: "three-health"}, {ID: "three-switch"}, {ID: "three-probe"}, {ID: "three-compensate"},
			{ID: "hook"},
		},
	}
	if got := releaseFailedOrdinalFromResult(task, TaskResultRecord{FailedStepID: "hook"}); got != 3 {
		t.Fatalf("hook failed ordinal = %d, want 3", got)
	}
	if got := releaseFailedOrdinalFromResult(task, TaskResultRecord{FailedStepID: "two-switch"}); got != 2 {
		t.Fatalf("core failed ordinal = %d, want 2", got)
	}
}

func TestFinalizeReleaseHookExecutionBatchReleasesSkippedReferences(t *testing.T) {
	// Rationale: hooks skipped by a successful release or an earlier phase failure
	// must release their Script and body-generation references without inventing an outcome.
	ctx := context.Background()
	now := time.Date(2026, 8, 31, 13, 0, 0, 0, time.UTC)
	store := newMemoryHierarchyStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	record := scriptCheckpointTestRecord(now)
	record.ScriptSetGeneration = record.EnvironmentID
	taskID := ids.NewAt(ids.KindTask, now, 20)
	stepID := ids.NewAt(ids.KindStep, now, 21)
	record.CurrentTaskID, record.StepID = taskID, stepID
	script, err := NewScriptRecord(record.EnvironmentID, record.ServiceID, core.Script{
		ID: record.ScriptID, Slug: "notify", ServiceName: "api", When: core.ScriptOnFailure, Body: "exit 0",
	})
	if err != nil {
		t.Fatal(err)
	}
	script.ActiveReferences = 1
	script.ScriptSetGeneration = record.ScriptSetGeneration
	executionValue, err := encodeEnvelope("script-execution", record)
	if err != nil {
		t.Fatal(err)
	}
	scriptValue, err := encodeScriptRecord(script)
	if err != nil {
		t.Fatal(err)
	}
	referenceValue, err := encodeEnvelope("script-body-reference", struct {
		ExecutionID         string `json:"script_execution_id"`
		ScriptID            string `json:"script_id"`
		Generation          uint64 `json:"generation"`
		ScriptSetGeneration string `json:"script_set_generation"`
	}{ExecutionID: record.ID, ScriptID: record.ScriptID, Generation: record.ScriptGeneration,
		ScriptSetGeneration: record.ScriptSetGeneration})
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: scriptExecutionKey(record.ID), Value: executionValue},
		{Type: MutationPut, Key: scriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, record.ScriptID), Value: scriptValue},
		{Type: MutationPut, Key: scriptSetBodyForwardReferenceKey(record.EnvironmentID, record.ScriptSetGeneration, record.ScriptID, record.ScriptGeneration, record.ID), Value: referenceValue},
		{Type: MutationPut, Key: scriptBodyReverseReferenceKey(record.ID), Value: referenceValue},
	})
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed hook execution = %#v, %v", seeded, err)
	}
	seedKeys := []string{
		scriptExecutionKey(record.ID),
		scriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, record.ScriptID),
		scriptSetBodyForwardReferenceKey(record.EnvironmentID, record.ScriptSetGeneration, record.ScriptID, record.ScriptGeneration, record.ID),
		scriptBodyReverseReferenceKey(record.ID),
	}
	seedRead, err := store.GetMany(ctx, GetManyRequest{Keys: seedKeys})
	if err != nil || len(seedRead.Values) != len(seedKeys) {
		t.Fatalf("read seeded hook references = %#v, %v", seedRead, err)
	}
	for index, value := range seedRead.Values {
		if value == nil {
			t.Fatalf("seeded hook reference %d is missing: %s", index, seedKeys[index])
		}
	}
	storedExecution, err := decodeEnvelope[ScriptExecutionRecord](seedRead.Values[0].Value, "script-execution")
	if err != nil || validateScriptExecutionRecord(storedExecution) != nil {
		t.Fatalf("seeded hook execution is invalid: %#v, %v", storedExecution, err)
	}
	storedScript, err := decodeScriptRecord(seedRead.Values[1].Value)
	if err != nil || storedScript.ScriptSetGeneration != record.ScriptSetGeneration {
		t.Fatalf("seeded Script set binding is invalid: %#v, %v", storedScript, err)
	}
	if err := validateScriptBodyReference(seedRead.Values[2].Value, record); err != nil {
		t.Fatalf("seeded forward reference is invalid: %v", err)
	}
	if err := validateScriptBodyReference(seedRead.Values[3].Value, record); err != nil {
		t.Fatalf("seeded reverse reference is invalid: %v", err)
	}
	task := TaskRecord{
		ID: taskID, OperationID: record.OperationID, PlanHash: record.PlanHash, Type: TaskDeploy,
		Params: map[string]string{ReleaseHookStepExecutionParam(stepID): record.ID}, Steps: []TaskStepRecord{{ID: stepID}},
	}
	read, err := store.Get(ctx, scriptExecutionKey(record.ID))
	if err != nil {
		t.Fatal(err)
	}
	processed, err := repository.finalizeReleaseHookExecutionBatch(ctx, task, now.Add(time.Second), read.ReadRevision)
	if err != nil || !processed {
		t.Fatalf("finalizeReleaseHookExecutionBatch() = %v, %v", processed, err)
	}
	values, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
		scriptExecutionKey(record.ID), scriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, record.ScriptID),
		scriptSetBodyForwardReferenceKey(record.EnvironmentID, record.ScriptSetGeneration, record.ScriptID, record.ScriptGeneration, record.ID),
		scriptBodyReverseReferenceKey(record.ID),
	}})
	if err != nil {
		t.Fatal(err)
	}
	updated, err := decodeEnvelope[ScriptExecutionRecord](values.Values[0].Value, "script-execution")
	if err != nil || updated.ActiveReference || updated.State != ScriptExecutionNotStarted || updated.Outcome != nil {
		t.Fatalf("released skipped hook = %#v, %v", updated, err)
	}
	updatedScript, err := decodeScriptRecord(values.Values[1].Value)
	if err != nil || updatedScript.ActiveReferences != 0 || values.Values[2] != nil || values.Values[3] != nil {
		t.Fatalf("released hook references = %#v/%#v/%#v/%#v", updatedScript, values.Values[2], values.Values[3], err)
	}
	processed, err = repository.finalizeReleaseHookExecutionBatch(ctx, task, now.Add(2*time.Second), values.ReadRevision)
	if err != nil || processed {
		t.Fatalf("replay terminalization = %v, %v", processed, err)
	}
}
