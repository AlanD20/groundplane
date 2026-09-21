package etcd

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testscriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	testscriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

func TestReleaseHookProjectionConditionsFenceDesiredHeadRootAndZoneTombstones(t *testing.T) {
	at := time.Date(2026, 9, 1, 2, 30, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	revisionID := ids.NewAt(ids.KindTask, at, 2)
	zoneID := ids.NewAt(ids.KindNetwork, at, 3)
	sources := testscriptsourcequeries.ScriptExecutionSources{
		Environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{ID: environmentID},
		},
		DesiredHead: testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintHead]{Revision: 31},
		DesiredProjection: testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
			Record: testenvironmentprojection.EnvironmentComposeProjection{
				EnvironmentID: environmentID,
				RevisionID:    revisionID,
			},
			Revision: 37,
		},
		Networks: []testkeyvalue.Versioned[testzones.Record]{{Record: testzones.Record{
			EnvironmentID: environmentID,
			Desired:       testScriptExecutionZone(zoneID, "hooks", environmentID),
		}}},
	}
	conditions := scriptExecutionProjectionConditions(sources)
	want := []testkeyvalue.Condition{
		{Key: testblueprints.EnvironmentBlueprintHeadKey(environmentID), ModRevision: 31},
		{Key: testblueprints.EnvironmentBlueprintRootKey(environmentID, revisionID), ModRevision: 37},
		{Key: testdeletions.TombstoneKey(string(testdeletions.DeletionTargetZone), zoneID)},
	}
	if !reflect.DeepEqual(conditions, want) {
		t.Fatalf("release hook projection conditions = %#v, want %#v", conditions, want)
	}
}

func TestReleaseHookFailureUsesOwningMemberOrdinal(t *testing.T) {
	task := TaskRecord{
		Params: map[string]string{
			testreleaserender.ReleaseHookStepMemberParam("hook"):    "3",
			testreleaserender.ReleaseHookStepExecutionParam("hook"): "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
		Steps: []testtaskjournal.TaskStepRecord{
			{
				Kind: testtaskjournal.TaskStepOperation,
				ID:   "one-apply",
			}, {Kind: testtaskjournal.TaskStepOperation, ID: "one-health"}, {Kind: testtaskjournal.TaskStepOperation, ID: "one-switch"}, {Kind: testtaskjournal.TaskStepOperation, ID: "one-probe"}, {Kind: testtaskjournal.TaskStepOperation, ID: "one-compensate"},
			{
				Kind: testtaskjournal.TaskStepOperation,
				ID:   "two-apply",
			}, {Kind: testtaskjournal.TaskStepOperation, ID: "two-health"}, {Kind: testtaskjournal.TaskStepOperation, ID: "two-switch"}, {Kind: testtaskjournal.TaskStepOperation, ID: "two-probe"}, {Kind: testtaskjournal.TaskStepOperation, ID: "two-compensate"},
			{
				Kind: testtaskjournal.TaskStepOperation,
				ID:   "three-apply",
			}, {Kind: testtaskjournal.TaskStepOperation, ID: "three-health"}, {Kind: testtaskjournal.TaskStepOperation, ID: "three-switch"}, {Kind: testtaskjournal.TaskStepOperation, ID: "three-probe"}, {Kind: testtaskjournal.TaskStepOperation, ID: "three-compensate"},
			{Kind: testtaskjournal.TaskStepOperation, ID: "hook"},
		},
	}
	if got := releaseFailedOrdinalFromResult(task, testtaskjournal.TaskResultRecord{FailedStepID: "hook"}); got != 3 {
		t.Fatalf("hook failed ordinal = %d, want 3", got)
	}
	if got := releaseFailedOrdinalFromResult(task, testtaskjournal.TaskResultRecord{FailedStepID: "two-switch"}); got != 2 {
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
	script, err := testscripts.NewRecord(record.EnvironmentID, record.ServiceID, core.Script{
		ID: record.ScriptID, Slug: "notify", ServiceName: "api", When: core.ScriptOnFailure, Body: "exit 0",
	})
	if err != nil {
		t.Fatal(err)
	}
	script.ActiveReferences = 1
	script.ScriptSetGeneration = record.ScriptSetGeneration
	executionValue, err := testrecordcodec.Encode("script-execution", record)
	if err != nil {
		t.Fatal(err)
	}
	scriptValue, err := testscripts.EncodeRecord(script)
	if err != nil {
		t.Fatal(err)
	}
	referenceValue, err := testrecordcodec.Encode("script-body-reference", struct {
		ExecutionID         string `json:"script_execution_id"`
		ScriptID            string `json:"script_id"`
		Generation          uint64 `json:"generation"`
		ScriptSetGeneration string `json:"script_set_generation"`
	}{ExecutionID: record.ID, ScriptID: record.ScriptID, Generation: record.ScriptGeneration,
		ScriptSetGeneration: record.ScriptSetGeneration})
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testscriptexecutions.ScriptExecutionKey(record.ID),
			Value: executionValue,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testscripts.ScriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, record.ScriptID),
			Value: scriptValue,
		},
		{
			Type: testkeyvalue.MutationPut,
			Key: testscriptexecutions.ScriptSetBodyForwardReferenceKey(
				record.EnvironmentID,
				record.ScriptSetGeneration,
				record.ScriptID,
				record.ScriptGeneration,
				record.ID,
			),
			Value: referenceValue,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testscriptexecutions.ScriptBodyReverseReferenceKey(record.ID),
			Value: referenceValue,
		},
	})
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed hook execution = %#v, %v", seeded, err)
	}
	seedKeys := []string{
		testscriptexecutions.ScriptExecutionKey(record.ID),
		testscripts.ScriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, record.ScriptID),
		testscriptexecutions.ScriptSetBodyForwardReferenceKey(
			record.EnvironmentID,
			record.ScriptSetGeneration,
			record.ScriptID,
			record.ScriptGeneration,
			record.ID,
		),
		testscriptexecutions.ScriptBodyReverseReferenceKey(record.ID),
	}
	seedRead, err := store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: seedKeys})
	if err != nil || len(seedRead.Values) != len(seedKeys) {
		t.Fatalf("read seeded hook references = %#v, %v", seedRead, err)
	}
	for index, value := range seedRead.Values {
		if value == nil {
			t.Fatalf("seeded hook reference %d is missing: %s", index, seedKeys[index])
		}
	}
	storedExecution, err := testrecordcodec.Decode[testscriptexecutions.ScriptExecutionRecord](
		seedRead.Values[0].Value,
		"script-execution",
	)
	if err != nil || testscriptexecutions.ValidateScriptExecutionRecord(storedExecution) != nil {
		t.Fatalf("seeded hook execution is invalid: %#v, %v", storedExecution, err)
	}
	storedScript, err := testscripts.DecodeRecord(seedRead.Values[1].Value)
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
		ID: taskID, OperationID: record.OperationID, PlanHash: record.PlanHash, Type: testtaskjournal.TaskDeploy,
		Params: map[string]string{
			testreleaserender.ReleaseHookStepExecutionParam(stepID): record.ID,
		}, Steps: []testtaskjournal.TaskStepRecord{{Kind: testtaskjournal.TaskStepOperation, ID: stepID}},
	}
	read, err := store.Get(ctx, testscriptexecutions.ScriptExecutionKey(record.ID))
	if err != nil {
		t.Fatal(err)
	}
	processed, err := repository.finalizeReleaseHookExecutionBatch(ctx, task, now.Add(time.Second), read.ReadRevision)
	if err != nil || !processed {
		t.Fatalf("finalizeReleaseHookExecutionBatch() = %v, %v", processed, err)
	}
	values, err := store.GetMany(
		ctx,
		testkeyvalue.GetManyRequest{Keys: []string{testscriptexecutions.ScriptExecutionKey(
			record.ID,
		), testscripts.ScriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, record.ScriptID), testscriptexecutions.ScriptSetBodyForwardReferenceKey(
			record.EnvironmentID,
			record.ScriptSetGeneration,
			record.ScriptID,
			record.ScriptGeneration,
			record.ID,
		), testscriptexecutions.ScriptBodyReverseReferenceKey(record.ID),
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	updated, err := testrecordcodec.Decode[testscriptexecutions.ScriptExecutionRecord](
		values.Values[0].Value,
		"script-execution",
	)
	if err != nil || updated.ActiveReference || updated.State != testscriptexecutions.ScriptExecutionNotStarted ||
		updated.Outcome != nil {
		t.Fatalf("released skipped hook = %#v, %v", updated, err)
	}
	updatedScript, err := testscripts.DecodeRecord(values.Values[1].Value)
	if err != nil || updatedScript.ActiveReferences != 0 || values.Values[2] != nil || values.Values[3] != nil {
		t.Fatalf("released hook references = %#v/%#v/%#v/%#v", updatedScript, values.Values[2], values.Values[3], err)
	}
	processed, err = repository.finalizeReleaseHookExecutionBatch(
		ctx,
		task,
		now.Add(2*time.Second),
		values.ReadRevision,
	)
	if err != nil || processed {
		t.Fatalf("replay terminalization = %v, %v", processed, err)
	}
}

func TestFinalizeReleaseHookExecutionBatchPreservesExecutedAssignmentForAcknowledgementReplay(t *testing.T) {
	// Rationale: release acknowledgement is batched. Releasing a completed
	// hook reference must not erase assignment evidence required to decode the
	// execution when the next acknowledgement batch resumes.
	ctx := context.Background()
	now := time.Date(2026, 9, 1, 15, 0, 0, 0, time.UTC)
	store := newMemoryHierarchyStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	scripts, err := newScriptRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	record := scriptCheckpointTestRecord(now)
	record.ScriptSetGeneration = record.EnvironmentID
	record.CurrentTaskID = ids.NewAt(ids.KindTask, now, 20)
	record.StepID = ids.NewAt(ids.KindStep, now, 21)
	input := scriptCheckpointTestInput(record, now.Add(time.Second))
	assignmentID := input.AssignmentID
	input.State = testscriptexecutions.ScriptExecutionStartAuthorized
	input.Evidence = testscriptexecutions.ScriptCheckpointEvidence{
		Kind: testscriptexecutions.ScriptCheckpointEvidenceStartAuthorized, StartAuthorized: &testscriptexecutions.ScriptStartAuthorizedEvidence{},
	}
	record = advanceScriptCheckpointTest(t, record, input)
	input.ExpectedState, input.State = record.State, testscriptexecutions.ScriptExecutionBodyPrepared
	input.PayloadSHA256 = strings.Repeat("2", 64)
	input.Evidence = testscriptexecutions.ScriptCheckpointEvidence{
		Kind: testscriptexecutions.ScriptCheckpointEvidenceBodyPrepared,
		BodyPrepared: &testscriptexecutions.ScriptBodyPreparedEvidence{
			BodySHA256: record.BodySHA256, UID: 65534, GID: 65534, Device: 10, Inode: 20, Leaf: "body",
		},
	}
	record = advanceScriptCheckpointTest(t, record, input)
	input.ExpectedState, input.State = record.State, testscriptexecutions.ScriptExecutionContainerCreated
	input.PayloadSHA256 = strings.Repeat("3", 64)
	input.Evidence = testscriptexecutions.ScriptCheckpointEvidence{
		Kind: testscriptexecutions.ScriptCheckpointEvidenceContainerCreated,
		ContainerCreated: &testscriptexecutions.ScriptContainerCreatedEvidence{
			ContainerID: strings.Repeat("a", 64), OwnershipLabelsSHA256: strings.Repeat("b", 64),
		},
	}
	record = advanceScriptCheckpointTest(t, record, input)
	input.ExpectedState, input.State = record.State, testscriptexecutions.ScriptExecutionOutcomeRecorded
	input.PayloadSHA256 = strings.Repeat("4", 64)
	input.Evidence = testscriptexecutions.ScriptCheckpointEvidence{
		Kind: testscriptexecutions.ScriptCheckpointEvidenceOutcome,
		Outcome: &testscriptexecutions.ScriptOutcomeEvidence{
			Reason:     testscriptexecutions.ScriptOutcomeRuntimeFailure,
			ObservedAt: now.Add(2 * time.Second),
		},
	}
	record = advanceScriptCheckpointTest(t, record, input)
	input.ExpectedState, input.State = record.State, testscriptexecutions.ScriptExecutionCleanupProven
	input.PayloadSHA256 = strings.Repeat("5", 64)
	input.Evidence = testscriptexecutions.ScriptCheckpointEvidence{
		Kind: testscriptexecutions.ScriptCheckpointEvidenceCleanup,
		Cleanup: &testscriptexecutions.ScriptCleanupEvidence{
			ContainerID: strings.Repeat("a", 64), BodyDevice: 10, BodyInode: 20, BodyLeaf: "body",
			ContainerAbsent: true, BodyAbsent: true, ExecutionDirectoryAbsent: true,
		},
	}
	record = advanceScriptCheckpointTest(t, record, input)

	script, err := testscripts.NewRecord(record.EnvironmentID, record.ServiceID, core.Script{
		ID: record.ScriptID, Slug: "migrate", ServiceName: "api", When: core.ScriptPostDeploy, Body: "exit 1",
	})
	if err != nil {
		t.Fatal(err)
	}
	script.ActiveReferences = 1
	script.ScriptSetGeneration = record.ScriptSetGeneration
	executionValue, err := testrecordcodec.Encode("script-execution", record)
	if err != nil {
		t.Fatal(err)
	}
	scriptValue, err := testscripts.EncodeRecord(script)
	if err != nil {
		t.Fatal(err)
	}
	referenceValue, err := testrecordcodec.Encode("script-body-reference", struct {
		ExecutionID         string `json:"script_execution_id"`
		ScriptID            string `json:"script_id"`
		Generation          uint64 `json:"generation"`
		ScriptSetGeneration string `json:"script_set_generation"`
	}{record.ID, record.ScriptID, record.ScriptGeneration, record.ScriptSetGeneration})
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testscriptexecutions.ScriptExecutionKey(record.ID),
			Value: executionValue,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testscripts.ScriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, record.ScriptID),
			Value: scriptValue,
		},
		{
			Type: testkeyvalue.MutationPut,
			Key: testscriptexecutions.ScriptSetBodyForwardReferenceKey(
				record.EnvironmentID,
				record.ScriptSetGeneration,
				record.ScriptID,
				record.ScriptGeneration,
				record.ID,
			),
			Value: referenceValue,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testscriptexecutions.ScriptBodyReverseReferenceKey(record.ID),
			Value: referenceValue,
		},
	})
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed executed release hook = %#v, %v", seeded, err)
	}
	task := TaskRecord{
		ID: record.CurrentTaskID, OperationID: record.OperationID, PlanHash: record.PlanHash,
		Type: testtaskjournal.TaskDeploy, Params: map[string]string{testreleaserender.TaskReleasePublicationParam: ids.NewULID(), testreleaserender.ReleaseHookStepExecutionParam(record.StepID): record.ID},
		Steps: []testtaskjournal.TaskStepRecord{{Kind: testtaskjournal.TaskStepOperation, ID: record.StepID}},
	}
	read, err := store.Get(ctx, testscriptexecutions.ScriptExecutionKey(record.ID))
	if err != nil {
		t.Fatal(err)
	}
	processed, err := repository.finalizeReleaseHookExecutionBatch(ctx, task, now.Add(3*time.Second), read.ReadRevision)
	if err != nil || !processed {
		t.Fatalf("finalize executed release hook = %v, %v", processed, err)
	}
	stored, err := scripts.GetScriptExecution(ctx, record.ID)
	if err != nil || stored.Record.ActiveReference || stored.Record.AssignmentID != assignmentID {
		t.Fatalf("round-tripped executed hook = %#v, %v", stored, err)
	}
	processed, err = repository.finalizeReleaseHookExecutionBatch(
		ctx, task, now.Add(4*time.Second), stored.ReadRevision,
	)
	if err != nil || processed {
		t.Fatalf("acknowledgement replay after hook release = %v, %v", processed, err)
	}
}
