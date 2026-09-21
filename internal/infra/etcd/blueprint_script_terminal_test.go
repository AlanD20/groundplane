package etcd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testscriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: terminal completion must validate the executed resource set before
// releasing hook sources, and an interrupted source release must remain retryable.
func TestBlueprintCompletedHooksReleaseSourceFenceBeforeNextPublication(t *testing.T) {
	ctx := context.Background()
	published, err := publishEnvironmentBlueprintAtomicShape(t, environmentBlueprintAtomicShape{
		name: "successful hook publication", releases: 1, hooks: 1, physicalSources: 1, realHookSources: true,
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	seedBlueprintTerminalCandidateLedger(t, published)
	tasks, _ := newTaskRepository(published.store)
	tasks.blueprintTerminalStore = published.store
	scripts, _ := newScriptRepository(published.store)
	task := published.task
	agentID := ids.NewAt(ids.KindAgent, task.CreatedAt, 19990)
	claim, found, err := tasks.ClaimNextTask(ctx, agentID, 1, task.CreatedAt.Add(time.Second))
	if err != nil || !found {
		t.Fatalf("claim = %t, %v", found, err)
	}
	prepare := func() error {
		page, listErr := scripts.ListScripts(ctx, published.environmentID, testkeyvalue.PageRequest{Limit: 64})
		if listErr != nil {
			return listErr
		}
		desired := make([]testscripts.Record, len(page.Items))
		for i, item := range page.Items {
			desired[i] = item.Record
			desired[i].ActiveReferences = 0
		}
		change, prepareErr := scripts.PrepareBlueprintScriptPublication(ctx, published.environmentID,
			page.Items[0].ReadRevision, scriptBlueprintGenerationID(991), page.Items, desired, nil)
		change.Clear()
		return prepareErr
	}
	if err := prepare(); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("active hook publication = %v", err)
	}
	steps, err := releaseHookExecutionSteps(task)
	if err != nil || len(steps) != 1 {
		t.Fatalf("hook steps = %v, %v", steps, err)
	}
	value, err := published.store.Get(ctx, testscriptexecutions.ScriptExecutionKey(steps[0].executionID))
	if err != nil {
		t.Fatal(err)
	}
	record, err := testrecordcodec.Decode[testscriptexecutions.ScriptExecutionRecord](
		value.Entry.Value,
		"script-execution",
	)
	if err != nil {
		t.Fatal(err)
	}
	zero := int32(0)
	evidence := []struct {
		state    testscriptexecutions.ScriptExecutionState
		evidence testscriptexecutions.ScriptCheckpointEvidence
	}{
		{testscriptexecutions.ScriptExecutionStartAuthorized, testscriptexecutions.ScriptCheckpointEvidence{
			Kind:            testscriptexecutions.ScriptCheckpointEvidenceStartAuthorized,
			StartAuthorized: &testscriptexecutions.ScriptStartAuthorizedEvidence{},
		},
		},
		{testscriptexecutions.ScriptExecutionBodyPrepared, testscriptexecutions.ScriptCheckpointEvidence{
			Kind: testscriptexecutions.ScriptCheckpointEvidenceBodyPrepared,
			BodyPrepared: &testscriptexecutions.ScriptBodyPreparedEvidence{
				BodySHA256: record.BodySHA256,
				UID:        65534,
				GID:        65534,
				Device:     10,
				Inode:      20,
				Leaf:       "body",
			},
		},
		},
		{testscriptexecutions.ScriptExecutionContainerCreated, testscriptexecutions.ScriptCheckpointEvidence{
			Kind: testscriptexecutions.ScriptCheckpointEvidenceContainerCreated,
			ContainerCreated: &testscriptexecutions.ScriptContainerCreatedEvidence{
				ContainerID:           strings.Repeat("a", 64),
				OwnershipLabelsSHA256: strings.Repeat("b", 64),
			},
		},
		},
		{testscriptexecutions.ScriptExecutionOutcomeRecorded, testscriptexecutions.ScriptCheckpointEvidence{
			Kind: testscriptexecutions.ScriptCheckpointEvidenceOutcome,
			Outcome: &testscriptexecutions.ScriptOutcomeEvidence{
				Reason:     testscriptexecutions.ScriptOutcomeNormalExit,
				ExitCode:   &zero,
				ObservedAt: task.CreatedAt.Add(5 * time.Second),
			},
		},
		},
		{testscriptexecutions.ScriptExecutionCleanupProven, testscriptexecutions.ScriptCheckpointEvidence{
			Kind: testscriptexecutions.ScriptCheckpointEvidenceCleanup,
			Cleanup: &testscriptexecutions.ScriptCleanupEvidence{
				ContainerID:              strings.Repeat("a", 64),
				BodyDevice:               10,
				BodyInode:                20,
				BodyLeaf:                 "body",
				ContainerAbsent:          true,
				BodyAbsent:               true,
				ExecutionDirectoryAbsent: true,
			},
		},
		},
	}
	for i, checkpoint := range evidence {
		input := scriptCheckpointTestInput(record, task.CreatedAt.Add(time.Duration(i+2)*time.Second))
		input.AgentID, input.AssignmentID = agentID, claim.Assignment.Record.AssignmentID
		input.State, input.Evidence = checkpoint.state, checkpoint.evidence
		input.PayloadSHA256 = strings.Repeat(string(rune('1'+i)), 64)
		updated, checkpointErr := scripts.CheckpointScriptExecution(ctx, input)
		if checkpointErr != nil {
			t.Fatalf("checkpoint %s: %v", checkpoint.state, checkpointErr)
		}
		record = updated.Record
		if record.State == testscriptexecutions.ScriptExecutionOutcomeRecorded {
			_, prematureErr := tasks.AcknowledgeTask(
				ctx,
				agentID,
				1,
				task.ID,
				claim.Assignment.Record.AssignmentID,
				testtaskjournal.TaskStatusCompleted,
				testtaskjournal.TaskResultRecord{
					Kind:           testtaskjournal.TaskResultCompose,
					ExecutionEpoch: 1,
					Diagnostic:     testtaskjournal.TaskResultDiagnosticNone,
				},
				task.CreatedAt.Add(time.Minute),
			)
			if !isKind(prematureErr, errs.KindStateConflict) {
				t.Fatalf("completion before runner cleanup = %v", prematureErr)
			}
		}
	}
	if err := prepare(); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("cleanup alone released parent fence: %v", err)
	}
	result := testtaskjournal.TaskResultRecord{
		Kind:           testtaskjournal.TaskResultCompose,
		ExecutionEpoch: 1,
		Diagnostic:     testtaskjournal.TaskResultDiagnosticNone,
	}
	interrupted, _ := newTaskRepository(
		&scriptSourceReferenceFailureStore{memoryHierarchyStore: published.store, failAt: 2},
	)
	interrupted.blueprintTerminalStore = published.store
	if _, err := interrupted.AcknowledgeTask(ctx, agentID, 1, task.ID, claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, result, task.CreatedAt.Add(time.Minute)); !isKind(
		err,
		errs.KindInternal,
	) {
		t.Fatalf("interrupted source release = %v", err)
	}
	pending, err := published.store.Get(ctx, testtaskjournal.TaskStorageKey(task.ID))
	if err != nil {
		t.Fatal(err)
	}
	pendingTask, err := DecodeTaskRecord(pending.Entry.Value)
	if err != nil || pendingTask.Status != testtaskjournal.TaskStatusRunning {
		t.Fatalf("source release terminalized early: %s, %v", pendingTask.Status, err)
	}
	terminal, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		1,
		task.ID,
		claim.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		result,
		task.CreatedAt.Add(time.Minute),
	)
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("terminal = %s, %v", terminal.Record.Status, err)
	}
	if err := prepare(); err != nil {
		t.Fatalf("next Blueprint publication after completed hook: %v", err)
	}
	replay, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		1,
		task.ID,
		claim.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		result,
		task.CreatedAt.Add(2*time.Minute),
	)
	if err != nil || replay.Revision != terminal.Revision {
		t.Fatalf("terminal replay = %v, %v", replay.Revision, err)
	}
}

func blueprintTerminalFixtureScript(t *testing.T, task TaskRecord) testscripts.Record {
	t.Helper()
	hook := scriptCheckpointTestRecord(task.CreatedAt.Add(time.Millisecond))
	script, err := testscripts.NewRecord(
		task.Owner.EnvironmentID,
		ids.NewAt(ids.KindService, task.CreatedAt, 7600),
		core.Script{
			ID: hook.ScriptID, Slug: "migrate", ServiceName: "api", When: core.ScriptPostDeploy, Body: "exit 0",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	script.ScriptSetGeneration = task.ID
	return script
}
