package etcd

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testscriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (fixture *ExecutedArtifactFixture) HookDependencies(
	t *testing.T,
) (*ScriptRepository, *testscriptsourcepublication.Authority) {
	t.Helper()
	scripts, err := newScriptRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	sources, err := testscriptsourcepublication.NewAuthority(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	return scripts, sources
}

// StageHookScripts uses ordinary invisible Script generation preparation, not
// synthetic execution records or manufactured plan hashes.
func (fixture *ExecutedArtifactFixture) StageHookScripts(
	t *testing.T,
	task TaskRecord,
	service testservices.ServiceRecord,
	count int,
) []testscripts.Record {
	t.Helper()
	if count == 0 {
		return nil
	}
	scripts, _ := fixture.HookDependencies(t)
	records := make([]testscripts.Record, count)
	generations := make([]testscripts.BodyGenerationRecord, count)
	for index := range records {
		record, err := testscripts.NewRecord(task.Target, service.Desired.ID, core.Script{
			ID: ids.New(ids.KindScript), Slug: fmt.Sprintf("hook-%02d", index), ServiceName: service.Desired.Name,
			When: core.ScriptPreDeploy, Body: "exit 0",
		})
		if err != nil {
			t.Fatal(err)
		}
		record.ScriptSetGeneration = task.ID
		records[index], generations[index] = record, scriptBlueprintGeneration(record)
	}
	publication, err := scripts.PrepareBlueprintScriptPublication(context.Background(), task.Target,
		fixture.store.revision, task.ID, nil, records, generations)
	if err != nil {
		t.Fatal(err)
	}
	fixture.hookScripts = publication
	return records
}

// CompleteHookCheckpoints records cleanup against the actual generated plan.
// Docker evidence is supplied; this does not claim physical hook execution.
func (fixture *ExecutedArtifactFixture) CompleteHookCheckpoints(
	t *testing.T,
	agentID string,
	claim TaskAssignment,
	plan *agentpb.ExecutionPlan,
) {
	t.Helper()
	ctx := context.Background()
	task := claim.Task.Record
	scripts, _ := fixture.HookDependencies(t)
	for _, metadata := range plan.ScriptBodyArtifacts {
		versioned, err := scripts.GetScriptExecution(ctx, metadata.ScriptExecutionId)
		if err != nil {
			t.Fatal(err)
		}
		record := versioned.Record
		zero := int32(0)
		checkpoints := []struct {
			state testscriptexecutions.ScriptExecutionState
			proof testscriptexecutions.ScriptCheckpointEvidence
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
		for index, checkpoint := range checkpoints {
			input := scriptCheckpointTestInput(record, task.CreatedAt.Add(time.Duration(index+2)*time.Second))
			input.AgentID, input.AssignmentID = agentID, claim.Assignment.Record.AssignmentID
			input.State, input.Evidence = checkpoint.state, checkpoint.proof
			input.PayloadSHA256 = strings.Repeat(string(rune('1'+index)), 64)
			updated, err := scripts.CheckpointScriptExecution(ctx, input)
			if err != nil {
				t.Fatalf("actual hook checkpoint %s: %v", checkpoint.state, err)
			}
			record = updated.Record
		}
	}
}

// RejectOversizedHookTerminal proves budget rejection before any reference,
// closing-report, Task, assignment, or candidate mutation can be committed.
func (fixture *ExecutedArtifactFixture) RejectOversizedHookTerminal(
	t *testing.T,
	agentID string,
	claim TaskAssignment,
) {
	t.Helper()
	task := claim.Task.Record
	result := testtaskjournal.TaskResultRecord{
		Kind:           testtaskjournal.TaskResultCompose,
		Diagnostic:     testtaskjournal.TaskResultDiagnosticNone,
		ExecutionEpoch: claim.Assignment.Record.ExecutionEpoch,
		Projects: []testtaskjournal.TaskObservedProjectSummary{
			{ProjectName: strings.Repeat("x", testkeyvalue.MaximumBytes),
				ObservedAt: task.CreatedAt.Add(time.Minute)},
		},
	}
	if err := testtaskjournal.ValidateTaskResult(result, task.Steps, testtaskjournal.TaskStatusCompleted); err != nil {
		t.Fatalf("oversized report is not otherwise valid: %v", err)
	}
	before := fixture.store.revision
	_, err := fixture.Tasks.AcknowledgeTask(
		context.Background(),
		agentID,
		1,
		task.ID,
		claim.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		result,
		task.CreatedAt.Add(time.Minute),
	)
	if !isKind(err, errs.KindValidationFailed) || fixture.store.revision != before {
		t.Fatalf("oversized terminal report changed source authority before rejection: revision %d -> %d, error %v",
			before, fixture.store.revision, err)
	}
	// Use the production physical validator with a valid, long namespace. There
	// is no etcd client: any attempted commit instead of rejection is a test bug.
	physical, err := newTaskRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	physical.blueprintTerminalStore = &store{root: strings.Repeat("/namespace", 4096)}
	result.Projects = nil
	_, err = physical.AcknowledgeTask(
		context.Background(),
		agentID,
		1,
		task.ID,
		claim.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		result,
		task.CreatedAt.Add(time.Minute),
	)
	if !isKind(err, errs.KindValidationFailed) || fixture.store.revision != before {
		t.Fatalf("physical request rejection changed source authority: revision %d -> %d, error %v",
			before, fixture.store.revision, err)
	}
}

// ProveHookTerminalReconnect interrupts bounded source release after the
// generated hooks are closed and requires Controller-only completion.
func (fixture *ExecutedArtifactFixture) ProveHookTerminalReconnect(
	t *testing.T,
	agentID string,
	claim TaskAssignment,
	plan *agentpb.ExecutionPlan,
) {
	t.Helper()
	ctx := context.Background()
	task := claim.Task.Record
	scripts, _ := fixture.HookDependencies(t)
	failedStore := &scriptSourceReferenceFailureStore{
		memoryHierarchyStore: fixture.store.memoryHierarchyStore,
		failAt:               2,
	}
	event := testtaskjournal.TaskEventInput{Identity: testtaskjournal.TaskEventIdentity{
		AssignmentID: claim.Assignment.Record.AssignmentID, AgentID: agentID, AgentGeneration: 1,
		TaskID: task.ID, StepID: task.Steps[0].ID, Attempt: claim.Assignment.Record.ExecutionEpoch, Ordinal: 1,
	}, State: testtaskjournal.TaskEventStateRunning, Payload: []byte(`{"message":"progress before closure"}`)}
	if _, err := fixture.Tasks.AppendTaskEvent(ctx, event, task.CreatedAt.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	interrupted, err := newTaskRepository(failedStore)
	if err != nil {
		t.Fatal(err)
	}
	interrupted.blueprintTerminalStore = failedStore
	result := testtaskjournal.TaskResultRecord{
		Kind:           testtaskjournal.TaskResultCompose,
		ExecutionEpoch: claim.Assignment.Record.ExecutionEpoch,
		Diagnostic:     testtaskjournal.TaskResultDiagnosticNone,
	}
	before, err := fixture.Tasks.GetTaskAssignment(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := interrupted.AcknowledgeTask(ctx, agentID, 1, task.ID, claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, result, task.CreatedAt.Add(time.Minute)); !isKind(
		err,
		errs.KindInternal,
	) {
		t.Fatalf("actual interrupted hook completion: %v", err)
	}
	after, err := fixture.Tasks.GetTaskAssignment(ctx, task.ID)
	if err != nil || after.Task.Revision != before.Task.Revision ||
		after.Assignment.Revision != before.Assignment.Revision {
		t.Fatalf("interrupted source release changed Task/assignment authority: %v", err)
	}
	rootRead, err := fixture.store.Get(ctx, testscriptsourceevidence.ScriptSourceRootKey(task.OperationID))
	if err != nil || rootRead == nil || rootRead.Entry == nil {
		t.Fatalf("interrupted source root: %v", err)
	}
	root, err := testscriptsourceevidence.DecodeScriptOperationSourceRoot(rootRead.Entry.Value)
	if err != nil || root.Phase != testscriptsourceevidence.ScriptOperationSourceReleasing ||
		root.ReleasePath != testscriptsourceevidence.ScriptSourceReleaseNormal {
		t.Fatalf("interruption did not reach normal source release: %v", err)
	}
	report, reportValue, err := fixture.Tasks.readScriptClosingReport(ctx, after)
	if err != nil || reportValue == nil || reportValue.ModRevision != rootRead.Entry.ModRevision ||
		!report.ObservedAt.Equal(
			task.CreatedAt.Add(time.Minute),
		) || !testtaskjournal.TaskResultsEqual(report.Result, result) {
		t.Fatalf("original report was not captured atomically with source closure: %v", err)
	}
	beforeMismatch := fixture.store.revision
	duplicate, err := fixture.Tasks.AppendTaskEvent(ctx, event, task.CreatedAt.Add(2*time.Minute))
	if err != nil || !duplicate.Duplicate || fixture.store.revision != beforeMismatch {
		t.Fatalf("closing Task rejected or rewrote an identical event retransmission: %v", err)
	}
	changed := result
	changed.Projects = []testtaskjournal.TaskObservedProjectSummary{
		{ProjectName: "different", ObservedAt: report.ObservedAt},
	}
	if _, err := fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, task.ID, claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, changed, task.CreatedAt.Add(2*time.Minute)); !isKind(
		err,
		errs.KindStateConflict,
	) ||
		fixture.store.revision != beforeMismatch {
		t.Fatalf("changed closing report did not reject without writes: %v", err)
	}
	failure := result
	failure.ReconciliationRequired = true
	if _, err := fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, task.ID, claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failure, task.CreatedAt.Add(2*time.Minute)); !isKind(
		err,
		errs.KindStateConflict,
	) ||
		fixture.store.revision != beforeMismatch {
		t.Fatalf("competing failure changed closing execution authority: %v", err)
	}
	failure.ReconciliationRequired, failure.Diagnostic = false, testtaskjournal.TaskResultDiagnosticTimeoutBeforeEffect
	if _, err := fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, task.ID, claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failure, task.CreatedAt.Add(2*time.Minute)); !isKind(
		err,
		errs.KindStateConflict,
	) ||
		fixture.store.revision != beforeMismatch {
		t.Fatalf("timeout reclassified closing execution authority: %v", err)
	}
	_, lateErr := fixture.Tasks.AppendTaskEvent(
		ctx,
		testtaskjournal.TaskEventInput{Identity: testtaskjournal.TaskEventIdentity{
			AssignmentID: claim.Assignment.Record.AssignmentID, AgentID: agentID, AgentGeneration: 1,
			TaskID: task.ID, StepID: task.Steps[0].ID, Attempt: claim.Assignment.Record.ExecutionEpoch, Ordinal: 2,
		}, State: testtaskjournal.TaskEventStateRunning, Payload: []byte(`{"message":"late event after closure"}`)},
		task.CreatedAt.Add(2*time.Minute),
	)
	if !isKind(lateErr, errs.KindStateConflict) || fixture.store.revision != beforeMismatch {
		t.Fatalf("late event changed closing Task authority: %v", lateErr)
	}
	for _, metadata := range plan.ScriptBodyArtifacts {
		execution, err := scripts.GetScriptExecution(ctx, metadata.ScriptExecutionId)
		if err != nil || execution.Record.ActiveReference ||
			execution.Record.State != testscriptexecutions.ScriptExecutionCleanupProven {
			t.Fatalf("interrupted hook was not cleanup-proven and closed: %v", err)
		}
	}
	restarted, err := newTaskRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	restarted.blueprintTerminalStore = fixture.store
	if fixture.TerminalCommitFault != "" {
		fixture.proveClosingTerminalFaults(t, after)
		return
	}
	resumed, err := restarted.ReconnectAgentAssignment(ctx, claim)
	if err != nil || resumed.Task.Record.Status != testtaskjournal.TaskStatusCompleted {
		_, artifactErr := scripts.ResolveScriptAssignmentArtifacts(ctx, resumed.Task.Record, plan)
		t.Fatalf(
			"actual closing hook reconnect: status=%s mode=%s epoch=%d error=%v artifacts=%v",
			resumed.Task.Record.Status,
			resumed.Assignment.Record.ExecutionMode,
			resumed.Assignment.Record.ExecutionEpoch,
			err,
			artifactErr,
		)
	}
	if resumed.Assignment.Revision != before.Assignment.Revision ||
		resumed.Task.Record.FinishedAt == nil || !resumed.Task.Record.FinishedAt.Equal(report.ObservedAt) {
		t.Fatal("Controller completion changed assignment authority or original terminal timestamp")
	}
	for _, key := range []string{testscriptsourceevidence.ScriptSourceRootKey(task.OperationID), blueprintClosingReportKey(task.ID)} {
		read, err := fixture.store.Get(ctx, key)
		if err != nil || read.Entry != nil {
			t.Fatalf("terminal continuation authority was not removed: %s %v", key, err)
		}
	}
	replayed, err := restarted.AcknowledgeTask(
		ctx,
		agentID,
		1,
		task.ID,
		claim.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		result,
		task.CreatedAt.Add(3*time.Minute),
	)
	if err != nil || replayed.Revision != resumed.Task.Revision {
		t.Fatalf("restarted terminal replay changed Task authority: %v", err)
	}
}
