package app

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
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
) (*etcd.ScriptRepository, *testscriptsourcepublication.Authority) {
	t.Helper()
	scripts, err := etcd.NewScriptRepository(fixture.store)
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
	task etcd.TaskRecord,
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
		generation, err := testscripts.NewScriptBodyGeneration(record)
		if err != nil {
			t.Fatal(err)
		}
		records[index], generations[index] = record, generation
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
	claim etcd.TaskAssignment,
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
			input := testscriptexecutions.ScriptCheckpointInput{
				TaskID: record.CurrentTaskID, OperationID: record.OperationID,
				AssignmentID: claim.Assignment.Record.AssignmentID, AgentID: agentID,
				AgentGeneration: 1, StepID: record.StepID, ExecutionID: record.ID, PlanHash: record.PlanHash,
				ExpectedState: record.State, PayloadSHA256: strings.Repeat("1", 64),
				At: task.CreatedAt.Add(time.Duration(index+2) * time.Second),
			}
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
	claim etcd.TaskAssignment,
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
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) || fixture.store.revision != before {
		t.Fatalf("oversized terminal report changed source authority before rejection: revision %d -> %d, error %v",
			before, fixture.store.revision, err)
	}
}

// ProveHookTerminalReconnect interrupts bounded source release after the
// generated hooks are closed and requires Controller-only completion.
func (fixture *ExecutedArtifactFixture) ProveHookTerminalReconnect(
	t *testing.T,
	agentID string,
	claim etcd.TaskAssignment,
	plan *agentpb.ExecutionPlan,
) {
	t.Helper()
	ctx := context.Background()
	task := claim.Task.Record
	scripts, _ := fixture.HookDependencies(t)
	failedStore := &scriptSourceReferenceFailureStore{
		releasePlanningTestStore: fixture.store,
		failAt:                   2,
	}
	event := testtaskjournal.TaskEventInput{Identity: testtaskjournal.TaskEventIdentity{
		AssignmentID: claim.Assignment.Record.AssignmentID, AgentID: agentID, AgentGeneration: 1,
		TaskID: task.ID, StepID: task.Steps[0].ID, Attempt: claim.Assignment.Record.ExecutionEpoch, Ordinal: 1,
	}, State: testtaskjournal.TaskEventStateRunning, Payload: []byte(`{"message":"progress before closure"}`)}
	if _, err := fixture.Tasks.AppendTaskEvent(ctx, event, task.CreatedAt.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	interrupted, err := etcd.NewTaskRepository(failedStore)
	if err != nil {
		t.Fatal(err)
	}
	result := testtaskjournal.TaskResultRecord{
		Kind:           testtaskjournal.TaskResultCompose,
		ExecutionEpoch: claim.Assignment.Record.ExecutionEpoch,
		Diagnostic:     testtaskjournal.TaskResultDiagnosticNone,
	}
	before, err := fixture.Tasks.GetTaskAssignment(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	terminalAt := task.CreatedAt.Add(time.Minute)
	if _, err := interrupted.AcknowledgeTask(ctx, agentID, 1, task.ID, claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, result, terminalAt); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
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
	report, reportValue := readIntegrationClosingReport(t, fixture.store, task.ID)
	if reportValue == nil || reportValue.ModRevision != rootRead.Entry.ModRevision ||
		!report.ObservedAt.Equal(terminalAt) || !testtaskjournal.TaskResultsEqual(report.Result, result) {
		t.Fatal("original report was not captured atomically with source closure")
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
	if _, err := fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, task.ID, claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, changed, task.CreatedAt.Add(2*time.Minute)); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) ||
		fixture.store.revision != beforeMismatch {
		t.Fatalf("changed closing report did not reject without writes: %v", err)
	}
	failure := result
	failure.ReconciliationRequired = true
	if _, err := fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, task.ID, claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failure, task.CreatedAt.Add(2*time.Minute)); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) ||
		fixture.store.revision != beforeMismatch {
		t.Fatalf("competing failure changed closing execution authority: %v", err)
	}
	failure.ReconciliationRequired, failure.Diagnostic = false, testtaskjournal.TaskResultDiagnosticTimeoutBeforeEffect
	if _, err := fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, task.ID, claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failure, task.CreatedAt.Add(2*time.Minute)); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
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
	if !errors.Is(lateErr, errs.New(errs.KindStateConflict, "")) || fixture.store.revision != beforeMismatch {
		t.Fatalf("late event changed closing Task authority: %v", lateErr)
	}
	for _, metadata := range plan.ScriptBodyArtifacts {
		execution, err := scripts.GetScriptExecution(ctx, metadata.ScriptExecutionId)
		if err != nil || execution.Record.ActiveReference ||
			execution.Record.State != testscriptexecutions.ScriptExecutionCleanupProven {
			t.Fatalf("interrupted hook was not cleanup-proven and closed: %v", err)
		}
	}
	restarted, err := etcd.NewTaskRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.TerminalCommitFault != "" {
		fixture.proveClosingTerminalFaults(t, after, agentID, result, terminalAt)
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
		resumed.Task.Record.FinishedAt == nil || !resumed.Task.Record.FinishedAt.Equal(terminalAt) {
		t.Fatal("Controller completion changed assignment authority or original terminal timestamp")
	}
	for _, key := range []string{
		testscriptsourceevidence.ScriptSourceRootKey(task.OperationID),
		integrationBlueprintClosingReportKey(task.ID),
	} {
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

type scriptSourceReferenceFailureStore struct {
	*releasePlanningTestStore
	transacts int
	failAt    int
}

func (store *scriptSourceReferenceFailureStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	store.transacts++
	if store.transacts == store.failAt {
		return testkeyvalue.TransactionResult{}, errs.New(errs.KindInternal, "injected source release failure")
	}
	return store.releasePlanningTestStore.Transact(ctx, conditions, mutations)
}

type blueprintTerminalFaultStore struct {
	*releasePlanningTestStore
	t                 *testing.T
	epochKey          string
	calls             int
	committedRevision int64
	fault             string
	raceRevision      int64
}

func (store *blueprintTerminalFaultStore) TransactBlueprintTaskTerminal(
	ctx context.Context,
	envelope etcd.BlueprintTaskTerminalTransaction,
) (testkeyvalue.TransactionResult, error) {
	store.calls++
	conditions, mutations, err := envelope.Operations()
	if err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	defer testkeyvalue.ClearMutationValues(mutations)
	if store.calls != 1 {
		store.t.Fatal("terminal persistence repeated after authority loss or uncertain commit")
	}
	if store.fault == "compare-loss" {
		value := store.valueAt(store.epochKey, store.revision)
		if value == nil {
			store.t.Fatal("terminal fault fixture has no Environment epoch")
		}
		if _, err := store.releasePlanningTestStore.Transact(ctx, nil, []testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut,
			Key:  store.epochKey, Value: value.Value,
		}}); err != nil {
			store.t.Fatal(err)
		}
		store.raceRevision = store.revision
		return store.releasePlanningTestStore.Transact(ctx, conditions, mutations)
	}
	result, err := store.releasePlanningTestStore.Transact(ctx, conditions, mutations)
	if err != nil || !result.Succeeded {
		store.t.Fatalf("terminal composition did not commit: %v", err)
	}
	store.committedRevision = result.Revision
	for _, mutation := range mutations {
		if mutation.Prefix {
			continue
		}
		value := store.valueAt(mutation.Key, store.revision)
		if mutation.Type == testkeyvalue.MutationPut && (value == nil || value.ModRevision != result.Revision) ||
			mutation.Type == testkeyvalue.MutationDelete && value != nil {
			store.t.Fatalf("terminal mutation did not share the atomic commit: %s", mutation.Key)
		}
	}
	return testkeyvalue.TransactionResult{}, errs.New(errs.KindInternal, "injected lost terminal commit response")
}

func (fixture *ExecutedArtifactFixture) proveClosingTerminalFaults(
	t *testing.T,
	current etcd.TaskAssignment,
	agentID string,
	result testtaskjournal.TaskResultRecord,
	at time.Time,
) {
	t.Helper()
	ctx := context.Background()
	reportBefore, valueBefore := readIntegrationClosingReport(t, fixture.store, current.Task.Record.ID)
	faults := &blueprintTerminalFaultStore{
		releasePlanningTestStore: fixture.store,
		t:                        t,
		epochKey: testhierarchy.EnvironmentMutationEpochKey(
			current.Task.Record.Owner.EnvironmentID,
		),
		fault: fixture.TerminalCommitFault,
	}
	repository, err := etcd.NewTaskRepository(faults)
	if err != nil {
		t.Fatal(err)
	}
	_, reconnectErr := repository.ReconnectAgentAssignment(ctx, current)
	if fixture.TerminalCommitFault == "compare-loss" {
		after, err := fixture.Tasks.GetTaskAssignment(ctx, current.Task.Record.ID)
		if !errors.Is(reconnectErr, errs.New(errs.KindStateConflict, "")) || err != nil || faults.calls != 1 ||
			faults.raceRevision == 0 || fixture.store.revision != faults.raceRevision ||
			after.Task.Revision != current.Task.Revision || after.Assignment.Revision != current.Assignment.Revision {
			t.Fatalf("terminal compare loss changed Task/assignment authority: reconnect=%v read=%v", reconnectErr, err)
		}
		reportAfter, valueAfter := readIntegrationClosingReport(t, fixture.store, current.Task.Record.ID)
		if valueBefore == nil || valueAfter == nil || valueAfter.ModRevision != valueBefore.ModRevision ||
			reportAfter.Status != reportBefore.Status || !reportAfter.ObservedAt.Equal(reportBefore.ObservedAt) ||
			!testtaskjournal.TaskResultsEqual(reportAfter.Result, reportBefore.Result) {
			t.Fatal("terminal compare loss discarded the original continuation")
		}
		return
	}
	if !errors.Is(reconnectErr, errs.New(errs.KindInternal, "")) || faults.calls != 1 ||
		faults.committedRevision == 0 {
		t.Fatalf("expected uncertain commit: calls=%d error=%v", faults.calls, reconnectErr)
	}
	committed, err := fixture.Tasks.GetTask(ctx, current.Task.Record.ID)
	if err != nil || committed.Revision != faults.committedRevision || committed.Record.Status != reportBefore.Status {
		t.Fatalf("uncertain commit did not leave the complete terminal Task: %v", err)
	}
	restarted, err := etcd.NewTaskRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	before := fixture.store.revision
	replayed, err := restarted.AcknowledgeTask(
		ctx,
		agentID,
		1,
		current.Task.Record.ID,
		current.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		result,
		at,
	)
	if err != nil || replayed.Revision != committed.Revision || fixture.store.revision != before {
		t.Fatalf("uncertain terminal replay was not exact read-only success: %v", err)
	}
	if replayed.Record.FinishedAt == nil || !replayed.Record.FinishedAt.Equal(at) {
		t.Fatal("uncertain terminal replay lost the original completion time")
	}
	for _, key := range []string{
		testscriptsourceevidence.ScriptSourceRootKey(current.Task.Record.OperationID),
		integrationBlueprintClosingReportKey(current.Task.Record.ID),
	} {
		read, err := fixture.store.Get(ctx, key)
		if err != nil || read.Entry != nil {
			t.Fatalf("uncertain commit retained terminal continuation: %s %v", key, err)
		}
	}
}
