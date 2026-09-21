package app

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testscriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func manualScriptTerminalResult(
	assignment etcd.TaskAssignment,
	status testtaskjournal.TaskStatus,
) testtaskjournal.TaskResultRecord {
	result := testtaskjournal.TaskResultRecord{
		Kind:           testtaskjournal.TaskResultCompose,
		Diagnostic:     testtaskjournal.TaskResultDiagnosticNone,
		ExecutionEpoch: assignment.Assignment.Record.ExecutionEpoch,
	}
	if status == testtaskjournal.TaskStatusFailed {
		result.ExitCode, result.FailedStepID, result.Diagnostic = 7, assignment.Task.Record.Steps[0].ID, testtaskjournal.TaskResultDiagnosticComposeFailed
	}
	return result
}

func manualScriptCleanupCheckpoints(
	t *testing.T,
	scripts *etcd.ScriptRepository,
	assignment etcd.TaskAssignment,
	execution testscriptexecutions.ScriptExecutionRecord,
	status testtaskjournal.TaskStatus,
) {
	t.Helper()
	exitCode := int32(0)
	if status == testtaskjournal.TaskStatusFailed {
		exitCode = 7
	}
	outcome := &testscriptexecutions.ScriptOutcomeEvidence{
		Reason:     testscriptexecutions.ScriptOutcomeNormalExit,
		ExitCode:   &exitCode,
		ObservedAt: assignment.Task.Record.CreatedAt.Add(5 * time.Second),
	}
	if status == testtaskjournal.TaskStatusAborted {
		outcome.Reason, outcome.ExitCode = testscriptexecutions.ScriptOutcomeAbort, nil
	}
	checkpoints := []struct {
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
				BodySHA256: execution.BodySHA256, UID: 65534, GID: 65534, Device: 10, Inode: 20, Leaf: "body",
			},
		},
		},
		{testscriptexecutions.ScriptExecutionContainerCreated, testscriptexecutions.ScriptCheckpointEvidence{
			Kind: testscriptexecutions.ScriptCheckpointEvidenceContainerCreated,
			ContainerCreated: &testscriptexecutions.ScriptContainerCreatedEvidence{
				ContainerID: strings.Repeat("a", 64), OwnershipLabelsSHA256: strings.Repeat("b", 64),
			},
		},
		},
		{
			testscriptexecutions.ScriptExecutionOutcomeRecorded,
			testscriptexecutions.ScriptCheckpointEvidence{
				Kind:    testscriptexecutions.ScriptCheckpointEvidenceOutcome,
				Outcome: outcome,
			},
		},
		{
			testscriptexecutions.ScriptExecutionCleanupProven,
			testscriptexecutions.ScriptCheckpointEvidence{
				Kind: testscriptexecutions.ScriptCheckpointEvidenceCleanup,
				Cleanup: &testscriptexecutions.ScriptCleanupEvidence{
					ContainerID: strings.Repeat("a", 64), BodyDevice: 10, BodyInode: 20, BodyLeaf: "body",
					ContainerAbsent: true, BodyAbsent: true, ExecutionDirectoryAbsent: true,
				},
			},
		},
	}
	previous := testscriptexecutions.ScriptExecutionNotStarted
	for index, checkpoint := range checkpoints {
		input := scriptCheckpointTestInput(
			execution,
			assignment.Task.Record.CreatedAt.Add(time.Duration(index+2)*time.Second),
		)
		input.AgentID, input.AssignmentID = assignment.Assignment.Record.AgentID, assignment.Assignment.Record.AssignmentID
		input.ExpectedState, input.State, input.Evidence = previous, checkpoint.state, checkpoint.evidence
		input.PayloadSHA256 = scriptSourceReferenceDigest(string(checkpoint.state))
		if _, err := scripts.CheckpointScriptExecution(context.Background(), input); err != nil {
			t.Fatalf("checkpoint %s: %v", checkpoint.state, err)
		}
		previous = checkpoint.state
	}
}
