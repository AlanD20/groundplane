package app

import (
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testscriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/oklog/ulid/v2"
)

func scriptCheckpointTestInput(
	record testscriptexecutions.ScriptExecutionRecord,
	at time.Time,
) testscriptexecutions.ScriptCheckpointInput {
	return testscriptexecutions.ScriptCheckpointInput{
		TaskID: record.CurrentTaskID, OperationID: record.OperationID,
		AssignmentID: ids.NewAt(ids.KindAssignment, at, 9), AgentID: ids.NewAt(ids.KindAgent, at, 10),
		AgentGeneration: 1, StepID: record.StepID, ExecutionID: record.ID, PlanHash: record.PlanHash,
		ExpectedState: record.State, PayloadSHA256: strings.Repeat("1", 64), At: at,
	}
}

func scriptCheckpointTestRecord(at time.Time) testscriptexecutions.ScriptExecutionRecord {
	return testscriptexecutions.ScriptExecutionRecord{
		ID: ulid.MustNew(ulid.Timestamp(at), strings.NewReader(strings.Repeat("a", 32))).String(),
		SnapshotID: ulid.MustNew(ulid.Timestamp(at.Add(time.Millisecond)), strings.NewReader(strings.Repeat("b", 32))).
			String(),
		OperationID: ids.NewAt(ids.KindOperation, at, 1), CurrentTaskID: ids.NewAt(ids.KindTask, at, 2),
		StepID: ids.NewAt(ids.KindStep, at, 3), ScriptID: ids.NewAt(ids.KindScript, at, 4),
		ScriptGeneration: 1, EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 5),
		ServiceID: ids.NewAt(ids.KindService, at, 6), ReleaseID: ids.NewAt(ids.KindDeployment, at, 7),
		RenderGeneration: 1, PlanHash: strings.Repeat("a", 64), SnapshotSHA256: strings.Repeat("b", 64),
		BodySHA256: strings.Repeat("c", 64), RunnerProjectionSHA256: strings.Repeat("d", 64),
		Plan: []byte{1}, Snapshot: []byte{1}, State: testscriptexecutions.ScriptExecutionNotStarted,
		ActiveReference: true, CreatedAt: at, UpdatedAt: at,
	}
}
