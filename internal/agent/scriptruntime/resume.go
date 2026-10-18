package scriptruntime

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// resumeCheckpoint incorporates at most the one report whose acknowledgement
// was lost after assignment delivery. It never reconstructs an unrecorded effect.
func resumeCheckpoint(invocation Invocation, record *agentworkjournal.ScriptRecord) (*agentpb.ScriptExecutionCheckpoint, error) {
	checkpoint, err := executionplan.ValidateScriptExecutionCheckpoint(invocation.Checkpoint)
	if err != nil {
		return nil, err
	}
	if record == nil {
		if checkpoint.State != agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED {
			return nil, errs.New(errs.KindStateConflict, "Script checkpoint is missing from the Agent journal")
		}
		return checkpoint, nil
	}
	if record.Validate() != nil || record.Ack == nil || !workexecution.SameAssignment(record.Attempt, invocation.Attempt) {
		return nil, errs.New(errs.KindStateConflict, "Script journal has no acknowledged report for this attempt")
	}
	report := record.Report
	if report.OperationId != invocation.OperationID || report.ScriptExecutionId != invocation.Run.ScriptExecutionId ||
		report.InputId != invocation.Input.ID || hex.EncodeToString(report.InputSha256) != invocation.Input.SHA256 ||
		(checkpoint.State != report.ExpectedState && checkpoint.State != report.State) {
		return nil, errs.New(errs.KindStateConflict, "Script journal differs from captured delivery")
	}
	updated := proto.CloneOf(checkpoint)
	updated.State = report.State
	switch report.Evidence.(type) {
	case *agentpb.ScriptCheckpointRequest_StartAuthorized:
		updated.StartAuthorized = true
	case *agentpb.ScriptCheckpointRequest_BodyPrepared:
		updated.BodyPrepared = proto.CloneOf(report.GetBodyPrepared())
	case *agentpb.ScriptCheckpointRequest_ContainerCreated:
		updated.ContainerCreated = proto.CloneOf(report.GetContainerCreated())
	case *agentpb.ScriptCheckpointRequest_Outcome:
		updated.Outcome = proto.CloneOf(report.GetOutcome())
	case *agentpb.ScriptCheckpointRequest_Cleanup:
		updated.Cleanup = proto.CloneOf(report.GetCleanup())
	default:
		return nil, errs.New(errs.KindStateConflict, "Script journal has no typed checkpoint evidence")
	}
	if checkpoint.State == report.State && !proto.Equal(checkpoint, updated) {
		return nil, errs.New(errs.KindStateConflict, "Script delivery and journal disagree on acknowledged evidence")
	}
	return executionplan.ValidateScriptExecutionCheckpoint(updated)
}
