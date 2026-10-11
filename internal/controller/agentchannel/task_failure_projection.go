package agentchannel

import (
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateTaskFailure(ack *agentpb.TaskAck, value *agentpb.StepFailure) error {
	failure, err := taskprogress.FailureFromAgent(value)
	if err != nil {
		return err
	}
	if failure != nil &&
		(ack.GetTerminal() == agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED || failure.ExitCode != ack.GetExitCode()) {
		return errs.New(errs.KindValidationFailed, "Agent failure diagnostic contradicts its result")
	}
	return nil
}

// The acknowledgement boundary validates the diagnostic before projection.
func durableTaskFailure(value *agentpb.StepFailure) *taskprogress.Failure {
	if value == nil {
		return nil
	}
	return &taskprogress.Failure{Code: value.Code, ExitCode: value.ExitCode}
}
