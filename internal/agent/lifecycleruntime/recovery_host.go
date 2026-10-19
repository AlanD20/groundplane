package lifecycleruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (host *RuntimeHost) InspectRecovery(ctx context.Context, input Input) (lifecycleexecution.RecoveryReport, error) {
	if input.recovery == nil || input.recovery.ValidateFor(input.attempt, input.procedure) != nil ||
		input.attempt.StartedAt == nil ||
		input.recovery.Progress == nil ||
		input.recovery.Progress.BeganAt == nil {
		return lifecycleexecution.RecoveryReport{}, invalidInput()
	}
	if err := host.ownership.Check(); err != nil {
		return lifecycleexecution.RecoveryReport{}, err
	}
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	stages, err := lifecycleexecution.Stages(input.procedure)
	if err != nil {
		return lifecycleexecution.RecoveryReport{}, err
	}
	stage := stages[len(input.recovery.Progress.Completed)]
	var receipt *lifecycleexecution.Receipt
	if stage.Expected == lifecycleexecution.HookSucceeded {
		saved, err := host.journal.Lifecycle(bounded, input.recovery.Forward, input.procedure)
		if err != nil {
			return lifecycleexecution.RecoveryReport{}, err
		}
		if saved == nil {
			return lifecycleexecution.RecoveryReport{}, errs.New(
				errs.KindStateConflict,
				"Lifecycle hook recovery has no retained execution journal",
			)
		}
		if hook := saved.Hook; hook != nil {
			observed, err := host.commands.Inspect(bounded, hook.ContainerID, hook.ExecID)
			if err != nil {
				return lifecycleexecution.RecoveryReport{}, err
			}
			if observed.Running {
				return lifecycleexecution.RecoveryReport{}, errs.New(
					errs.KindStateConflict,
					"Previous lifecycle hook is still running",
				)
			}
			if hook.Result != nil {
				copy := *hook.Result
				receipt = &copy
			}
		}
	} else {
		raw, err := lifecycleexecution.EncodeRecovery(input.procedure, *input.recovery)
		if err != nil {
			return lifecycleexecution.RecoveryReport{}, err
		}
		identity, err := workexecution.EncodeAgentAttempt(input.attempt)
		if err != nil {
			return lifecycleexecution.RecoveryReport{}, err
		}
		request := &agentpb.ServiceLifecycleHelperRequest{
			Assignment: &agentpb.ServiceLifecycleAssignment{Attempt: identity, Input: input.procedure, RecoverySource: raw},
			Start:      &agentpb.WorkStartAck{Attempt: identity, StartedAt: timestamppb.New(*input.attempt.StartedAt)}, InspectOnly: true,
		}
		captured, err := executionplan.OpenLifecycleHelperRequest(request)
		if err != nil {
			return lifecycleexecution.RecoveryReport{}, err
		}
		response, err := host.helper.ExecuteLifecycleWork(bounded, host.root, request)
		if err != nil {
			return lifecycleexecution.RecoveryReport{}, err
		}
		receipt, err = executionplan.ReadLifecycleHelperReceipt(captured, response)
		if err != nil {
			return lifecycleexecution.RecoveryReport{}, err
		}
	}
	if err := host.ownership.Check(); err != nil {
		return lifecycleexecution.RecoveryReport{}, err
	}
	report := lifecycleexecution.RecoveryReport{AttemptID: input.attempt.ID, ForwardID: input.recovery.Forward.ID,
		Failure: input.recovery.Evidence, ExecutionStopped: true, ObservedAt: time.Now().UTC(), Receipt: receipt}
	return report, report.Validate(input.attempt, input.procedure, *input.recovery)
}
