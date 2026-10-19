package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func openLifecycleRecoveryHelper(request *agentpb.ServiceLifecycleHelperRequest) (LifecycleHelperInput, error) {
	if proto.Size(request) > MaximumPlanBytes || RejectUnknown(request) != nil || request.Assignment.Attempt == nil ||
		!request.InspectOnly || len(request.Assignment.Continuation) != 0 || request.Report != nil || request.Acknowledgement != nil || ValidateServiceLifecycleWorkInput(request.Assignment.Input) != nil {
		return LifecycleHelperInput{}, invalidLifecycleWork()
	}
	claimed := request.Assignment.Attempt
	identity := workexecution.Identity{
		Executor:   workexecution.Agent,
		HostID:     claimed.HostId,
		AgentID:    claimed.AgentId,
		Generation: claimed.ExecutorGeneration,
	}
	attempt, err := workexecution.DecodeAgentAttempt(claimed, identity)
	if err != nil {
		return LifecycleHelperInput{}, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(request.Start, attempt)
	if err != nil {
		return LifecycleHelperInput{}, err
	}
	attempt.StartedAt = &started
	input := request.Assignment.Input
	source, err := lifecycleexecution.DecodeRecovery(input, request.Assignment.RecoverySource)
	if err != nil || source.ValidateFor(attempt, input) != nil || source.Progress == nil ||
		source.Progress.BeganAt == nil {
		return LifecycleHelperInput{}, invalidLifecycleWork()
	}
	stages, err := lifecycleexecution.Stages(input)
	if err != nil || len(source.Progress.Completed) >= len(stages) {
		return LifecycleHelperInput{}, invalidLifecycleWork()
	}
	stage := stages[len(source.Progress.Completed)]
	if stage.Expected == lifecycleexecution.HookSucceeded {
		return LifecycleHelperInput{}, invalidLifecycleWork()
	}
	return LifecycleHelperInput{
		Attempt:     attempt,
		Procedure:   input,
		Progress:    *source.Progress,
		Stage:       stage,
		InspectOnly: true,
		Recovery:    &source,
	}, nil
}
