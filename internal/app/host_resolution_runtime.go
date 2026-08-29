package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent"
	"github.com/AlanD20/groundplane/internal/infra/docker/hostresolutionhelpercontainer"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type hostResolutionRuntime struct {
	helper *hostresolutionhelpercontainer.Executor
}

func (runtime *hostResolutionRuntime) ExecuteHostResolution(
	ctx context.Context,
	assignment agent.Assignment,
	step *agentpb.ExecutionStep,
) error {
	if runtime == nil || runtime.helper == nil || assignment.Plan == nil || step == nil {
		return errs.New(errs.KindInternal, "agent: host resolution runtime is not configured")
	}
	if apply := step.GetHostResolutionApply(); apply != nil {
		if apply.GetComponentId() != assignment.Plan.GetTargetId() ||
			apply.GetGeneration() != assignment.Plan.GetRenderGeneration() {
			return errs.New(errs.KindValidationFailed, "agent: host resolution apply identity is invalid")
		}
		return runtime.helper.Execute(ctx, hostresolutionhelpercontainer.OperationApply)
	}
	if restore := step.GetHostResolutionRestore(); restore != nil {
		if restore.GetComponentId() != assignment.Plan.GetTargetId() ||
			restore.GetGeneration() != assignment.Plan.GetRenderGeneration() {
			return errs.New(errs.KindValidationFailed, "agent: host resolution restore identity is invalid")
		}
		return runtime.helper.Execute(ctx, hostresolutionhelpercontainer.OperationRestore)
	}
	return errs.New(errs.KindValidationFailed, "agent: host resolution action is missing")
}
