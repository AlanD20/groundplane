package controller

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ScriptExecutionPlanReader interface {
	GetScriptExecutionPlan(context.Context, etcd.TaskRecord) (*agentpb.ExecutionPlan, error)
	GetReleaseScriptExecutionPlan(context.Context, etcd.TaskRecord) (*agentpb.ExecutionPlan, bool, error)
}

func (resolver *TaskPlanResolver) EnableScriptPlans(reader ScriptExecutionPlanReader) error {
	if resolver == nil || reader == nil {
		return errs.New(errs.KindInternal, "Script Task plan reader is required")
	}
	resolver.scriptPlans = reader
	return nil
}
