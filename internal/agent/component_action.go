package agent

import (
	"context"
	componentaction "github.com/AlanD20/groundplane/internal/agent/componentaction"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ComponentActionRuntime interface {
	ExecuteComponentAction(
		context.Context,
		taskassignment.Assignment,
		*agentpb.ExecutionStep,
		componentaction.ManagedConfigPayload,
	) (*componentaction.ComponentActionResult, error)
	FinalizeManagedConfig(
		context.Context,
		taskassignment.Assignment,
		*agentpb.ExecutionStep,
		bool,
	) (componentaction.ManagedConfigTransactionState, error)
}
