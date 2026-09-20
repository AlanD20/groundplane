package agent

import (
	"context"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

type HostResolutionRuntime interface {
	ExecuteHostResolution(context.Context, taskassignment.Assignment, *agentpb.ExecutionStep) error
}
