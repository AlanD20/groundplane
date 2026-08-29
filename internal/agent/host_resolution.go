package agent

import (
	"context"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

type HostResolutionRuntime interface {
	ExecuteHostResolution(context.Context, Assignment, *agentpb.ExecutionStep) error
}
