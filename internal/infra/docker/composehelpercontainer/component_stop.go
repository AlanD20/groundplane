package composehelpercontainer

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// StopComponentWork joins both exact runtime helpers before the Agent restores
// files. It cannot start runtime effects or release the failed Task's claims.
func (executor *Executor) StopComponentWork(ctx context.Context, request *agentpb.ComponentWorkHelperRequest) error {
	if executor == nil || executor.engine == nil || ctx == nil {
		return errs.New(errs.KindInternal, "Component helper cleanup is not configured")
	}
	input, err := componentexecution.OpenHelperRequest(request)
	if err != nil {
		return err
	}
	if input.Recovery == nil {
		return errs.New(errs.KindStateConflict, "Component helper cleanup requires captured recovery")
	}
	for _, attempt := range []workexecution.Attempt{input.Recovery.Forward, input.Attempt} {
		labels := releaseHelperLabels(attempt)
		labels["io.groundplane.role"] = "component-executor"
		if err := executor.stopNamedHelper(ctx, "gp-component-executor-"+attempt.ID, labels); err != nil {
			return err
		}
	}
	return nil
}
