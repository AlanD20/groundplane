package composehelpercontainer

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// StopEntryConfigurationWork joins both exact runtime helpers before the Agent restores
// files. It cannot start runtime effects or release the failed Task's claims.
func (executor *Executor) StopEntryConfigurationWork(ctx context.Context, request *agentpb.EntryConfigurationWorkHelperRequest) error {
	if executor == nil || executor.engine == nil || ctx == nil {
		return errs.New(errs.KindInternal, "Entry helper cleanup is not configured")
	}
	input, err := entryexecution.OpenHelperRequest(request)
	if err != nil {
		return err
	}
	if input.Recovery == nil {
		return errs.New(errs.KindStateConflict, "Entry helper cleanup requires captured recovery")
	}
	for _, attempt := range []workexecution.Attempt{input.Recovery.Forward, input.Attempt} {
		labels := releaseHelperLabels(attempt)
		labels["io.groundplane.role"] = "entry-executor"
		if err := executor.stopNamedHelper(ctx, "gp-entry-executor-"+attempt.ID, labels); err != nil {
			return err
		}
	}
	return nil
}
