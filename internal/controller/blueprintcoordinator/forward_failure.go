package blueprintcoordinator

import (
	"context"
	"errors"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (runner *Runner) forwardFailed(
	ctx context.Context, parent etcd.TaskRecord, snapshot blueprintunits.Snapshot, phase string, cause error,
) (bool, error) {
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) ||
		errs.IsRetryable(cause) || errors.Is(cause, errs.New(errs.KindResourceInUse, "")) {
		return false, cause
	}
	// A conflict may describe a racing head/receipt or a permanent prerequisite
	// failure. Retry only when the captured decision state actually changed.
	if isStateRace(cause) {
		current, err := runner.ledger.Load(ctx, parent.Owner.EnvironmentID)
		if err != nil {
			return false, err
		}
		if current.HeadTaskID != snapshot.HeadTaskID || current.HeadRevision != snapshot.HeadRevision ||
			current.EpochRevision != snapshot.EpochRevision {
			return true, nil
		}
	}
	code := errs.CodeInternal
	var domainError *errs.Error
	if errors.As(cause, &domainError) {
		code = domainError.ToProblem().Code
	}
	if err := runner.tasks.RequestBlueprintForwardFailure(ctx, parent.ID, snapshot, phase, code, runner.now().UTC()); err != nil {
		if isStateRace(err) {
			return true, nil
		}
		return false, err
	}
	return true, nil
}
