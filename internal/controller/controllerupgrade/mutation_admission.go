package controllerupgrade

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// CheckMutation uses the durable native journal on each admission: an open
// HTTP listener is only a qualification prerequisite, not permission for trial
// code to publish schema its predecessor cannot decode. Native acceptance
// replay is guarded separately by UpdateController's protected publication path.
func (service *Service) CheckMutation(ctx context.Context, nativeAbortTaskID string) error {
	if ctx == nil || service == nil {
		return errs.New(errs.KindInternal, "native mutation admission is not configured")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if service.catalog == nil {
		return nil
	} // Explicit foreground development without native bootstrap.
	journal, found, err := service.catalog.Current(ctx)
	if err != nil || !found {
		return err
	}
	if err := journal.Validate(); err != nil {
		return err
	}
	if journal.Phase.Settled() || nativeAbortTaskID == journal.TaskID {
		return nil
	}
	return errs.New(errs.KindResourceInUse, "Controller recovery is active; ordinary mutations are paused")
}
