package gitdelivery

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	minimumAutomaticInterval = 30 * time.Second
	automaticCallTimeout     = 30 * time.Second
	automaticBindingsPerPoll = 8
)

// ObserveHead accepts an observed source commit for an opt-in Binding. An
// authenticated event receiver and the branch watcher use the same key, while
// Begin rechecks the actual branch and the exact Binding revision.
func (service *Service) ObserveHead(ctx context.Context, observed BindingSnapshot, commit string) (Run, error) {
	if !observed.Binding.Automatic || observed.Revision <= 0 {
		return Run{}, errs.New(
			errs.KindRequestForbidden,
			"GitHub deployment binding is not enabled for automatic delivery",
		)
	}
	return service.beginRequest(ctx, observed.Binding.ServiceID, StartInput{
		IdempotencyKey: "branch." + strconv.FormatInt(observed.Revision, 10) + "." + commit,
		ExpectedCommit: commit,
		Automatic:      true,
	}, observed.Revision, nil)
}

func (service *Service) pollAutomatic(ctx context.Context, interval time.Duration, logger *slog.Logger) {
	timer := time.NewTicker(interval)
	defer timer.Stop()
	var after string
	for ctx.Err() == nil {
		services, next, err := service.repository.index(
			ctx,
			autoPrefix,
			ids.KindService,
			after,
			automaticBindingsPerPoll,
		)
		if err != nil {
			kind, _ := errs.KindOf(err)
			logger.WarnContext(ctx, "Automatic GitHub binding inventory could not be read", "kind", kind)
		} else {
			after = next
			for _, serviceID := range services {
				if ctx.Err() != nil {
					return
				}
				if err := service.pollBinding(ctx, serviceID); err != nil && ctx.Err() == nil {
					kind, _ := errs.KindOf(err)
					logger.DebugContext(ctx, "Automatic GitHub delivery could not check its branch", "service", serviceID, "kind", kind)
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}

func (service *Service) pollBinding(ctx context.Context, serviceID string) error {
	call, cancel := context.WithTimeout(ctx, automaticCallTimeout)
	defer cancel()
	captured, err := service.repository.binding(call, serviceID)
	if err != nil || !captured.Binding.Automatic {
		return err
	}
	commit, err := service.github.ResolveSource(call, captured.Binding)
	if err != nil {
		return err
	}
	_, err = service.ObserveHead(call, captured, commit)
	return err
}
