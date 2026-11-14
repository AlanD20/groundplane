package runner

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Run resolves upstream releases hourly. Both automatic and manual updates use
// the same pinned publication, idle gate, host journal and recovery path.
func (service *SoftwareService) Run(ctx context.Context, logger *slog.Logger) {
	timer := time.NewTimer(time.Minute)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := service.updateAutomatic(ctx); err != nil && ctx.Err() == nil {
				logger.Warn("Automatic GitHub Runner update admission failed", slog.Any("error", err))
			}
			timer.Reset(time.Hour)
		}
	}
}
func (service *SoftwareService) updateAutomatic(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	releases, err := service.catalog.List(ctx)
	if err != nil {
		return err
	}
	if len(releases) == 0 {
		return nil
	}
	latest := releases[0]
	for _, candidate := range releases {
		if runnersoftware.Newer(candidate.Version, latest.Version) {
			latest = candidate
		}
	}
	var firstError error
	page := keyvalue.PageRequest{Limit: 100}
	for {
		items, err := service.runners.ListRuntimeRunners(ctx, page)
		if err != nil {
			return err
		}
		for _, item := range items.Items {
			record := item.Record
			if !record.Desired.AutoUpdate || record.ProvisioningState != runners.RunnerProvisioningReady {
				continue
			}
			observation, found, err := service.runners.GetRunnerObservation(ctx, record.Desired.ID)
			if err != nil {
				return err
			}
			if !found || !observation.Record.Online ||
				time.Since(observation.Record.ObservedAt) > ObservationLifetime ||
				!runnersoftware.Newer(latest.Version, observation.Record.GitHubVersion) {
				continue
			}
			key := "runner-auto-" + record.Desired.ID + "-" + latest.Version + "-" + time.Now().
				UTC().
				Format("2006010215")
			_, err = service.update(
				ctx,
				record.Desired.ID,
				apiTypes.RunnerUpdateRequest{Revision: item.Revision, Version: latest.Version},
				key,
				true,
				&latest,
			)
			if err != nil && firstError == nil {
				firstError = err
			} // Admission failures are reported once per bounded pass.
		}
		if items.NextCursor == "" {
			return firstError
		}
		page.Cursor = items.NextCursor
		page.Revision = items.Revision
	}
}
