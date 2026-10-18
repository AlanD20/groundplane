package hostplatform

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (platform *Platform) newControllerSoftware(ctx context.Context, coordinator *controllerupgrade.Coordinator) (*controllersoftware.Repository, *controllerupgrade.Work, error) {
	records, err := controllersoftware.New(platform.dependencies.Store)
	if err != nil {
		return nil, nil, err
	}
	queue, err := workqueue.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, nil, err
	}
	diagnostics, err := executiondiagnostics.New(platform.dependencies.Store, queue)
	if err != nil {
		return nil, nil, err
	}
	work, err := controllerupgrade.NewWork(queue, records, coordinator, diagnostics)
	if err != nil {
		return nil, nil, err
	}
	if coordinator != nil {
		if err := coordinator.RestoreJournal(ctx, work); err != nil {
			return nil, nil, err
		}
	}
	return records, work, nil
}
