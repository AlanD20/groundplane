package hostplatform

import (
	"github.com/AlanD20/groundplane/internal/controller/releasegroup"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (platform *Platform) newReleaseGroupWork(queue *workqueue.Repository) (*releasegroup.RemovalWork, error) {
	records, err := releasegroups.NewRemovalRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	return releasegroup.NewRemovalWork(records, queue)
}
