package hostplatform

import (
	"github.com/AlanD20/groundplane/internal/controller/environment"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (platform *Platform) newEnvironmentWork(queue *workqueue.Repository) (*environment.Work, error) {
	records, err := environmentprovision.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	owner, err := environment.NewWork(records, queue)
	if err != nil {
		return nil, err
	}
	if err := platform.dependencies.Channel.ConfigureEnvironmentProvision(owner); err != nil {
		return nil, err
	}
	return owner, nil
}
