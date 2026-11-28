package remoteagentplatform

import (
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/handlers"
	"github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/remotehost"
	"github.com/AlanD20/groundplane/internal/controller/remoteimage"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/remotehosts"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/registryimages"
)

type ControllerServices struct {
	Hosts  handlers.RemoteHostOperator
	Fetch  handlers.RemoteImageFetcher
	Images *remoteimage.Service
}

func NewControllerServices(service *remotehost.Service, store keyvalue.Store, submitter *acceptedaction.Submitter,
	intents *idempotency.Coordinator, replays *etcd.IdempotencyRepository,
) (ControllerServices, error) {
	if service == nil {
		return ControllerServices{}, nil
	}
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		return ControllerServices{}, err
	}
	hosts, err := remotehost.NewOperator(service, remotehosts.NewRepository(store), intents, replays)
	if err != nil {
		return ControllerServices{}, err
	}
	images, err := remoteimage.New(store, queue, submitter, registryimages.Local{}, intents, replays)
	if err != nil {
		return ControllerServices{}, err
	}
	return ControllerServices{Hosts: hosts, Fetch: images, Images: images}, nil
}
