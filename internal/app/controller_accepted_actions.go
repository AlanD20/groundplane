package app

import (
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/acceptedactions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func newAcceptedActions(store acceptedactions.Store, protector *secretvalue.Protector,
	queue *workqueue.Repository, replay *etcd.IdempotencyRepository, coordinator *requestidempotency.Coordinator,
) (*acceptedaction.Submitter, error) {
	repository, err := acceptedactions.NewRepository(store)
	if err != nil {
		return nil, err
	}
	payloads, err := acceptedaction.NewPayloads(repository, protector)
	if err != nil {
		return nil, err
	}
	return acceptedaction.NewSubmitter(payloads, queue, replay, coordinator)
}
