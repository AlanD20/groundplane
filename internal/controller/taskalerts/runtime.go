package taskalerts

import (
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/discordwebhook"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	records "github.com/AlanD20/groundplane/internal/infra/etcd/taskalerts"
	"log/slog"
)

// NewRuntime gives the API and delivery worker one repository and cipher.
func NewRuntime(store records.Store, replay *etcd.IdempotencyRepository,
	coordinator *requestidempotency.Coordinator, cipher Cipher, logger *slog.Logger,
) (*Service, *Worker, error) {
	repository, err := records.NewRepository(store)
	if err != nil {
		return nil, nil, err
	}
	service, err := NewService(repository, replay, coordinator, cipher)
	if err != nil {
		return nil, nil, err
	}
	worker, err := NewWorker(repository, cipher, discordwebhook.NewClient(), logger)
	return service, worker, err
}
