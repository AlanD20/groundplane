// Package publicingressplatform composes opt-in Host ingress with native Task
// authority and the Agent's closed Docker runtime.
package publicingressplatform

import (
	"context"
	"log/slog"

	"github.com/AlanD20/groundplane/internal/agent"
	"github.com/AlanD20/groundplane/internal/agent/ingressruntime"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/handlers"
	"github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/publicingress"
	"github.com/AlanD20/groundplane/internal/controller/publicingressauthority"
	"github.com/AlanD20/groundplane/internal/controller/publicingressoperator"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	storage "github.com/AlanD20/groundplane/internal/infra/etcd/publicingress"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	runtime "github.com/AlanD20/groundplane/internal/infra/publicingress"
)

type Controller struct {
	Operator handlers.PublicIngressOperator
	Work     *publicingressoperator.Work
}

func NewController(
	cfg *config.PublicIngressConfig,
	store keyvalue.Store,
	ledger *etcd.ReleaseLedger,
	submitter *acceptedaction.Submitter,
	replays *etcd.IdempotencyRepository,
	intents *idempotency.Coordinator,
	logger *slog.Logger,
) (Controller, error) {
	if cfg == nil {
		return Controller{}, nil
	}
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		return Controller{}, err
	}
	local, err := runtime.NewLocal(*cfg, runner.New(logger))
	if err != nil {
		return Controller{}, err
	}
	authority := publicingressauthority.New(store, ledger, local)
	service := publicingress.New(storage.NewRepository(store), authority)
	operator, err := publicingressoperator.New(service, authority, submitter, replays, intents)
	if err != nil {
		return Controller{}, err
	}
	work, err := publicingressoperator.NewWork(operator, queue)
	if err != nil {
		return Controller{}, err
	}
	return Controller{Operator: operator, Work: work}, nil
}

func ConfigureAgent(ctx context.Context, configPath string, local bool, client *agent.Client,
	journal *agentworkjournal.Journal, logger *slog.Logger,
) error {
	if !local {
		return nil
	}
	if configPath == agentprotocol.RuntimeConfigPath {
		configPath = runtime.AgentCompanionBase
	}
	cfg, err := config.LoadPublicIngress(ctx, configPath)
	if err != nil || cfg == nil {
		return err
	}
	host, err := runtime.NewLocal(*cfg, runner.New(logger))
	if err != nil {
		return err
	}
	executor, err := ingressruntime.NewExecutor(journal, host)
	if err != nil {
		return err
	}
	return client.ConfigurePublicIngress(executor)
}
