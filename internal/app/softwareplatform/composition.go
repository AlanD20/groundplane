// Package softwareplatform composes preparation, native bundle staging and the
// independent activation coordinator. Product policy remains in their owners.
package softwareplatform

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"log/slog"
	"net/http"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/controller/agentmanagement"
	"github.com/AlanD20/groundplane/internal/controller/controllerupgrade"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/controller/softwareactivation"
	"github.com/AlanD20/groundplane/internal/controller/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/controllerrelease"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	activationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/softwarestaging"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Dependencies struct {
	Store             etcdstore.Store
	Tasks             *etcd.TaskRepository
	Evidence          *etcd.IdempotencyRepository
	Intents           *requestidempotency.Coordinator
	Agents            *localagent.Manager
	AgentUpdates      *agentmanagement.MutationService
	ControllerUpdates *controllerupgrade.Service
	Releases          *controllerrelease.Store
	WorkspaceRoot     string
	Interval          time.Duration
	Logger            *slog.Logger
}

type Composition struct {
	Preparations *softwarepreparation.Service
	Releases     *preparation.Preparer
	Activations  *softwareactivation.Service
	interval     time.Duration
	logger       *slog.Logger
}

func New(deps Dependencies) (*Composition, error) {
	if deps.Interval <= 0 || deps.Logger == nil {
		return nil, errs.New(errs.KindInternal, "software runtime timing is invalid")
	}
	preparer, err := preparation.NewPreparer(runner.New(deps.Logger), deps.WorkspaceRoot)
	if err != nil {
		return nil, err
	}
	records, err := preparationrecord.NewRepository(deps.Store)
	if err != nil {
		return nil, err
	}
	preparations, err := softwarepreparation.NewService(softwarepreparation.ServiceDependencies{
		Preparer: preparer, Records: records, Tasks: deps.Tasks, Evidence: deps.Evidence, Intents: deps.Intents,
	})
	if err != nil {
		return nil, err
	}
	result := &Composition{Preparations: preparations, Releases: preparer, interval: deps.Interval, logger: deps.Logger}
	// An ordinary development foreground process may have no installed native
	// release store. Preparation still works; activation is explicitly unavailable.
	if deps.Releases == nil {
		return result, nil
	}
	stager, err := softwarestaging.New(deps.Releases, deps.WorkspaceRoot)
	if err != nil {
		return nil, err
	}
	activations, err := activationrecord.NewRepository(deps.Store)
	if err != nil {
		return nil, err
	}
	result.Activations, err = softwareactivation.NewService(softwareactivation.ServiceDependencies{
		Preparations: records, Records: activations, Tasks: deps.Tasks, Health: deps.Agents, Stager: stager,
		ControllerUpdates: controllerUpdates{deps.ControllerUpdates}, AgentUpdates: agentUpdates{deps.AgentUpdates},
		Evidence: deps.Evidence, Intents: deps.Intents,
	})
	return result, err
}

func (composition *Composition) Run(ctx context.Context) {
	if composition.Activations != nil {
		composition.Activations.Run(ctx, composition.interval, composition.logger)
	}
}

type controllerUpdates struct{ *controllerupgrade.Service }

func (updates controllerUpdates) PublishControllerUpdate(
	ctx context.Context,
	release, key string,
	predecessor *upgrade.AgentPredecessor,
) (softwareactivation.ChildAcceptance, error) {
	response, err := updates.UpdateControllerFromPredecessor(ctx, release, key, predecessor)
	return acceptedChild(response, err)
}

type agentUpdates struct {
	*agentmanagement.MutationService
}

func (updates agentUpdates) PublishAgentUpdate(
	ctx context.Context,
	predecessor upgrade.AgentPredecessor,
	image, key string,
) (softwareactivation.ChildAcceptance, error) {
	response, err := updates.UpdateAgentFromPredecessor(ctx, predecessor, image, key)
	return acceptedChild(response, err)
}

func acceptedChild(
	response idempotencyrecord.IdempotencyResponse,
	err error,
) (softwareactivation.ChildAcceptance, error) {
	if err != nil {
		return softwareactivation.ChildAcceptance{}, err
	}
	defer clear(response.Body)
	accepted, err := jcs.Decode[apiTypes.TaskAccepted](response.Body)
	if err != nil || response.Status != http.StatusAccepted || ids.Validate(ids.KindTask, accepted.TaskID) != nil {
		return softwareactivation.ChildAcceptance{}, errs.New(errs.KindInternal, "software child acceptance is invalid")
	}
	return softwareactivation.ChildAcceptance{TaskID: accepted.TaskID}, nil
}
