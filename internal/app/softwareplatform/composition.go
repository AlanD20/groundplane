// Package softwareplatform composes preparation, native bundle staging and the
// native activation work. Product policy remains in their owners.
package softwareplatform

import (
	"log/slog"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/controller/controllerupgrade"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/controller/softwareactivation"
	"github.com/AlanD20/groundplane/internal/controller/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/controllerrelease"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	activationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/softwarestaging"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Dependencies struct {
	Store             etcdstore.Store
	Tasks             *etcd.TaskRepository
	Evidence          *etcd.IdempotencyRepository
	Intents           *requestidempotency.Coordinator
	Agents            *localagent.Manager
	ControllerUpdates *controllerupgrade.Service
	Releases          *controllerrelease.Store
	WorkspaceRoot     string
	Logger            *slog.Logger
}

type Composition struct {
	Preparations *softwarepreparation.Service
	Releases     *preparation.Preparer
	Activations  *softwareactivation.Service
}

func New(deps Dependencies) (*Composition, error) {
	if deps.Logger == nil {
		return nil, errs.New(errs.KindInternal, "software runtime logging is required")
	}
	preparer, err := preparation.NewPreparer(runner.New(deps.Logger), deps.WorkspaceRoot)
	if err != nil {
		return nil, err
	}
	records, err := preparationrecord.NewRepository(deps.Store)
	if err != nil {
		return nil, err
	}
	queue, err := workqueue.NewRepository(deps.Store)
	if err != nil {
		return nil, err
	}
	diagnostics, err := executiondiagnostics.New(deps.Store, queue)
	if err != nil {
		return nil, err
	}
	preparations, err := softwarepreparation.NewService(softwarepreparation.ServiceDependencies{
		Preparer: preparer, Records: records, Tasks: deps.Tasks, Evidence: deps.Evidence, Intents: deps.Intents,
		Queue: queue, Diagnostics: diagnostics,
	})
	if err != nil {
		return nil, err
	}
	result := &Composition{Preparations: preparations, Releases: preparer}
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
	controllerRecords, err := controllersoftware.New(deps.Store)
	if err != nil {
		return nil, err
	}
	agentRecords, err := agentoperations.New(deps.Store)
	if err != nil {
		return nil, err
	}
	result.Activations, err = softwareactivation.NewService(softwareactivation.ServiceDependencies{
		Preparations: records, Records: activations, Tasks: deps.Tasks, Health: deps.Agents, Stager: stager,
		ControllerSource: deps.ControllerUpdates, Queue: queue, Diagnostics: diagnostics, ControllerRecords: controllerRecords, AgentRecords: agentRecords,
		Evidence: deps.Evidence, Intents: deps.Intents,
	})
	return result, err
}
