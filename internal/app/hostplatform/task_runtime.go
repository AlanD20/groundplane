package hostplatform

import (
	"context"
	taskdispatch "github.com/AlanD20/groundplane/internal/controller/controllertask/dispatch"

	"github.com/AlanD20/groundplane/internal/app/softwareplatform"
	"github.com/AlanD20/groundplane/internal/controller/backingchecks"
	"github.com/AlanD20/groundplane/internal/controller/connectors"
	"github.com/AlanD20/groundplane/internal/controller/controllertask"
	"github.com/AlanD20/groundplane/internal/controller/dnsresolver"
	"github.com/AlanD20/groundplane/internal/controller/edgechecks"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	edgerecord "github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
)

func (platform *Platform) newSoftwareRuntime(
	dependencies ExecutionDependencies,
) (*softwareplatform.Composition, controllertask.Handler, error) {
	checkRecords, err := connectorchecks.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, nil, err
	}
	checks, err := connectors.NewCheckExecutor(
		checkRecords,
		platform.dependencies.Tasks,
		platform.dependencies.Protector,
	)
	if err != nil {
		return nil, nil, err
	}
	backingRecords, err := checkrecord.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, nil, err
	}
	backingChecks, err := backingchecks.NewExecutor(
		backingRecords,
		platform.dependencies.Tasks,
		platform.dependencies.Channel.Registry,
		platform.dependencies.Protector,
	)
	if err != nil {
		return nil, nil, err
	}
	dnsRecords, err := dnschecks.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, nil, err
	}
	dnsExecutor, err := dnsresolver.NewConnectionCheckExecutor(
		dnsRecords,
		platform.dependencies.Tasks,
		platform.dependencies.Channel.Registry,
	)
	if err != nil {
		return nil, nil, err
	}
	edgeRecords, err := edgerecord.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, nil, err
	}
	edgeExecutor, err := edgechecks.NewExecutor(
		edgeRecords,
		platform.dependencies.Tasks,
		platform.dependencies.Channel.Registry,
	)
	if err != nil {
		return nil, nil, err
	}
	handler, err := taskdispatch.NewResourceHandler(
		platform.agents,
		dependencies.BackingZones,
		dependencies.Images,
		checks,
		backingChecks,
		dnsExecutor,
		edgeExecutor,
		dependencies.RunnerChecks,
		dependencies.RunnerLifecycle,
	)
	if err != nil {
		return nil, nil, err
	}
	software, err := softwareplatform.New(softwareplatform.Dependencies{
		Store: platform.dependencies.Store, Tasks: platform.dependencies.Tasks, Evidence: platform.dependencies.Idempotency, Intents: platform.dependencies.Intents,
		Agents: platform.agents, AgentUpdates: platform.AgentMutations, ControllerUpdates: platform.Upgrades,
		Releases: platform.releases, WorkspaceRoot: platform.dependencies.Config.Storage.VolumeRoot,
		Interval: platform.dependencies.Tick, Logger: platform.dependencies.Logger,
	})
	if err != nil {
		return nil, nil, err
	}
	dispatcher, err := taskdispatch.NewSoftwarePreparationDispatcher(
		platform.EtcdConfig.Dispatcher(handler),
		software.Preparations,
	)
	if err != nil {
		return nil, nil, err
	}
	return software, dispatcher, nil
}

// Native Task composition establishes recovery admission before NewController
// returns; no scheduler or Agent listener is running during this barrier.
func (platform *Platform) newTaskRuntime(
	ctx context.Context,
	handler controllertask.Handler,
	dependencies ExecutionDependencies,
) (*controllertask.Runner, error) {
	updates, err := localagent.NewTaskUpdates(
		platform.agents,
		platform.dependencies.Channel.Registry,
		platform.dependencies.Logger,
	)
	if err != nil {
		return nil, err
	}
	platformUpdates, err := controllertask.NewUpdateDispatcher(updates, platform.native)
	if err != nil {
		return nil, err
	}
	rotations, err := controllertask.NewDispatcher(handler, dependencies.Keys)
	if err != nil {
		return nil, err
	}
	deletions, err := taskdispatch.NewHierarchyDispatcher(rotations, dependencies.Hierarchy)
	if err != nil {
		return nil, err
	}
	runtime, err := controllertask.New(
		platform.dependencies.Tasks,
		deletions,
		platformUpdates,
		platform.dependencies.Tick,
		platform.dependencies.Logger,
	)
	if err != nil {
		return nil, err
	}
	if err := runtime.Restore(ctx); err != nil {
		return nil, err
	}
	return runtime, nil
}
