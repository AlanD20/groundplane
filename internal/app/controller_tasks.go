package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/controller/backupkey"
	"github.com/AlanD20/groundplane/internal/controller/controllertask"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Native Task composition establishes recovery admission before NewController
// returns; no scheduler or Agent listener is running during this barrier.
func newControllerTaskRuntime(
	ctx context.Context,
	tasks *etcd.TaskRepository,
	handler *controllerTaskHandler,
	keys *backupkey.Service,
	hierarchy hierarchyDeletionTaskExecutor,
	agents *localagent.Manager,
	sessions *agentchannel.Registry,
	native controllertask.UpdateExecutor,
	interval time.Duration,
	logger *slog.Logger,
) (*controllertask.Runner, error) {
	updates, err := localagent.NewTaskUpdates(agents, sessions, logger)
	if err != nil {
		return nil, err
	}
	platformUpdates, err := controllertask.NewUpdateDispatcher(updates, native)
	if err != nil {
		return nil, err
	}
	rotations, err := controllertask.NewDispatcher(handler, keys)
	if err != nil {
		return nil, err
	}
	deletions, err := newHierarchyDeletionTaskDispatcher(rotations, hierarchy)
	if err != nil {
		return nil, err
	}
	runtime, err := controllertask.New(tasks, deletions, platformUpdates, interval, logger)
	if err != nil {
		return nil, err
	}
	if err := runtime.Restore(ctx); err != nil {
		return nil, err
	}
	return runtime, nil
}
