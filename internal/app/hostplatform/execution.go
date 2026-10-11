package hostplatform

import (
	"context"
	"fmt"

	"github.com/AlanD20/groundplane/internal/app/softwareplatform"
	"github.com/AlanD20/groundplane/internal/controller/backupkey"
	"github.com/AlanD20/groundplane/internal/controller/controllertask"
	taskdispatch "github.com/AlanD20/groundplane/internal/controller/controllertask/dispatch"
	runnercapability "github.com/AlanD20/groundplane/internal/controller/runner"
)

// ExecutionDependencies contains the feature executors connected to the host's
// already-captured native runtime. Task authority remains with the journal.
type ExecutionDependencies struct {
	BackingZones    taskdispatch.Executor
	Images          taskdispatch.Executor
	RunnerChecks    *runnercapability.ConnectionCheckExecutor
	RunnerLifecycle *runnercapability.Executor
	Keys            *backupkey.Service
	Hierarchy       taskdispatch.HierarchyExecutor
}

// NewExecution connects dispatch and performs native recovery admission before
// the composition root starts any scheduler or Agent listener.
func (platform *Platform) NewExecution(
	ctx context.Context, dependencies ExecutionDependencies,
) (*softwareplatform.Composition, *controllertask.Runner, error) {
	software, handler, err := platform.newSoftwareRuntime(dependencies)
	if err != nil {
		return nil, nil, err
	}
	runtime, err := platform.newTaskRuntime(ctx, handler, dependencies)
	if err != nil {
		return nil, nil, fmt.Errorf("controller: initialize Controller Task runner: %w", err)
	}
	return software, runtime, nil
}
