package hostplatform

import (
	"context"
	"fmt"

	"github.com/AlanD20/groundplane/internal/app/softwareplatform"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/backup"
	"github.com/AlanD20/groundplane/internal/controller/backupkey"
	"github.com/AlanD20/groundplane/internal/controller/blueprint"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/controller/controllertask"
	"github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/controller/imagedelivery"
	runnercapability "github.com/AlanD20/groundplane/internal/controller/runner"
	"github.com/AlanD20/groundplane/internal/controller/taskcheckpoint"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
)

// ExecutionDependencies contains the feature executors connected to the host's
// already-captured native runtime. Task authority remains with the journal.
type ExecutionDependencies struct {
	BackupSecrets   *backup.BackupSecretResolver
	ScriptArtifacts *taskplanning.ScriptArtifactService
	Releases        *blueprintrelease.Service
	CapturedFiles   taskcheckpoint.CapturedFileReader
	Blueprints      *blueprint.Service
	Entries         blueprint.EntryPreparer
	Definitions     blueprint.DefinitionPreparer
	Images          *imagedelivery.Service
	RunnerChecks    *runnercapability.ConnectionCheckExecutor
	RunnerLifecycle *runnercapability.Executor
	Keys            *backupkey.Service
	Hierarchy       *hierarchydeletion.Service
	AttachFacts     *attachments.FactService
	BackupRemoval   *backup.DeletionCleanup
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
