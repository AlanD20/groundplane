package hostplatform

import (
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/backup"
	"github.com/AlanD20/groundplane/internal/controller/backupkey"
	"github.com/AlanD20/groundplane/internal/controller/blueprint"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
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
