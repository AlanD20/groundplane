package hostplatform

import (
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/backup"
	"github.com/AlanD20/groundplane/internal/controller/backupkey"
	"github.com/AlanD20/groundplane/internal/controller/blueprint"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/controller/imagedelivery"
	"github.com/AlanD20/groundplane/internal/controller/operationretention"
	runnercapability "github.com/AlanD20/groundplane/internal/controller/runner"
	"github.com/AlanD20/groundplane/internal/controller/taskcheckpoint"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
)

// ExecutionDependencies contains the feature executors connected to the host's
// already-captured native runtime. Task authority remains with the journal.
type ExecutionDependencies struct {
	AcceptedPreparations []taskdependencies.Registration
	BackupSecrets        *backup.BackupSecretResolver
	ScriptArtifacts      *taskplanning.ScriptArtifactService
	Releases             *blueprintrelease.Service
	CapturedFiles        taskcheckpoint.CapturedFileReader
	Blueprints           *blueprint.Service
	Entries              blueprint.EntryPreparer
	Definitions          blueprint.DefinitionPreparer
	Images               *imagedelivery.Service
	RunnerChecks         *runnercapability.ConnectionCheckExecutor
	RunnerSoftware       *runnercapability.SoftwareExecutor
	RunnerLifecycle      *runnercapability.Executor
	Keys                 *backupkey.Service
	Hierarchy            *hierarchydeletion.Service
	AttachFacts          *attachments.FactService
	BackupRemoval        *backup.DeletionCleanup
	Extensions           WorkExtensions
}

// WorkExtensions connects closed operation owners to the same native Task
// runtime. Feature composition supplies these registrations before startup.
type WorkExtensions struct {
	Admission     []workdispatch.Registration
	Controller    []workdispatch.ControllerRegistration
	Retry         []workdispatch.RetryRegistration
	Completion    []workdispatch.CompletionRegistration
	AgentStart    []workdispatch.AgentStartRegistration
	Retention     []operationretention.WorkRegistration
	AgentDelivery func(*workdispatch.AgentDeliveries) error
}
