package app

import (
	"context"
	"fmt"
	"github.com/AlanD20/groundplane/internal/componentregistration"
	backupcapability "github.com/AlanD20/groundplane/internal/controller/backup"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	handlers "github.com/AlanD20/groundplane/internal/controller/handlers"
	taskcheckpoint "github.com/AlanD20/groundplane/internal/controller/taskcheckpoint"
	taskmaterialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/controller/attachments"

	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type controllerExecutionComposition struct {
	attachRecords           *etcd.AttachRepository
	attachFactValues        *attachments.FactService
	attachFactReads         handlers.AttachFactReader
	planResolver            *taskplanning.TaskPlanResolver
	backingHookCheckpoints  *taskcheckpoint.BackingHookCheckpointService
	materializationResolver *taskmaterialization.TaskMaterializationResolver
	scriptArtifacts         *taskplanning.ScriptArtifactService
	scriptSourceReferences  *etcd.ScriptSourceReferenceAuthority
	backupSecrets           *backupcapability.BackupSecretResolver
}

func newControllerExecutionComposition(
	ctx context.Context, cfg config.ControllerConfig, store etcd.Store,
	authority controllerAuthorityComposition, dataServices controllerDataComposition,
	componentCatalog []componentrender.EnvironmentComponentRegistration,
) (controllerExecutionComposition, error) {
	attachRecords, err := etcd.NewAttachRepository(store)
	if err != nil {
		_ = store.Close()
		return controllerExecutionComposition{}, fmt.Errorf("controller: initialize Attach repository: %w", err)
	}
	attachFactValues, err := attachments.NewFactService(attachRecords, authority.intentProtector)
	if err != nil {
		_ = store.Close()
		return controllerExecutionComposition{}, fmt.Errorf("controller: initialize Attach fact service: %w", err)
	}
	if err := attachFactValues.EnableBackingHookInputs(dataServices.secretRecords); err != nil {
		_ = store.Close()
		return controllerExecutionComposition{}, fmt.Errorf("controller: initialize backing hook inputs: %w", err)
	}
	attachFactReads, err := attachments.NewFactReadService(attachRecords, attachFactValues)
	if err != nil {
		_ = store.Close()
		return controllerExecutionComposition{}, fmt.Errorf("controller: initialize Attach fact reads: %w", err)
	}
	planResolver, err := taskplanning.NewTaskPlanResolverWithAttachments(
		cfg.Storage.VolumeRoot,
		authority.hierarchyRecords,
		attachRecords,
		dataServices.serviceRecords,
		attachFactValues,
		componentCatalog,
	)
	if err != nil {
		_ = store.Close()
		return controllerExecutionComposition{}, fmt.Errorf("controller: initialize execution plan resolver: %w", err)
	}
	if err := componentregistration.ConfigureReleasePlans(planResolver, authority.releaseLedger); err != nil {
		_ = store.Close()
		return controllerExecutionComposition{}, fmt.Errorf("controller: initialize release plan resolver: %w", err)
	}
	if err := planResolver.EnableScriptPlans(dataServices.scriptRecords); err != nil {
		_ = store.Close()
		return controllerExecutionComposition{}, fmt.Errorf("controller: initialize Script plan resolver: %w", err)
	}
	if err := planResolver.EnableBackupPlans(dataServices.backupRuntimeRecords); err != nil {
		_ = store.Close()
		return controllerExecutionComposition{}, fmt.Errorf("controller: initialize backup plan resolver: %w", err)
	}
	backingHookCheckpoints, err := taskcheckpoint.NewBackingHookCheckpointService(
		authority.tasks, planResolver, attachRecords, attachFactValues,
	)
	if err != nil {
		_ = store.Close()
		return controllerExecutionComposition{}, fmt.Errorf("controller: initialize backing hook checkpoint service: %w", err)
	}
	materializationResolver, err := initializeTaskMaterializationResolver(
		store, authority.hierarchyRecords, dataServices.entryValues, dataServices.secretRecords, planResolver, authority.intentProtector,
	)
	if err != nil {
		_ = store.Close()
		return controllerExecutionComposition{}, err
	}
	scriptArtifacts, err := taskplanning.NewScriptArtifactService(dataServices.scriptRecords, materializationResolver)
	if err != nil {
		_ = store.Close()
		return controllerExecutionComposition{}, fmt.Errorf("controller: initialize Script artifact service: %w", err)
	}
	scriptSourceReferences, err := initializeExecutionSourceReferences(ctx, store)
	if err != nil {
		_ = store.Close()
		return controllerExecutionComposition{}, fmt.Errorf("controller: initialize Script source-reference authority: %w", err)
	}
	backupSecretEvidence, err := etcd.NewBackupSecretResolutionReader(store)
	if err != nil {
		// Rationale: initialization is already failing; store shutdown is
		// best-effort and must not replace the primary typed error.
		_ = store.Close()
		return controllerExecutionComposition{}, errs.Wrap(errs.KindInternal, err)
	}
	backupSecrets, err := backupcapability.NewBackupSecretResolver(backupSecretEvidence, authority.intentProtector)
	if err != nil {
		// Rationale: initialization is already failing; store shutdown is
		// best-effort and must not replace the primary typed error.
		_ = store.Close()
		return controllerExecutionComposition{}, errs.Wrap(errs.KindInternal, err)
	}
	return controllerExecutionComposition{
		attachRecords:           attachRecords,
		attachFactValues:        attachFactValues,
		attachFactReads:         attachFactReads,
		planResolver:            planResolver,
		backingHookCheckpoints:  backingHookCheckpoints,
		materializationResolver: materializationResolver,
		scriptArtifacts:         scriptArtifacts,
		scriptSourceReferences:  scriptSourceReferences,
		backupSecrets:           backupSecrets,
	}, nil
}
