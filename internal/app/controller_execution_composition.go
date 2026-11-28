package app

import (
	"context"
	"fmt"

	"github.com/AlanD20/groundplane/internal/app/componentregistration"
	"github.com/AlanD20/groundplane/internal/app/hostplatform"
	backupcapability "github.com/AlanD20/groundplane/internal/controller/backup"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	handlers "github.com/AlanD20/groundplane/internal/controller/handlers"
	taskmaterialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	backupsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/backupsecrets"
	scriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"

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
	materializationResolver *taskmaterialization.TaskMaterializationResolver
	scriptArtifacts         *taskplanning.ScriptArtifactService
	scriptSourceReferences  *scriptsourcepublication.Authority
	backupSecrets           *backupcapability.BackupSecretResolver
}

func newControllerExecutionComposition(
	ctx context.Context, cfg config.ControllerConfig, store etcd.EnvironmentBlueprintStore,
	authority controllerAuthorityComposition, dataServices controllerDataComposition,
	componentCatalog []componentrender.EnvironmentComponentRegistration,
) (controllerExecutionComposition, error) {
	attachRecords, err := etcd.NewAttachRepository(store)
	if err != nil {
		return controllerExecutionComposition{}, fmt.Errorf("controller: initialize Attach repository: %w", err)
	}
	attachFactValues, err := attachments.NewFactService(attachRecords, authority.intentProtector)
	if err != nil {
		return controllerExecutionComposition{}, fmt.Errorf("controller: initialize Attach fact service: %w", err)
	}
	if err := attachFactValues.EnableBackingHookInputs(dataServices.secretRecords); err != nil {
		return controllerExecutionComposition{}, fmt.Errorf("controller: initialize backing hook inputs: %w", err)
	}
	attachFactReads, err := attachments.NewFactReadService(attachRecords, attachFactValues)
	if err != nil {
		return controllerExecutionComposition{}, fmt.Errorf("controller: initialize Attach fact reads: %w", err)
	}
	planResolver, err := taskplanning.NewTaskPlanResolverWithBlueprints(
		cfg.Storage.VolumeRoot,
		authority.hierarchyRecords,
		componentCatalog,
	)
	if err != nil {
		return controllerExecutionComposition{}, fmt.Errorf("controller: initialize execution plan resolver: %w", err)
	}
	if err := componentregistration.ConfigureServiceProxyImages(planResolver); err != nil {
		return controllerExecutionComposition{}, fmt.Errorf("controller: initialize release plan resolver: %w", err)
	}
	materializationResolver, err := hostplatform.NewMaterializationResolver(
		store,
		authority.hierarchyRecords,
		dataServices.entryValues,
		dataServices.secretRecords,
		planResolver,
		authority.intentProtector,
	)
	if err != nil {
		return controllerExecutionComposition{}, err
	}
	scriptArtifacts, err := taskplanning.NewScriptArtifactService(dataServices.scriptRecords, materializationResolver)
	if err != nil {
		return controllerExecutionComposition{}, fmt.Errorf("controller: initialize Script artifact service: %w", err)
	}
	scriptSourceReferences, err := initializeExecutionSourceReferences(ctx, store)
	if err != nil {
		return controllerExecutionComposition{}, fmt.Errorf(
			"controller: initialize Script source-reference authority: %w",
			err,
		)
	}
	backupSecretEvidence, err := backupsecrets.NewReader(store)
	if err != nil {
		// Rationale: initialization is already failing; store shutdown is
		// best-effort and must not replace the primary typed error.
		return controllerExecutionComposition{}, errs.Wrap(errs.KindInternal, err)
	}
	backupSecrets, err := backupcapability.NewBackupSecretResolver(ctx, backupSecretEvidence, authority.intentProtector)
	if err != nil {
		// Rationale: initialization is already failing; store shutdown is
		// best-effort and must not replace the primary typed error.
		return controllerExecutionComposition{}, errs.Wrap(errs.KindInternal, err)
	}
	return controllerExecutionComposition{
		attachRecords:           attachRecords,
		attachFactValues:        attachFactValues,
		attachFactReads:         attachFactReads,
		planResolver:            planResolver,
		materializationResolver: materializationResolver,
		scriptArtifacts:         scriptArtifacts,
		scriptSourceReferences:  scriptSourceReferences,
		backupSecrets:           backupSecrets,
	}, nil
}
