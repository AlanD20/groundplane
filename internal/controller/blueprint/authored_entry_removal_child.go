package blueprint

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
	"github.com/AlanD20/groundplane/internal/controller/configurationrecovery"
	entrycapability "github.com/AlanD20/groundplane/internal/controller/entry"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredEntryConfigurationPreparer interface {
	PrepareBlueprintEntryConfiguration(context.Context, etcd.TaskRecord, int64) (etcd.TaskRecord, error)
}

type authoredEntryRecoveryPreparer interface {
	PrepareConfigurationRecovery(context.Context, etcd.TaskRecord) (configurationrecovery.Prepared, error)
}

func (service *Service) prepareAuthoredEntryRemovalChild(
	ctx context.Context, parent etcd.TaskRecord, unit blueprintunits.Unit, timeoutSeconds int64,
) (blueprintcoordinator.PreparedChild, error) {
	if !unit.Removal || unit.Target.Kind != ids.KindEnvEntry || service.plans == nil {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindValidationFailed, "Blueprint Entry removal child identity is invalid",
		)
	}
	entry, err := service.repository.GetEntry(ctx, unit.Target.ID)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if entry.Record.EnvironmentID != parent.Owner.EnvironmentID {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Entry removal owner changed",
		)
	}
	applied, found, err := service.repository.GetEnvironmentAppliedComposeProjection(
		ctx, parent.Owner.EnvironmentID,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if !found {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Entry applied projection is unavailable",
		)
	}
	createdAt := service.now().UTC()
	child := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), PlanID: ids.New(ids.KindPlan),
		Owner: parent.Owner, Actor: taskjournal.TaskActorSystem, Executor: taskjournal.TaskExecutorAgent,
		Type: taskjournal.TaskRemove, Target: unit.Target.ID, RenderGeneration: parent.RenderGeneration,
		TargetName:     taskjournal.CaptureTargetName("Entry " + entries.BlueprintIdentityKey(entry.Record)),
		Params:         map[string]string{taskjournal.TaskBlueprintParentParam: parent.ID},
		TimeoutSeconds: timeoutSeconds, Status: taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	intent, err := environmentchanges.NewAuthoredEntryRemovalIntent(
		child.ID, parent.Owner.EnvironmentID, unit.Target.ID, entry.Revision,
		parent.ID, uint64(parent.RenderGeneration), applied, createdAt,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	identity, err := service.authoredEntryRemovalIdentity(ctx, parent.Owner.EnvironmentID)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	child, err = service.plans.PrepareBlueprintEntryRemovalMaterializations(
		ctx, child, intent, ids.New(ids.KindConfig), service.materials, identity,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	configuration, ok := service.repository.(authoredEntryConfigurationPreparer)
	if !ok {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindInternal, "Blueprint Entry configuration preparer is not configured",
		)
	}
	child, err = configuration.PrepareBlueprintEntryConfiguration(ctx, child, applied.ReadRevision)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	recoverySources, ok := service.materials.(authoredEntryRecoveryPreparer)
	if !ok {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindInternal, "Blueprint Entry recovery sources are not configured",
		)
	}
	recovery, err := recoverySources.PrepareConfigurationRecovery(ctx, child)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	for _, file := range recovery.Files {
		child.Steps = append(child.Steps,
			taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: file.Probe.StepID},
			taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: file.Compensate.StepID},
		)
	}
	plan, err := service.plans.BuildBlueprintEntryRemovalPlan(ctx, child, intent, recovery)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	child.PlanHash = hex.EncodeToString(plan.GetPlanHash())
	child.Steps = taskjournal.CaptureStepDescriptions(child.Steps, plan)
	publication, err := etcd.NewBlueprintEntryRemovalPublication(child, intent)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	return blueprintcoordinator.PreparedChild{Task: child, EntryRemovalPublication: publication}, nil
}

func (service *Service) authoredEntryRemovalIdentity(
	ctx context.Context, environmentID string,
) (entrycapability.RemovalEnvironmentIdentity, error) {
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return entrycapability.RemovalEnvironmentIdentity{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return entrycapability.RemovalEnvironmentIdentity{}, err
	}
	identity := entrycapability.RemovalEnvironmentIdentity{
		ProjectID: project.Record.ID, ProjectSlug: project.Record.Slug,
		EnvironmentID: environment.Record.ID, EnvironmentName: environment.Record.Name,
		AuthorizedVolumeDir: environment.Record.VolumeDir, TenantID: project.Record.TenantID,
	}
	if project.Record.TenantID != "" {
		tenant, err := service.repository.GetTenant(ctx, project.Record.TenantID)
		if err != nil {
			return entrycapability.RemovalEnvironmentIdentity{}, err
		}
		identity.TenantSlug = tenant.Record.Slug
	}
	return identity, nil
}
