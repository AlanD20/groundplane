package taskplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	taskmaterialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"math"
)

func (resolver *TaskPlanResolver) resolveEntryRemovalPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	reader, ok := resolver.blueprints.(entryRemovalPlanStateReader)
	if !ok {
		return nil, errs.New(errs.KindInternal, "entry removal plan state reader is unavailable")
	}
	stored, found, err := reader.GetEntryRemovalIntent(ctx, task.ID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errs.New(errs.KindInternal, "entry removal intent is missing")
	}
	return resolver.buildEntryRemovalPlan(ctx, task, stored.Record)
}

func (resolver *TaskPlanResolver) buildEntryRemovalPlan(
	ctx context.Context,
	task etcd.TaskRecord,
	intent environmentchanges.EntryRemovalIntent,
) (*agentpb.ExecutionPlan, error) {
	if resolver == nil || resolver.blueprints == nil || task.Executor != taskjournal.TaskExecutorAgent ||
		task.Type != taskjournal.TaskRemove || ids.Validate(ids.KindEnvEntry, task.Target) != nil ||
		task.ID != intent.TaskID || task.Target != intent.EntryID || !task.CreatedAt.Equal(intent.CreatedAt) ||
		intent.Status != taskjournal.TaskStatusPending || intent.CurrentProjection == nil || intent.CandidateProjection == nil ||
		task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 || len(task.Params) != 8 {
		return nil, errs.New(errs.KindInternal, "durable Entry removal Task shape is invalid")
	}
	candidate := *intent.CandidateProjection
	revisionID := task.Params[blueprints.EnvironmentDesiredRevisionParam]
	artifactID := task.Params[taskjournal.TaskComposeArtifactParam]
	if task.Params[taskjournal.TaskEntryEnvironmentParam] != intent.EnvironmentID ||
		task.Params[taskjournal.TaskMaterializationEnvironmentParam] != intent.EnvironmentID ||
		revisionID != candidate.RevisionID || ids.Validate(ids.KindTask, revisionID) != nil ||
		ids.Validate(ids.KindConfig, artifactID) != nil || uint64(task.RenderGeneration) != candidate.RenderGeneration {
		return nil, errs.New(errs.KindInternal, "durable Entry removal Task parameters are invalid")
	}
	templates, err := entryRemovalMaterializationTemplates(intent)
	if err != nil || len(task.Steps) != len(templates) || len(task.Materializations) != len(templates) {
		return nil, errs.New(errs.KindInternal, "durable Entry removal materialization count changed")
	}
	references := make(map[string]materializationrecord.Record, len(task.Materializations))
	for _, reference := range task.Materializations {
		if _, duplicate := references[reference.StepID]; duplicate {
			return nil, errs.New(errs.KindInternal, "durable Entry removal step binding is duplicated")
		}
		references[reference.StepID] = reference
	}
	steps := make([]*agentpb.ExecutionStep, len(task.Steps))
	for index, step := range task.Steps {
		reference, found := references[step.ID]
		if !found || !sameEntryRemovalMaterializationTemplate(reference, templates[index]) {
			return nil, errs.New(errs.KindInternal, "durable Entry removal materialization changed")
		}
		steps[index], err = taskmaterialization.BuildTaskMaterializationStep(
			reference,
			artifactID,
			uint32(task.TimeoutSeconds),
		)
		if err != nil {
			return nil, err
		}
	}

	identity, err := pinnedEntryRemovalIdentity(task, intent)
	if err != nil {
		return nil, err
	}
	artifact, err := resolver.renderPinnedEnvironmentArtifact(
		ctx,
		task,
		identity,
		revisionID,
		artifactID,
		candidate,
		nil,
	)
	if err != nil {
		return nil, err
	}
	return taskplan.Build(taskplan.BuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_REMOVE, TargetID: task.Target,
		Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: steps,
	})
}

func pinnedEntryRemovalIdentity(
	task etcd.TaskRecord,
	intent environmentchanges.EntryRemovalIntent,
) (pinnedEnvironmentIdentity, error) {
	identity := pinnedEnvironmentIdentity{
		TenantID: task.Owner.TenantID, TenantSlug: task.Params[taskjournal.TaskEntryTenantSlugParam],
		ProjectID: task.Owner.ProjectID, ProjectSlug: task.Params[taskjournal.TaskEntryProjectSlugParam],
		EnvironmentID:       task.Owner.EnvironmentID,
		EnvironmentName:     task.Params[taskjournal.TaskEntryEnvironmentNameParam],
		AuthorizedVolumeDir: task.Params[taskjournal.TaskEntryAuthorizedVolumeDirParam],
	}
	validWorkspace := task.Owner.WorkspaceType == taskjournal.TaskWorkspaceTenant &&
		ids.Validate(ids.KindTenant, identity.TenantID) == nil && identity.TenantSlug != ""
	if identity.TenantID == "" {
		validWorkspace = task.Owner.WorkspaceType == taskjournal.TaskWorkspacePlatform && identity.TenantSlug == ""
	}
	if !validWorkspace || ids.Validate(ids.KindProject, identity.ProjectID) != nil ||
		ids.Validate(
			ids.KindEnvironment,
			identity.EnvironmentID,
		) != nil || identity.EnvironmentID != intent.EnvironmentID ||
		identity.ProjectSlug == "" || identity.EnvironmentName == "" || identity.AuthorizedVolumeDir == "" {
		return pinnedEnvironmentIdentity{}, errs.New(errs.KindInternal, "durable Entry removal identity is invalid")
	}
	return identity, nil
}
