package taskplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/controller/configurationrecovery"
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
	var recovery configurationrecovery.Prepared
	if intent.Authored != nil {
		if resolver.configurationRecovery == nil {
			return nil, errs.New(errs.KindInternal, "authored Entry recovery sources are unavailable")
		}
		var err error
		recovery, err = resolver.configurationRecovery.Prepare(ctx, task)
		if err != nil {
			return nil, err
		}
	}
	return resolver.buildEntryRemovalPlanWithRecovery(ctx, task, intent, recovery)
}

func (resolver *TaskPlanResolver) BuildBlueprintEntryRemovalPlan(
	ctx context.Context,
	task etcd.TaskRecord,
	intent environmentchanges.EntryRemovalIntent,
	recovery configurationrecovery.Prepared,
) (*agentpb.ExecutionPlan, error) {
	if intent.Authored == nil {
		return nil, errs.New(errs.KindValidationFailed, "authored Entry removal recovery is absent")
	}
	return resolver.buildEntryRemovalPlanWithRecovery(ctx, task, intent, recovery)
}

func (resolver *TaskPlanResolver) buildEntryRemovalPlanWithRecovery(
	ctx context.Context,
	task etcd.TaskRecord,
	intent environmentchanges.EntryRemovalIntent,
	recovery configurationrecovery.Prepared,
) (*agentpb.ExecutionPlan, error) {
	parameterCount := 8
	if intent.Authored != nil {
		parameterCount = 10
	}
	if resolver == nil || resolver.blueprints == nil || task.Executor != taskjournal.TaskExecutorAgent ||
		task.Type != taskjournal.TaskRemove || ids.Validate(ids.KindEnvEntry, task.Target) != nil ||
		task.ID != intent.TaskID || task.Target != intent.EntryID || !task.CreatedAt.Equal(intent.CreatedAt) ||
		intent.Status != taskjournal.TaskStatusPending || intent.CurrentProjection == nil || intent.CandidateProjection == nil ||
		task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 || len(task.Params) != parameterCount {
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
	if intent.Authored != nil && (task.Actor != taskjournal.TaskActorSystem ||
		task.Params[taskjournal.TaskBlueprintParentParam] != intent.Authored.ParentTaskID ||
		task.Params[taskjournal.TaskBlueprintEntryUnitParam] != intent.EntryID ||
		intent.Authored.RenderGeneration != uint64(task.RenderGeneration)) {
		return nil, errs.New(errs.KindInternal, "durable authored Entry removal identity is invalid")
	}
	templates, err := entryRemovalMaterializationTemplates(intent)
	if err != nil || len(task.Materializations) != len(templates) {
		return nil, errs.New(errs.KindInternal, "durable Entry removal materialization count changed")
	}
	references := make(map[string]materializationrecord.Record, len(task.Materializations))
	for _, reference := range task.Materializations {
		if _, duplicate := references[reference.StepID]; duplicate {
			return nil, errs.New(errs.KindInternal, "durable Entry removal step binding is duplicated")
		}
		references[reference.StepID] = reference
	}
	forwardCount := len(templates)
	if len(task.Steps) < forwardCount {
		return nil, errs.New(errs.KindInternal, "durable Entry removal steps are incomplete")
	}
	steps := make([]*agentpb.ExecutionStep, 0, len(task.Steps))
	for index, step := range task.Steps[:forwardCount] {
		reference, found := references[step.ID]
		if !found || !sameEntryRemovalMaterializationTemplate(reference, templates[index]) {
			return nil, errs.New(errs.KindInternal, "durable Entry removal materialization changed")
		}
		built, err := taskmaterialization.BuildTaskMaterializationStep(
			reference,
			artifactID,
			uint32(task.TimeoutSeconds),
		)
		if err != nil {
			return nil, err
		}
		if intent.Authored != nil {
			built.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD
		}
		steps = append(steps, built)
	}
	var releaseProcedure *agentpb.CandidateReleaseProcedure
	operation := agentpb.PlanOperation_PLAN_OPERATION_REMOVE
	if intent.Authored != nil {
		if recovery.Procedure == nil || len(recovery.Files) != len(templates) ||
			len(task.Steps) != forwardCount+len(recovery.Files)*2 {
			return nil, errs.New(errs.KindInternal, "durable authored Entry recovery is incomplete")
		}
		for index, file := range recovery.Files {
			probe, buildErr := taskmaterialization.BuildTaskMaterializationStep(
				file.Probe, artifactID, uint32(task.TimeoutSeconds),
			)
			if buildErr != nil {
				return nil, buildErr
			}
			compensate, buildErr := taskmaterialization.BuildTaskMaterializationStep(
				file.Compensate, artifactID, uint32(task.TimeoutSeconds),
			)
			if buildErr != nil {
				return nil, buildErr
			}
			probe.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE
			compensate.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE
			compensate.PrerequisiteStepId = recovery.Procedure.Files[index].ForwardStepId
			wantProbe := task.Steps[forwardCount+index*2]
			wantCompensate := task.Steps[forwardCount+index*2+1]
			if wantProbe.ID != probe.GetStepId() || wantCompensate.ID != compensate.GetStepId() {
				return nil, errs.New(errs.KindInternal, "durable authored Entry recovery order changed")
			}
			steps = append(steps, probe, compensate)
		}
		releaseProcedure = &agentpb.CandidateReleaseProcedure{ConfigurationRestoration: recovery.Procedure}
		operation = agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
	} else if len(task.Steps) != forwardCount {
		return nil, errs.New(errs.KindInternal, "durable Entry removal step count changed")
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
		Operation:        operation, TargetID: task.Target,
		Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: steps,
		CandidateReleaseProcedure: releaseProcedure,
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
