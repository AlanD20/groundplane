package taskplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/ids"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	entrycapability "github.com/AlanD20/groundplane/internal/controller/entry"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"math"
	"sort"
)

type entryRemovalPlanStateReader interface {
	GetEntryRemovalIntent(
		context.Context,
		string,
	) (etcdstore.Versioned[etcd.EntryRemovalIntent], bool, error)
}

// EntryRemovalMaterializationResolver resolves one closed durable source while
// the application prepares immutable length and digest metadata.
type EntryRemovalMaterializationResolver interface {
	ResolveTaskMaterializationSource(
		context.Context,
		string,
		materializationrecord.Source,
	) ([]byte, error)
}

type entryRemovalTaskProcedureIDs struct {
	ArtifactID string
}

func (planner *EntryRemovalPlanner) PrepareEntryRemoval(
	ctx context.Context,
	request entrycapability.RemovalPlanRequest,
) (entrycapability.RemovalTaskPlan, error) {
	if planner == nil || planner.plans == nil || planner.plans.blueprints == nil || planner.hierarchy == nil ||
		ids.Validate(ids.KindTask, request.TaskID) != nil || ids.Validate(ids.KindPlan, request.PlanID) != nil ||
		ids.Validate(ids.KindEnvEntry, request.EntryID) != nil ||
		ids.Validate(ids.KindEnvironment, request.EnvironmentID) != nil ||
		ids.Validate(ids.KindConfig, request.ArtifactID) != nil || request.EntryRevision <= 0 ||
		request.ProjectionRevision <= 0 || request.CreatedAt.IsZero() ||
		request.Identity.EnvironmentID != request.EnvironmentID ||
		request.Identity.ProjectID == "" || request.Identity.ProjectSlug == "" ||
		request.Identity.EnvironmentName == "" || request.Identity.AuthorizedVolumeDir == "" {
		return entrycapability.RemovalTaskPlan{}, errs.New(errs.KindInternal, "entry removal plan request is invalid")
	}
	projection, found, err := planner.hierarchy.GetEnvironmentAppliedComposeProjection(ctx, request.EnvironmentID)
	if err != nil {
		return entrycapability.RemovalTaskPlan{}, err
	}
	if !found || projection.Revision != request.ProjectionRevision {
		return entrycapability.RemovalTaskPlan{}, errs.New(errs.KindStateConflict, "entry removal projection changed")
	}
	intent, err := etcd.NewEntryRemovalIntent(
		request.TaskID, request.EnvironmentID, request.EntryID, request.EntryRevision, &projection, request.CreatedAt,
	)
	if err != nil {
		return entrycapability.RemovalTaskPlan{}, err
	}
	owner := taskjournal.TaskOwner{
		WorkspaceType: taskjournal.TaskWorkspacePlatform,
		ProjectID:     request.Identity.ProjectID, EnvironmentID: request.Identity.EnvironmentID,
	}
	if request.Identity.TenantID != "" {
		owner.WorkspaceType = taskjournal.TaskWorkspaceTenant
		owner.TenantID = request.Identity.TenantID
	}
	task := etcd.TaskRecord{
		ID: request.TaskID, PlanID: request.PlanID, Executor: taskjournal.TaskExecutorAgent,
		Type: taskjournal.TaskRemove, Target: request.EntryID, TimeoutSeconds: 120,
		Status: taskjournal.TaskStatusPending, CreatedAt: request.CreatedAt, Owner: owner,
	}
	prepared, err := planner.plans.prepareEntryRemovalTask(
		ctx, task, intent, entryRemovalTaskProcedureIDs{ArtifactID: request.ArtifactID},
		planner.materials, request.Identity,
	)
	if err != nil {
		return entrycapability.RemovalTaskPlan{}, err
	}
	return entryRemovalTaskPlan(prepared)
}

func (resolver *TaskPlanResolver) prepareEntryRemovalTask(
	ctx context.Context,
	task etcd.TaskRecord,
	intent etcd.EntryRemovalIntent,
	procedure entryRemovalTaskProcedureIDs,
	materials EntryRemovalMaterializationResolver,
	identity entrycapability.RemovalEnvironmentIdentity,
) (etcd.TaskRecord, error) {
	templates, err := entryRemovalMaterializationTemplates(intent)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	if ids.Validate(ids.KindConfig, procedure.ArtifactID) != nil || intent.CandidateProjection == nil ||
		intent.CandidateProjection.RenderGeneration > math.MaxInt32 {
		return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "entry removal procedure ids are invalid")
	}
	task.Params = map[string]string{
		etcd.TaskEntryEnvironmentParam:                  intent.EnvironmentID,
		taskjournal.TaskMaterializationEnvironmentParam: intent.EnvironmentID,
		etcd.EnvironmentDesiredRevisionParam:            intent.CandidateProjection.RevisionID,
		etcd.TaskComposeArtifactParam:                   procedure.ArtifactID,
		etcd.TaskEntryTenantSlugParam:                   identity.TenantSlug,
		etcd.TaskEntryProjectSlugParam:                  identity.ProjectSlug,
		etcd.TaskEntryEnvironmentNameParam:              identity.EnvironmentName,
		etcd.TaskEntryAuthorizedVolumeDirParam:          identity.AuthorizedVolumeDir,
	}
	task.RenderGeneration = int32(intent.CandidateProjection.RenderGeneration)
	task.Steps = make([]taskjournal.TaskStepRecord, len(templates))
	task.Materializations = make([]materializationrecord.Record, len(templates))
	seenMaterializations := make(map[string]struct{}, len(templates))
	seenSteps := make(map[string]struct{}, len(templates))
	for index, template := range templates {
		materializationID := ids.New(ids.KindConfig)
		stepID := ids.New(ids.KindStep)
		if ids.Validate(ids.KindConfig, materializationID) != nil || ids.Validate(ids.KindStep, stepID) != nil {
			return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "entry removal procedure ids are invalid")
		}
		if _, duplicate := seenMaterializations[materializationID]; duplicate {
			return etcd.TaskRecord{}, errs.New(
				errs.KindValidationFailed,
				"entry removal materialization id is duplicated",
			)
		}
		if _, duplicate := seenSteps[stepID]; duplicate {
			return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "entry removal step id is duplicated")
		}
		seenMaterializations[materializationID] = struct{}{}
		seenSteps[stepID] = struct{}{}
		reference := template
		reference.MaterializationID = materializationID
		reference.StepID = stepID
		content := []byte(nil)
		if !entryRemovalOutputRemoves(reference.OutputKind) {
			if materials == nil {
				return etcd.TaskRecord{}, errs.New(
					errs.KindInternal,
					"entry removal materialization resolver is unavailable",
				)
			}
			content, err = materials.ResolveTaskMaterializationSource(ctx, intent.EnvironmentID, reference.Source)
			if err != nil {
				clear(content)
				return etcd.TaskRecord{}, err
			}
		}
		digest := sha256.Sum256(content)
		reference.Length = uint64(len(content))
		reference.SHA256 = hex.EncodeToString(digest[:])
		clear(content)
		task.Steps[index] = taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: stepID}
		task.Materializations[index] = reference
	}
	sort.Slice(task.Materializations, func(left int, right int) bool {
		return task.Materializations[left].StepID < task.Materializations[right].StepID
	})
	plan, err := resolver.buildEntryRemovalPlan(ctx, task, intent)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	return task, nil
}
