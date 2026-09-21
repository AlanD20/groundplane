package taskplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"math"

	"github.com/AlanD20/groundplane/internal/common/ids"
	entrycapability "github.com/AlanD20/groundplane/internal/controller/entry"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EntryRemovalPlanner struct {
	plans     *TaskPlanResolver
	materials EntryRemovalMaterializationResolver
	hierarchy *etcd.HierarchyRepository
}

func NewEntryRemovalPlanner(
	plans *TaskPlanResolver,
	materials EntryRemovalMaterializationResolver,
	hierarchy *etcd.HierarchyRepository,
) (*EntryRemovalPlanner, error) {
	if plans == nil || materials == nil || hierarchy == nil {
		return nil, errs.New(errs.KindInternal, "entry removal planner is not configured")
	}
	return &EntryRemovalPlanner{plans: plans, materials: materials, hierarchy: hierarchy}, nil
}

// PrepareDesiredEntryRemoval pins cleanup from applied state while binding its
// terminal metadata change to the already claimed desired revision.
func (planner *EntryRemovalPlanner) PrepareDesiredEntryRemoval(
	ctx context.Context, task etcd.TaskRecord, claim blueprints.EnvironmentBlueprintStageClaim,
) (etcd.TaskRecord, error) {
	if planner == nil || planner.hierarchy == nil || task.Type != taskjournal.TaskRemove ||
		ids.Validate(ids.KindEnvEntry, task.Target) != nil || task.ID != claim.TaskID ||
		task.Owner.EnvironmentID != claim.EnvironmentID || !task.CreatedAt.Equal(claim.CreatedAt) ||
		claim.RenderGeneration > math.MaxInt32 {
		return etcd.TaskRecord{}, errs.New(errs.KindInternal, "Entry removal desired Task identity is invalid")
	}
	desired, found, err := planner.hierarchy.GetEnvironmentComposeProjection(ctx, claim.EnvironmentID)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	if !found || desired.Revision != claim.BaselineHeadRevision {
		return etcd.TaskRecord{}, errs.New(errs.KindStateConflict, "Entry removal desired head changed")
	}
	contains := false
	for _, entry := range desired.Record.Entries {
		contains = contains || entry.Entry.ID == task.Target
	}
	if !contains {
		return etcd.TaskRecord{}, errs.New(errs.KindEntryNotFound, "Entry was not found")
	}
	applied, found, err := planner.hierarchy.GetEnvironmentAppliedComposeProjection(ctx, claim.EnvironmentID)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	var cleanup *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
	if found {
		for _, entry := range applied.Record.Entries {
			if entry.Entry.ID == task.Target {
				cleanup = &applied
				break
			}
		}
	}
	intent, err := environmentchanges.NewDesiredEntryRemovalIntent(task.Target, desired.Record.RevisionID, claim, cleanup)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	if cleanup == nil {
		task.Executor, task.TimeoutSeconds = taskjournal.TaskExecutorController, 30
		task.RenderGeneration = int32(claim.RenderGeneration)
		task.Params = map[string]string{taskjournal.TaskResourceKindParam: taskjournal.TaskResourceEntry,
			taskjournal.TaskEntryEnvironmentParam: claim.EnvironmentID, blueprints.EnvironmentDesiredRevisionParam: claim.RevisionID}
		task.Materializations = nil
		task.Steps = []taskjournal.TaskStepRecord{{ID: ids.New(ids.KindStep), Kind: taskjournal.TaskStepOperation}}
		digest := sha256.Sum256(
			[]byte(task.PlanID + "/" + task.Target + "/" + claim.DescriptorID + "/" + claim.RevisionID),
		)
		task.PlanHash = hex.EncodeToString(digest[:])
		return task, nil
	}
	environment, err := planner.hierarchy.GetEnvironment(ctx, claim.EnvironmentID)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	project, err := planner.hierarchy.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	identity := entrycapability.RemovalEnvironmentIdentity{
		ProjectID:           project.Record.ID,
		ProjectSlug:         project.Record.Slug,
		EnvironmentID:       environment.Record.ID,
		EnvironmentName:     environment.Record.Name,
		AuthorizedVolumeDir: environment.Record.VolumeDir,
		TenantID:            project.Record.TenantID,
	}
	if project.Record.TenantID != "" {
		tenant, err := planner.hierarchy.GetTenant(ctx, project.Record.TenantID)
		if err != nil {
			return etcd.TaskRecord{}, err
		}
		identity.TenantSlug = tenant.Record.Slug
	}
	task.Executor, task.TimeoutSeconds = taskjournal.TaskExecutorAgent, 120
	return planner.plans.prepareEntryRemovalTask(ctx, task, intent,
		entryRemovalTaskProcedureIDs{ArtifactID: ids.New(ids.KindConfig)}, planner.materials, identity)
}
