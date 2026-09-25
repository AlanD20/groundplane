package taskplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"math"

	"github.com/AlanD20/groundplane/internal/common/ids"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

type blueprintVolumeUnitStateReader interface {
	GetEnvironmentDesiredInputRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput], bool, error)
	GetEnvironmentOwnedIdentitiesRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities], bool, error)
}

type blueprintVolumeUnitInput struct {
	project  *composetypes.Project
	identity projectionrecord.OwnedIdentity
	volume   composetypes.VolumeConfig
	create   bool
}

// PrepareBlueprintVolumeUnit seals one resource-scoped Volume ensure from the
// parent's immutable authored revision. A newborn Volume creates its directory
// before Docker ownership; an existing Volume is verified without recreation.
func (resolver *TaskPlanResolver) PrepareBlueprintVolumeUnit(
	ctx context.Context,
	task etcd.TaskRecord,
) (etcd.TaskRecord, *agentpb.ExecutionPlan, error) {
	if len(task.Steps) != 0 || task.PlanHash != "" {
		return etcd.TaskRecord{}, nil, errs.New(
			errs.KindValidationFailed, "Blueprint Volume child is already prepared",
		)
	}
	input, err := resolver.loadBlueprintVolumeUnitInput(ctx, task)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	stepIDs, err := blueprintVolumeUnitStepIDs(task, input.create)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	prepared := task
	prepared.Steps = make([]taskjournal.TaskStepRecord, len(stepIDs))
	for index, stepID := range stepIDs {
		prepared.Steps[index] = taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: stepID}
	}
	plan, err := resolver.buildBlueprintVolumeUnitPlan(ctx, prepared)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	prepared.PlanHash = hex.EncodeToString(plan.GetPlanHash())
	return prepared, plan, nil
}

func (resolver *TaskPlanResolver) resolveBlueprintVolumeUnitPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	return resolver.buildBlueprintVolumeUnitPlan(ctx, task)
}

func (resolver *TaskPlanResolver) buildBlueprintVolumeUnitPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	input, err := resolver.loadBlueprintVolumeUnitInput(ctx, task)
	if err != nil {
		return nil, err
	}
	expectedStepIDs, err := blueprintVolumeUnitStepIDs(task, input.create)
	if err != nil || !blueprintVolumeUnitStepsMatch(task.Steps, expectedStepIDs) {
		return nil, errs.New(errs.KindInternal, "durable Blueprint Volume child procedure changed")
	}
	environmentID := task.Owner.EnvironmentID
	environment, err := resolver.blueprints.GetEnvironment(ctx, environmentID)
	if err != nil {
		return nil, err
	}
	project, err := resolver.blueprints.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return nil, err
	}
	expectedOwner, err := taskjournal.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return nil, err
	}
	if task.Owner != expectedOwner ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Volume child Environment hierarchy changed")
	}
	ownerKind := composerender.ComposeProjectOwnerBacking
	tenantID := ""
	if project.Record.Kind == hierarchyrecord.ProjectKindTenant {
		tenant, tenantErr := resolver.blueprints.GetTenant(ctx, project.Record.TenantID)
		if tenantErr != nil {
			return nil, tenantErr
		}
		if tenant.Record.ID != project.Record.TenantID {
			return nil, errs.New(errs.KindStateConflict, "Blueprint Volume child Tenant changed")
		}
		ownerKind = composerender.ComposeProjectOwnerTenant
		tenantID = tenant.Record.ID
	} else if project.Record.Kind != hierarchyrecord.ProjectKindBacking || project.Record.TenantID != "" {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Volume child Project ownership changed")
	}
	artifactID, err := blueprintVolumeUnitArtifactID(task)
	if err != nil {
		return nil, err
	}
	artifact, err := composerender.RenderCompose(composerender.ComposeRenderInput{
		Project: &composetypes.Project{
			Name:    input.project.Name,
			Volumes: composetypes.Volumes{input.identity.Name: input.volume},
		},
		ArtifactID: artifactID, ProjectOwnerKind: ownerKind,
		TenantID: tenantID, ProjectID: project.Record.ID, EnvironmentID: environmentID,
		PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
		AuthorizedVolumeDir: environment.Record.VolumeDir,
		Identities: composeidentity.Snapshot{Volumes: []composeidentity.Resource{{
			ID: input.identity.ID, Name: input.identity.Name,
		}}},
	})
	if err != nil {
		return nil, err
	}
	if len(artifact.GetVolumes()) != 1 || artifact.GetVolumes()[0].GetVolumeId() != task.Target ||
		len(artifact.GetServices()) != 0 || len(artifact.GetNetworks()) != 0 {
		return nil, errs.New(errs.KindInternal, "Blueprint Volume child artifact is not resource scoped")
	}
	intentSHA256, err := decodeBlueprintVolumeIntent(task.Params[taskjournal.TaskBlueprintVolumeIntentParam])
	if err != nil {
		return nil, err
	}
	steps := make([]*agentpb.ExecutionStep, 0, len(expectedStepIDs))
	prerequisite := ""
	if input.create {
		prerequisite = expectedStepIDs[0]
		steps = append(steps, &agentpb.ExecutionStep{
			StepId: prerequisite, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
			Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{
				ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{
					ArtifactId: artifactID, VolumeIds: []string{task.Target}, IntentSha256: intentSHA256,
				},
			},
		})
	}
	steps = append(steps, &agentpb.ExecutionStep{
		StepId: expectedStepIDs[len(expectedStepIDs)-1], PrerequisiteStepId: prerequisite,
		TimeoutSeconds: uint32(task.TimeoutSeconds),
		Policy:         agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
		Payload: &agentpb.ExecutionStep_ManagedVolumeEnsure{ManagedVolumeEnsure: &agentpb.ManagedVolumeEnsure{
			ArtifactId: artifactID, VolumeId: task.Target, RequireExisting: !input.create,
		}},
	})
	return taskplan.Build(taskplan.BuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
		TargetID:         environmentID, Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: steps,
	})
}

func (resolver *TaskPlanResolver) loadBlueprintVolumeUnitInput(
	ctx context.Context,
	task etcd.TaskRecord,
) (blueprintVolumeUnitInput, error) {
	parentID := task.Params[taskjournal.TaskBlueprintParentParam]
	if resolver == nil || resolver.blueprints == nil || ctx == nil ||
		task.Type != taskjournal.TaskUpdate || task.Actor != taskjournal.TaskActorSystem ||
		task.Executor != taskjournal.TaskExecutorAgent || ids.Validate(ids.KindVolume, task.Target) != nil ||
		task.Params[taskjournal.TaskBlueprintVolumeUnitParam] != task.Target ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] != parentID ||
		ids.Validate(ids.KindTask, parentID) != nil || parentID == task.ID ||
		task.Owner.EnvironmentID == "" || ids.Validate(ids.KindPlan, task.PlanID) != nil ||
		task.RenderGeneration <= 0 || task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 ||
		len(task.Params) != 5 || len(task.Materializations) != 0 || task.EntryRuntime != nil ||
		task.Configuration != nil || len(task.ComponentActionStepIDs) != 0 ||
		len(task.ManagedComponentTeardownSources) != 0 {
		return blueprintVolumeUnitInput{}, errs.New(
			errs.KindInternal, "durable Blueprint Volume child shape is invalid",
		)
	}
	if _, err := decodeBlueprintVolumeIntent(task.Params[taskjournal.TaskBlueprintVolumeIntentParam]); err != nil {
		return blueprintVolumeUnitInput{}, err
	}
	authority, ok := resolver.blueprints.(blueprintVolumeUnitStateReader)
	if !ok {
		return blueprintVolumeUnitInput{}, errs.New(
			errs.KindInternal, "Blueprint Volume child desired authority is not configured",
		)
	}
	desired, identities, err := loadBlueprintVolumeRevision(
		ctx, authority, task.Owner.EnvironmentID, parentID, uint64(task.RenderGeneration),
	)
	if err != nil {
		return blueprintVolumeUnitInput{}, err
	}
	identity, err := selectBlueprintVolumeIdentity(identities, task.Target)
	if err != nil {
		return blueprintVolumeUnitInput{}, err
	}
	project, err := composerender.LoadNormalizedEnvironmentDesiredProject(ctx, desired, identities)
	if err != nil {
		return blueprintVolumeUnitInput{}, err
	}
	volume, found := project.Volumes[identity.Name]
	if !found || !blueprintVolumeSyntaxSupported(volume, identity) {
		return blueprintVolumeUnitInput{}, errs.New(
			errs.KindStateConflict, "Blueprint Volume child authored storage shape is unsupported",
		)
	}
	mode := task.Params[taskjournal.TaskBlueprintVolumeModeParam]
	if mode != taskjournal.TaskBlueprintVolumeModeCreate && mode != taskjournal.TaskBlueprintVolumeModeVerify {
		return blueprintVolumeUnitInput{}, errs.New(
			errs.KindInternal, "durable Blueprint Volume execution mode is invalid",
		)
	}
	create := mode == taskjournal.TaskBlueprintVolumeModeCreate
	if !create {
		birthDesired, birthIdentities, birthErr := loadBlueprintVolumeRevision(
			ctx, authority, task.Owner.EnvironmentID, identity.BirthRevisionID, 0,
		)
		if birthErr != nil {
			return blueprintVolumeUnitInput{}, birthErr
		}
		birthIdentity, birthErr := selectBlueprintVolumeIdentity(birthIdentities, task.Target)
		if birthErr != nil || birthIdentity.Name != identity.Name {
			return blueprintVolumeUnitInput{}, errs.New(
				errs.KindStateConflict, "Blueprint Volume child birth authority changed",
			)
		}
		birthProject, birthErr := composerender.LoadNormalizedEnvironmentDesiredProject(
			ctx, birthDesired, birthIdentities,
		)
		if birthErr != nil {
			return blueprintVolumeUnitInput{}, birthErr
		}
		birthVolume, found := birthProject.Volumes[birthIdentity.Name]
		if !found || !maps.Equal(birthVolume.Labels, volume.Labels) {
			return blueprintVolumeUnitInput{}, errs.New(
				errs.KindStateConflict, "Blueprint cannot change physical labels of an existing Volume",
			)
		}
	}
	return blueprintVolumeUnitInput{project: project, identity: identity, volume: volume, create: create}, nil
}

func blueprintVolumeSyntaxSupported(
	volume composetypes.VolumeConfig,
	identity projectionrecord.OwnedIdentity,
) bool {
	slug, slugged := volume.Extensions[composerender.ComposeVolumeSlugExtension].(string)
	return volume.Name == "" && !bool(volume.External) &&
		(volume.Driver == "" || volume.Driver == "local") && len(volume.DriverOpts) == 0 &&
		len(volume.CustomLabels) == 0 && len(volume.Extensions) == 1 && slugged && slug == identity.Slug
}

func loadBlueprintVolumeRevision(
	ctx context.Context,
	authority blueprintVolumeUnitStateReader,
	environmentID string,
	revisionID string,
	expectedGeneration uint64,
) (projectionrecord.EnvironmentDesiredInput, projectionrecord.EnvironmentOwnedIdentities, error) {
	desired, found, err := authority.GetEnvironmentDesiredInputRevision(ctx, environmentID, revisionID)
	if err != nil {
		return projectionrecord.EnvironmentDesiredInput{}, projectionrecord.EnvironmentOwnedIdentities{}, err
	}
	if !found || desired.Record.EnvironmentID != environmentID || desired.Record.RevisionID != revisionID ||
		expectedGeneration != 0 && desired.Record.RenderGeneration != expectedGeneration {
		return projectionrecord.EnvironmentDesiredInput{}, projectionrecord.EnvironmentOwnedIdentities{}, errs.New(
			errs.KindStateConflict, "Blueprint Volume child desired input changed",
		)
	}
	identities, found, err := authority.GetEnvironmentOwnedIdentitiesRevision(ctx, environmentID, revisionID)
	if err != nil {
		return projectionrecord.EnvironmentDesiredInput{}, projectionrecord.EnvironmentOwnedIdentities{}, err
	}
	if !found || identities.Record.EnvironmentID != environmentID || identities.Record.RevisionID != revisionID ||
		identities.Record.RenderGeneration != desired.Record.RenderGeneration {
		return projectionrecord.EnvironmentDesiredInput{}, projectionrecord.EnvironmentOwnedIdentities{}, errs.New(
			errs.KindStateConflict, "Blueprint Volume child identity authority changed",
		)
	}
	return desired.Record, identities.Record, nil
}

func selectBlueprintVolumeIdentity(
	identities projectionrecord.EnvironmentOwnedIdentities,
	volumeID string,
) (projectionrecord.OwnedIdentity, error) {
	var selected projectionrecord.OwnedIdentity
	for _, identity := range identities.Volumes {
		if identity.ID != volumeID {
			continue
		}
		if selected.ID != "" {
			return projectionrecord.OwnedIdentity{}, errs.New(
				errs.KindInternal, "Blueprint Volume child identity is duplicated",
			)
		}
		selected = identity
	}
	if selected.ID == "" {
		return projectionrecord.OwnedIdentity{}, errs.New(
			errs.KindStateConflict, "Blueprint Volume child identity is absent",
		)
	}
	return selected, nil
}

func blueprintVolumeUnitStepIDs(task etcd.TaskRecord, create bool) ([]string, error) {
	planTime, err := ids.Timestamp(ids.KindPlan, task.PlanID)
	if err != nil {
		return nil, err
	}
	ensure := ids.DeriveAt(ids.KindStep, planTime, task.PlanID, "blueprint-volume-unit-ensure:"+task.Target)
	if !create {
		return []string{ensure}, nil
	}
	directories := ids.DeriveAt(ids.KindStep, planTime, task.PlanID, "blueprint-volume-unit-directories:"+task.Target)
	return []string{directories, ensure}, nil
}

func blueprintVolumeUnitArtifactID(task etcd.TaskRecord) (string, error) {
	planTime, err := ids.Timestamp(ids.KindPlan, task.PlanID)
	if err != nil {
		return "", err
	}
	return ids.DeriveAt(ids.KindConfig, planTime, task.PlanID, "blueprint-volume-unit:"+task.Target), nil
}

func blueprintVolumeUnitStepsMatch(actual []taskjournal.TaskStepRecord, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	for index, step := range actual {
		if step.Kind != taskjournal.TaskStepOperation || step.ID != expected[index] {
			return false
		}
	}
	return true
}

func decodeBlueprintVolumeIntent(value string) ([]byte, error) {
	if len(value) != sha256.Size*2 {
		return nil, errs.New(errs.KindInternal, "durable Blueprint Volume intent digest is invalid")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || hex.EncodeToString(decoded) != value {
		return nil, errs.New(errs.KindInternal, "durable Blueprint Volume intent digest is invalid")
	}
	return decoded, nil
}
