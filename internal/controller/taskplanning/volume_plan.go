package taskplanning

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"math"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// These are the only Volume-specific Task parameters. The generic desired
// revision and candidate artifact parameters remain the ADR 0051 publication
// identity; a baseline revision is needed only while removing the old leaf.
const (
	VolumeTaskActionParam           = "volume_action"
	VolumeTaskComposeKeyParam       = "volume_compose_key"
	VolumeTaskIntentSHA256Param     = "volume_intent_sha256"
	VolumeTaskBaselineRevisionParam = "volume_baseline_revision_id"

	VolumeTaskActionAdd    = "add"
	VolumeTaskActionEdit   = "edit"
	VolumeTaskActionRemove = "remove"
)

func (resolver *TaskPlanResolver) resolveVolumePlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	params := task.Params
	if task.Type == etcd.TaskRemove {
		var err error
		params, err = resolver.volumeRemovalPlanParams(ctx, task)
		if err != nil {
			return nil, err
		}
	}
	action := params[VolumeTaskActionParam]
	expectedParams := 7
	if action == VolumeTaskActionRemove {
		expectedParams++
	}
	if resolver == nil || resolver.blueprints == nil || task.Executor != etcd.TaskExecutorAgent ||
		ids.Validate(ids.KindVolume, task.Target) != nil || task.TimeoutSeconds <= 0 ||
		task.TimeoutSeconds > math.MaxUint32 || task.RenderGeneration <= 0 || len(params) != expectedParams ||
		params[etcd.TaskResourceKindParam] != etcd.TaskResourceVolume ||
		ids.Validate(ids.KindEnvironment, params[etcd.TaskMaterializationEnvironmentParam]) != nil ||
		params[etcd.TaskMaterializationEnvironmentParam] == "" ||
		ids.Validate(ids.KindTask, params[etcd.EnvironmentDesiredRevisionParam]) != nil ||
		ids.Validate(ids.KindConfig, params[etcd.TaskComposeArtifactParam]) != nil ||
		!composerender.ValidVolumeArtifactKey(params[VolumeTaskComposeKeyParam]) {
		return nil, errs.New(errs.KindInternal, "durable Volume Task shape is invalid")
	}
	if (action != VolumeTaskActionAdd && action != VolumeTaskActionEdit && action != VolumeTaskActionRemove) ||
		(action == VolumeTaskActionAdd && task.Type != etcd.TaskCreate) ||
		(action == VolumeTaskActionEdit && task.Type != etcd.TaskUpdate) ||
		(action == VolumeTaskActionRemove && task.Type != etcd.TaskRemove) {
		return nil, errs.New(errs.KindInternal, "durable Volume Task action is invalid")
	}
	intentDigest, err := decodeVolumeTaskDigest(params[VolumeTaskIntentSHA256Param])
	if err != nil {
		return nil, err
	}
	environmentID := params[etcd.TaskMaterializationEnvironmentParam]
	candidateRevisionID := params[etcd.EnvironmentDesiredRevisionParam]
	candidateArtifactID := params[etcd.TaskComposeArtifactParam]
	candidateProjection, found, err := resolver.blueprints.GetEnvironmentComposeProjectionRevision(
		ctx, environmentID, candidateRevisionID,
	)
	if err != nil {
		return nil, err
	}
	if !found || candidateProjection.Record.EnvironmentID != environmentID ||
		candidateProjection.Record.RevisionID != candidateRevisionID ||
		candidateProjection.Record.RenderGeneration != uint64(task.RenderGeneration) {
		return nil, errs.New(errs.KindInternal, "durable Volume candidate projection is stale")
	}
	candidateArtifact, err := decodeVolumePlanArtifact(
		candidateProjection.Record.ComposeArtifact,
		environmentID,
		candidateArtifactID,
	)
	if err != nil {
		return nil, err
	}
	environment, err := resolver.blueprints.GetEnvironment(ctx, environmentID)
	if err != nil {
		return nil, err
	}
	project, err := resolver.blueprints.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return nil, err
	}
	tenant, err := resolver.blueprints.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return nil, err
	}
	if environment.Record.ID != environmentID ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady ||
		project.Record.ID != environment.Record.ProjectID ||
		project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		tenant.Record.ID != project.Record.TenantID ||
		candidateArtifact.AuthorizedVolumeDir != environment.Record.VolumeDir {
		return nil, errs.New(errs.KindInternal, "durable Volume Environment hierarchy is invalid")
	}

	key := params[VolumeTaskComposeKeyParam]
	if action == VolumeTaskActionAdd {
		if task.Steps == nil || len(task.Steps) != 2 || !volumeTaskStepIDsValid(task.Steps) ||
			!volumeArtifactHasExactVolume(candidateArtifact, task.Target, key) {
			return nil, errs.New(errs.KindInternal, "durable Volume add procedure is invalid")
		}
		steps := []*agentpb.ExecutionStep{
			{
				StepId:         task.Steps[0].ID,
				TimeoutSeconds: uint32(task.TimeoutSeconds),
				Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{
					ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{
						ArtifactId: candidateArtifactID, VolumeIds: []string{task.Target}, IntentSha256: intentDigest,
					},
				},
			},
			{
				StepId:         task.Steps[1].ID,
				TimeoutSeconds: uint32(task.TimeoutSeconds),
				Payload: &agentpb.ExecutionStep_ManagedVolumeEnsure{
					ManagedVolumeEnsure: &agentpb.ManagedVolumeEnsure{
						ArtifactId: candidateArtifactID,
						VolumeId:   task.Target,
					},
				},
			},
		}
		return taskplan.Build(taskplan.BuildInput{
			VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
			Operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE, TargetID: task.Target,
			Artifacts: []*agentpb.ComposeArtifact{candidateArtifact}, Steps: steps,
		})
	}
	if action == VolumeTaskActionEdit {
		if task.Steps == nil || len(task.Steps) != 1 || !volumeTaskStepIDsValid(task.Steps) ||
			!volumeArtifactHasExactVolume(candidateArtifact, task.Target, key) {
			return nil, errs.New(errs.KindInternal, "durable Volume edit procedure is invalid")
		}
		return taskplan.Build(taskplan.BuildInput{
			VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
			Operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE, TargetID: task.Target,
			Artifacts: []*agentpb.ComposeArtifact{candidateArtifact}, Steps: []*agentpb.ExecutionStep{{
				StepId: task.Steps[0].ID, TimeoutSeconds: uint32(task.TimeoutSeconds), Payload: &agentpb.ExecutionStep_ManagedVolumeEnsure{
					ManagedVolumeEnsure: &agentpb.ManagedVolumeEnsure{
						ArtifactId:      candidateArtifactID,
						VolumeId:        task.Target,
						RequireExisting: true,
					},
				},
			}},
		})
	}

	baselineRevisionID := params[VolumeTaskBaselineRevisionParam]
	if ids.Validate(ids.KindTask, baselineRevisionID) != nil || len(task.Steps) < 2 || len(task.Steps) > 3 ||
		!volumeTaskStepIDsValid(task.Steps) {
		return nil, errs.New(errs.KindInternal, "durable Volume removal procedure is invalid")
	}
	baselineProjection, found, err := resolver.blueprints.GetEnvironmentComposeProjectionRevision(
		ctx, environmentID, baselineRevisionID,
	)
	if err != nil {
		return nil, err
	}
	if !found || baselineProjection.Record.EnvironmentID != environmentID ||
		baselineProjection.Record.RevisionID != baselineRevisionID ||
		baselineProjection.Record.RenderGeneration+1 != candidateProjection.Record.RenderGeneration {
		return nil, errs.New(errs.KindInternal, "durable Volume baseline projection is stale")
	}
	baselineArtifact, err := decodeVolumePlanArtifact(
		baselineProjection.Record.ComposeArtifact, environmentID, "",
	)
	if err != nil {
		return nil, err
	}
	if baselineArtifact.ArtifactId == candidateArtifact.ArtifactId ||
		!volumeArtifactHasExactVolume(baselineArtifact, task.Target, key) ||
		volumeArtifactContainsVolume(candidateArtifact, task.Target, key) {
		return nil, errs.New(errs.KindInternal, "durable Volume removal artifacts are inconsistent")
	}
	for _, volume := range candidateArtifact.Volumes {
		if volume != nil && (volume.VolumeId == task.Target || volume.ComposeName == key) {
			return nil, errs.New(errs.KindInternal, "durable Volume removal retained its identity")
		}
	}
	expectedCandidate, err := composerender.MutateEnvironmentVolumeArtifact(baselineArtifact, composerender.VolumeArtifactMutation{
		Action: composerender.VolumeArtifactRemove, VolumeID: task.Target, Key: key, ArtifactID: candidateArtifactID,
		PlanID: task.PlanID, TenantID: tenant.Record.ID, ProjectID: project.Record.ID,
		RenderGeneration: uint64(task.RenderGeneration),
	})
	if err != nil {
		return nil, err
	}
	if !volumePlanArtifactsEqual(expectedCandidate, candidateArtifact) {
		return nil, errs.New(errs.KindStateConflict, "durable Volume candidate artifact changed")
	}
	consumers, err := volumePlanConsumerIDs(baselineProjection.Record, task.Target)
	if err != nil {
		return nil, err
	}
	expectedStepCount := 2
	if len(consumers) != 0 {
		expectedStepCount = 3
	}
	if len(task.Steps) != expectedStepCount {
		return nil, errs.New(errs.KindInternal, "durable Volume removal step count is invalid")
	}
	for _, mount := range candidateProjection.Record.VolumeMounts {
		if mount.VolumeID == task.Target {
			return nil, errs.New(errs.KindInternal, "durable Volume candidate still has a consumer")
		}
	}
	cleanupArtifactID, err := StableVolumeCleanupArtifactID(candidateArtifactID)
	if err != nil {
		return nil, err
	}
	cleanupArtifact, err := rebindVolumeArtifactForPlan(
		baselineArtifact, cleanupArtifactID, task.PlanID, uint64(task.RenderGeneration),
	)
	if err != nil {
		return nil, err
	}
	steps := make([]*agentpb.ExecutionStep, 0, len(task.Steps))
	stepIndex := 0
	if len(consumers) != 0 {
		steps = append(steps, &agentpb.ExecutionStep{
			StepId: task.Steps[stepIndex].ID, TimeoutSeconds: uint32(task.TimeoutSeconds), Payload: &agentpb.ExecutionStep_ComposeApply{
				ComposeApply: &agentpb.ComposeApply{
					ArtifactId:     candidateArtifactID,
					ServiceIds:     consumers,
					NoDependencies: true,
				},
			},
		})
		stepIndex++
	}
	steps = append(
		steps,
		&agentpb.ExecutionStep{
			StepId:         task.Steps[stepIndex].ID,
			TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_ManagedVolumeRemove{
				ManagedVolumeRemove: &agentpb.ManagedVolumeRemove{
					VolumeId:   task.Target,
					DockerName: "gp_vol_" + strings.ToLower(task.Target),
				},
			},
		},
		&agentpb.ExecutionStep{
			StepId:         task.Steps[stepIndex+1].ID,
			TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoryRemove{
				ManagedVolumeDirectoryRemove: &agentpb.ManagedVolumeDirectoryRemove{
					ArtifactId: cleanupArtifact.ArtifactId, VolumeId: task.Target, ComposeKey: key,
					IntentSha256: intentDigest,
				},
			},
		},
	)
	return taskplan.Build(taskplan.BuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
		Operation: agentpb.PlanOperation_PLAN_OPERATION_REMOVE, TargetID: task.Target,
		Artifacts: []*agentpb.ComposeArtifact{candidateArtifact, cleanupArtifact}, Steps: steps,
	})
}

// StableVolumeCleanupArtifactID reserves a second deterministic artifact
// identity for the historical baseline artifact carried by a removal plan.
// The identity stays a valid cfg_ ULID and cannot collide with the candidate.
func StableVolumeCleanupArtifactID(candidateArtifactID string) (string, error) {
	if ids.Validate(ids.KindConfig, candidateArtifactID) != nil {
		return "", errs.New(errs.KindInternal, "Volume candidate artifact identity is invalid")
	}
	const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	last := candidateArtifactID[len(candidateArtifactID)-1]
	index := strings.IndexByte(crockford, last)
	if index < 0 {
		return "", errs.New(errs.KindInternal, "Volume candidate artifact identity is invalid")
	}
	variant := []byte(candidateArtifactID)
	variant[len(variant)-1] = crockford[(index+1)%len(crockford)]
	if string(variant) == candidateArtifactID {
		return "", errs.New(errs.KindInternal, "Volume cleanup artifact identity collides with candidate")
	}
	return string(variant), nil
}

func decodeVolumeTaskDigest(value string) ([]byte, error) {
	if len(value) != sha256.Size*2 {
		return nil, errs.New(errs.KindInternal, "durable Volume intent digest is invalid")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || hex.EncodeToString(decoded) != value {
		return nil, errs.New(errs.KindInternal, "durable Volume intent digest is invalid")
	}
	return decoded, nil
}

func decodeVolumePlanArtifact(value []byte, environmentID, artifactID string) (*agentpb.ComposeArtifact, error) {
	artifact := &agentpb.ComposeArtifact{}
	if len(value) == 0 || (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(value, artifact) != nil ||
		(artifactID != "" && artifact.ArtifactId != artifactID) ||
		artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.OwnerId != environmentID {
		return nil, errs.New(errs.KindInternal, "durable Volume Compose artifact is corrupt")
	}
	return proto.Clone(artifact).(*agentpb.ComposeArtifact), nil
}

func volumeArtifactHasExactVolume(artifact *agentpb.ComposeArtifact, volumeID, key string) bool {
	for _, volume := range artifact.GetVolumes() {
		if volume != nil && (volume.VolumeId == volumeID || volume.ComposeName == key) {
			return volume.VolumeId == volumeID && volume.ComposeName == key &&
				volume.DockerName == "gp_vol_"+strings.ToLower(volumeID)
		}
	}
	return false
}

func volumeArtifactContainsVolume(artifact *agentpb.ComposeArtifact, volumeID, key string) bool {
	for _, volume := range artifact.GetVolumes() {
		if volume != nil && (volume.VolumeId == volumeID || volume.ComposeName == key) {
			return true
		}
	}
	return false
}

func volumeTaskStepIDsValid(steps []etcd.TaskStepRecord) bool {
	seen := make(map[string]struct{}, len(steps))
	for _, step := range steps {
		if ids.Validate(ids.KindStep, step.ID) != nil {
			return false
		}
		if _, duplicate := seen[step.ID]; duplicate {
			return false
		}
		seen[step.ID] = struct{}{}
	}
	return true
}

func volumePlanConsumerIDs(projection projectionrecord.EnvironmentComposeProjection, volumeID string) ([]string, error) {
	seen := make(map[string]struct{})
	for _, mount := range projection.VolumeMounts {
		if mount.VolumeID != volumeID {
			continue
		}
		if ids.Validate(ids.KindService, mount.ServiceID) != nil {
			return nil, errs.New(errs.KindInternal, "durable Volume consumer identity is invalid")
		}
		seen[mount.ServiceID] = struct{}{}
	}
	result := make([]string, 0, len(seen))
	for serviceID := range seen {
		result = append(result, serviceID)
	}
	sort.Strings(result)
	return result, nil
}

func volumePlanArtifactsEqual(left, right *agentpb.ComposeArtifact) bool {
	encodedLeft, leftErr := (proto.MarshalOptions{Deterministic: true}).Marshal(left)
	encodedRight, rightErr := (proto.MarshalOptions{Deterministic: true}).Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(encodedLeft, encodedRight)
}

func rebindVolumeArtifactForPlan(
	source *agentpb.ComposeArtifact,
	artifactID string,
	planID string,
	renderGeneration uint64,
) (*agentpb.ComposeArtifact, error) {
	if source == nil || ids.Validate(ids.KindConfig, artifactID) != nil || ids.Validate(ids.KindPlan, planID) != nil ||
		renderGeneration == 0 {
		return nil, errs.New(errs.KindInternal, "historical Volume artifact identity is invalid")
	}
	owned := proto.Clone(source).(*agentpb.ComposeArtifact)
	owned.ArtifactId = artifactID
	if err := composerender.ValidateRuntimeServiceOwnership(owned); err != nil {
		return nil, err
	}
	return owned, nil
}
