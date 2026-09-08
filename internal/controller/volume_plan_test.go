package controller

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

type volumePlanReader struct {
	tenant      etcd.TenantRecord
	project     etcd.ProjectRecord
	environment etcd.EnvironmentRecord
	projections map[string]etcd.EnvironmentComposeProjection
}

func (reader *volumePlanReader) GetTenant(context.Context, string) (etcd.Versioned[etcd.TenantRecord], error) {
	return etcd.Versioned[etcd.TenantRecord]{Record: reader.tenant}, nil
}

func (reader *volumePlanReader) GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error) {
	return etcd.Versioned[etcd.ProjectRecord]{Record: reader.project}, nil
}

func (reader *volumePlanReader) GetEnvironment(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return etcd.Versioned[etcd.EnvironmentRecord]{Record: reader.environment}, nil
}

func (reader *volumePlanReader) GetEnvironmentBlueprintRevision(
	context.Context,
	string,
	string,
) (etcd.Versioned[etcd.EnvironmentBlueprintRevision], bool, error) {
	return etcd.Versioned[etcd.EnvironmentBlueprintRevision]{}, false, nil
}

func (reader *volumePlanReader) GetEnvironmentComposeProjection(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return etcd.Versioned[etcd.EnvironmentComposeProjection]{}, false, nil
}

func (reader *volumePlanReader) GetEnvironmentComposeProjectionRevision(
	_ context.Context,
	environmentID string,
	revisionID string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	projection, found := reader.projections[revisionID]
	if !found || projection.EnvironmentID != environmentID {
		return etcd.Versioned[etcd.EnvironmentComposeProjection]{}, false, nil
	}
	return etcd.Versioned[etcd.EnvironmentComposeProjection]{
		Record:       projection,
		Revision:     1,
		ReadRevision: 1,
	}, true, nil
}

func TestResolveVolumeAddPlanReplaysStableHash(t *testing.T) {
	t.Parallel()
	state := newVolumePlanState(t, false)
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", state.reader, nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	first, err := resolver.ResolveExecutionPlan(context.Background(), state.addTask)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(add) error = %v", err)
	}
	second, err := resolver.ResolveExecutionPlan(context.Background(), state.addTask)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(add replay) error = %v", err)
	}
	ensure := first.Steps[0].GetManagedVolumeDirectoriesEnsure()
	if ensure == nil || len(ensure.VolumeIds) != 1 || ensure.VolumeIds[0] != state.volumeID ||
		!bytes.Equal(ensure.IntentSha256, state.intentDigest) ||
		first.Steps[1].GetComposeApply() == nil || !first.Steps[1].GetComposeApply().FullReconcile ||
		!bytes.Equal(first.PlanHash, second.PlanHash) {
		t.Fatalf("resolved add plans = %#v / %#v", first, second)
	}
}

func TestResolveVolumeRemovePlanDetachesConsumersBeforeCleanup(t *testing.T) {
	t.Parallel()
	state := newVolumePlanState(t, true)
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", state.reader, nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	first, err := resolver.ResolveExecutionPlan(context.Background(), state.removeTask)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(remove) error = %v", err)
	}
	second, err := resolver.ResolveExecutionPlan(context.Background(), state.removeTask)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(remove replay) error = %v", err)
	}
	apply := first.Steps[0].GetComposeApply()
	dockerRemove := first.Steps[1].GetManagedVolumeRemove()
	directoryRemove := first.Steps[2].GetManagedVolumeDirectoryRemove()
	cleanupArtifactID, err := StableVolumeCleanupArtifactID(state.candidateArtifactID)
	if err != nil {
		t.Fatalf("StableVolumeCleanupArtifactID() error = %v", err)
	}
	if apply == nil || apply.FullReconcile || len(apply.ServiceIds) != 1 || apply.ServiceIds[0] != state.serviceID ||
		dockerRemove == nil || dockerRemove.VolumeId != state.volumeID ||
		directoryRemove == nil || directoryRemove.ArtifactId != cleanupArtifactID ||
		directoryRemove.ComposeKey != state.volumeKey || !bytes.Equal(directoryRemove.IntentSha256, state.intentDigest) ||
		len(directoryRemove.Cursor) != 0 || !bytes.Equal(first.PlanHash, second.PlanHash) {
		t.Fatalf("resolved remove plans = %#v / %#v", first, second)
	}
}

func TestResolveVolumeEditRejectsControllerExecution(t *testing.T) {
	t.Parallel()
	state := newVolumePlanState(t, false)
	state.editTask.Executor = etcd.TaskExecutorController
	state.editTask.Type = etcd.TaskUpdate
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", state.reader, nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	_, err = resolver.ResolveExecutionPlan(context.Background(), state.editTask)
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("ResolveExecutionPlan(controller edit) error = %v", err)
	}
}

type volumePlanState struct {
	reader              *volumePlanReader
	addTask             etcd.TaskRecord
	editTask            etcd.TaskRecord
	removeTask          etcd.TaskRecord
	volumeID            string
	serviceID           string
	volumeKey           string
	intentDigest        []byte
	baselineArtifactID  string
	candidateArtifactID string
}

func newVolumePlanState(t *testing.T, removal bool) volumePlanState {
	t.Helper()
	at := time.Date(2026, 8, 26, 14, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, at, 1)
	projectID := ids.NewAt(ids.KindProject, at, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 3)
	volumeID := ids.NewAt(ids.KindVolume, at, 4)
	serviceID := ids.NewAt(ids.KindService, at, 5)
	baselineRevisionID := ids.NewAt(ids.KindTask, at, 6)
	candidateRevisionID := ids.NewAt(ids.KindTask, at, 7)
	addTaskID := ids.NewAt(ids.KindTask, at, 8)
	removeTaskID := ids.NewAt(ids.KindTask, at, 9)
	addPlanID := ids.NewAt(ids.KindPlan, at, 10)
	removePlanID := ids.NewAt(ids.KindPlan, at, 11)
	baselineArtifactID := ids.NewAt(ids.KindConfig, at, 12)
	candidateArtifactID := ids.NewAt(ids.KindConfig, at, 13)
	addArtifactID := ids.NewAt(ids.KindConfig, at, 14)
	serviceStepID := ids.NewAt(ids.KindStep, at, 15)
	dockerStepID := ids.NewAt(ids.KindStep, at, 16)
	directoryStepID := ids.NewAt(ids.KindStep, at, 17)
	volumeKey := "app-data"
	volumeDir := "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environmentID
	baseProject := &types.Project{
		Services: types.Services{"api": {Image: "example/api:1"}},
	}
	if removal {
		baseProject.Services["api"] = types.ServiceConfig{
			Image:   "example/api:1",
			Volumes: []types.ServiceVolumeConfig{{Type: types.VolumeTypeVolume, Source: volumeKey, Target: "/data"}},
		}
		baseProject.Volumes = types.Volumes{volumeKey: types.VolumeConfig{}}
	}
	identities := ComposeIdentitySnapshot{Services: []ComposeResourceIdentity{{ID: serviceID, Name: "api"}}}
	if removal {
		identities.Volumes = []ComposeResourceIdentity{{ID: volumeID, Name: volumeKey}}
	}
	baseArtifact, err := RenderCompose(ComposeRenderInput{
		Project: baseProject, ArtifactID: baselineArtifactID,
		ProjectOwnerKind: ComposeProjectOwnerTenant, TenantID: tenantID, ProjectID: projectID,
		EnvironmentID: environmentID, PlanID: ids.NewAt(ids.KindPlan, at, 19), RenderGeneration: 1,
		AuthorizedVolumeDir: volumeDir, Identities: identities,
	})
	if err != nil {
		t.Fatalf("RenderCompose(base) error = %v", err)
	}
	var candidateArtifact *agentpb.ComposeArtifact
	if removal {
		candidateArtifact, err = MutateEnvironmentVolumeArtifact(baseArtifact, VolumeArtifactMutation{
			Action: VolumeArtifactRemove, VolumeID: volumeID, Key: volumeKey, ArtifactID: candidateArtifactID,
			PlanID: removePlanID, TenantID: tenantID, ProjectID: projectID, RenderGeneration: 2,
		})
	} else {
		candidateArtifact, err = MutateEnvironmentVolumeArtifact(baseArtifact, VolumeArtifactMutation{
			Action: VolumeArtifactAdd, VolumeID: volumeID, Key: volumeKey, ArtifactID: addArtifactID,
			PlanID: addPlanID, TenantID: tenantID, ProjectID: projectID, RenderGeneration: 2,
		})
	}
	if err != nil {
		t.Fatalf("MutateEnvironmentVolumeArtifact(candidate) error = %v", err)
	}
	baselineValue, err := (proto.MarshalOptions{Deterministic: true}).Marshal(baseArtifact)
	if err != nil {
		t.Fatalf("marshal baseline artifact: %v", err)
	}
	candidateValue, err := (proto.MarshalOptions{Deterministic: true}).Marshal(candidateArtifact)
	if err != nil {
		t.Fatalf("marshal candidate artifact: %v", err)
	}
	intentDigest := bytes.Repeat([]byte{0xab}, 32)
	reader := &volumePlanReader{
		tenant: tenantRecord(
			tenantID,
		), project: etcd.ProjectRecord{ID: projectID, TenantID: tenantID, Kind: etcd.ProjectKindTenant},
		environment: etcd.EnvironmentRecord{
			ID:                environmentID,
			ProjectID:         projectID,
			VolumeDir:         volumeDir,
			ProvisioningState: etcd.EnvironmentProvisioningReady,
		},
		projections: map[string]etcd.EnvironmentComposeProjection{
			candidateRevisionID: {
				EnvironmentID: environmentID, RevisionID: candidateRevisionID, RenderGeneration: 2, ComposeArtifact: candidateValue,
			},
			baselineRevisionID: {
				EnvironmentID: environmentID, RevisionID: baselineRevisionID, RenderGeneration: 1, ComposeArtifact: baselineValue,
				VolumeMounts: func() []etcd.EnvironmentServiceVolumeMount {
					if !removal {
						return nil
					}
					return []etcd.EnvironmentServiceVolumeMount{
						{ServiceID: serviceID, VolumeID: volumeID, Target: "/data"},
					}
				}(),
			},
		},
	}
	addTask := volumeTask(
		addTaskID,
		addPlanID,
		etcd.TaskCreate,
		volumeID,
		environmentID,
		candidateRevisionID,
		addArtifactID,
		volumeKey,
		intentDigest,
		"",
		nil,
	)
	editTask := volumeTask(
		addTaskID,
		addPlanID,
		etcd.TaskUpdate,
		volumeID,
		environmentID,
		candidateRevisionID,
		addArtifactID,
		volumeKey,
		intentDigest,
		"",
		nil,
	)
	removeTask := volumeTask(
		removeTaskID,
		removePlanID,
		etcd.TaskRemove,
		volumeID,
		environmentID,
		candidateRevisionID,
		candidateArtifactID,
		volumeKey,
		intentDigest,
		baselineRevisionID,
		[]etcd.TaskStepRecord{
			{Kind: etcd.TaskStepOperation, ID: serviceStepID},
			{Kind: etcd.TaskStepOperation, ID: dockerStepID},
			{Kind: etcd.TaskStepOperation, ID: directoryStepID},
		},
	)
	addTask.Steps = []etcd.TaskStepRecord{
		{Kind: etcd.TaskStepOperation, ID: serviceStepID},
		{Kind: etcd.TaskStepOperation, ID: dockerStepID},
	}
	return volumePlanState{reader: reader, addTask: addTask, editTask: editTask, removeTask: removeTask,
		volumeID: volumeID, serviceID: serviceID, volumeKey: volumeKey, intentDigest: intentDigest,
		baselineArtifactID: baselineArtifactID, candidateArtifactID: candidateArtifactID}
}

func volumeTask(
	taskID, planID string,
	taskType etcd.TaskType,
	volumeID, environmentID, revisionID, artifactID, volumeKey string,
	intentDigest []byte,
	baselineRevisionID string,
	steps []etcd.TaskStepRecord,
) etcd.TaskRecord {
	params := map[string]string{
		etcd.TaskResourceKindParam:               etcd.TaskResourceVolume,
		etcd.TaskMaterializationEnvironmentParam: environmentID,
		etcd.EnvironmentDesiredRevisionParam:     revisionID,
		etcd.TaskComposeArtifactParam:            artifactID,
		VolumeTaskActionParam: func() string {
			if taskType == etcd.TaskRemove {
				return VolumeTaskActionRemove
			}
			if taskType == etcd.TaskUpdate {
				return VolumeTaskActionEdit
			}
			return VolumeTaskActionAdd
		}(),
		VolumeTaskComposeKeyParam:   volumeKey,
		VolumeTaskIntentSHA256Param: hex.EncodeToString(intentDigest),
	}
	if baselineRevisionID != "" {
		params[VolumeTaskBaselineRevisionParam] = baselineRevisionID
	}
	return etcd.TaskRecord{ID: taskID, OperationID: ids.NewAt(ids.KindOperation, time.Unix(0, 0).UTC(), 1),
		Executor: etcd.TaskExecutorAgent, PlanID: planID, Type: taskType, Target: volumeID, Params: params,
		Steps: steps, RenderGeneration: 2, TimeoutSeconds: 120, Status: etcd.TaskStatusPending}
}

func tenantRecord(id string) etcd.TenantRecord {
	return etcd.TenantRecord{ID: id, Slug: "tenant", Name: "Tenant"}
}
