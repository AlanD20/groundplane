package taskplanning

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

type volumePlanReader struct {
	tenant      testhierarchy.TenantRecord
	project     testhierarchy.ProjectRecord
	environment testhierarchy.EnvironmentRecord
	projections map[string]testenvironmentprojection.EnvironmentComposeProjection
	manifest    removalrecord.EvidenceManifest
}

func (reader *volumePlanReader) Manifest(context.Context, string) (removalrecord.EvidenceManifest, bool, error) {
	return reader.manifest, true, nil
}

func (reader *volumePlanReader) GetTenant(
	context.Context,
	string,
) (testkeyvalue.Versioned[testhierarchy.TenantRecord], error) {
	return testkeyvalue.Versioned[testhierarchy.TenantRecord]{Record: reader.tenant}, nil
}

func (reader *volumePlanReader) GetProject(
	context.Context,
	string,
) (testkeyvalue.Versioned[testhierarchy.ProjectRecord], error) {
	return testkeyvalue.Versioned[testhierarchy.ProjectRecord]{Record: reader.project}, nil
}

func (reader *volumePlanReader) GetEnvironment(
	context.Context, string,

) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	return testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{Record: reader.environment}, nil
}

func (reader *volumePlanReader) GetEnvironmentBlueprintRevision(
	context.Context, string, string,

) (testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintRevision], bool, error) {
	return testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintRevision]{}, false, nil
}

func (reader *volumePlanReader) GetEnvironmentComposeProjection(
	context.Context, string,

) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{}, false, nil
}

func (reader *volumePlanReader) GetEnvironmentComposeProjectionRevision(
	_ context.Context,
	environmentID string,
	revisionID string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	projection, found := reader.projections[revisionID]
	if !found || projection.EnvironmentID != environmentID {
		return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{}, false, nil
	}
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
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
		first.Steps[1].GetManagedVolumeEnsure() == nil || first.Steps[1].GetManagedVolumeEnsure().RequireExisting ||
		!bytes.Equal(first.PlanHash, second.PlanHash) {
		t.Fatalf("resolved add plans = %#v / %#v", first, second)
	}
}

// Rationale: slug edits prove existing ownership without a full Compose apply,
// which would restart independently serving workloads and create missing data.
func TestResolveVolumeEditPlanOnlyVerifiesExistingVolume(t *testing.T) {
	state := newVolumePlanState(t, false)
	state.editTask.Steps = state.addTask.Steps[:1]
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", state.reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := resolver.ResolveExecutionPlan(t.Context(), state.editTask)
	if err != nil {
		t.Fatal(err)
	}
	verify := plan.Steps[0].GetManagedVolumeEnsure()
	if verify == nil || !verify.RequireExisting || verify.VolumeId != state.volumeID {
		t.Fatal("slug edit can mutate runtime")
	}
}

// Rationale: detaching a Volume must not start dependencies of its consumers.
func TestResolveVolumeRemovePlanDetachesConsumersBeforeCleanup(t *testing.T) {
	t.Parallel()
	state := newVolumePlanState(t, true)
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", state.reader, nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	if err := resolver.EnableVolumeRemovalPlans(state.reader); err != nil {
		t.Fatal(err)
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
	if apply == nil || apply.FullReconcile || !apply.NoDependencies || apply.ForceRecreate ||
		len(apply.ServiceIds) != 1 || apply.ServiceIds[0] != state.serviceID ||
		dockerRemove == nil || dockerRemove.VolumeId != state.volumeID ||
		directoryRemove == nil || directoryRemove.ArtifactId != cleanupArtifactID ||
		directoryRemove.ComposeKey != state.volumeKey || !bytes.Equal(directoryRemove.IntentSha256, state.intentDigest) ||
		!bytes.Equal(first.PlanHash, second.PlanHash) {
		t.Fatalf("resolved remove plans = %#v / %#v", first, second)
	}
}

func TestResolveVolumeEditRejectsControllerExecution(t *testing.T) {
	t.Parallel()
	state := newVolumePlanState(t, false)
	state.editTask.Executor = testtaskjournal.TaskExecutorController
	state.editTask.Type = testtaskjournal.TaskUpdate
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
	candidateArtifactID := ids.NewAt(ids.KindConfig, at, 9)
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
	identities := testcomposeidentity.Snapshot{Services: []testcomposeidentity.Resource{{ID: serviceID, Name: "api"}}}
	if removal {
		identities.Volumes = []testcomposeidentity.Resource{{ID: volumeID, Name: volumeKey}}
	}
	baseArtifact, err := testcomposerender.RenderCompose(testcomposerender.ComposeRenderInput{
		Project: baseProject, ArtifactID: baselineArtifactID,
		ProjectOwnerKind: testcomposerender.ComposeProjectOwnerTenant, TenantID: tenantID, ProjectID: projectID,
		EnvironmentID: environmentID, PlanID: ids.NewAt(ids.KindPlan, at, 19), RenderGeneration: 1,
		AuthorizedVolumeDir: volumeDir, Identities: identities,
	})
	if err != nil {
		t.Fatalf("RenderCompose(base) error = %v", err)
	}
	var candidateArtifact *agentpb.ComposeArtifact
	if removal {
		candidateArtifact, err = testcomposerender.MutateEnvironmentVolumeArtifact(
			baseArtifact,
			testcomposerender.VolumeArtifactMutation{
				Action: testcomposerender.VolumeArtifactRemove, VolumeID: volumeID, Key: volumeKey, ArtifactID: candidateArtifactID,
				PlanID: removePlanID, TenantID: tenantID, ProjectID: projectID, RenderGeneration: 2,
			},
		)
	} else {
		candidateArtifact, err = testcomposerender.MutateEnvironmentVolumeArtifact(baseArtifact, testcomposerender.VolumeArtifactMutation{
			Action: testcomposerender.VolumeArtifactAdd, VolumeID: volumeID, Key: volumeKey, ArtifactID: addArtifactID,
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
		), project: testhierarchy.ProjectRecord{ID: projectID, TenantID: tenantID, Kind: testhierarchy.ProjectKindTenant},
		environment: testhierarchy.EnvironmentRecord{
			ID:                environmentID,
			ProjectID:         projectID,
			VolumeDir:         volumeDir,
			ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
		},
		projections: map[string]testenvironmentprojection.EnvironmentComposeProjection{
			candidateRevisionID: {
				EnvironmentID: environmentID, RevisionID: candidateRevisionID, RenderGeneration: 2, ComposeArtifact: candidateValue,
			},
			baselineRevisionID: {
				EnvironmentID: environmentID, RevisionID: baselineRevisionID, RenderGeneration: 1, ComposeArtifact: baselineValue,
				VolumeMounts: func() []testenvironmentprojection.EnvironmentServiceVolumeMount {
					if !removal {
						return nil
					}
					return []testenvironmentprojection.EnvironmentServiceVolumeMount{
						{ServiceID: serviceID, VolumeID: volumeID, Target: "/data"},
					}
				}(),
			},
		},
	}
	addTask := volumeTask(
		addTaskID,
		addPlanID, testtaskjournal.TaskCreate, volumeID,
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
		addPlanID, testtaskjournal.TaskUpdate, volumeID,
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
		removePlanID, testtaskjournal.TaskRemove, volumeID,
		environmentID,
		candidateRevisionID,
		candidateArtifactID,
		volumeKey,
		intentDigest,
		baselineRevisionID,
		[]testtaskjournal.TaskStepRecord{
			{Kind: testtaskjournal.TaskStepOperation, ID: serviceStepID},
			{Kind: testtaskjournal.TaskStepOperation, ID: dockerStepID},
			{Kind: testtaskjournal.TaskStepOperation, ID: directoryStepID},
		},
	)
	if removal {
		reader.manifest = removalrecord.EvidenceManifest{
			OperationID: removeTask.OperationID, EnvironmentID: environmentID, VolumeID: volumeID, Key: volumeKey,
			SourceRevisionID: baselineRevisionID, DesiredRevisionID: candidateRevisionID, ReadRevision: 1,
			ImpactSHA256: sha256.Sum256(
				[]byte("accepted plan fixture impact"),
			), OrderedSHA256: removalrecord.EmptyEvidenceDigest(),
		}
		value, err := removalrecord.EncodeEvidenceManifest(reader.manifest)
		if err != nil {
			t.Fatal(err)
		}
		removeTask.Params = etcd.EnvironmentVolumeRemovalTaskParams(removalrecord.Runtime{
			EnvironmentID: environmentID, DesiredRevisionID: candidateRevisionID, OriginTaskID: removeTask.ID,
			Key: volumeKey, ImpactSHA256: reader.manifest.ImpactSHA256,
			EvidenceManifestSHA256: sha256.Sum256(value), IntentSHA256: [sha256.Size]byte(intentDigest),
		}, 1)
		removeTask.TimeoutSeconds = removalrecord.TimeoutSeconds
	}
	addTask.Steps = []testtaskjournal.TaskStepRecord{
		{Kind: testtaskjournal.TaskStepOperation, ID: serviceStepID},
		{Kind: testtaskjournal.TaskStepOperation, ID: dockerStepID},
	}
	return volumePlanState{reader: reader, addTask: addTask, editTask: editTask, removeTask: removeTask,
		volumeID: volumeID, serviceID: serviceID, volumeKey: volumeKey, intentDigest: intentDigest,
		baselineArtifactID: baselineArtifactID, candidateArtifactID: candidateArtifactID}
}

func volumeTask(
	taskID, planID string,
	taskType testtaskjournal.TaskType,
	volumeID, environmentID, revisionID, artifactID, volumeKey string,
	intentDigest []byte,
	baselineRevisionID string,
	steps []testtaskjournal.TaskStepRecord,
) etcd.TaskRecord {
	params := map[string]string{
		testtaskjournal.TaskResourceKindParam:               testtaskjournal.TaskResourceVolume,
		testtaskjournal.TaskMaterializationEnvironmentParam: environmentID,
		testblueprints.EnvironmentDesiredRevisionParam:      revisionID,
		testtaskjournal.TaskComposeArtifactParam:            artifactID,
		VolumeTaskActionParam: func() string {
			if taskType == testtaskjournal.TaskRemove {
				return VolumeTaskActionRemove
			}
			if taskType == testtaskjournal.TaskUpdate {
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
		Executor: testtaskjournal.TaskExecutorAgent, PlanID: planID, Type: taskType, Target: volumeID, Params: params,
		Steps: steps, RenderGeneration: 2, TimeoutSeconds: 120, Status: testtaskjournal.TaskStatusPending}
}

func tenantRecord(id string) testhierarchy.TenantRecord {
	return testhierarchy.TenantRecord{ID: id, Slug: "tenant", Name: "Tenant"}
}
