package volume

import (
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"google.golang.org/protobuf/proto"
)

func TestVolumeMutationProjectionAndNativeWorkAddRemove(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 26, 15, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, at, 1)
	projectID := ids.NewAt(ids.KindProject, at, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 3)
	volumeID := ids.NewAt(ids.KindVolume, at, 4)
	environment := testhierarchy.EnvironmentRecord{
		ID:        environmentID,
		VolumeDir: "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environmentID,
	}
	addRevision := ids.NewAt(ids.KindTask, at, 5)
	addRequest := volumeMutationRequest{
		action: volumeMutationActionAdd, environmentID: environmentID,
		volumeID: volumeID, slug: "websocket-data", key: "websocket-data",
	}
	added, _, addArtifact, err := buildVolumeMutationProjection(
		tenantID, projectID, environment, testenvironmentprojection.EnvironmentComposeProjection{}, false,
		addRequest, addRevision, 1,
	)
	if err != nil {
		t.Fatalf("buildVolumeMutationProjection(add) error = %v", err)
	}
	scope := workexecution.Scope{TenantID: tenantID, ProjectID: projectID, EnvironmentID: environmentID}
	placement := workexecution.Placement{Executor: workexecution.Agent, HostID: ids.NewAt(ids.KindHost, at, 8)}
	addTask, addItem, addInput, err := buildVolumeWork(scope, placement, volumeWorkTestClaim(addRevision, at),
		addRequest, nil, addArtifact, make([]byte, 32), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(added.Volumes) != 1 || added.Volumes[0].ID != volumeID ||
		addTask.Action != workexecution.ActionCreate || addItem.TaskID != addTask.ID ||
		addInput.Action != agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_CREATE {
		t.Fatalf("add projection/work = %#v / %#v", added, addTask)
	}
	artifactBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(addArtifact)
	if err != nil {
		t.Fatal(err)
	}
	added.ComposeArtifact = artifactBytes
	if _, err := testblueprints.EnvironmentBlueprintDependencyDigest(added); err != nil {
		t.Fatalf("EnvironmentBlueprintDependencyDigest(add) error = %v", err)
	}
	removeRevision := ids.NewAt(ids.KindTask, at, 6)
	removeRequest := volumeMutationRequest{
		action: volumeMutationActionRemove, environmentID: environmentID,
		volumeID: volumeID, slug: "websocket-data", key: "websocket-data", impactToken: strings.Repeat("a", 64),
	}
	removed, oldArtifact, removeArtifact, err := buildVolumeMutationProjection(
		tenantID, projectID, environment, added, true, removeRequest, removeRevision, 2,
	)
	if err != nil {
		t.Fatalf("buildVolumeMutationProjection(remove) error = %v", err)
	}
	removeTask, _, removeInput, err := buildVolumeWork(scope, placement, volumeWorkTestClaim(removeRevision, at),
		removeRequest, oldArtifact, removeArtifact, make([]byte, 32), nil)
	if err != nil {
		t.Fatal(err)
	}
	removedArtifactBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(removeArtifact)
	if err != nil {
		t.Fatal(err)
	}
	removed.ComposeArtifact = removedArtifactBytes
	if _, err := testblueprints.EnvironmentBlueprintDependencyDigest(removed); err != nil {
		t.Fatalf("EnvironmentBlueprintDependencyDigest(remove) error = %v", err)
	}
	if len(removed.Volumes) != 0 || removeTask.Action != workexecution.ActionRemove ||
		removeInput.Action != agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE ||
		len(removeInput.RemovalImpactSha256) != 32 {
		t.Fatalf("remove projection/work = %#v, %#v", removed, removeTask)
	}
}

// Rationale: the public lifecycle vocabulary must never expose the internal
// Task word pending, and a slug edit must not make active data look new.
func TestVolumeMutationResponseUsesPublicLifecycleStates(t *testing.T) {
	t.Parallel()
	environment := testhierarchy.EnvironmentRecord{
		ID:        "env_01K7T8AFR0ABCDEF0123456789",
		VolumeDir: "/var/lib/groundplane/vol/test",
	}
	request := volumeMutationRequest{
		action: volumeMutationActionAdd, environmentID: environment.ID,
		volumeID: "vol_01K7T8AFR0ABCDEF0123456789", slug: "data", key: "data",
	}
	response, err := volumeMutationResponse(request, environment, "task_01K7T8AFR0ABCDEF0123456789")
	if err != nil {
		t.Fatal(err)
	}
	var created apiTypes.VolumeMutationResponse
	if err := json.Unmarshal(response.Body, &created); err != nil || created.Volume.State != "creating" ||
		created.Volume.CreateTaskID == nil || created.Volume.OriginTaskID == nil || created.Volume.CurrentTaskID == nil {
		t.Fatalf("create response = %#v, %v", created, err)
	}
	request.action = volumeMutationActionEdit
	response, err = volumeMutationResponse(request, environment, "task_01K7T8AFR0ABCDEF0123456789")
	if err != nil {
		t.Fatal(err)
	}
	var edited apiTypes.VolumeMutationResponse
	if err := json.Unmarshal(response.Body, &edited); err != nil || edited.Volume.State != "active" ||
		edited.Volume.CreateTaskID != nil || edited.Volume.OriginTaskID != nil || edited.Volume.CurrentTaskID == nil {
		t.Fatalf("edit response = %#v, %v", edited, err)
	}
}

func volumeWorkTestClaim(taskID string, at time.Time) testblueprints.EnvironmentBlueprintStageClaim {
	return testblueprints.EnvironmentBlueprintStageClaim{TaskID: taskID, CreatedAt: at,
		Intent: testidempotency.ProtectedIntentRecord{CiphertextDigest: strings.Repeat("a", 64)}}
}
