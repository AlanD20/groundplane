package volume

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"google.golang.org/protobuf/proto"
)

func TestBuildVolumeMutationProjectionAndPlansRoundTripAddRemove(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 26, 15, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, at, 1)
	projectID := ids.NewAt(ids.KindProject, at, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 3)
	volumeID := ids.NewAt(ids.KindVolume, at, 4)
	environment := etcd.EnvironmentRecord{
		ID:        environmentID,
		VolumeDir: "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environmentID,
	}
	addRevision := ids.NewAt(ids.KindTask, at, 5)
	addRequest := volumeMutationRequest{
		action: volumeMutationActionAdd, environmentID: environmentID,
		volumeID: volumeID, slug: "websocket-data", key: "websocket-data",
	}
	added, _, addArtifact, err := buildVolumeMutationProjection(
		tenantID, projectID, environment, etcd.EnvironmentComposeProjection{}, false,
		addRequest, addRevision, 1,
	)
	if err != nil {
		t.Fatalf("buildVolumeMutationProjection(add) error = %v", err)
	}
	addPlan, addSteps, err := buildVolumeMutationPlan(
		"/var/lib/groundplane/vol", stableIDFromTask(ids.KindPlan, addRevision), 1,
		addRequest, nil, addArtifact, make([]byte, 32), nil,
	)
	if err != nil {
		t.Fatalf("buildVolumeMutationPlan(add) error = %v", err)
	}
	if len(added.Volumes) != 1 || added.Volumes[0].ID != volumeID ||
		len(addSteps) != 2 || addPlan.Steps[0].GetManagedVolumeDirectoriesEnsure() == nil ||
		addPlan.Steps[1].GetComposeApply() == nil {
		t.Fatalf("add projection/plan = %#v, %#v", added, addPlan)
	}
	artifactBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(addArtifact)
	if err != nil {
		t.Fatal(err)
	}
	added.ComposeArtifact = artifactBytes
	if _, err := etcd.EnvironmentBlueprintDependencyDigest(added); err != nil {
		t.Fatalf("EnvironmentBlueprintDependencyDigest(add) error = %v", err)
	}
	removeRevision := ids.NewAt(ids.KindTask, at, 6)
	removeRequest := volumeMutationRequest{
		action: volumeMutationActionRemove, environmentID: environmentID,
		volumeID: volumeID, slug: "websocket-data", key: "websocket-data",
	}
	removed, oldArtifact, removeArtifact, err := buildVolumeMutationProjection(
		tenantID, projectID, environment, added, true, removeRequest, removeRevision, 2,
	)
	if err != nil {
		t.Fatalf("buildVolumeMutationProjection(remove) error = %v", err)
	}
	removePlan, removeSteps, err := buildVolumeMutationPlan(
		"/var/lib/groundplane/vol", stableIDFromTask(ids.KindPlan, removeRevision), 2,
		removeRequest, oldArtifact, removeArtifact, make([]byte, 32), nil,
	)
	if err != nil {
		t.Fatalf("buildVolumeMutationPlan(remove) error = %v", err)
	}
	removedArtifactBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(removeArtifact)
	if err != nil {
		t.Fatal(err)
	}
	removed.ComposeArtifact = removedArtifactBytes
	if _, err := etcd.EnvironmentBlueprintDependencyDigest(removed); err != nil {
		t.Fatalf("EnvironmentBlueprintDependencyDigest(remove) error = %v", err)
	}
	if len(removed.Volumes) != 0 || len(removeSteps) != 2 ||
		removePlan.Steps[0].GetManagedVolumeRemove() == nil ||
		removePlan.Steps[1].GetManagedVolumeDirectoryRemove() == nil {
		t.Fatalf("remove projection/plan = %#v, %#v", removed, removePlan)
	}
}

// Rationale: the public lifecycle vocabulary must never expose the internal
// Task word pending, and a slug edit must not make active data look new.
func TestVolumeMutationResponseUsesPublicLifecycleStates(t *testing.T) {
	t.Parallel()
	environment := etcd.EnvironmentRecord{
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
