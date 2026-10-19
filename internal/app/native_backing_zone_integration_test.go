package app

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	networkcontroller "github.com/AlanD20/groundplane/internal/controller/network"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	networkmodel "github.com/AlanD20/groundplane/internal/core/network"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkwork"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TASK-83: for an initially ready Backing Environment with no consumers, the
// actual public capability captures one bridge in a bounded native graph. This
// does not execute provisioning, detach a consumer or remove a Docker network.
func TestNativeBackingZoneRemovalPublishesCapturedGraph(t *testing.T) {
	f := newNativeRouteFixture(t)
	at := time.Now().UTC().Add(-time.Hour)
	project := hierarchy.ProjectRecord{ID: ids.New(ids.KindProject), Kind: hierarchy.ProjectKindBacking,
		Slug: "backing-zone", Name: "Backing Zone"}
	nativeVolumePut(t, f.base.store, hierarchy.ProjectKey(project.ID), project, hierarchy.EncodeProject)
	coordination, err := hierarchydeletion.EncodeInitialCoordination(
		hierarchydeletion.HierarchyDeletionTargetProject,
		project.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.store.Put(t.Context(), hierarchydeletion.HierarchyCoordinationKey("project", project.ID), coordination); err != nil {
		t.Fatal(err)
	}
	environment, err := hierarchy.NewProvisioningEnvironment("/var/lib/groundplane/vol", project,
		ids.New(ids.KindEnvironment), "main", "10.200.0.0/24", ids.New(ids.KindTask), at)
	if err != nil {
		t.Fatal(err)
	}
	environment, err = hierarchy.CompleteEnvironmentProvisioning(environment, environment.CreateTaskID, true)
	if err != nil {
		t.Fatal(err)
	}
	nativeVolumePut(t, f.base.store, hierarchy.EnvironmentKey(environment.ID), environment, hierarchy.EncodeEnvironment)
	nativeVolumePut(
		t,
		f.base.store,
		hierarchy.EnvironmentMutationEpochKey(environment.ID),
		backupruntime.EnvironmentMutationEpochRecord{
			EnvironmentID: environment.ID,
		},
		backupruntime.EncodeEnvironmentMutationEpochRecord,
	)
	response, err := f.network.CreateZone(t.Context(), networkmodel.CreateZoneRequest{
		EnvironmentID: environment.ID, Name: "data", Subnet: "10.200.0.0/28", Internal: true,
	}, "native-backing-zone-ready")
	if err != nil {
		t.Fatal(err)
	}
	var zone api.Zone
	if err := json.Unmarshal(response.Body, &zone); err != nil {
		t.Fatal(err)
	}
	impact, err := f.network.GetZoneRemovalImpact(t.Context(), zone.ID)
	if err != nil || impact.ImpactToken == "" || impact.Mode != networkmodel.ZoneRemovalImpactCascade {
		t.Fatalf("ready Backing impact: %v", err)
	}
	removed, err := f.network.RemoveZoneWithImpact(
		t.Context(),
		zone.ID,
		"native-backing-zone-remove",
		impact.ImpactToken,
	)
	if err != nil {
		t.Fatal(err)
	}
	var accepted api.TaskAccepted
	if err := json.Unmarshal(removed.Body, &accepted); err != nil || removed.Status != http.StatusAccepted {
		t.Fatalf("Backing Zone response: %v", err)
	}
	inputs, err := networkwork.NewInputRepository(f.base.store)
	if err != nil {
		t.Fatal(err)
	}
	task, revision, err := f.base.queue.ReadTask(t.Context(), accepted.TaskID, 0)
	if err != nil || task.Task.PublicationComplete || task.Task.Scope.ProjectID != project.ID ||
		task.Task.Scope.TenantID != "" || task.Task.TargetID != zone.ID || task.Task.ExpectedItems != 1 {
		t.Fatalf("Backing Zone publication authority: %v", err)
	}
	graph, _, err := inputs.LoadCascadeGraph(t.Context(), task, revision)
	if err != nil || len(graph) != 1 || graph[0].TargetID != zone.ID ||
		graph[0].Operation != workexecution.Network || graph[0].Placement.HostID != f.base.authorization.Identity().HostID {
		t.Fatalf("captured Backing bridge: %v", err)
	}
	owner, err := networkcontroller.NewAgentWork(inputs, f.base.queue)
	if err != nil {
		t.Fatal(err)
	}
	publications, err := workdispatch.NewPublications(f.base.queue, []workdispatch.PublicationRegistration{{
		Action: workexecution.ActionRemove, Owner: owner,
	}})
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := publications.Advance(t.Context(), accepted.TaskID, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	task, _, err = f.base.queue.ReadTask(t.Context(), accepted.TaskID, 0)
	if err != nil || !task.Task.PublicationComplete {
		t.Fatalf("bounded graph publication did not finish: %v", err)
	}
	page, err := f.base.queue.ListWork(t.Context(), accepted.TaskID, "", 10, 0)
	if err != nil || len(page.Items) != 1 || !reflect.DeepEqual(page.Items[0].Item, graph[0]) {
		t.Fatalf("published graph differs from captured bridge: %v", err)
	}
	revision = f.base.store.revision
	_, err = f.network.RemoveZoneWithImpact(t.Context(), zone.ID, "native-backing-zone-remove", impact.ImpactToken)
	if !errors.Is(err, errs.New(errs.KindIdempotencyInProgress, "")) || f.base.store.revision != revision {
		t.Fatalf("Backing Zone replay lost its accepted graph or changed state: %v", err)
	}
}
