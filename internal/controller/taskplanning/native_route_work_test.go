package taskplanning

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// NET-05/TASK-09: Route changes use their captured provider input and current
// runtime snapshot; they never grant execution of authored application Services.
func TestNativeRouteMutationAndRemovalCaptureOnlyRouterWork(t *testing.T) {
	for _, action := range []workexecution.Action{workexecution.ActionUpdate, workexecution.ActionRemove} {
		t.Run(string(action), func(t *testing.T) {
			fixture := newNativeRouteFixture(t)
			catalog := fixture.catalog
			capture, err := NewRouteProviderCapture(fixture.state, catalog)
			if err != nil {
				t.Fatal(err)
			}
			task, item := nativeRouteWork(fixture, action)
			candidate := fixture.projection
			candidate.RevisionID, candidate.RenderGeneration = task.ID, 2
			var desired *routes.Record
			removed, expected := "", "new.example.test\n"
			if action == workexecution.ActionRemove {
				removed, expected = task.TargetID, "# no routes\n"
				candidate.DesiredRoutes = nil
			} else {
				route := fixture.projection.DesiredRoutes[0].Desired
				route.Host = "new.example.test"
				desired = &routes.Record{EnvironmentID: fixture.environment.ID, Desired: route}
				candidate.DesiredRoutes = append(candidate.DesiredRoutes[:0:0], fixture.projection.DesiredRoutes[0])
				candidate.DesiredRoutes[0].Desired = route
			}
			pin, err := capture.Capture(
				t.Context(),
				fixture.environment.ID,
				17,
				fixture.projection,
				desired,
				removed,
				2,
			)
			if err != nil || pin == nil {
				t.Fatal("provider capture", err)
			}
			if pin.InputRevision != 43 || len(fixture.state.reads) != 1 || fixture.state.reads[0] != 43 {
				t.Fatal("Route captured runtime at the older desired-projection revision")
			}
			rendered, err := capture.RenderWork(
				t.Context(),
				task,
				item,
				fixture.project,
				fixture.environment,
				candidate,
				*pin,
			)
			if err != nil {
				t.Fatal(err)
			}
			if string(rendered.Content) != expected || len(rendered.Artifact.Services) != 1 ||
				rendered.Artifact.Services[0].OwnerComponentId != pin.ComponentID || rendered.Artifact.Services[0].ServiceId != pin.ServiceID {
				t.Fatal("Route work retained a removed host or granted application workload authority")
			}
			again, err := capture.RenderWork(
				t.Context(),
				task,
				item,
				fixture.project,
				fixture.environment,
				candidate,
				*pin,
			)
			if err != nil || !proto.Equal(again.Artifact, rendered.Artifact) || string(again.Content) != expected {
				t.Fatal("same captured Route did not reconstruct the same router work", err)
			}
			selectedRoutes := candidate.DesiredRoutes
			candidate.DesiredRoutes = append(
				fixture.projection.DesiredRoutes[:0:0],
				fixture.projection.DesiredRoutes[0],
			)
			candidate.DesiredRoutes[0].Desired.Host = "future.example.test"
			content, err := capture.Configuration(*pin, candidate)
			if err != nil || string(content) != expected {
				t.Fatal("later desired Route edits replaced the pinned provider configuration", err)
			}
			candidate.DesiredRoutes = selectedRoutes
			files, err := NewComponentConfigurationCapture(catalog, nativeRouteContents{t: t})
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(rendered.Content)
			configuration := runtimeconfiguration.WorkCapture{TaskID: task.ID, WorkItemID: item.ID,
				Placement: item.Placement, ReadRevision: 43, Generation: 2, Destinations: []string{pin.Destination},
				Current: runtimeconfiguration.Reference{
					ID:            ids.New(ids.KindConfig),
					EnvironmentID: fixture.environment.ID,
					Generation:    2,
					SHA256:        hex.EncodeToString(digest[:]),
				}}
			request := RouteWorkRequest{OperationID: ids.New(ids.KindOperation), RevisionID: task.ID,
				Provider: *pin, Component: candidate.Components[0], Artifact: rendered.Artifact, File: rendered.File,
				Configuration: configuration}
			input, err := files.PrepareRouteWork(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			if input.Remove || !input.Predecessor.Absent || input.ComponentId != pin.ComponentID ||
				input.ConfigurationAction.ActionId != "activate-config" || input.Files[0].Destination != pin.Destination {
				t.Fatal("Route removal selected Component teardown or lost managed-config activation")
			}
			if action == workexecution.ActionRemove {
				assertNativeComponentRemovalCapture(t, task, item, input)
			}
			changed := request
			changed.Provider = environmentchanges.CloneRouteProviderPin(*pin)
			changed.Provider.CatalogDigest = strings.Repeat("f", 64)
			if _, err := files.PrepareRouteWork(t.Context(), changed); err == nil {
				t.Fatal("changed provider catalog acquired router activation authority")
			}
			fixture.state.returnedRevision = 17
			if _, _, err := capture.CapturedRuntime(t.Context(), *pin); err == nil {
				t.Fatal("older runtime snapshot replaced the captured provider")
			}
		})
	}
}

// The replacement for generic historical-teardown steps binds removal to exact
// captured bytes. A newer generated Service cannot inherit the old grant.
func assertNativeComponentRemovalCapture(
	t *testing.T,
	task workexecution.Task,
	item workexecution.Item,
	applied *agentpb.ComponentWorkInput,
) {
	t.Helper()
	input := &agentpb.ComponentWorkInput{OperationId: ids.New(ids.KindOperation), EnvironmentId: applied.EnvironmentId,
		ComponentId: applied.ComponentId, RenderGeneration: applied.RenderGeneration + 1, Remove: true,
		Predecessor: &agentpb.ComponentWorkPredecessor{Runtime: proto.CloneOf(applied.Candidate)}}
	_, raw, reference, err := executionplan.CaptureComponentWorkInput(ids.New(ids.KindPlan), input)
	clear(raw)
	if err != nil {
		t.Fatal(err)
	}
	attempt := workexecution.Attempt{ID: ids.New(ids.KindAttempt), TaskID: task.ID, WorkItemID: item.ID,
		Number: 1, Direction: workexecution.Forward, Input: reference, AssignedAt: task.CreatedAt, Deadline: task.Deadline,
		Executor: workexecution.Identity{Executor: workexecution.Agent, HostID: item.Placement.HostID,
			AgentID: ids.New(ids.KindAgent), Generation: 1}}
	opened, err := executionplan.OpenComponentWorkInput(attempt, input)
	if err != nil || opened.Candidate != nil || !proto.Equal(opened.Predecessor.Runtime, applied.Candidate) {
		t.Fatal("native removal did not retain its exact captured predecessor", err)
	}
	foreign := proto.CloneOf(input)
	foreign.Predecessor.Runtime.Services[0].ServiceId = ids.New(ids.KindService)
	if _, err := executionplan.OpenComponentWorkInput(attempt, foreign); err == nil {
		t.Fatal("new generated Service inherited the captured removal grant")
	}
	foreign = proto.CloneOf(input)
	foreign.ComponentId = ids.New(ids.KindComponent)
	if _, err := executionplan.OpenComponentWorkInput(attempt, foreign); err == nil {
		t.Fatal("another Component inherited the captured removal grant")
	}
}

// NET-05: pure input selection replaces only the chosen Route head, or removes
// it, while preserving the fixed Environment's Service definitions.
func TestNativeRouteProviderSelectionUsesDesiredHead(t *testing.T) {
	fixture := newNativeRouteFixture(t)
	desired := fixture.projection.DesiredRoutes[0].Desired
	desired.Host = "selected.example.test"
	environment, revision, err := routeProviderEnvironment(fixture.environment.ID, 17, fixture.projection,
		&routes.Record{EnvironmentID: fixture.environment.ID, Desired: desired}, "")
	if err != nil || revision != 17 || environment.Services["api"].Image != "app:captured" ||
		len(environment.Routes) != 1 ||
		environment.Routes[0].Host != "selected.example.test" {
		t.Fatal("Route selection changed its fixed Environment context", err)
	}
	environment, _, err = routeProviderEnvironment(fixture.environment.ID, 17, fixture.projection, nil, desired.ID)
	if err != nil || len(environment.Routes) != 0 || environment.Services["api"].Image != "app:captured" {
		t.Fatal("Route omission reconstructed old authored input or altered a Service", err)
	}
}
