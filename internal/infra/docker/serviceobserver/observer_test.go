package serviceobserver

import (
	"context"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"google.golang.org/protobuf/proto"
)

type readEngine struct {
	listed        []container.Summary
	inspects      map[string]client.ContainerInspectResult
	inspectErr    error
	inspectErrors map[string]error
	listErr       error
	calls         []string
	options       client.ContainerListOptions
}

func (engine *readEngine) ContainerList(
	ctx context.Context,
	options client.ContainerListOptions,
) (client.ContainerListResult, error) {
	engine.calls = append(engine.calls, "list")
	engine.options = options
	return client.ContainerListResult{Items: engine.listed}, engine.listErr
}

func (engine *readEngine) ContainerInspect(
	ctx context.Context,
	id string,
	_ client.ContainerInspectOptions,
) (client.ContainerInspectResult, error) {
	engine.calls = append(engine.calls, id)
	if err := ctx.Err(); err != nil {
		return client.ContainerInspectResult{}, err
	}
	if err := engine.inspectErrors[id]; err != nil {
		return client.ContainerInspectResult{}, err
	}
	return engine.inspects[id], engine.inspectErr
}

func (engine *readEngine) Close() error { return nil }

func readRequest() *agentpb.ObserveServices {
	return &agentpb.ObserveServices{
		RequestId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Targets: []*agentpb.ServiceObservationTarget{{
			EnvironmentId: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			ReleaseId: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", PlanId: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			RenderGeneration: 3, ComposeName: "api-blue", RuntimeRole: "slot", Slot: "blue",
		}},
	}
}

func readLabels(target *agentpb.ServiceObservationTarget, replica int) map[string]string {
	return map[string]string{
		"com.groundplane.managed": "true", "com.groundplane.kind": "service",
		"com.groundplane.environment-id": target.EnvironmentId, "com.groundplane.service-id": target.ServiceId,
		"com.groundplane.release-id": target.ReleaseId, "com.groundplane.plan-id": target.PlanId,
		"com.groundplane.render-generation": strconv.FormatUint(target.RenderGeneration, 10),
		"com.groundplane.runtime-role":      target.RuntimeRole, "com.groundplane.slot": target.Slot,
		"com.docker.compose.service": target.ComposeName, "com.docker.compose.container-number": strconv.Itoa(replica),
		"com.docker.compose.oneoff": "False",
	}
}

func (engine *readEngine) add(id string, labels map[string]string, state *container.State) {
	if engine.inspects == nil {
		engine.inspects = map[string]client.ContainerInspectResult{}
	}
	engine.listed = append(engine.listed, container.Summary{ID: id, Labels: maps.Clone(labels)})
	engine.inspects[id] = client.ContainerInspectResult{Container: container.InspectResponse{
		ID: id, Config: &container.Config{Labels: maps.Clone(labels), Env: []string{"SECRET=must-not-leak"}}, State: state,
	}}
}

func runRead(t *testing.T, engine *readEngine, request *agentpb.ObserveServices) *agentpb.ServiceObservationResult {
	t.Helper()
	observer, err := NewWithEngine(engine)
	if err != nil {
		t.Fatal(err)
	}
	result, err := observer.Observe(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := serviceobservation.ValidateResult(request, result); err != nil {
		t.Fatal(err)
	}
	return result
}

// Rationale: retained slots and stable proxies share a Service id but are not
// serving replicas. Unrelated runtime must not even be inspected by this read.
func TestObservationSelectsOnlyServingWorkload(t *testing.T) {
	request := readRequest()
	labels := readLabels(request.Targets[0], 1)
	engine := &readEngine{}
	engine.add("current", labels, &container.State{Status: container.StateRunning, Running: true,
		Health: &container.Health{
			Status: container.Healthy,
			Log:    []*container.HealthcheckResult{{Output: "private-probe-output"}},
		}})
	retained := maps.Clone(labels)
	retained["com.groundplane.release-id"] = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	retained["com.groundplane.slot"] = "green"
	engine.add("retained", retained, nil)
	proxy := maps.Clone(labels)
	proxy["com.groundplane.runtime-role"], proxy["com.groundplane.release-id"] = "proxy", ""
	engine.add("proxy", proxy, nil)
	script := maps.Clone(labels)
	script["com.groundplane.kind"] = "script"
	engine.add("script", script, nil)
	foreign := maps.Clone(labels)
	foreign["com.groundplane.environment-id"] = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	engine.add("foreign", foreign, nil)
	result := runRead(t, engine, request)
	if got := result.Observations[0].GetReplicas(); got == nil || got.Healthy != 1 ||
		serviceobservation.Total(got) != 1 {
		t.Fatalf("wrong serving counts: %v", result)
	}
	if !slices.Equal(engine.calls, []string{"list", "current"}) || !engine.options.All {
		t.Fatalf("read authority escaped target or omitted stopped containers: %v", engine.calls)
	}
	if strings.Contains(result.String(), "must-not-leak") || strings.Contains(result.String(), "private-probe-output") {
		t.Fatal("observation exposed private runtime data")
	}
}

// Rationale: inspect must recheck immutable authority; a valid list summary is
// insufficient after a concurrent replacement, malformed replica or state read.
func TestObservationRejectsChangedOrMalformedInspect(t *testing.T) {
	for name, mutate := range map[string]func(*container.InspectResponse){
		"wrong id":             func(c *container.InspectResponse) { c.ID = "replacement" },
		"no config":            func(c *container.InspectResponse) { c.Config = nil },
		"no state":             func(c *container.InspectResponse) { c.State = nil },
		"generation":           func(c *container.InspectResponse) { c.Config.Labels["com.groundplane.render-generation"] = "2" },
		"plan":                 func(c *container.InspectResponse) { c.Config.Labels["com.groundplane.plan-id"] = "plan_old" },
		"Release":              func(c *container.InspectResponse) { c.Config.Labels["com.groundplane.release-id"] = "dep_old" },
		"Environment":          func(c *container.InspectResponse) { c.Config.Labels["com.groundplane.environment-id"] = "env_other" },
		"Service":              func(c *container.InspectResponse) { c.Config.Labels["com.groundplane.service-id"] = "svc_other" },
		"slot":                 func(c *container.InspectResponse) { c.Config.Labels["com.groundplane.slot"] = "green" },
		"proxy":                func(c *container.InspectResponse) { c.Config.Labels["com.groundplane.runtime-role"] = "proxy" },
		"unmanaged":            func(c *container.InspectResponse) { delete(c.Config.Labels, "com.groundplane.managed") },
		"Component":            func(c *container.InspectResponse) { c.Config.Labels["com.groundplane.component-id"] = "cmp_hidden" },
		"Compose name":         func(c *container.InspectResponse) { c.Config.Labels["com.docker.compose.service"] = "api-green" },
		"oneoff":               func(c *container.InspectResponse) { c.Config.Labels["com.docker.compose.oneoff"] = "True" },
		"missing ordinal":      func(c *container.InspectResponse) { delete(c.Config.Labels, "com.docker.compose.container-number") },
		"unknown state":        func(c *container.InspectResponse) { c.State.Status = "other" },
		"inconsistent running": func(c *container.InspectResponse) { c.State.Running = false },
		"unknown health":       func(c *container.InspectResponse) { c.State.Health = &container.Health{Status: "other"} },
	} {
		t.Run(name, func(t *testing.T) {
			request := readRequest()
			engine := &readEngine{}
			engine.add(
				"current",
				readLabels(request.Targets[0], 1),
				&container.State{Status: container.StateRunning, Running: true},
			)
			item := engine.inspects["current"]
			mutate(&item.Container)
			engine.inspects["current"] = item
			result := runRead(t, engine, request)
			if row := result.Observations[0]; !row.GetUnavailable() || row.GetReplicas() != nil {
				t.Fatalf("invalid evidence became counts: %v", row)
			}
		})
	}
}

// Rationale: Docker states are disjoint observations, not desired intent; no
// healthcheck must stay distinct from a healthy check, including zero replicas.
func TestObservationCountsRuntimeStatesAndAbsence(t *testing.T) {
	request := readRequest()
	for name, state := range map[string]*container.State{
		"running":      {Status: container.StateRunning, Running: true},
		"healthy":      {Status: container.StateRunning, Running: true, Health: &container.Health{Status: container.Healthy}},
		"starting":     {Status: container.StateRunning, Running: true, Health: &container.Health{Status: container.Starting}},
		"unhealthy":    {Status: container.StateRunning, Running: true, Health: &container.Health{Status: container.Unhealthy}},
		"transitional": {Status: container.StateRestarting, Running: true, Restarting: true},
		"stopped":      {Status: container.StateExited},
		"failed":       {Status: container.StateExited, ExitCode: 1},
	} {
		t.Run(name, func(t *testing.T) {
			engine := &readEngine{}
			engine.add("current", readLabels(request.Targets[0], 1), state)
			counts := runRead(t, engine, request).Observations[0].GetReplicas()
			if counts == nil || serviceobservation.Total(counts) != 1 || !strings.Contains(counts.String(), name+":1") {
				t.Fatalf("wrong state bucket %s: %v", name, counts)
			}
		})
	}
	empty := runRead(t, &readEngine{}, request).Observations[0]
	if empty.GetReplicas() == nil || serviceobservation.Total(empty.GetReplicas()) != 0 || empty.GetUnavailable() {
		t.Fatalf("complete empty observation differs from absent: %v", empty)
	}
}

// Rationale: duplicates or a failed late replica cannot leave a partial healthy
// count; list bounds and cancellation must fail without excess inspections.
func TestObservationBoundsFailuresAndDuplicateReplicas(t *testing.T) {
	request := readRequest()
	for _, duplicateID := range []bool{false, true} {
		engine := &readEngine{}
		labels := readLabels(request.Targets[0], 1)
		engine.add("current", labels, &container.State{Status: container.StateRunning, Running: true})
		otherID := "duplicate"
		if duplicateID {
			otherID = "current"
		}
		engine.add(otherID, labels, &container.State{Status: container.StateRunning, Running: true})
		if row := runRead(t, engine, request).Observations[0]; !row.GetUnavailable() {
			t.Fatalf("duplicate replica became available: %v", row)
		}
	}
	engine := &readEngine{inspectErr: errs.New(errs.KindInternal, "Docker unavailable")}
	engine.add("current", readLabels(request.Targets[0], 1), nil)
	if row := runRead(t, engine, request).Observations[0]; !row.GetUnavailable() {
		t.Fatal("failed inspection became available")
	}
	engine = &readEngine{listed: make([]container.Summary, serviceobservation.MaximumContainers+1)}
	observer, err := NewWithEngine(engine)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := observer.Observe(context.Background(), request); err == nil || len(engine.calls) != 1 {
		t.Fatalf("overflow inspected resources: calls=%v err=%v", engine.calls, err)
	}
	engine.calls = nil
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := observer.Observe(ctx, request); err == nil || len(engine.calls) != 0 {
		t.Fatalf("cancelled read reached Docker: calls=%v err=%v", engine.calls, err)
	}
	invalid := proto.CloneOf(request)
	invalid.Targets[0].RuntimeRole = "proxy"
	if _, err := observer.Observe(context.Background(), invalid); err == nil || len(engine.calls) != 0 {
		t.Fatalf("invalid target reached Docker: calls=%v err=%v", engine.calls, err)
	}
}
