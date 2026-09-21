package serviceobserver

import (
	"context"
	"crypto/sha256"
	"errors"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

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
	execOutput    []byte
	execExit      int
	execContainer string
	execOptions   client.ExecCreateOptions
	execAttach    client.ExecAttachOptions
	execAttached  chan struct{}
	execClosed    chan struct{}
	execHoldOpen  bool
	execNilReader bool
	execPeer      net.Conn
}

type trackedConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (connection *trackedConn) Close() error {
	connection.once.Do(func() { close(connection.closed) })
	return connection.Conn.Close()
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

func (engine *readEngine) ExecCreate(
	ctx context.Context,
	containerID string,
	options client.ExecCreateOptions,
) (client.ExecCreateResult, error) {
	engine.calls = append(engine.calls, "exec-create")
	engine.execContainer, engine.execOptions = containerID, options
	return client.ExecCreateResult{ID: "exec-observation"}, ctx.Err()
}

func (engine *readEngine) ExecAttach(
	ctx context.Context,
	execID string,
	options client.ExecAttachOptions,
) (client.ExecAttachResult, error) {
	if err := ctx.Err(); err != nil {
		return client.ExecAttachResult{}, err
	}
	engine.calls = append(engine.calls, "exec-attach")
	engine.execAttach = options
	reader, writer := net.Pipe()
	engine.execPeer = writer
	engine.execClosed = make(chan struct{})
	connection := &trackedConn{Conn: reader, closed: engine.execClosed}
	if engine.execAttached != nil {
		close(engine.execAttached)
	}
	if engine.execNilReader {
		return client.ExecAttachResult{HijackedResponse: client.HijackedResponse{Conn: connection}}, nil
	}
	if engine.execHoldOpen {
		return client.ExecAttachResult{HijackedResponse: client.NewHijackedResponse(connection, "")}, nil
	}
	go func() {
		defer writer.Close()
		if _, err := writer.Write(engine.execOutput); err != nil {
			return
		}
	}()
	// Match the Docker client: successful attachment transfers ownership even
	// if cancellation arrives immediately afterward; errors return no connection.
	return client.ExecAttachResult{HijackedResponse: client.NewHijackedResponse(connection, "")}, nil
}

func proxiedReadRequest(config []byte) *agentpb.ObserveServices {
	request := readRequest()
	digest := sha256.Sum256(config)
	target := request.Targets[0]
	target.ProxyComposeName = "api"
	target.ProxyPlanId = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	target.ProxyRenderGeneration = 2
	target.ProxyConfigSha256 = digest[:]
	return request
}

func proxyLabels(target *agentpb.ServiceObservationTarget) map[string]string {
	return map[string]string{
		"com.groundplane.managed": "true", "com.groundplane.kind": "service",
		"com.groundplane.environment-id": target.EnvironmentId, "com.groundplane.service-id": target.ServiceId,
		"com.groundplane.release-id": "", "com.groundplane.plan-id": target.ProxyPlanId,
		"com.groundplane.render-generation": strconv.FormatUint(target.ProxyRenderGeneration, 10),
		"com.groundplane.runtime-role":      "proxy", "com.groundplane.slot": "",
		"com.docker.compose.service": target.ProxyComposeName, "com.docker.compose.container-number": "1",
		"com.docker.compose.oneoff": "False",
	}
}

// Rationale: OBS-05; healthy serving replicas cannot hide a missing, stopped,
// or wrong-target stable proxy, while an exact live Caddy config remains usable.
func TestObservationIncludesStableProxyServingProof(t *testing.T) {
	expectedConfig := []byte(`{"apps":{"http":{"servers":{"gp":{"listen":[":8080"]}}}}}`)
	wrongConfig := []byte(`{"apps":{"http":{"servers":{"gp":{"listen":[":8081"]}}}}}`)
	for _, test := range []struct {
		name       string
		proxyState *container.State
		output     []byte
		want       agentpb.ServiceProxyObservationState
	}{
		{name: "matching", proxyState: &container.State{Status: container.StateRunning, Running: true}, output: expectedConfig,
			want: agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_MATCHING},
		{name: "wrong target", proxyState: &container.State{Status: container.StateRunning, Running: true}, output: wrongConfig,
			want: agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_CONFIG_MISMATCH},
		{name: "stopped", proxyState: &container.State{Status: container.StateExited, ExitCode: 0},
			want: agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_STOPPED},
		{name: "absent", want: agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_MISSING},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := proxiedReadRequest(expectedConfig)
			engine := &readEngine{execOutput: test.output}
			engine.add("workload", readLabels(request.Targets[0], 1), &container.State{
				Status: container.StateRunning, Running: true, Health: &container.Health{Status: container.Healthy},
			})
			if test.proxyState != nil {
				engine.add("proxy", proxyLabels(request.Targets[0]), test.proxyState)
			}
			result := runRead(t, engine, request)
			row := result.Observations[0]
			if row.GetReplicas().GetHealthy() != 1 || row.GetProxyState() != test.want {
				t.Fatalf("proxy observation = %v", row)
			}
			if test.name == "matching" && (!engine.execOptions.TTY || !engine.execOptions.AttachStdout ||
				engine.execOptions.AttachStdin || engine.execOptions.Privileged || !engine.execAttach.TTY ||
				!slices.Equal(engine.execOptions.Cmd, []string{
					"wget", "-q", "-T", "4", "-t", "1", "-O", "-", "http://127.0.0.1:2019/config/",
				})) {
				t.Fatalf(
					"proxy probe escaped fixed read-only command: create=%+v attach=%+v",
					engine.execOptions,
					engine.execAttach,
				)
			}
		})
	}
}

// Rationale: OBS-05; a container sharing the proxy's coarse Service labels but
// not its acknowledged plan ownership is ambiguous authority, not absence.
func TestObservationRejectsProxyWithStaleOwnership(t *testing.T) {
	config := []byte(`{"apps":{"http":{"servers":{"gp":{}}}}}`)
	request := proxiedReadRequest(config)
	engine := &readEngine{}
	engine.add("workload", readLabels(request.Targets[0], 1), &container.State{
		Status: container.StateRunning, Running: true, Health: &container.Health{Status: container.Healthy},
	})
	labels := proxyLabels(request.Targets[0])
	labels["com.groundplane.plan-id"] = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	engine.add("proxy", labels, &container.State{Status: container.StateRunning, Running: true})
	row := runRead(t, engine, request).Observations[0]
	if !row.GetUnavailable() || row.GetProxyState() !=
		agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_UNSPECIFIED {
		t.Fatalf("stale proxy authority became available: %v", row)
	}
}

func TestProxyProbeClosesIncompleteAttachment(t *testing.T) {
	config := []byte(`{"apps":{"http":{"servers":{"gp":{}}}}}`)
	request := proxiedReadRequest(config)
	engine := &readEngine{execNilReader: true}
	defer func() { _ = engine.execPeer.Close() }()
	engine.add("workload", readLabels(request.Targets[0], 1), &container.State{
		Status: container.StateRunning, Running: true, Health: &container.Health{Status: container.Healthy},
	})
	engine.add(
		"proxy",
		proxyLabels(request.Targets[0]),
		&container.State{Status: container.StateRunning, Running: true},
	)
	row := runRead(t, engine, request).Observations[0]
	if !row.GetUnavailable() {
		t.Fatalf("incomplete proxy attachment became available: %v", row)
	}
	select {
	case <-engine.execClosed:
	default:
		t.Fatal("incomplete proxy attachment was not closed")
	}
}

func TestProxyProbeCancellationUnblocksAttachedRead(t *testing.T) {
	config := []byte(`{"apps":{"http":{"servers":{"gp":{}}}}}`)
	request := proxiedReadRequest(config)
	engine := &readEngine{execAttached: make(chan struct{}), execHoldOpen: true}
	defer func() { _ = engine.execPeer.Close() }()
	engine.add("workload", readLabels(request.Targets[0], 1), &container.State{
		Status: container.StateRunning, Running: true, Health: &container.Health{Status: container.Healthy},
	})
	engine.add(
		"proxy",
		proxyLabels(request.Targets[0]),
		&container.State{Status: container.StateRunning, Running: true},
	)
	observer, err := NewWithEngine(engine)
	if err != nil {
		t.Fatalf("new observer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, observeErr := observer.Observe(ctx, request)
		result <- observeErr
	}()
	<-engine.execAttached
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("observe error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled proxy observation remained blocked on attachment")
	}
	select {
	case <-engine.execClosed:
	default:
		t.Fatal("cancelled proxy attachment was not closed")
	}
}

func (engine *readEngine) ExecInspect(
	ctx context.Context,
	execID string,
	_ client.ExecInspectOptions,
) (client.ExecInspectResult, error) {
	engine.calls = append(engine.calls, "exec-inspect")
	return client.ExecInspectResult{
		ID: execID, ContainerID: engine.execContainer, ExitCode: engine.execExit,
	}, ctx.Err()
}

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
