package executionquiescence

import (
	"context"
	"errors"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// A free worker slot must never hide a surviving helper, a possible future
// helper start, or uncertain Docker observation. Application containers stay up.
func TestExecutionHelperInspection(t *testing.T) {
	for _, state := range []string{"running", "paused", "restarting", "created", "removing", "", "exited", "dead"} {
		t.Run(state, func(t *testing.T) {
			labels := map[string]string{"io.groundplane.role": "entry-executor"}
			engine := &readEngine{
				listed: []container.Summary{
					{
						ID:     "app",
						Labels: map[string]string{"com.groundplane.managed": "true", "com.groundplane.kind": "service"},
					},
					{ID: "helper", Labels: labels},
				},
				inspected: container.InspectResponse{ID: "helper", Config: &container.Config{Labels: labels},
					HostConfig: &container.HostConfig{}, State: &container.State{Status: container.ContainerState(state)}},
			}
			inspector, err := New(engine)
			if err != nil {
				t.Fatal(err)
			}
			err = inspector.RequireQuiescent(t.Context())
			if (err == nil) != (state == "exited" || state == "dead") {
				t.Fatalf("state %q: %v", state, err)
			}
			if len(engine.calls) != 2 || engine.calls[0] != "app" || engine.calls[1] != "helper" {
				t.Fatalf("unexpected observations: %v", engine.calls)
			}
		})
	}
}

func TestExecutionHelperInspectionRejectsUncertainty(t *testing.T) {
	labels := map[string]string{"com.groundplane.managed": "true", "com.groundplane.kind": "configuration-materializer"}
	for _, change := range []func(*readEngine){
		func(engine *readEngine) { engine.err = errors.New("Docker unavailable") },
		func(engine *readEngine) { engine.inspected.ID = "replacement" },
		func(engine *readEngine) { engine.inspected.State.Pid = 10 },
		func(engine *readEngine) { engine.inspected.ExecIDs = []string{"uncertain-exec"} },
		func(engine *readEngine) { engine.inspected.HostConfig.RestartPolicy.Name = "always" },
	} {
		engine := &readEngine{listed: []container.Summary{{ID: "helper", Labels: labels}},
			inspected: container.InspectResponse{ID: "helper", Config: &container.Config{Labels: labels},
				HostConfig: &container.HostConfig{}, State: &container.State{Status: "exited"}}}
		change(engine)
		inspector, err := New(engine)
		if err != nil {
			t.Fatal(err)
		}
		if err := inspector.RequireQuiescent(t.Context()); err == nil {
			t.Fatal("uncertain executor qualified")
		}
	}
}

func TestServingExecPreventsQualificationWithoutStoppingApplication(t *testing.T) {
	labels := map[string]string{"com.groundplane.managed": "true", "com.groundplane.kind": "service"}
	engine := &readEngine{listed: []container.Summary{{ID: "app", Labels: labels}}, appExecs: []string{"orphan-exec"}}
	inspector, err := New(engine)
	if err != nil {
		t.Fatal(err)
	}
	if err := inspector.RequireQuiescent(t.Context()); err == nil {
		t.Fatal("serving exec was treated as stopped")
	}
	engine.appExecs = nil
	if err := inspector.RequireQuiescent(t.Context()); err != nil {
		t.Fatal(err)
	}
}

type readEngine struct {
	appExecs  []string
	listed    []container.Summary
	inspected container.InspectResponse
	err       error
	calls     []string
}

func (engine *readEngine) ContainerList(
	context.Context,
	client.ContainerListOptions,
) (client.ContainerListResult, error) {
	return client.ContainerListResult{Items: engine.listed}, engine.err
}

func (engine *readEngine) ContainerInspect(
	_ context.Context,
	id string,
	_ client.ContainerInspectOptions,
) (client.ContainerInspectResult, error) {
	engine.calls = append(engine.calls, id)
	if id == "app" {
		return client.ContainerInspectResult{
			Container: container.InspectResponse{
				ID:         id,
				ExecIDs:    engine.appExecs,
				Config:     &container.Config{Labels: engine.listed[0].Labels},
				HostConfig: &container.HostConfig{},
				State:      &container.State{Status: "running", Running: true, Pid: 42},
			},
		}, nil
	}
	return client.ContainerInspectResult{Container: engine.inspected}, engine.err
}
