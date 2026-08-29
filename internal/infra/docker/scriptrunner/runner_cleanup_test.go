package scriptrunner

import (
	"context"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestCleanupMutatesOnlyCapturedOwnedContainer(t *testing.T) {
	t.Parallel()

	runner, engine, request, prepared := cleanupFixture(t)
	engine.running = true

	if err := runner.cleanup(request, engine.containerID, &prepared); err != nil {
		t.Fatalf("cleanup owned container: %v", err)
	}
	want := []string{"inspect", "stop", "inspect", "inspect", "remove", "inspect"}
	if !slices.Equal(engine.operations, want) {
		t.Fatalf("operations = %v, want %v", engine.operations, want)
	}
	if _, err := os.Stat(prepared.hostPath); !os.IsNotExist(err) {
		t.Fatalf("body still exists after proven container removal: %v", err)
	}
}

func TestCleanupRefusesContainerWithMismatchedOwnership(t *testing.T) {
	t.Parallel()

	runner, engine, request, prepared := cleanupFixture(t)
	engine.running = true
	engine.inspected.Container.Config.Labels["com.groundplane.task-id"] = "task_01k5v8k8yr0000000000000000"

	if err := runner.cleanup(request, engine.containerID, &prepared); err == nil {
		t.Fatal("expected ownership mismatch to fail cleanup")
	}
	if !slices.Equal(engine.operations, []string{"inspect"}) {
		t.Fatalf("operations = %v, want inspect only", engine.operations)
	}
	if _, err := os.Stat(prepared.hostPath); err != nil {
		t.Fatalf("body was removed without proving container absence: %v", err)
	}
}

func TestCleanupRemovesBodyWhenNoContainerWasCaptured(t *testing.T) {
	t.Parallel()

	runner, engine, request, prepared := cleanupFixture(t)

	if err := runner.cleanup(request, "", &prepared); err != nil {
		t.Fatalf("cleanup body without container: %v", err)
	}
	if len(engine.operations) != 0 {
		t.Fatalf("operations = %v, want none", engine.operations)
	}
	if _, err := os.Stat(prepared.hostPath); !os.IsNotExist(err) {
		t.Fatalf("body still exists without a captured container: %v", err)
	}
}

func cleanupFixture(t *testing.T) (*Runner, *cleanupEngine, scriptexecution.Request, preparedBody) {
	t.Helper()

	root := t.TempDir()
	if err := os.Chmod(root, directoryMode); err != nil {
		t.Fatalf("make body store private: %v", err)
	}
	uid, gid := uint32(os.Geteuid()), uint32(os.Getegid())
	bodies, err := newBodyStore(root, uid, gid)
	if err != nil {
		t.Fatalf("create body store: %v", err)
	}
	t.Cleanup(func() { _ = bodies.Close() })
	prepared, err := bodies.Prepare(ids.New(ids.KindAssignment), ids.NewULID(), []byte("echo migration\n"), uid, gid)
	if err != nil {
		t.Fatalf("prepare body: %v", err)
	}

	request := scriptexecution.Request{
		AssignmentID: prepared.assignmentID, ExecutionID: prepared.executionID,
		Projection: &agentpb.ScriptRunnerProjection{
			Name: "gp-script-01k5v8k8yr0000000000000000", Image: "app@sha256:" + strings.Repeat("b", 64),
			Uid: uid, Gid: gid, Entrypoint: []string{"/bin/sh"}, Command: []string{bodyTarget},
			Labels: []*agentpb.ScriptStringPair{
				{Key: "com.groundplane.managed", Value: "true"},
				{Key: "com.groundplane.kind", Value: "script-runner"},
			},
		}}
	containerID := strings.Repeat("a", 64)
	engine := &cleanupEngine{containerID: containerID}
	engine.inspected = client.ContainerInspectResult{Container: container.InspectResponse{
		ID: containerID, Name: "/" + request.Projection.Name,
		Config: &container.Config{
			Image:      request.Projection.Image,
			User:       strconv.FormatUint(uint64(uid), 10) + ":" + strconv.FormatUint(uint64(gid), 10),
			Entrypoint: request.Projection.Entrypoint, Cmd: request.Projection.Command,
			Labels: pairMap(request.Projection.Labels),
		},
		HostConfig: &container.HostConfig{LogConfig: container.LogConfig{Type: "none"}},
		State:      &container.State{Status: container.StateCreated},
		Mounts: []container.MountPoint{{
			Type: mount.TypeBind, Source: prepared.hostPath, Destination: bodyTarget,
		}},
	}}
	return &Runner{client: engine, bodies: bodies}, engine, request, prepared
}

type cleanupEngine struct {
	containerID string
	inspected   client.ContainerInspectResult
	running     bool
	removed     bool
	operations  []string
}

func (engine *cleanupEngine) ContainerCreate(context.Context, client.ContainerCreateOptions) (client.ContainerCreateResult, error) {
	return client.ContainerCreateResult{}, nil
}

func (engine *cleanupEngine) ContainerAttach(context.Context, string, client.ContainerAttachOptions) (client.ContainerAttachResult, error) {
	return client.ContainerAttachResult{}, nil
}

func (engine *cleanupEngine) ContainerStart(context.Context, string, client.ContainerStartOptions) (client.ContainerStartResult, error) {
	return client.ContainerStartResult{}, nil
}

func (engine *cleanupEngine) ContainerWait(context.Context, string, client.ContainerWaitOptions) client.ContainerWaitResult {
	return client.ContainerWaitResult{}
}

func (engine *cleanupEngine) ContainerInspect(context.Context, string, client.ContainerInspectOptions) (client.ContainerInspectResult, error) {
	engine.operations = append(engine.operations, "inspect")
	if engine.removed {
		return client.ContainerInspectResult{}, containerderrdefs.ErrNotFound
	}
	engine.inspected.Container.State.Running = engine.running
	if engine.running {
		engine.inspected.Container.State.Status = container.StateRunning
	} else {
		engine.inspected.Container.State.Status = container.StateExited
	}
	return engine.inspected, nil
}

func (engine *cleanupEngine) ContainerStop(context.Context, string, client.ContainerStopOptions) (client.ContainerStopResult, error) {
	engine.operations = append(engine.operations, "stop")
	engine.running = false
	return client.ContainerStopResult{}, nil
}

func (engine *cleanupEngine) ContainerKill(context.Context, string, client.ContainerKillOptions) (client.ContainerKillResult, error) {
	engine.operations = append(engine.operations, "kill")
	engine.running = false
	return client.ContainerKillResult{}, nil
}

func (engine *cleanupEngine) ContainerRemove(context.Context, string, client.ContainerRemoveOptions) (client.ContainerRemoveResult, error) {
	engine.operations = append(engine.operations, "remove")
	engine.removed = true
	return client.ContainerRemoveResult{}, nil
}

func (engine *cleanupEngine) Close() error { return nil }
