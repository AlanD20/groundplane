package scriptrunner

import (
	"context"
	"crypto/sha256"
	"errors"
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
	"github.com/AlanD20/groundplane/pkg/errs"
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

// Rationale: after exact container and ownership evidence is checkpointed,
// cleanup must remove that container even if a non-ownership runner predicate differs.
func TestCleanupUsesCheckpointedOwnershipAfterRunnerShapeMismatch(t *testing.T) {
	t.Parallel()

	runner, engine, request, prepared := cleanupFixture(t)
	engine.inspected.Container.Config.Image = "other@sha256:" + strings.Repeat("c", 64)
	body := bodyEvidence(prepared)
	container := containerEvidence(request, engine.containerID)

	proof, err := runner.Cleanup(request, &body, &container)
	if err != nil {
		t.Fatalf("cleanup checkpoint-owned container: %v", err)
	}
	if !engine.removed || !proof.ContainerAbsent || !proof.BodyAbsent || !proof.ExecutionDirectoryAbsent {
		t.Fatalf("removed/proof = %t/%#v, want exact container and artifacts absent", engine.removed, proof)
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

// Rationale: a successful Docker create establishes immutable cleanup authority
// even when the immediate ownership validation fails.
func TestCreateContainerReturnsCapturedEvidenceWhenValidationFails(t *testing.T) {
	t.Parallel()

	runner, engine, request, prepared := cleanupFixture(t)
	request.Projection.WorkingDir = "/srv/app"
	engine.inspected.Container.Config.WorkingDir = "/unexpected"
	engine.createErr = errors.New("Docker create response was ambiguous")
	ctx, cancel := context.WithCancel(context.Background())
	engine.cancelCreate = cancel

	evidence, err := runner.CreateContainer(ctx, request, bodyEvidence(prepared))
	if err == nil {
		t.Fatal("expected created-container validation to fail")
	}
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("validation error kind = %v/%t, want state conflict: %v", kind, ok, err)
	}
	want := containerEvidence(request, engine.containerID)
	if evidence.ID != engine.containerID ||
		!slices.Equal(evidence.OwnershipLabelsSHA256, want.OwnershipLabelsSHA256) {
		t.Fatalf("container evidence = %#v, want %#v", evidence, want)
	}
	if !slices.Equal(engine.operations, []string{"inspect"}) || engine.removed {
		t.Fatalf("operations/removed = %v/%t, want inspect only and retained", engine.operations, engine.removed)
	}
}

// Rationale: an ID-plus-error create response may be adopted only after exact
// ownership and stopped-created state are proven, never by optimistic removal.
func TestCreateContainerAdoptsExactCreatedIDReturnedWithError(t *testing.T) {
	t.Parallel()

	runner, engine, request, prepared := cleanupFixture(t)
	engine.created = true
	engine.createErr = errors.New("Docker create response was ambiguous")

	evidence, err := runner.CreateContainer(context.Background(), request, bodyEvidence(prepared))
	if err != nil {
		t.Fatalf("adopt exact created container: %v", err)
	}
	if evidence.ID != engine.containerID || !slices.Equal(engine.operations, []string{"inspect"}) || engine.removed {
		t.Fatalf("evidence/operations/removed = %#v/%v/%t", evidence, engine.operations, engine.removed)
	}
}

// Rationale: a returned ID naming a running container is recovery evidence,
// not removal authority, even if request cancellation races its inspection.
func TestCreateContainerReturnsRecoveryConflictForRunningIDDuringCancellation(t *testing.T) {
	t.Parallel()

	runner, engine, request, prepared := cleanupFixture(t)
	engine.running = true
	engine.createErr = errors.New("Docker create response was ambiguous")
	ctx, cancel := context.WithCancel(context.Background())
	engine.cancelCreate = cancel

	evidence, err := runner.CreateContainer(ctx, request, bodyEvidence(prepared))
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("running-container error kind = %v/%t, want state conflict: %v", kind, ok, err)
	}
	if evidence.ID != engine.containerID || !slices.Equal(engine.operations, []string{"inspect"}) || engine.removed {
		t.Fatalf("evidence/operations/removed = %#v/%v/%t", evidence, engine.operations, engine.removed)
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
	body := []byte("echo migration\n")
	prepared, err := bodies.Prepare(ids.New(ids.KindAssignment), ids.NewULID(), body, uid, gid)
	if err != nil {
		t.Fatalf("prepare body: %v", err)
	}
	bodyDigest := sha256.Sum256(body)

	request := scriptexecution.Request{
		TaskID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), StepID: ids.New(ids.KindStep),
		AssignmentID: prepared.assignmentID, ExecutionID: prepared.executionID,
		PlanHash: append([]byte(nil), bodyDigest[:]...), Body: append([]byte(nil), body...),
		BodyMetadata: &agentpb.ScriptBodyArtifactMetadata{
			ScriptExecutionId: prepared.executionID, Size: uint32(len(body)), Sha256: append([]byte(nil), bodyDigest[:]...),
			Uid: uid, Gid: gid,
		},
		Projection: &agentpb.ScriptRunnerProjection{
			Name: "gp-script-" + strings.ToLower(prepared.executionID), Image: "app@sha256:" + strings.Repeat("b", 64),
			Uid: uid, Gid: gid, Entrypoint: []string{"/bin/sh"}, Command: []string{bodyTarget},
			StopGraceSeconds: stopSeconds,
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
	containerID  string
	inspected    client.ContainerInspectResult
	createErr    error
	cancelCreate context.CancelFunc
	created      bool
	running      bool
	removed      bool
	operations   []string
}

func (engine *cleanupEngine) ContainerCreate(context.Context, client.ContainerCreateOptions) (client.ContainerCreateResult, error) {
	if engine.cancelCreate != nil {
		engine.cancelCreate()
	}
	return client.ContainerCreateResult{ID: engine.containerID}, engine.createErr
}

func (engine *cleanupEngine) NetworkConnect(context.Context, string, client.NetworkConnectOptions) (client.NetworkConnectResult, error) {
	return client.NetworkConnectResult{}, nil
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
	if engine.created {
		engine.inspected.Container.State.Status = container.StateCreated
	} else if engine.running {
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
