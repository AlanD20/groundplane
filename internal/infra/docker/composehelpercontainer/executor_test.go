package composehelpercontainer

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const helperImage = "registry.example/groundplane-agent@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// Rationale: one request gets one unnamed helper with only static process
// configuration, the Docker socket, and no persistent Agent or host-root mount.
func TestExecuteUsesExactContainerPolicyAndRemovesByImmutableID(t *testing.T) {
	engine := newFakeEngine(t, successfulResponse())
	executor, err := NewWithEngine(engine, helperImage)
	if err != nil {
		t.Fatalf("NewWithEngine() error = %v", err)
	}
	request := helperRequest(t)
	response, err := executor.Execute(context.Background(), request)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if response.Outcome != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED {
		t.Fatalf("response = %#v", response)
	}
	if len(engine.createCalls) != 1 {
		t.Fatalf("create calls = %d, want 1", len(engine.createCalls))
	}
	assertCreatePolicy(t, engine.createCalls[0])
	if !reflect.DeepEqual(engine.attachIDs, []string{"helper-id"}) ||
		!reflect.DeepEqual(engine.startIDs, []string{"helper-id"}) ||
		!reflect.DeepEqual(engine.removeIDs, []string{"helper-id"}) {
		t.Fatalf("lifecycle ids: attach=%v start=%v remove=%v", engine.attachIDs, engine.startIDs, engine.removeIDs)
	}
	if len(engine.stopIDs) != 0 {
		t.Fatalf("stop calls = %v, want none on successful exit", engine.stopIDs)
	}
	decoded, err := composehelper.ReadRequest(context.Background(), bytes.NewReader(engine.connection.written.Bytes()))
	if err != nil {
		t.Fatalf("decode attached request: %v", err)
	}
	if decoded.TaskId != request.TaskId || !bytes.Equal(decoded.Plan.PlanHash, request.Plan.PlanHash) {
		t.Fatalf("attached request = %#v", decoded)
	}
}

// Rationale: helper cleanup is an execution invariant; a remove failure must
// replace an otherwise successful response and remain attributable internally.
func TestExecuteCleanupFailureTakesPrecedence(t *testing.T) {
	removeErr := errors.New("daemon refused removal")
	engine := newFakeEngine(t, successfulResponse())
	engine.removeErr = removeErr
	executor, err := NewWithEngine(engine, helperImage)
	if err != nil {
		t.Fatalf("NewWithEngine() error = %v", err)
	}
	response, err := executor.Execute(context.Background(), helperRequest(t))
	if response != nil || !errors.Is(err, removeErr) || !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("response=%#v error=%v, want cleanup internal failure", response, err)
	}
}

func TestExecuteReportsBoundedHelperFailure(t *testing.T) {
	engine := newFakeEngine(t, successfulResponse())
	engine.reader = multiplexedReader(stdcopy.Stderr, []byte("docker compose: network is missing\n"))
	engine.exitStatus = 17
	executor, err := NewWithEngine(engine, helperImage)
	if err != nil {
		t.Fatalf("NewWithEngine() error = %v", err)
	}
	response, err := executor.Execute(context.Background(), helperRequest(t))
	if response != nil || !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("response=%#v error=%v, want internal helper failure", response, err)
	}
	for _, want := range []string{"exit code 17", "docker compose: network is missing"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Execute() error = %q, want %q", err, want)
		}
	}
}

// Rationale: the helper image is executable authority and must never resolve
// through a mutable tag.
func TestNewWithEngineRejectsUnpinnedImage(t *testing.T) {
	engine := newFakeEngine(t, successfulResponse())
	if _, err := NewWithEngine(
		engine,
		"registry.example/groundplane-agent:latest",
	); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("NewWithEngine(tag) error = %v, want validation.failed", err)
	}
}

func assertCreatePolicy(t *testing.T, options client.ContainerCreateOptions) {
	t.Helper()
	if options.Name != "" || options.Config == nil || options.HostConfig == nil {
		t.Fatalf("create options = %#v", options)
	}
	config := options.Config
	if config.Image != helperImage || config.User != "0" || !reflect.DeepEqual(config.Cmd, []string{helperArgument}) ||
		!config.AttachStdin || !config.AttachStdout || !config.AttachStderr || !config.OpenStdin ||
		!config.StdinOnce || config.Tty || config.WorkingDir != composehelper.WorkDirectory ||
		!config.NetworkDisabled || len(config.Env) != 0 || len(config.Labels) != 0 {
		t.Fatalf("container config = %#v", config)
	}
	host := options.HostConfig
	if host.NetworkMode != container.NetworkMode("none") || !host.ReadonlyRootfs || host.AutoRemove ||
		host.RestartPolicy.Name != container.RestartPolicyDisabled ||
		!reflect.DeepEqual([]string(host.CapDrop), []string{"ALL"}) ||
		!reflect.DeepEqual(host.SecurityOpt, []string{"no-new-privileges"}) ||
		!reflect.DeepEqual(host.Tmpfs, map[string]string{composehelper.WorkDirectory: helperWorkTmpfs}) {
		t.Fatalf("host config = %#v", host)
	}
	wantMounts := []mount.Mount{{
		Type: mount.TypeBind, Source: dockerSocketPath, Target: dockerSocketPath,
	}}
	if !reflect.DeepEqual(host.Mounts, wantMounts) {
		t.Fatalf("mounts = %#v, want %#v", host.Mounts, wantMounts)
	}
}

func helperRequest(t *testing.T) *agentpb.ComposeHelperRequest {
	t.Helper()
	yaml := []byte("services:\n  api:\n    image: registry.example/api@sha256:" +
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n")
	yamlHash := sha256.Sum256(yaml)
	planID := "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	artifactID := "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	serviceID := "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	stepID := "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: planID, RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, TargetId: serviceID,
		Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM,
			ProjectName: "groundplane-infra", CanonicalYaml: yaml, YamlSha256: yamlHash[:],
			Services: []*agentpb.ComposeService{{
				ServiceId: serviceID, ComposeName: "api", ExpectedReplicas: 1, HasHealthcheck: true,
				ExpectedLabels: []*agentpb.LabelPair{
					{Key: "com.groundplane.kind", Value: "service"},
					{Key: "com.groundplane.managed", Value: "true"},
					{Key: "com.groundplane.plan-id", Value: planID},
					{Key: "com.groundplane.render-generation", Value: "1"},
					{Key: "com.groundplane.service-id", Value: serviceID},
				},
			}},
		}},
		Steps: []*agentpb.ExecutionStep{{
			StepId: stepID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: artifactID, ServiceIds: []string{serviceID},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("seal plan: %v", err)
	}
	return &agentpb.ComposeHelperRequest{
		Schema:       composehelper.SchemaVersion,
		AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		TaskId:       "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		OperationId:  "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Plan:         plan, StepId: stepID, TimeoutSeconds: 30,
	}
}

func successfulResponse() *agentpb.ComposeHelperResponse {
	return &agentpb.ComposeHelperResponse{
		Schema:     composehelper.SchemaVersion,
		Outcome:    agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
	}
}

func multiplexedReader(stream stdcopy.StdType, value []byte) *bufio.Reader {
	var multiplexed bytes.Buffer
	header := make([]byte, 8)
	header[0] = byte(stream)
	binary.BigEndian.PutUint32(header[4:], uint32(len(value)))
	multiplexed.Write(header)
	multiplexed.Write(value)
	return bufio.NewReader(bytes.NewReader(multiplexed.Bytes()))
}

type fakeEngine struct {
	connection  *memoryConn
	reader      *bufio.Reader
	createCalls []client.ContainerCreateOptions
	attachIDs   []string
	startIDs    []string
	stopIDs     []string
	removeIDs   []string
	waitResult  chan container.WaitResponse
	waitError   chan error
	exitStatus  int64
	removeErr   error
}

func (*fakeEngine) ImageInspect(
	context.Context,
	string,
	...client.ImageInspectOption,
) (client.ImageInspectResult, error) {
	return client.ImageInspectResult{}, errors.New("unexpected image inspection")
}

func (*fakeEngine) ImagePull(context.Context, string, client.ImagePullOptions) (client.ImagePullResponse, error) {
	return nil, errors.New("unexpected image pull")
}

func newFakeEngine(t *testing.T, response *agentpb.ComposeHelperResponse) *fakeEngine {
	t.Helper()
	var framed bytes.Buffer
	if err := composehelper.WriteResponse(context.Background(), &framed, response); err != nil {
		t.Fatalf("frame response: %v", err)
	}
	var multiplexed bytes.Buffer
	header := make([]byte, 8)
	header[0] = byte(stdcopy.Stdout)
	binary.BigEndian.PutUint32(header[4:], uint32(framed.Len()))
	multiplexed.Write(header)
	multiplexed.Write(framed.Bytes())
	return &fakeEngine{
		connection: &memoryConn{}, reader: bufio.NewReader(bytes.NewReader(multiplexed.Bytes())),
		waitResult: make(chan container.WaitResponse, 1), waitError: make(chan error, 1),
	}
}

func (engine *fakeEngine) ContainerCreate(
	_ context.Context,
	options client.ContainerCreateOptions,
) (client.ContainerCreateResult, error) {
	engine.createCalls = append(engine.createCalls, options)
	return client.ContainerCreateResult{ID: "helper-id"}, nil
}

func (engine *fakeEngine) ContainerAttach(
	_ context.Context,
	id string,
	_ client.ContainerAttachOptions,
) (client.ContainerAttachResult, error) {
	engine.attachIDs = append(engine.attachIDs, id)
	return client.ContainerAttachResult{HijackedResponse: client.HijackedResponse{
		Conn: engine.connection, Reader: engine.reader,
	}}, nil
}

func (engine *fakeEngine) ContainerWait(
	context.Context,
	string,
	client.ContainerWaitOptions,
) client.ContainerWaitResult {
	return client.ContainerWaitResult{Result: engine.waitResult, Error: engine.waitError}
}

func (engine *fakeEngine) ContainerStart(
	_ context.Context,
	id string,
	_ client.ContainerStartOptions,
) (client.ContainerStartResult, error) {
	engine.startIDs = append(engine.startIDs, id)
	engine.waitResult <- container.WaitResponse{StatusCode: engine.exitStatus}
	return client.ContainerStartResult{}, nil
}

func (engine *fakeEngine) ContainerStop(
	_ context.Context,
	id string,
	_ client.ContainerStopOptions,
) (client.ContainerStopResult, error) {
	engine.stopIDs = append(engine.stopIDs, id)
	return client.ContainerStopResult{}, nil
}

func (engine *fakeEngine) ContainerRemove(
	_ context.Context,
	id string,
	_ client.ContainerRemoveOptions,
) (client.ContainerRemoveResult, error) {
	engine.removeIDs = append(engine.removeIDs, id)
	return client.ContainerRemoveResult{}, engine.removeErr
}

func (engine *fakeEngine) Close() error { return nil }

type memoryConn struct {
	written bytes.Buffer
}

func (connection *memoryConn) Read([]byte) (int, error) { return 0, io.EOF }
func (connection *memoryConn) Write(value []byte) (int, error) {
	return connection.written.Write(value)
}
func (connection *memoryConn) Close() error                     { return nil }
func (connection *memoryConn) CloseWrite() error                { return nil }
func (connection *memoryConn) LocalAddr() net.Addr              { return memoryAddress("local") }
func (connection *memoryConn) RemoteAddr() net.Addr             { return memoryAddress("remote") }
func (connection *memoryConn) SetDeadline(time.Time) error      { return nil }
func (connection *memoryConn) SetReadDeadline(time.Time) error  { return nil }
func (connection *memoryConn) SetWriteDeadline(time.Time) error { return nil }

type memoryAddress string

func (address memoryAddress) Network() string { return "memory" }
func (address memoryAddress) String() string  { return string(address) }
