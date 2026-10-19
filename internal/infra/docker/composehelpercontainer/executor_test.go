package composehelpercontainer

import (
	"bufio"
	"bytes"
	"context"
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
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const helperImage = "registry.example/groundplane-agent@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// TASK-31: the active frame transport must join output, deliver exact input and
// remove its helper by immutable Docker ID. Native permission decoding and
// operation-specific helper naming are separate boundaries.
func TestExecuteFrameDeliversBytesAndRemovesByImmutableID(t *testing.T) {
	expected := []byte("bounded helper response")
	engine := newFakeEngine(expected)
	executor, err := NewWithEngine(engine, helperImage)
	if err != nil {
		t.Fatal(err)
	}
	request := []byte("captured native request")
	response, err := executor.executeFrame(t.Context(), createOptions(helperImage, "release-helper", nil), request)
	if err != nil || !bytes.Equal(response, expected) {
		t.Fatalf("response=%q error=%v", response, err)
	}
	if len(engine.createCalls) != 1 {
		t.Fatalf("create calls=%d", len(engine.createCalls))
	}
	options := engine.createCalls[0]
	if options.Config.Image != helperImage || options.Config.WorkingDir != composehelper.WorkDirectory ||
		!options.HostConfig.ReadonlyRootfs || options.HostConfig.NetworkMode != "none" {
		t.Fatalf("helper lost isolation: %#v", options)
	}
	if !reflect.DeepEqual(engine.attachIDs, []string{"helper-id"}) ||
		!reflect.DeepEqual(engine.startIDs, []string{"helper-id"}) ||
		!reflect.DeepEqual(engine.removeIDs, []string{"helper-id"}) || len(engine.stopIDs) != 0 {
		t.Fatalf("lifecycle attach=%v start=%v stop=%v remove=%v",
			engine.attachIDs, engine.startIDs, engine.stopIDs, engine.removeIDs)
	}
	if !bytes.Equal(engine.connection.written.Bytes(), request) {
		t.Fatalf("attached request=%q", engine.connection.written.Bytes())
	}
}

// TASK-31: cleanup failure leaves the effect outcome unknown even after a
// successful helper response; callers must not accept that response as proof.
func TestExecuteFrameCleanupFailureTakesPrecedence(t *testing.T) {
	removeErr := errors.New("daemon refused removal")
	engine := newFakeEngine([]byte("successful response"))
	engine.removeErr = removeErr
	executor, err := NewWithEngine(engine, helperImage)
	if err != nil {
		t.Fatal(err)
	}
	response, err := executor.executeFrame(
		t.Context(),
		createOptions(helperImage, "release-helper", nil),
		[]byte("request"),
	)
	if response != nil || !errors.Is(err, removeErr) || !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("response=%q error=%v, want cleanup internal failure", response, err)
	}
}

// TASK-31: a process failure reports bounded stderr instead of a successful
// stdout frame, while still reclaiming the exact helper.
func TestExecuteFrameReportsHelperFailure(t *testing.T) {
	engine := newFakeEngine(nil)
	engine.reader = multiplexedReader(stdcopy.Stderr, []byte("docker compose: network is missing\n"))
	engine.exitStatus = 17
	executor, err := NewWithEngine(engine, helperImage)
	if err != nil {
		t.Fatal(err)
	}
	response, err := executor.executeFrame(
		t.Context(),
		createOptions(helperImage, "release-helper", nil),
		[]byte("request"),
	)
	if response != nil || !errors.Is(err, errs.New(errs.KindInternal, "")) ||
		!reflect.DeepEqual(engine.removeIDs, []string{"helper-id"}) {
		t.Fatalf("response=%q error=%v removes=%v", response, err, engine.removeIDs)
	}
	for _, want := range []string{"exit code 17", "docker compose: network is missing"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error=%q, want %q", err, want)
		}
	}
}

// TASK-31: executable helper authority cannot resolve through a mutable tag.
func TestNewWithEngineRejectsUnpinnedImage(t *testing.T) {
	if _, err := NewWithEngine(newFakeEngine(nil), "registry.example/groundplane-agent:latest"); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("NewWithEngine(tag) error=%v", err)
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

func (*fakeEngine) ContainerInspect(
	context.Context,
	string,
	client.ContainerInspectOptions,
) (client.ContainerInspectResult, error) {
	return client.ContainerInspectResult{}, errors.New(
		"unexpected recovery inspection during forward Compose execution",
	)
}

func newFakeEngine(response []byte) *fakeEngine {
	return &fakeEngine{
		connection: &memoryConn{}, reader: multiplexedReader(stdcopy.Stdout, response),
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
