package managedconfighelpercontainer

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

func TestNativeValidatorRequiresExactSuccessfulExitAndCleanup(t *testing.T) {
	// Rationale: unlike the long-running resolver probe, a native file validator
	// succeeds only at zero exit plus complete output and owned-container cleanup.
	for _, test := range []struct {
		name      string
		exit      int64
		cleanup   error
		cancel    bool
		wantError bool
	}{
		{name: "zero exit"},
		{name: "rejected native file", exit: 1, wantError: true},
		{name: "cleanup failure", cleanup: errors.New("cleanup denied"), wantError: true},
		{name: "cancel after input", cancel: true, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			content := []byte("complete candidate config\n")
			engine := newNativeValidatorEngine(content)
			engine.exitCode, engine.removeErr = test.exit, test.cleanup
			if test.cancel {
				engine.cancel = cancel
			}
			err := mustExecutor(
				t,
				engine,
			).ValidateToExit(ctx, validatorTestImage(), []string{"validator", "--config", "-"}, content)
			if (err != nil) != test.wantError || !engine.removed {
				t.Fatalf("ValidateToExit error=%v, removed=%v", err, engine.removed)
			}
			if string(engine.received) != string(content) {
				t.Fatalf("received input=%q", engine.received)
			}
			options := engine.options
			if !reflect.DeepEqual(options.Config.Entrypoint, []string{"validator"}) ||
				!reflect.DeepEqual(options.Config.Cmd, []string{"--config", "-"}) ||
				options.Config.User != "65534:65534" || !options.Config.NetworkDisabled ||
				!options.HostConfig.ReadonlyRootfs || options.HostConfig.NetworkMode != "none" ||
				len(options.HostConfig.Mounts) != 0 || len(options.HostConfig.Binds) != 0 ||
				!reflect.DeepEqual(
					options.HostConfig.CapAdd,
					[]string{"NET_BIND_SERVICE"},
				) || options.HostConfig.LogConfig.Type != "none" ||
				len(options.HostConfig.Tmpfs) != 1 || options.HostConfig.Tmpfs["/tmp"] == "" {
				t.Fatalf("native validator isolation=%+v / %+v", options.Config, options.HostConfig)
			}
		})
	}
}

type nativeValidatorEngine struct {
	fakeEngine
	options    client.ContainerCreateOptions
	input      []byte
	received   []byte
	peer       net.Conn
	connection *nativeValidatorConnection
	wait       chan container.WaitResponse
	exitCode   int64
	removeErr  error
	removed    bool
	cancel     context.CancelFunc
	finished   chan struct{}
}

func newNativeValidatorEngine(content []byte) *nativeValidatorEngine {
	return &nativeValidatorEngine{
		fakeEngine: fakeEngine{imageID: pinnedValidatorID, createdID: "native-validator"},
		input:      content, wait: make(chan container.WaitResponse, 1), finished: make(chan struct{}),
	}
}

func (engine *nativeValidatorEngine) ContainerCreate(
	_ context.Context,
	options client.ContainerCreateOptions,
) (client.ContainerCreateResult, error) {
	engine.options = options
	return client.ContainerCreateResult{ID: engine.createdID}, nil
}

func (engine *nativeValidatorEngine) ContainerAttach(
	context.Context,
	string,
	client.ContainerAttachOptions,
) (client.ContainerAttachResult, error) {
	conn, peer := net.Pipe()
	engine.peer = peer
	engine.connection = &nativeValidatorConnection{Conn: conn, writeClosed: make(chan struct{})}
	return client.ContainerAttachResult{HijackedResponse: client.HijackedResponse{
		Conn: engine.connection, Reader: bufio.NewReader(conn),
	}}, nil
}

func (engine *nativeValidatorEngine) ContainerWait(
	context.Context,
	string,
	client.ContainerWaitOptions,
) client.ContainerWaitResult {
	return client.ContainerWaitResult{Result: engine.wait, Error: make(chan error)}
}

func (engine *nativeValidatorEngine) ContainerStart(
	context.Context,
	string,
	client.ContainerStartOptions,
) (client.ContainerStartResult, error) {
	go func() {
		defer close(engine.finished)
		engine.received = make([]byte, len(engine.input))
		_, err := io.ReadFull(engine.peer, engine.received)
		if err == nil {
			<-engine.connection.writeClosed
		}
		if engine.cancel != nil {
			engine.cancel()
		}
		_ = engine.peer.Close()
		engine.wait <- container.WaitResponse{StatusCode: engine.exitCode}
	}()
	return client.ContainerStartResult{}, nil
}

func (engine *nativeValidatorEngine) ContainerRemove(
	context.Context,
	string,
	client.ContainerRemoveOptions,
) (client.ContainerRemoveResult, error) {
	engine.removed = true
	if engine.peer != nil {
		_ = engine.peer.Close()
	}
	<-engine.finished
	return client.ContainerRemoveResult{}, engine.removeErr
}

type nativeValidatorConnection struct {
	net.Conn
	writeClosed chan struct{}
	once        sync.Once
}

func (connection *nativeValidatorConnection) CloseWrite() error {
	connection.once.Do(func() { close(connection.writeClosed) })
	return nil
}
