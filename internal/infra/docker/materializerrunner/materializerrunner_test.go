package materializerrunner

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	testImage = "registry.example/groundplane-agent@sha256:" +
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	testVolumeDir = "/infra/vol/" +
		"tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
		"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
		"env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// Rationale: the helper is safe only when every Docker field matches the
// accepted least-privilege policy and plaintext travels solely through stdin.
func TestRunUsesExactHelperPolicyAndLifecycle(t *testing.T) {
	t.Parallel()

	stream := newTrackingStream([]byte("framed-secret-request"))
	connection := &recordingConn{}
	fake := &fakeEngine{attachConn: connection, waitStatus: 0}
	runner := newTestRunner(fake)

	if err := runner.Run(context.Background(), Request{
		VolumeDir: testVolumeDir, Stream: stream,
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got, want := fake.callNames(), []string{
		"create",
		"attach",
		"start",
		"wait",
		"remove",
	}; !equalStrings(got, want) {
		t.Fatalf("Docker calls = %v, want %v", got, want)
	}
	assertCreatePolicy(t, fake.createOptions)
	if fake.attachID != "helper-id" ||
		fake.attachOptions != (client.ContainerAttachOptions{Stream: true, Stdin: true}) {
		t.Fatalf("attach = id %q options %#v", fake.attachID, fake.attachOptions)
	}
	if fake.startID != "helper-id" {
		t.Fatalf("start id = %q, want helper-id", fake.startID)
	}
	if fake.waitID != "helper-id" || fake.waitOptions.Condition != container.WaitConditionNotRunning {
		t.Fatalf("wait = id %q options %#v", fake.waitID, fake.waitOptions)
	}
	if fake.removeID != "helper-id" || !fake.removeOptions.Force || fake.removeOptions.RemoveVolumes {
		t.Fatalf("remove = id %q options %#v", fake.removeID, fake.removeOptions)
	}
	if fake.removeContextErr != nil {
		t.Fatalf("remove context error = %v", fake.removeContextErr)
	}
	if got := connection.Bytes(); string(got) != "framed-secret-request" {
		t.Fatalf("helper stdin = %q", got)
	}
	if !connection.WriteClosed() || !connection.Closed() {
		t.Fatal("helper attachment was not half-closed and closed")
	}
	if stream.CloseCalls() != 1 {
		t.Fatalf("stream close calls = %d, want 1", stream.CloseCalls())
	}
}

// Rationale: an impossible image, environment path, or missing owned stream
// must fail before the Docker socket can amplify the invalid request.
func TestRunRejectsInvalidRequestBeforeDocker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		volumeDir string
		stream    io.ReadCloser
	}{
		{name: "relative path", volumeDir: "infra/vol/tnt/prj/env"},
		{name: "trailing slash", volumeDir: testVolumeDir + "/"},
		{name: "double separator", volumeDir: "/infra//vol/tnt/prj/env"},
		{name: "traversal", volumeDir: testVolumeDir + "/../other"},
		{name: "wrong root", volumeDir: "/var/lib/groundplane/vol/tnt/prj/env"},
		{
			name: "wrong tenant kind",
			volumeDir: "/infra/vol/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
				"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
		{
			name: "wrong project kind",
			volumeDir: "/infra/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
				"env_01ARZ3NDEKTSV4RRFFQ69G5FAV/env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
		{
			name: "wrong environment kind",
			volumeDir: "/infra/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
				"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
		{name: "missing stream", volumeDir: testVolumeDir, stream: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			stream := test.stream
			if stream == nil && test.name != "missing stream" {
				stream = newTrackingStream(nil)
			}
			fake := &fakeEngine{}
			err := newTestRunner(fake).Run(context.Background(), Request{
				VolumeDir: test.volumeDir, Stream: stream,
			})
			if !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("Run() error = %v, want internal", err)
			}
			if len(fake.callNames()) != 0 {
				t.Fatalf("Docker calls = %v, want none", fake.callNames())
			}
			if tracked, ok := stream.(*trackingStream); ok && tracked.CloseCalls() != 1 {
				t.Fatalf("stream close calls = %d, want 1", tracked.CloseCalls())
			}
		})
	}
}

// Rationale: image identity belongs to the runner construction boundary and
// cannot be selected or weakened by an individual materialization request.
func TestRunRejectsInvalidOwnedImageBeforeDocker(t *testing.T) {
	t.Parallel()

	stream := newTrackingStream(nil)
	fake := &fakeEngine{}
	runner := newTestRunner(fake)
	runner.image = "registry.example/groundplane-agent:latest"
	err := runner.Run(context.Background(), Request{VolumeDir: testVolumeDir, Stream: stream})
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("Run() error = %v, want internal", err)
	}
	if len(fake.callNames()) != 0 || stream.CloseCalls() != 1 {
		t.Fatalf("Docker calls = %v, stream closes = %d", fake.callNames(), stream.CloseCalls())
	}
}

// Rationale: every failure after Docker returns an immutable helper id must
// force explicit removal rather than leave a plaintext-bearing helper behind.
func TestRunRemovesCreatedHelperOnEveryFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		configure   func(*fakeEngine, *trackingStream, *recordingConn)
		wantCause   error
		wantStarted bool
	}{
		{
			name: "create returns id and error",
			configure: func(fake *fakeEngine, _ *trackingStream, _ *recordingConn) {
				fake.createErr = errCreate
			},
			wantCause: errCreate,
		},
		{
			name: "attach",
			configure: func(fake *fakeEngine, _ *trackingStream, _ *recordingConn) {
				fake.attachErr = errAttach
			},
			wantCause: errAttach,
		},
		{
			name: "start",
			configure: func(fake *fakeEngine, _ *trackingStream, _ *recordingConn) {
				fake.startErr = errStart
			},
			wantCause: errStart,
		},
		{
			name: "stream read",
			configure: func(_ *fakeEngine, stream *trackingStream, _ *recordingConn) {
				stream.readErr = errStream
			},
			wantCause:   errStream,
			wantStarted: true,
		},
		{
			name: "close stdin",
			configure: func(_ *fakeEngine, _ *trackingStream, connection *recordingConn) {
				connection.closeWriteErr = errCloseWrite
			},
			wantCause:   errCloseWrite,
			wantStarted: true,
		},
		{
			name: "wait",
			configure: func(fake *fakeEngine, _ *trackingStream, _ *recordingConn) {
				fake.waitErr = errWait
			},
			wantCause:   errWait,
			wantStarted: true,
		},
		{
			name: "wait response error",
			configure: func(fake *fakeEngine, _ *trackingStream, _ *recordingConn) {
				fake.waitResponseError = true
			},
			wantStarted: true,
		},
		{
			name: "nonzero exit",
			configure: func(fake *fakeEngine, _ *trackingStream, _ *recordingConn) {
				fake.waitStatus = 17
			},
			wantStarted: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			stream := newTrackingStream([]byte("framed"))
			connection := &recordingConn{}
			fake := &fakeEngine{attachConn: connection}
			test.configure(fake, stream, connection)

			err := newTestRunner(fake).Run(context.Background(), Request{
				VolumeDir: testVolumeDir, Stream: stream,
			})
			if !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("Run() error = %v, want internal", err)
			}
			if test.wantCause != nil && !errors.Is(err, test.wantCause) {
				t.Fatalf("Run() error = %v, want cause %v", err, test.wantCause)
			}
			if fake.removeID != "helper-id" || !fake.removeOptions.Force {
				t.Fatalf("remove = id %q options %#v", fake.removeID, fake.removeOptions)
			}
			if test.wantStarted && fake.startID != "helper-id" {
				t.Fatalf("start id = %q, want helper-id", fake.startID)
			}
			if stream.CloseCalls() != 1 {
				t.Fatalf("stream close calls = %d, want 1", stream.CloseCalls())
			}
		})
	}
}

// Rationale: a cleanup failure can leave plaintext or ambiguous helper state,
// so it must replace an earlier operation failure rather than be discarded.
func TestRunCleanupFailureTakesPrecedence(t *testing.T) {
	t.Parallel()

	stream := newTrackingStream([]byte("framed"))
	fake := &fakeEngine{
		attachConn: &recordingConn{},
		startErr:   errStart,
		removeErr:  errRemove,
	}
	err := newTestRunner(fake).Run(context.Background(), Request{
		VolumeDir: testVolumeDir, Stream: stream,
	})
	if !errors.Is(err, errRemove) || errors.Is(err, errStart) {
		t.Fatalf("Run() error = %v, want only removal failure precedence", err)
	}
}

// Rationale: attachment shutdown is part of plaintext cleanup; its failure
// must be retained, and a concurrent removal failure must remain discoverable.
func TestRunPreservesAttachmentAndRemovalCleanupFailures(t *testing.T) {
	t.Parallel()

	t.Run("attachment close", func(t *testing.T) {
		t.Parallel()
		connection := &recordingConn{closeErr: errAttachClose}
		fake := &fakeEngine{attachConn: connection}
		err := newTestRunner(fake).Run(context.Background(), Request{
			VolumeDir: testVolumeDir, Stream: newTrackingStream([]byte("framed")),
		})
		if !errors.Is(err, errs.New(errs.KindInternal, "")) || !errors.Is(err, errAttachClose) {
			t.Fatalf("Run() error = %v, want internal attachment-close failure", err)
		}
		if fake.removeID != "helper-id" {
			t.Fatalf("remove id = %q, want helper-id", fake.removeID)
		}
	})

	t.Run("attachment close and removal", func(t *testing.T) {
		t.Parallel()
		connection := &recordingConn{closeErr: errAttachClose}
		fake := &fakeEngine{attachConn: connection, removeErr: errRemove}
		err := newTestRunner(fake).Run(context.Background(), Request{
			VolumeDir: testVolumeDir, Stream: newTrackingStream([]byte("framed")),
		})
		if !errors.Is(err, errs.New(errs.KindInternal, "")) ||
			!errors.Is(err, errAttachClose) ||
			!errors.Is(err, errRemove) {
			t.Fatalf("Run() error = %v, want joined attachment and removal failures", err)
		}
	})
}

// Rationale: a dependency can return a context sentinel without the task
// context being cancelled; it remains a private Internal cause in that case.
func TestRunWrapsLiveContextDependencyCancellationSentinels(t *testing.T) {
	t.Parallel()

	for _, dependencyErr := range []error{context.Canceled, context.DeadlineExceeded} {
		dependencyErr := dependencyErr
		t.Run(dependencyErr.Error(), func(t *testing.T) {
			t.Parallel()
			fake := &fakeEngine{attachConn: &recordingConn{}, startErr: dependencyErr}
			err := newTestRunner(fake).Run(context.Background(), Request{
				VolumeDir: testVolumeDir, Stream: newTrackingStream([]byte("framed")),
			})
			if err == dependencyErr || !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("Run() error = %v, want wrapped internal dependency sentinel", err)
			}
			if !errors.Is(err, dependencyErr) {
				t.Fatalf("Run() error = %v, want private cause %v", err, dependencyErr)
			}
		})
	}
}

// Rationale: even after a zero helper exit, both stream ownership cleanup and
// explicit helper removal are part of success rather than best-effort work.
func TestRunRequiresSuccessfulStreamCloseAndRemoval(t *testing.T) {
	t.Parallel()

	t.Run("stream close", func(t *testing.T) {
		t.Parallel()
		stream := newTrackingStream([]byte("framed"))
		stream.closeErr = errStreamClose
		fake := &fakeEngine{attachConn: &recordingConn{}}
		err := newTestRunner(fake).Run(context.Background(), Request{
			VolumeDir: testVolumeDir, Stream: stream,
		})
		if !errors.Is(err, errStreamClose) || fake.removeID != "helper-id" {
			t.Fatalf("Run() error = %v, remove id = %q", err, fake.removeID)
		}
	})

	t.Run("remove", func(t *testing.T) {
		t.Parallel()
		fake := &fakeEngine{attachConn: &recordingConn{}, removeErr: errRemove}
		err := newTestRunner(fake).Run(context.Background(), Request{
			VolumeDir: testVolumeDir, Stream: newTrackingStream([]byte("framed")),
		})
		if !errors.Is(err, errRemove) {
			t.Fatalf("Run() error = %v, want removal failure", err)
		}
	})
}

// Rationale: cancellation must actively unblock the owned plaintext stream,
// close the attachment, and still remove the helper with a live context.
func TestRunCancellationUnblocksStreamAndForcesCleanup(t *testing.T) {
	t.Parallel()

	stream := newBlockingStream()
	connection := &recordingConn{}
	fake := &fakeEngine{attachConn: connection}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- newTestRunner(fake).Run(ctx, Request{
			VolumeDir: testVolumeDir, Stream: stream,
		})
	}()

	select {
	case <-stream.readStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not begin reading the request stream")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after cancellation")
	}
	if stream.CloseCalls() != 1 {
		t.Fatalf("stream close calls = %d, want 1", stream.CloseCalls())
	}
	if !connection.Closed() {
		t.Fatal("attachment remained open after cancellation")
	}
	if fake.removeID != "helper-id" || fake.removeContextErr != nil {
		t.Fatalf("remove id = %q context error = %v", fake.removeID, fake.removeContextErr)
	}
}

// Rationale: cancellation during Docker wait must force removal before
// cancelling the independent wait request and drain the SDK's unbuffered send.
func TestRunCancellationDrainsDockerWait(t *testing.T) {
	t.Parallel()

	waitStarted := make(chan struct{})
	waitForwarded := make(chan struct{})
	fake := &fakeEngine{
		attachConn:         &recordingConn{},
		waitOnCancellation: true,
		waitStarted:        waitStarted,
		waitForwarded:      waitForwarded,
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- newTestRunner(fake).Run(ctx, Request{
			VolumeDir: testVolumeDir,
			Stream:    newTrackingStream([]byte("framed")),
		})
	}()

	select {
	case <-waitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not begin Docker wait")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after wait cancellation")
	}
	select {
	case <-waitForwarded:
	case <-time.After(2 * time.Second):
		t.Fatal("Docker wait sender was not drained")
	}
	if got := fake.callNames(); !containsOrdered(got, "wait", "remove", "remove") {
		t.Fatalf("Docker calls = %v, want wait then cancellation and final removals", got)
	}
}

// Rationale: helper absence is the desired cleanup postcondition, including
// when Docker reports that another terminal path already removed it.
func TestRunTreatsNotFoundRemovalAsSuccessful(t *testing.T) {
	t.Parallel()

	fake := &fakeEngine{attachConn: &recordingConn{}, removeErr: containerderrdefs.ErrNotFound}
	if err := newTestRunner(fake).Run(context.Background(), Request{
		VolumeDir: testVolumeDir,
		Stream:    newTrackingStream([]byte("framed")),
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

// Rationale: Docker cleanup cannot extend a task forever; the runner's
// injected cleanup budget must convert a stuck removal into an Internal error.
func TestRunBoundsCleanupWithInjectedTimeout(t *testing.T) {
	t.Parallel()

	fake := &fakeEngine{
		attachConn:           &recordingConn{},
		startErr:             errStart,
		removeWaitForContext: true,
	}
	runner := newTestRunner(fake)
	runner.cleanupTimeout = 10 * time.Millisecond
	err := runner.Run(context.Background(), Request{
		VolumeDir: testVolumeDir,
		Stream:    newTrackingStream([]byte("framed")),
	})
	if !errors.Is(err, errs.New(errs.KindInternal, "")) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want bounded internal cleanup timeout", err)
	}
}

func assertCreatePolicy(t *testing.T, options client.ContainerCreateOptions) {
	t.Helper()
	if options.Name != "" || options.NetworkingConfig != nil || options.Platform != nil || options.Image != "" {
		t.Fatalf("create options contain an unexpected outer field: %#v", options)
	}
	config := options.Config
	if config == nil || config.Image != testImage || config.User != "0" {
		t.Fatalf("container config = %#v", config)
	}
	if !config.AttachStdin || !config.OpenStdin || !config.StdinOnce || config.AttachStdout || config.AttachStderr {
		t.Fatalf("stdin policy = %#v", config)
	}
	if !equalStrings(config.Cmd, []string{helperCommand}) ||
		!equalStrings(config.Entrypoint, []string{helperEntrypoint}) {
		t.Fatalf("helper command = cmd %#v entrypoint %#v", config.Cmd, config.Entrypoint)
	}
	if config.Env != nil || config.Labels != nil || !config.NetworkDisabled {
		t.Fatalf("request-bearing config surface is open: env=%#v labels=%#v network_disabled=%t",
			config.Env, config.Labels, config.NetworkDisabled)
	}
	if config.Hostname != "" || config.Domainname != "" || len(config.ExposedPorts) != 0 || config.Tty ||
		config.Healthcheck != nil || config.ArgsEscaped || len(config.Volumes) != 0 || config.WorkingDir != "" ||
		len(config.OnBuild) != 0 || config.StopSignal != "" || config.StopTimeout != nil || len(config.Shell) != 0 {
		t.Fatalf("forbidden container config surface is configured: %#v", config)
	}
	host := options.HostConfig
	if host == nil || host.NetworkMode != container.NetworkMode("none") || !host.ReadonlyRootfs || host.Privileged {
		t.Fatalf("host isolation policy = %#v", host)
	}
	if host.RestartPolicy.Name != container.RestartPolicyDisabled || host.RestartPolicy.MaximumRetryCount != 0 {
		t.Fatalf("restart policy = %#v", host.RestartPolicy)
	}
	if !equalStrings(host.CapDrop, []string{"ALL"}) || !equalStrings(host.CapAdd, []string{"CHOWN", "FOWNER"}) {
		t.Fatalf("capabilities = add %#v drop %#v", host.CapAdd, host.CapDrop)
	}
	if !equalStrings(host.SecurityOpt, []string{"no-new-privileges=true"}) {
		t.Fatalf("security options = %#v", host.SecurityOpt)
	}
	if host.LogConfig.Type != "none" || len(host.LogConfig.Config) != 0 {
		t.Fatalf("log config = %#v, want disabled", host.LogConfig)
	}
	if len(host.Mounts) != 1 {
		t.Fatalf("mounts = %#v, want exactly one", host.Mounts)
	}
	mounted := host.Mounts[0]
	if mounted.Type != mount.TypeBind || mounted.Source != testVolumeDir || mounted.Target != helperMountPath ||
		mounted.ReadOnly || mounted.Consistency != "" || mounted.BindOptions == nil ||
		mounted.BindOptions.Propagation != mount.PropagationRPrivate || mounted.BindOptions.NonRecursive ||
		mounted.BindOptions.CreateMountpoint || mounted.BindOptions.ReadOnlyNonRecursive ||
		mounted.BindOptions.ReadOnlyForceRecursive || mounted.VolumeOptions != nil || mounted.ImageOptions != nil ||
		mounted.TmpfsOptions != nil || mounted.ClusterOptions != nil {
		t.Fatalf("materialization mount = %#v", mounted)
	}
	if len(host.Binds) != 0 || host.ContainerIDFile != "" || len(host.PortBindings) != 0 || host.AutoRemove ||
		host.VolumeDriver != "" || len(host.VolumesFrom) != 0 || host.ConsoleSize != [2]uint{} ||
		len(host.Annotations) != 0 || host.CgroupnsMode != "" || len(host.DNS) != 0 ||
		len(host.DNSOptions) != 0 || len(host.DNSSearch) != 0 || len(host.ExtraHosts) != 0 ||
		len(host.GroupAdd) != 0 || host.IpcMode != "" || host.Cgroup != "" || len(host.Links) != 0 ||
		host.OomScoreAdj != 0 || host.PidMode != "" || host.PublishAllPorts || len(host.StorageOpt) != 0 ||
		len(host.Tmpfs) != 0 || host.UTSMode != "" || host.UsernsMode != "" || host.ShmSize != 0 ||
		len(host.Sysctls) != 0 || host.Runtime != "" || host.Isolation != "" || len(host.MaskedPaths) != 0 ||
		len(host.ReadonlyPaths) != 0 || host.Init != nil {
		t.Fatalf("additional host access is configured: %#v", host)
	}
	assertNoResourcePrivileges(t, host.Resources)
}

func assertNoResourcePrivileges(t *testing.T, resources container.Resources) {
	t.Helper()
	if resources.CPUShares != 0 || resources.Memory != 0 || resources.NanoCPUs != 0 ||
		resources.CgroupParent != "" || resources.BlkioWeight != 0 || len(resources.BlkioWeightDevice) != 0 ||
		len(resources.BlkioDeviceReadBps) != 0 || len(resources.BlkioDeviceWriteBps) != 0 ||
		len(resources.BlkioDeviceReadIOps) != 0 || len(resources.BlkioDeviceWriteIOps) != 0 ||
		resources.CPUPeriod != 0 || resources.CPUQuota != 0 || resources.CPURealtimePeriod != 0 ||
		resources.CPURealtimeRuntime != 0 || resources.CpusetCpus != "" || resources.CpusetMems != "" ||
		len(resources.Devices) != 0 || len(resources.DeviceCgroupRules) != 0 || len(resources.DeviceRequests) != 0 ||
		resources.MemoryReservation != 0 || resources.MemorySwap != 0 || resources.MemorySwappiness != nil ||
		resources.OomKillDisable != nil || resources.PidsLimit != nil || len(resources.Ulimits) != 0 ||
		resources.CPUCount != 0 || resources.CPUPercent != 0 || resources.IOMaximumIOps != 0 ||
		resources.IOMaximumBandwidth != 0 {
		t.Fatalf("resource privilege surface is configured: %#v", resources)
	}
}

var (
	errCreate      = errors.New("create failed")
	errAttach      = errors.New("attach failed")
	errStart       = errors.New("start failed")
	errStream      = errors.New("stream failed")
	errCloseWrite  = errors.New("close write failed")
	errWait        = errors.New("wait failed")
	errRemove      = errors.New("remove failed")
	errStreamClose = errors.New("stream close failed")
	errAttachClose = errors.New("attachment close failed")
)

type fakeEngine struct {
	mu                   sync.Mutex
	calls                []string
	createOptions        client.ContainerCreateOptions
	createID             string
	createErr            error
	attachID             string
	attachOptions        client.ContainerAttachOptions
	attachConn           net.Conn
	attachErr            error
	startID              string
	startErr             error
	waitID               string
	waitOptions          client.ContainerWaitOptions
	waitStatus           int64
	waitErr              error
	waitResponseError    bool
	waitOnCancellation   bool
	waitStarted          chan struct{}
	waitForwarded        chan struct{}
	removeID             string
	removeOptions        client.ContainerRemoveOptions
	removeContextErr     error
	removeErr            error
	removeWaitForContext bool
	closeErr             error
}

func (f *fakeEngine) ContainerCreate(
	_ context.Context,
	options client.ContainerCreateOptions,
) (client.ContainerCreateResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "create")
	f.createOptions = options
	id := f.createID
	if id == "" {
		id = "helper-id"
	}
	return client.ContainerCreateResult{ID: id}, f.createErr
}

func (f *fakeEngine) ContainerAttach(
	_ context.Context,
	id string,
	options client.ContainerAttachOptions,
) (client.ContainerAttachResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "attach")
	f.attachID = id
	f.attachOptions = options
	return client.ContainerAttachResult{HijackedResponse: client.NewHijackedResponse(f.attachConn, "")}, f.attachErr
}

func (f *fakeEngine) ContainerStart(
	_ context.Context,
	id string,
	_ client.ContainerStartOptions,
) (client.ContainerStartResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "start")
	f.startID = id
	return client.ContainerStartResult{}, f.startErr
}

func (f *fakeEngine) ContainerWait(
	ctx context.Context,
	id string,
	options client.ContainerWaitOptions,
) client.ContainerWaitResult {
	f.mu.Lock()
	f.calls = append(f.calls, "wait")
	f.waitID = id
	f.waitOptions = options
	status := f.waitStatus
	waitErr := f.waitErr
	waitResponseError := f.waitResponseError
	waitOnCancellation := f.waitOnCancellation
	waitStarted := f.waitStarted
	waitForwarded := f.waitForwarded
	f.mu.Unlock()

	if waitOnCancellation {
		results := make(chan container.WaitResponse)
		errorsChannel := make(chan error)
		if waitStarted != nil {
			close(waitStarted)
		}
		go func() {
			<-ctx.Done()
			errorsChannel <- ctx.Err()
			if waitForwarded != nil {
				close(waitForwarded)
			}
		}()
		return client.ContainerWaitResult{Result: results, Error: errorsChannel}
	}

	results := make(chan container.WaitResponse, 1)
	errorsChannel := make(chan error, 1)
	if waitErr != nil {
		errorsChannel <- waitErr
	} else {
		response := container.WaitResponse{StatusCode: status}
		if waitResponseError {
			response.Error = &container.WaitExitError{Message: "private daemon detail"}
		}
		results <- response
	}
	return client.ContainerWaitResult{Result: results, Error: errorsChannel}
}

func (f *fakeEngine) ContainerRemove(
	ctx context.Context,
	id string,
	options client.ContainerRemoveOptions,
) (client.ContainerRemoveResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "remove")
	f.removeID = id
	f.removeOptions = options
	f.removeContextErr = ctx.Err()
	removeErr := f.removeErr
	waitForContext := f.removeWaitForContext
	f.mu.Unlock()
	if waitForContext {
		<-ctx.Done()
		return client.ContainerRemoveResult{}, ctx.Err()
	}
	return client.ContainerRemoveResult{}, removeErr
}

func (f *fakeEngine) Close() error {
	return f.closeErr
}

func (f *fakeEngine) callNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func newTestRunner(client engineClient) *Runner {
	return &Runner{client: client, image: testImage, cleanupTimeout: time.Second}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func containsOrdered(values []string, expected ...string) bool {
	next := 0
	for _, value := range values {
		if next < len(expected) && value == expected[next] {
			next++
		}
	}
	return next == len(expected)
}

type trackingStream struct {
	mu         sync.Mutex
	reader     *bytes.Reader
	readErr    error
	closeErr   error
	closeCalls int
}

func newTrackingStream(content []byte) *trackingStream {
	return &trackingStream{reader: bytes.NewReader(content)}
}

func (s *trackingStream) Read(buffer []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readErr != nil {
		return 0, s.readErr
	}
	return s.reader.Read(buffer)
}

func (s *trackingStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCalls++
	return s.closeErr
}

func (s *trackingStream) CloseCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCalls
}

type blockingStream struct {
	readStarted chan struct{}
	closed      chan struct{}
	readOnce    sync.Once
	closeOnce   sync.Once
	mu          sync.Mutex
	closeCalls  int
}

func newBlockingStream() *blockingStream {
	return &blockingStream{readStarted: make(chan struct{}), closed: make(chan struct{})}
}

func (s *blockingStream) Read([]byte) (int, error) {
	s.readOnce.Do(func() { close(s.readStarted) })
	<-s.closed
	return 0, errors.New("stream closed")
}

func (s *blockingStream) Close() error {
	s.mu.Lock()
	s.closeCalls++
	s.mu.Unlock()
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func (s *blockingStream) CloseCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCalls
}

type recordingConn struct {
	mu            sync.Mutex
	buffer        bytes.Buffer
	closed        bool
	writeClosed   bool
	closeErr      error
	closeWriteErr error
}

func (c *recordingConn) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (c *recordingConn) Write(content []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.writeClosed {
		return 0, net.ErrClosed
	}
	return c.buffer.Write(content)
}

func (c *recordingConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return c.closeErr
}

func (c *recordingConn) CloseWrite() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeWriteErr != nil {
		return c.closeWriteErr
	}
	c.writeClosed = true
	return nil
}

func (c *recordingConn) LocalAddr() net.Addr              { return fakeAddr("local") }
func (c *recordingConn) RemoteAddr() net.Addr             { return fakeAddr("remote") }
func (c *recordingConn) SetDeadline(time.Time) error      { return nil }
func (c *recordingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *recordingConn) SetWriteDeadline(time.Time) error { return nil }

func (c *recordingConn) Bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return bytes.Clone(c.buffer.Bytes())
}

func (c *recordingConn) Closed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *recordingConn) WriteClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeClosed
}

type fakeAddr string

func (a fakeAddr) Network() string { return string(a) }
func (a fakeAddr) String() string  { return string(a) }
