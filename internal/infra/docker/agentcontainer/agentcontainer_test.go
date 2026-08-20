package agentcontainer

import (
	"context"
	"errors"
	"reflect"
	"testing"

	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const testDigest = "registry.example/groundplane-agent@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestReconcileCreatesAndStartsMissingContainerWithExactPolicy(t *testing.T) {
	t.Parallel()

	desired := testDesired()
	fake := &fakeEngine{
		inspectErr:   containerderrdefs.ErrNotFound,
		createResult: client.ContainerCreateResult{ID: "created-id"},
	}
	manager := &Manager{client: fake}

	result, err := manager.Reconcile(context.Background(), desired)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Action != ActionCreated || !result.State.Running || result.State.ID != "created-id" {
		t.Fatalf("Reconcile() result = %#v, want created running container", result)
	}
	if len(fake.createCalls) != 1 {
		t.Fatalf("create calls = %d, want 1", len(fake.createCalls))
	}
	assertCreatePolicy(t, fake.createCalls[0], desired)
	if !reflect.DeepEqual(fake.startCalls, []string{"created-id"}) {
		t.Fatalf("start calls = %v, want [created-id]", fake.startCalls)
	}
}

func TestReconcileMatchingRunningContainerIsNoOp(t *testing.T) {
	t.Parallel()

	desired := testDesired()
	fake := &fakeEngine{inspectResult: matchingInspect(desired, true)}
	result, err := (&Manager{client: fake}).Reconcile(context.Background(), desired)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Action != ActionNone || !result.State.Running {
		t.Fatalf("Reconcile() result = %#v, want running no-op", result)
	}
	assertNoMutations(t, fake)
}

func TestReconcileStartsMatchingStoppedContainer(t *testing.T) {
	t.Parallel()

	desired := testDesired()
	fake := &fakeEngine{inspectResult: matchingInspect(desired, false)}
	result, err := (&Manager{client: fake}).Reconcile(context.Background(), desired)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Action != ActionStarted || !result.State.Running {
		t.Fatalf("Reconcile() result = %#v, want started", result)
	}
	if !reflect.DeepEqual(fake.startCalls, []string{"existing-id"}) {
		t.Fatalf("start calls = %v, want [existing-id]", fake.startCalls)
	}
	if len(fake.createCalls) != 0 || len(fake.removeCalls) != 0 {
		t.Fatalf("unexpected create/remove calls: create=%d remove=%d", len(fake.createCalls), len(fake.removeCalls))
	}
}

func TestReconcileReplacesOwnedSpecDrift(t *testing.T) {
	t.Parallel()

	desired := testDesired()
	inspect := matchingInspect(desired, true)
	inspect.Container.Config.Labels[labelGeneration] = "old-generation"
	fake := &fakeEngine{inspectResult: inspect, createResult: client.ContainerCreateResult{ID: "replacement-id"}}
	result, err := (&Manager{client: fake}).Reconcile(context.Background(), desired)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Action != ActionReplaced || result.State.ID != "replacement-id" {
		t.Fatalf("Reconcile() result = %#v, want replacement", result)
	}
	if len(fake.removeCalls) != 1 || fake.removeCalls[0].id != "existing-id" || !fake.removeCalls[0].options.Force {
		t.Fatalf("remove calls = %#v, want forced existing-id removal", fake.removeCalls)
	}
	if len(fake.createCalls) != 1 || !reflect.DeepEqual(fake.startCalls, []string{"replacement-id"}) {
		t.Fatalf("replacement calls: create=%d start=%v", len(fake.createCalls), fake.startCalls)
	}
}

func TestReconcileRefusesUnownedNameCollision(t *testing.T) {
	t.Parallel()

	desired := testDesired()
	inspect := matchingInspect(desired, true)
	delete(inspect.Container.Config.Labels, labelManaged)
	fake := &fakeEngine{inspectResult: inspect}
	_, err := (&Manager{client: fake}).Reconcile(context.Background(), desired)
	if err == nil || !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("Reconcile() error = %v, want canonical internal collision", err)
	}
	assertNoMutations(t, fake)
}

func TestRemoveIsIdempotentAndOwnershipGuarded(t *testing.T) {
	t.Parallel()

	t.Run("missing", func(t *testing.T) {
		fake := &fakeEngine{inspectErr: containerderrdefs.ErrNotFound}
		if err := (&Manager{client: fake}).Remove(context.Background()); err != nil {
			t.Fatalf("Remove() error = %v", err)
		}
		assertNoMutations(t, fake)
	})

	t.Run("owned", func(t *testing.T) {
		fake := &fakeEngine{inspectResult: matchingInspect(testDesired(), true)}
		if err := (&Manager{client: fake}).Remove(context.Background()); err != nil {
			t.Fatalf("Remove() error = %v", err)
		}
		if len(fake.removeCalls) != 1 || !fake.removeCalls[0].options.Force {
			t.Fatalf("remove calls = %#v, want one forced removal", fake.removeCalls)
		}
	})

	t.Run("unowned", func(t *testing.T) {
		inspect := matchingInspect(testDesired(), true)
		inspect.Container.Config.Labels[labelKind] = "workload"
		fake := &fakeEngine{inspectResult: inspect}
		if err := (&Manager{client: fake}).Remove(context.Background()); err == nil {
			t.Fatal("Remove() error = nil, want unowned collision")
		}
		assertNoMutations(t, fake)
	})

	t.Run("concurrent disappearance", func(t *testing.T) {
		fake := &fakeEngine{
			inspectResult: matchingInspect(testDesired(), true),
			removeErr:     containerderrdefs.ErrNotFound,
		}
		if err := (&Manager{client: fake}).Remove(context.Background()); err != nil {
			t.Fatalf("Remove() error = %v", err)
		}
	})
}

func TestInspectWrapsDockerDaemonError(t *testing.T) {
	t.Parallel()

	daemonErr := errors.New("daemon unavailable")
	_, err := (&Manager{client: &fakeEngine{inspectErr: daemonErr}}).Inspect(context.Background())
	if err == nil || !errors.Is(err, daemonErr) || !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("Inspect() error = %v, want wrapped canonical internal daemon error", err)
	}
}

func TestReconcileHonorsCanceledContextBeforeDockerCall(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fake := &fakeEngine{}
	_, err := (&Manager{client: fake}).Reconcile(ctx, testDesired())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Reconcile() error = %v, want context.Canceled", err)
	}
	if fake.inspectCalls != 0 {
		t.Fatalf("inspect calls = %d, want 0", fake.inspectCalls)
	}
}

func TestReconcileRejectsUnpinnedImageBeforeDockerCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		image string
	}{
		{name: "tag only", image: "registry.example/groundplane-agent:latest"},
		{
			name:  "malformed prefix",
			image: "bad prefix@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		},
		{
			name:  "uppercase repository",
			image: "registry.example/Groundplane-Agent@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		},
		{name: "short digest", image: "registry.example/groundplane-agent@sha256:0123456789abcdef"},
		{
			name:  "uppercase digest",
			image: "registry.example/groundplane-agent@sha256:0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			desired := testDesired()
			desired.Image = test.image
			fake := &fakeEngine{}
			_, err := (&Manager{client: fake}).Reconcile(context.Background(), desired)
			if err == nil || !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Reconcile() error = %v, want validation.failed", err)
			}
			if fake.inspectCalls != 0 {
				t.Fatalf("inspect calls = %d, want 0", fake.inspectCalls)
			}
		})
	}
}

func TestReconcileRejectsInvalidAgentIDsBeforeDockerCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		agentID string
	}{
		{name: "path traversal", agentID: "agt_../../root"},
		{name: "wrong prefix", agentID: "agent_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		{name: "lowercase ULID", agentID: "agt_01arz3ndektsv4rrffq69g5fav"},
		{name: "invalid Crockford character", agentID: "agt_01ARZ3NDEKTSV4RRFFQ69G5FAI"},
		{name: "short ULID", agentID: "agt_01ARZ3NDEKTSV4RR"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			desired := testDesired()
			desired.AgentID = test.agentID
			fake := &fakeEngine{}
			_, err := (&Manager{client: fake}).Reconcile(context.Background(), desired)
			if err == nil || !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Reconcile() error = %v, want validation.failed", err)
			}
			if fake.inspectCalls != 0 {
				t.Fatalf("inspect calls = %d, want 0", fake.inspectCalls)
			}
		})
	}
}

func TestRuntimePathsForAgentReturnsApprovedHostLayout(t *testing.T) {
	t.Parallel()

	paths, err := RuntimePathsForAgent("agt_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if err != nil {
		t.Fatalf("RuntimePathsForAgent() error = %v", err)
	}
	want := RuntimePaths{
		Directory: "/run/groundplane/agents/agt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Config:    "/run/groundplane/agents/agt_01ARZ3NDEKTSV4RRFFQ69G5FAV/config.yaml",
		Token:     "/run/groundplane/agents/agt_01ARZ3NDEKTSV4RRFFQ69G5FAV/token",
	}
	if !reflect.DeepEqual(paths, want) {
		t.Fatalf("RuntimePathsForAgent() = %#v, want %#v", paths, want)
	}
}

func testDesired() Desired {
	return Desired{
		Image:      testDigest,
		AgentID:    "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Generation: "7",
	}
}

func matchingInspect(desired Desired, running bool) client.ContainerInspectResult {
	options := createOptions(desired)
	return client.ContainerInspectResult{Container: container.InspectResponse{
		ID:         "existing-id",
		Config:     options.Config,
		HostConfig: options.HostConfig,
		State:      &container.State{Running: running},
	}}
}

func assertCreatePolicy(t *testing.T, options client.ContainerCreateOptions, desired Desired) {
	t.Helper()
	if options.Name != ContainerName {
		t.Errorf("container name = %q, want %q", options.Name, ContainerName)
	}
	if options.Config == nil || options.Config.Image != desired.Image || options.Config.User != "0" {
		t.Fatalf("container config = %#v, want pinned image and root user", options.Config)
	}
	wantLabels := map[string]string{
		labelManaged: "true", labelKind: "agent", labelAgentID: desired.AgentID, labelGeneration: desired.Generation,
	}
	if !reflect.DeepEqual(options.Config.Labels, wantLabels) {
		t.Errorf("labels = %#v, want %#v", options.Config.Labels, wantLabels)
	}
	if options.HostConfig == nil || options.HostConfig.NetworkMode != container.NetworkMode("host") {
		t.Fatalf("host config = %#v, want host network", options.HostConfig)
	}
	if options.HostConfig.RestartPolicy.Name != container.RestartPolicyDisabled {
		t.Errorf("restart policy = %q, want %q", options.HostConfig.RestartPolicy.Name, container.RestartPolicyDisabled)
	}
	wantMounts := []mount.Mount{
		{Type: mount.TypeBind, Source: "/var/run/docker.sock", Target: "/var/run/docker.sock"},
		{Type: mount.TypeBind, Source: "/var/lib/groundplane/agent", Target: "/var/lib/groundplane/agent"},
		{
			Type:     mount.TypeBind,
			Source:   "/run/groundplane/controller",
			Target:   "/run/groundplane/controller",
			ReadOnly: true,
		},
		{
			Type:     mount.TypeBind,
			Source:   "/run/groundplane/agents/agt_01ARZ3NDEKTSV4RRFFQ69G5FAV/config.yaml",
			Target:   agentprotocol.RuntimeConfigPath,
			ReadOnly: true,
		},
		{
			Type:     mount.TypeBind,
			Source:   "/run/groundplane/agents/agt_01ARZ3NDEKTSV4RRFFQ69G5FAV/token",
			Target:   agentprotocol.TokenPath,
			ReadOnly: true,
		},
	}
	if !reflect.DeepEqual(options.HostConfig.Mounts, wantMounts) {
		t.Errorf("mounts = %#v, want %#v", options.HostConfig.Mounts, wantMounts)
	}
}

func assertNoMutations(t *testing.T, fake *fakeEngine) {
	t.Helper()
	if len(fake.createCalls) != 0 || len(fake.startCalls) != 0 || len(fake.removeCalls) != 0 {
		t.Fatalf(
			"unexpected mutations: create=%d start=%d remove=%d",
			len(fake.createCalls),
			len(fake.startCalls),
			len(fake.removeCalls),
		)
	}
}

type removeCall struct {
	id      string
	options client.ContainerRemoveOptions
}

type fakeEngine struct {
	inspectCalls  int
	inspectResult client.ContainerInspectResult
	inspectErr    error
	createCalls   []client.ContainerCreateOptions
	createResult  client.ContainerCreateResult
	createErr     error
	startCalls    []string
	startErr      error
	removeCalls   []removeCall
	removeErr     error
	closeErr      error
}

func (f *fakeEngine) ContainerInspect(
	context.Context,
	string,
	client.ContainerInspectOptions,
) (client.ContainerInspectResult, error) {
	f.inspectCalls++
	return f.inspectResult, f.inspectErr
}

func (f *fakeEngine) ContainerCreate(
	_ context.Context,
	options client.ContainerCreateOptions,
) (client.ContainerCreateResult, error) {
	f.createCalls = append(f.createCalls, options)
	return f.createResult, f.createErr
}

func (f *fakeEngine) ContainerStart(
	_ context.Context,
	id string,
	_ client.ContainerStartOptions,
) (client.ContainerStartResult, error) {
	f.startCalls = append(f.startCalls, id)
	return client.ContainerStartResult{}, f.startErr
}

func (f *fakeEngine) ContainerRemove(
	_ context.Context,
	id string,
	options client.ContainerRemoveOptions,
) (client.ContainerRemoveResult, error) {
	f.removeCalls = append(f.removeCalls, removeCall{id: id, options: options})
	return client.ContainerRemoveResult{}, f.removeErr
}

func (f *fakeEngine) Close() error {
	return f.closeErr
}
