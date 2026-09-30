package etcdcontainer

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// Rationale: Save must not activate on a Controller restart, while explicit
// Apply must resume after loss of its acknowledgement without restarting twice.
func TestSaveRestartAndApplyReplay(t *testing.T) {
	ctx := t.Context()
	engine := newConfigEngine()
	desired, active := configPaths(t)
	manager := configManager(engine)
	if err := manager.openDocuments(ctx, desired, active); err != nil {
		t.Fatal(err)
	}
	_, revision, _, err := manager.document.Current(ctx)
	if err != nil {
		t.Fatal(err)
	}
	candidate := "log-level: debug\n"
	_, _, required, err := manager.document.Replace(ctx, "save-etcd-config-0001", revision, candidate)
	if err != nil || !required {
		t.Fatalf("Save = required %v, %v", required, err)
	}
	restarted := configManager(engine)
	if err := restarted.openDocuments(ctx, desired, active); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if engine.stops != 0 || engine.creates != 0 {
		t.Fatal("Controller restart activated pending Save")
	}
	previous, err := restarted.AppliedConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Apply(ctx, "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", candidate, previous); err != nil {
		t.Fatal(err)
	}
	if engine.stops != 1 || engine.creates != 1 || engine.removes != 1 {
		t.Fatalf("Apply effects = stops %d creates %d removes %d", engine.stops, engine.creates, engine.removes)
	}
	replay := configManager(engine)
	if err := replay.openDocuments(ctx, desired, active); err != nil {
		t.Fatal(err)
	}
	if err := replay.Apply(ctx, "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", candidate, previous); err != nil {
		t.Fatal(err)
	}
	if engine.stops != 1 || engine.creates != 1 {
		t.Fatal("acknowledgement replay restarted etcd")
	}
	_, _, pending, err := replay.document.Current(ctx)
	if err != nil || pending {
		t.Fatalf("Apply observation = pending %v, %v", pending, err)
	}
}

// Rationale: a failed replacement must restore the retained data mount and
// exact predecessor settings, and replay must not retry the failed candidate.
func TestApplyRestoresPredecessorWithoutRetryLoop(t *testing.T) {
	ctx := t.Context()
	engine := newConfigEngine()
	engine.rejectDebug = true
	manager := configManager(engine)
	desired, active := configPaths(t)
	if err := manager.openDocuments(ctx, desired, active); err != nil {
		t.Fatal(err)
	}
	previous, err := manager.AppliedConfiguration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, revision, _, err := manager.document.Current(ctx)
	if err != nil {
		t.Fatal(err)
	}
	candidate := "log-level: debug\n"
	if _, _, _, err := manager.document.Replace(ctx, "save-etcd-config-0002", revision, candidate); err != nil {
		t.Fatal(err)
	}
	taskID := "task_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	if err := manager.Apply(ctx, taskID, candidate, previous); err == nil {
		t.Fatal("failed Docker start accepted")
	}
	if !matchesDesired(engine.current, config.DefaultEtcdConfig()) || !engine.current.State.Running {
		t.Fatal("predecessor runtime was not restored")
	}
	effects := engine.creates
	replay := configManager(engine)
	if err := replay.openDocuments(ctx, desired, active); err != nil {
		t.Fatal(err)
	}
	if err := replay.Apply(ctx, taskID, candidate, previous); err == nil {
		t.Fatal("failed Task replay reported success")
	}
	if engine.creates != effects {
		t.Fatal("failed candidate retried on replay")
	}
	_, _, pending, err := replay.document.Current(ctx)
	if err != nil || !pending {
		t.Fatalf("failed Apply lost saved candidate = pending %v, %v", pending, err)
	}
}

// Rationale: the native Task cannot be read while its candidate etcd process
// is unavailable. Bootstrap must use the independent receipt, not get stuck
// trying to open the store before it can discover the retained predecessor.
func TestInterruptedApplyRestoresBeforeOpeningEtcd(t *testing.T) {
	ctx := t.Context()
	engine := newConfigEngine()
	manager := configManager(engine)
	desired, active := configPaths(t)
	if err := manager.openDocuments(ctx, desired, active); err != nil {
		t.Fatal(err)
	}
	previous, revision, _, err := manager.activation.Current(ctx)
	if err != nil {
		t.Fatal(err)
	}
	record := activationRecord{
		TaskID:    "task_01ARZ3NDEKTSV4RRFFQ69G5FAY",
		Candidate: "log-level: debug\n",
		Previous:  previous,
		Phase:     "prepared",
	}
	if err := manager.recordActivation(ctx, record); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := manager.activation.Replace(ctx, record.TaskID+":apply", revision, record.Candidate); err != nil {
		t.Fatal(err)
	}
	options := createOptions(
		config.EtcdConfig{
			QuotaBackendBytes:       2 << 30,
			SnapshotCount:           10000,
			HeartbeatInterval:       100,
			ElectionTimeout:         1000,
			AutoCompactionRetention: "0",
			LogLevel:                "debug",
		},
	)
	engine.current = container.InspectResponse{
		ID:         "interrupted-candidate",
		Config:     options.Config,
		HostConfig: options.HostConfig,
		State:      &container.State{},
	}
	engine.rejectDebug = true
	restarted := configManager(engine)
	if err := restarted.openDocuments(ctx, desired, active); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if !matchesDesired(engine.current, config.DefaultEtcdConfig()) || !engine.current.State.Running {
		t.Fatal("bootstrap did not restore predecessor")
	}
	if err := restarted.Apply(ctx, record.TaskID, record.Candidate, record.Previous); err == nil {
		t.Fatal("interrupted activation became false success")
	}
}

func configPaths(t *testing.T) (string, string) {
	t.Helper()
	directory := t.TempDir()
	return filepath.Join(directory, "etcd.yaml"), filepath.Join(directory, "applied.yaml")
}
func configManager(engine *configEngine) *Manager {
	return &Manager{
		client: engine,
		active: config.DefaultEtcdConfig(),
		probe:  func(context.Context) error { return nil },
	}
}

type configEngine struct {
	engineClient
	current                 container.InspectResponse
	stops, creates, removes int
	rejectDebug             bool
}

func newConfigEngine() *configEngine {
	options := createOptions(config.DefaultEtcdConfig())
	return &configEngine{
		current: container.InspectResponse{
			ID:         "existing",
			Config:     options.Config,
			HostConfig: options.HostConfig,
			State:      &container.State{Running: true},
		},
	}
}

func (e *configEngine) ContainerInspect(
	context.Context,
	string,
	client.ContainerInspectOptions,
) (client.ContainerInspectResult, error) {
	return client.ContainerInspectResult{Container: e.current}, nil
}

func (e *configEngine) ContainerStop(
	context.Context,
	string,
	client.ContainerStopOptions,
) (client.ContainerStopResult, error) {
	e.stops++
	e.current.State.Running = false
	return client.ContainerStopResult{}, nil
}

func (e *configEngine) ContainerRemove(
	context.Context,
	string,
	client.ContainerRemoveOptions,
) (client.ContainerRemoveResult, error) {
	e.removes++
	return client.ContainerRemoveResult{}, nil
}
func (e *configEngine) ImagePull(context.Context, string, client.ImagePullOptions) (client.ImagePullResponse, error) {
	return readyPull{}, nil
}

type readyPull struct{ client.ImagePullResponse }

func (readyPull) Wait(context.Context) error { return nil }
func (readyPull) Close() error               { return nil }

func (e *configEngine) ContainerCreate(
	_ context.Context,
	options client.ContainerCreateOptions,
) (client.ContainerCreateResult, error) {
	e.creates++
	e.current = container.InspectResponse{
		ID:         "replacement",
		Config:     options.Config,
		HostConfig: options.HostConfig,
		State:      &container.State{},
	}
	return client.ContainerCreateResult{ID: e.current.ID}, nil
}

func (e *configEngine) ContainerStart(
	context.Context,
	string,
	client.ContainerStartOptions,
) (client.ContainerStartResult, error) {
	if e.rejectDebug && strings.Contains(strings.Join(e.current.Config.Cmd, " "), "--log-level=debug") {
		return client.ContainerStartResult{}, errors.New("candidate failed to start")
	}
	e.current.State.Running = true
	return client.ContainerStartResult{}, nil
}
