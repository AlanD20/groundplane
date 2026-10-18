package app

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/controller/agentmanagement"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	agentruntime "github.com/AlanD20/groundplane/internal/controller/localagent/runtime"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type agentLifecycleFixture struct {
	ctx                 context.Context
	store               *agentLifecycleStore
	agents              *agentregistration.Repository
	queue               *workqueue.Repository
	inputs              *agentoperations.Repository
	registry            *agentchannel.Registry
	owner               *agentmanagement.Work
	capture             agentoperations.Capture
	delivery            workqueue.Delivery
	runtime             *lifecycleRuntime
	container           *lifecycleContainer
	managerDependencies localagent.Dependencies
	diagnostics         *executiondiagnostics.Repository
}

func newAgentLifecycleFixture(t *testing.T, elapsed time.Duration) *agentLifecycleFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	t.Cleanup(cancel)
	f := &agentLifecycleFixture{
		ctx:      ctx,
		store:    &agentLifecycleStore{memoryHierarchyStore: newMemoryHierarchyStore()},
		registry: agentchannel.NewRegistry(),
	}
	f.store.revision = 1
	at := time.Now().UTC().Add(-elapsed)
	agents, err := agentregistration.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	f.agents = agents
	record := agentConfigurationTestRecord()
	record.CreatedAt, record.TokenUpdatedAt = at, at
	created, err := agents.CreateSingleton(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agents.MarkReady(ctx, record.ID, 1, created.Revision, at, buildversion.BuildIdentity{Version: "test"}); err != nil {
		t.Fatal(err)
	}
	f.capture = agentoperations.Capture{TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem),
		PlanID: ids.New(ids.KindPlan), AgentID: record.ID, Action: workexecution.ActionUpdate,
		Image: "registry.example/agent@sha256:" + strings.Repeat("b", 64), PreviousImage: record.Image,
		StartingGeneration: 1, CreatedAt: at}
	tasks, err := etcd.NewTaskRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	marker := agentConfigurationTestMarker()
	marker.Kind, marker.State, marker.TaskID = idempotency.IdempotencyMarkerTask, idempotency.IdempotencyMarkerPending, f.capture.TaskID
	marker.CreatedAt, marker.UpdatedAt, marker.TerminalAt, marker.RetainUntil = at, at, time.Time{}, time.Time{}
	marker.Locator = idempotency.IdempotencyLocator{
		ScopeKind: idempotency.IdempotencyScopePlatform,
		ScopeID:   "-",
		Method:    "POST",
		Route:     "/agents/{id}/update",
		Key:       "native-agent-update-qa-0001",
	}
	marker.Response = idempotency.IdempotencyResponse{
		Status:      202,
		ContentKind: "application/json",
		Body:        []byte(fmt.Sprintf(`{"task_id":%q}`, f.capture.TaskID)),
	}
	if _, err := tasks.CreateAgentWork(ctx, f.capture, marker); err != nil {
		t.Fatal(err)
	}
	f.queue, err = workqueue.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	f.inputs, err = agentoperations.New(f.store)
	if err != nil {
		t.Fatal(err)
	}
	executor := workqueue.ExecutorRecord{
		Identity:   workexecution.Identity{Executor: workexecution.Controller, Generation: 1},
		Ready:      true,
		Capacity:   1,
		Operations: []workexecution.Operation{workexecution.AgentSoftware},
	}
	raw, err := workqueue.EncodeExecutor(executor)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := f.store.Transact(
		ctx,
		nil,
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: workqueue.ExecutorKey(executor.Identity), Value: raw}},
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err := f.queue.ReadWork(ctx, f.capture.TaskID, f.capture.WorkItemID, 0)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := f.store.Get(ctx, workqueue.QueueKey(view.Item.Item))
	if err != nil {
		t.Fatal(err)
	}
	change, attempt, err := workqueue.PrepareAssignment(workqueue.Assignment{ReadRevision: view.ReadRevision,
		Task: view.Task, Item: view.Item, Executor: workqueue.ExecutorSnapshot{Record: executor, Revision: seed.Revision},
		Claims: []workqueue.ClaimSnapshot{{Claim: view.Item.Item.Claims[0]}}, QueueRevision: queued.Entry.ModRevision,
		AttemptID: ids.New(
			ids.KindAttempt,
		), Direction: workexecution.Forward, At: at.Add(time.Second), Deadline: view.Item.Item.Deadline})
	if err != nil {
		t.Fatal(err)
	}
	commitAgentLifecycleChange(t, f.queue, change)
	resume, err := f.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		t.Fatal(err)
	}
	change, err = workqueue.PrepareStart(resume.Work.Task, resume.Work.Item, resume.Attempt,
		workqueue.ExecutorSnapshot{Record: executor, Revision: seed.Revision}, at.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	commitAgentLifecycleChange(t, f.queue, change)
	resume, err = f.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		t.Fatal(err)
	}
	f.delivery = workqueue.Delivery{Item: resume.Work.Item, Attempt: resume.Attempt}
	evidence, err := etcd.NewIdempotencyRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	protector, err := secretvalue.NewProtector(lifecycleUnusedCipher{}, lifecycleUnusedCipher{})
	if err != nil {
		t.Fatal(err)
	}
	intents, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	configReplay, err := agentruntime.NewConfigIdempotency(intents, evidence)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := agentruntime.NewRepository(agents, configReplay)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := agentruntime.NewSessions(f.registry)
	if err != nil {
		t.Fatal(err)
	}
	maintenance, err := agentruntime.NewWork(f.queue, agents)
	if err != nil {
		t.Fatal(err)
	}
	f.runtime = &lifecycleRuntime{}
	f.container = &lifecycleContainer{registry: f.registry, sessions: map[uint64]*agentchannel.Session{}}
	t.Cleanup(func() {
		for _, session := range f.container.sessions {
			session.Close()
		}
		for _, joined := range f.container.offline {
			<-joined
		}
	})
	f.managerDependencies = localagent.Dependencies{Repository: repository, Runtime: f.runtime,
		Container: f.container, Sessions: sessions, Tasks: maintenance, Clock: localagent.SystemClock{}}
	f.diagnostics, err = executiondiagnostics.New(f.store, f.queue)
	if err != nil {
		t.Fatal(err)
	}
	f.resetOwner(t)
	return f
}

func (f *agentLifecycleFixture) resetOwner(t *testing.T) {
	t.Helper()
	manager, err := localagent.New(f.managerDependencies)
	if err != nil {
		t.Fatal(err)
	}
	f.owner, err = agentmanagement.NewWork(f.queue, f.inputs, f.agents, manager, f.registry, f.diagnostics)
	if err != nil {
		t.Fatal(err)
	}
}

func (f *agentLifecycleFixture) installCandidate(t *testing.T, ready bool) {
	t.Helper()
	current, err := f.agents.GetSingleton(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("preinstalled-candidate-token"))
	updated, err := f.agents.ReplaceGeneration(
		f.ctx,
		current,
		f.capture.Image,
		[]byte("candidate-encrypted-token"),
		base64.RawURLEncoding.EncodeToString(digest[:]),
		nil,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !ready {
		return
	}
	if _, err := f.agents.MarkReplacementReady(f.ctx, f.capture.AgentID, 2, updated.Revision, buildversion.BuildIdentity{Version: "test"}); err != nil {
		t.Fatal(err)
	}
	if err := f.container.Converge(f.ctx, localagent.ContainerDesired{AgentID: f.capture.AgentID, Generation: 2, Image: f.capture.Image}); err != nil {
		t.Fatal(err)
	}
}

func commitAgentLifecycleChange(t *testing.T, queue *workqueue.Repository, change workqueue.Change) {
	t.Helper()
	result, err := queue.Commit(t.Context(), change)
	if err != nil || !result.Succeeded {
		t.Fatalf("native lifecycle commit = %v, %v", result, err)
	}
}

type lifecycleRuntime struct{ generated int }

func (runtime *lifecycleRuntime) GenerateCredential(context.Context, string) (localagent.Credential, error) {
	runtime.generated++
	digest := sha256.Sum256([]byte(fmt.Sprintf("generated-token-%d", runtime.generated)))
	return localagent.Credential{
		EncryptedToken: []byte("generated-encrypted-token"),
		Digest:         base64.RawURLEncoding.EncodeToString(digest[:]),
	}, nil
}
func (*lifecycleRuntime) Materialize(context.Context, localagent.RuntimeMaterial) error { return nil }
func (*lifecycleRuntime) Remove(context.Context, string) error                          { return nil }

type lifecycleContainer struct {
	registry *agentchannel.Registry
	sessions map[uint64]*agentchannel.Session
	onReady  func()
	offline  []<-chan struct{}
}

func (container *lifecycleContainer) Converge(ctx context.Context, desired localagent.ContainerDesired) error {
	if snapshot, found := container.registry.Snapshot(desired.AgentID); found && snapshot.Online &&
		snapshot.Generation == desired.Generation {
		return nil
	}
	session, err := container.registry.Open(ctx, desired.AgentID, desired.Generation)
	if err != nil {
		return err
	}
	container.sessions[desired.Generation] = session
	joined := make(chan struct{})
	container.offline = append(container.offline, joined)
	go func() {
		<-session.Done()
		session.Close()
		close(joined)
	}()
	if err := session.RecordReady(time.Now().UTC(), 1, "test", buildversion.BuildProvenance{}); err != nil {
		return err
	}
	if container.onReady != nil {
		container.onReady()
	}
	return nil
}

type agentLifecycleStore struct {
	*memoryHierarchyStore
	failAfterFence bool
	failPrimary    bool
}

func (store *agentLifecycleStore) Get(ctx context.Context, key string) (*keyvalue.GetResult, error) {
	if store.failPrimary && key == localagents.LocalAgentSingletonKey {
		return nil, errs.New(errs.KindStorageUnavailable, "simulated Agent record outage")
	}
	return store.memoryHierarchyStore.Get(ctx, key)
}

func (store *agentLifecycleStore) GetMany(
	ctx context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	if store.failPrimary {
		for _, key := range request.Keys {
			if key == localagents.LocalAgentSingletonKey {
				return nil, errs.New(errs.KindStorageUnavailable, "simulated Agent record outage")
			}
		}
	}
	return store.memoryHierarchyStore.GetMany(ctx, request)
}

func (store *agentLifecycleStore) Transact(
	ctx context.Context,
	conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	result, err := store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
	if err == nil && result.Succeeded && store.failAfterFence {
		for _, mutation := range mutations {
			if mutation.Type == keyvalue.MutationPut &&
				strings.HasPrefix(mutation.Key, "/v1/execution/host-forward-fences/") {
				store.failPrimary = true
			}
		}
	}
	return result, err
}
func (container *lifecycleContainer) Remove(_ context.Context, _ string, generation uint64) error {
	if session := container.sessions[generation]; session != nil {
		session.Close()
	}
	return nil
}

// Config replay is not invoked by these lifecycle journeys.
type lifecycleUnusedCipher struct{}

func (lifecycleUnusedCipher) Seal(context.Context, []byte) ([]byte, error) {
	return nil, fmt.Errorf("unexpected config cipher use")
}
func (lifecycleUnusedCipher) Open(context.Context, []byte) ([]byte, error) {
	return nil, fmt.Errorf("unexpected config cipher use")
}
