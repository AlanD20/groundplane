package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/controller/volume"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Uses the public mutation producer and actual native repositories over MVCC
// memory storage. Host observations are supplied by the test, not by Docker.
type nativeVolumeFixture struct {
	store         *nativeVolumeStore
	agents        *agentregistration.Repository
	queue         *workqueue.Repository
	inputs        *volumework.InputRepository
	reports       *volume.WorkReports
	mutations     *volume.MutationService
	reads         *volume.ReadService
	blueprint     *etcd.EnvironmentBlueprintRepository
	authorization agentregistration.WorkAuthorization
	environmentID string
	created       api.VolumeMutationResponse
}

type nativeDesiredRepository struct {
	*etcd.EnvironmentBlueprintRepository
	*desiredrevision.Repository
}

// beforeCommit injects a real competing write immediately before the selected
// atomic transaction, without bypassing the store's comparisons.
type nativeVolumeStore struct {
	*memoryHierarchyStore
	beforeCommit         func([]keyvalue.Mutation)
	afterBlueprintCommit func(keyvalue.TransactionResult)
}

func (store *nativeVolumeStore) Transact(
	ctx context.Context,
	conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	if store.beforeCommit != nil {
		store.beforeCommit(mutations)
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}

func (store *nativeVolumeStore) TransactEnvironmentBlueprint(
	ctx context.Context,
	conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	if store.beforeCommit != nil {
		store.beforeCommit(mutations)
	}
	result, err := store.memoryHierarchyStore.TransactEnvironmentBlueprint(ctx, conditions, mutations)
	if err == nil && store.afterBlueprintCommit != nil {
		store.afterBlueprintCommit(result)
	}
	return result, err
}

func newNativeVolumeFixture(t *testing.T) *nativeVolumeFixture {
	t.Helper()
	ctx, at := t.Context(), time.Now().UTC().Add(-time.Hour)
	f := &nativeVolumeFixture{store: &nativeVolumeStore{memoryHierarchyStore: newMemoryHierarchyStore()}}
	f.store.revision = 1
	var err error
	f.queue, err = workqueue.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	f.agents, err = agentregistration.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	record := agentConfigurationTestRecord()
	record.CreatedAt, record.TokenUpdatedAt = at, at
	registered, err := f.agents.CreateSingleton(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.agents.MarkReady(ctx, record.ID, 1, registered.Revision, at.Add(time.Second), buildversion.BuildIdentity{Version: "test"}); err != nil {
		t.Fatal(err)
	}
	identity, err := f.agents.RegisterWorkProcess(
		ctx,
		record.ID,
		1,
		[16]byte{1},
		2,
		[]workexecution.Operation{workexecution.Volume},
	)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := f.queue.ReadExecutor(
		ctx,
		workexecution.Placement{Executor: workexecution.Agent, HostID: identity.HostID},
	)
	if err != nil {
		t.Fatal(err)
	}
	available, err := workqueue.PrepareAvailability(executor, identity, true, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	f.commit(t, available)
	f.authorize(t)

	tenant := hierarchy.TenantRecord{ID: ids.New(ids.KindTenant), Slug: "volume-check", Name: "Volume check"}
	parents, err := etcd.NewHierarchyRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parents.CreateTenant(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	project := hierarchy.ProjectRecord{ID: ids.New(ids.KindProject), TenantID: tenant.ID,
		Slug: "volume-check", Name: "Volume check", Kind: hierarchy.ProjectKindTenant}
	if _, err := parents.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	environment, err := hierarchy.NewProvisioningEnvironment("/var/lib/groundplane/vol", project,
		ids.New(ids.KindEnvironment), "qa", "10.40.0.0/24", ids.New(ids.KindTask), at)
	if err != nil {
		t.Fatal(err)
	}
	environment, err = hierarchy.CompleteEnvironmentProvisioning(environment, environment.CreateTaskID, true)
	if err != nil {
		t.Fatal(err)
	}
	f.environmentID = environment.ID
	if _, err := parents.CreateEnvironment(ctx, environment); err != nil {
		t.Fatal(err)
	}
	f.blueprint, err = etcd.NewEnvironmentBlueprintRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	desired, err := desiredrevision.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	f.reads, err = volume.NewReadService(f.blueprint)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := etcd.NewIdempotencyRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	policies, err := etcd.NewBackupPolicyRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ageinfra.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	cipher := nativeVolumeCipher{key: key}
	protector, err := secretvalue.NewProtector(cipher, cipher)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	f.mutations, err = volume.NewMutationService(
		nativeDesiredRepository{f.blueprint, desired},
		coordinator,
		replay,
		f.reads,
		policies,
	)
	if err != nil {
		t.Fatal(err)
	}
	f.inputs, err = volumework.NewInputRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	f.reports, err = volume.NewWorkReports(f.queue, f.inputs)
	if err != nil {
		t.Fatal(err)
	}
	response, err := f.mutations.CreateVolume(
		ctx,
		api.VolumeCreate{EnvironmentID: environment.ID, Slug: "data", Key: "data"},
		"native-volume-create-key",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(response.Body, &f.created); err != nil || f.created.TaskID == "" {
		t.Fatalf("Volume create response: %v", err)
	}
	return f
}

func (f *nativeVolumeFixture) authorize(t *testing.T) {
	t.Helper()
	record, err := f.agents.GetSingleton(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	f.authorization, err = f.agents.ResolveWorkIdentity(t.Context(), record.Record.ID, 1, [16]byte{1})
	if err != nil {
		t.Fatal(err)
	}
}

func (f *nativeVolumeFixture) assign(t *testing.T, taskID string) workqueue.ResumeView {
	t.Helper()
	ctx := t.Context()
	workID := ""
	page, err := f.queue.ListWork(ctx, taskID, "", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("Volume action has %d items", len(page.Items))
	}
	workID = page.Items[0].Item.ID
	view, err := f.queue.ReadWork(ctx, taskID, workID, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.authorize(t)
	executor, err := f.queue.ReadExecutor(ctx, view.Item.Item.Placement)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := f.store.Get(ctx, workqueue.QueueKey(view.Item.Item))
	if err != nil || queued.Entry == nil {
		t.Fatalf("Volume not queued: %v", err)
	}
	at := time.Now().UTC()
	candidate, err := f.queue.ReadCandidate(
		ctx,
		workqueue.QueueEntry{TaskID: taskID, WorkItemID: workID, Revision: queued.Entry.ModRevision},
		executor.Record.Identity,
		at,
		nil,
	)
	if err != nil || candidate.Wait != nil || candidate.Stopped {
		t.Fatalf("Volume admission: %v, %v", err, candidate.Wait)
	}
	request := candidate.Assignment
	request.AttemptID, request.At, request.Deadline = ids.New(ids.KindAttempt), at, view.Item.Item.Deadline
	change, attempt, err := workqueue.PrepareAssignment(request)
	if err != nil {
		t.Fatal(err)
	}
	f.commit(t, change)
	resume, err := f.queue.ReadResume(ctx, taskID, workID, attempt.ID, executor.Record.Identity)
	if err != nil {
		t.Fatal(err)
	}
	return resume
}

func (f *nativeVolumeFixture) start(t *testing.T, taskID string) workqueue.ResumeView {
	t.Helper()
	resume := f.assign(t, taskID)
	owner, err := volume.NewAgentWork(f.inputs, f.queue)
	if err != nil {
		t.Fatal(err)
	}
	starts, err := workdispatch.NewAgentStarts(f.queue, []workdispatch.AgentStartRegistration{
		{Operation: workexecution.Volume, Owner: owner},
	})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := workexecution.EncodeAgentAttempt(resume.Attempt.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := starts.Start(t.Context(), f.authorization, &agentpb.WorkStartRequest{Attempt: wire}); err != nil {
		t.Fatal(err)
	}
	resume, err = f.queue.ReadResume(t.Context(), taskID, resume.Work.Item.Item.ID,
		resume.Attempt.Attempt.ID, f.authorization.Identity())
	if err != nil {
		t.Fatal(err)
	}
	return resume
}

func (f *nativeVolumeFixture) commit(t *testing.T, changes ...workqueue.Change) {
	t.Helper()
	result, err := f.queue.Commit(t.Context(), changes...)
	if err != nil || !result.Succeeded {
		t.Fatalf("native Volume transaction: committed=%t, %v", result.Succeeded, err)
	}
}

func (f *nativeVolumeFixture) checkpoint(
	t *testing.T,
	view workqueue.ResumeView,
	progress volumeexecution.Progress,
) *agentpb.VolumeWorkCheckpointRequest {
	t.Helper()
	input, err := f.inputs.LoadVolumeWorkInput(t.Context(), view.Work)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := volumeexecution.EncodeProgress(view.Attempt.Attempt, input.Action, input.VolumeId, progress)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := workexecution.EncodeAgentAttempt(view.Attempt.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	request := &agentpb.VolumeWorkCheckpointRequest{Attempt: attempt, Progress: raw}
	f.authorize(t)
	if _, err := f.reports.CheckpointVolume(t.Context(), f.authorization, request); err != nil {
		t.Fatal(err)
	}
	return request
}

func nativeVolumePut[T any](
	t *testing.T,
	store *nativeVolumeStore,
	key string,
	record T,
	encode func(T) ([]byte, error),
) {
	t.Helper()
	raw, err := encode(record)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(raw)
	if _, err := store.Put(t.Context(), key, raw); err != nil {
		t.Fatal(err)
	}
}

type nativeVolumeCipher struct{ key ageinfra.Keypair }

func (cipher nativeVolumeCipher) Seal(ctx context.Context, value []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ageinfra.Encrypt(cipher.key.Recipient, value)
}

func (cipher nativeVolumeCipher) Open(ctx context.Context, value []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ageinfra.Decrypt(cipher.key.Identity, value)
}
