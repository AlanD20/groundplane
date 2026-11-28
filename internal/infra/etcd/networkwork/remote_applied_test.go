package networkwork

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/internal/common/remotehost"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/remotehosts"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestNetworkAppliedReceiptsStayOnTheirAssignedHost(t *testing.T) {
	ctx := t.Context()
	store := &appliedReceiptStore{revision: 1, values: make(map[string]keyvalue.KeyValue)}
	repository := &InputRepository{store: store}
	now := time.Now().UTC()
	local, first, second := newNetworkExecutor(), newNetworkExecutor(), newNetworkExecutor()
	anchor, err := recordcodec.Encode("local-host", struct {
		ID string `json:"id"`
	}{local.HostID})
	if err != nil {
		t.Fatal(err)
	}
	store.put(hostbindings.LocalIdentityKey(), anchor)
	for _, executor := range []workexecution.Identity{first, second} {
		store.enroll(t, executor, now)
	}
	environmentID, networkID := ids.New(ids.KindEnvironment), ids.New(ids.KindNetwork)
	name, err := networkname.New(networkID)
	if err != nil {
		t.Fatal(err)
	}
	input := &agentpb.NetworkWorkInput{Action: agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_ENSURE,
		EnvironmentId: environmentID, NetworkId: networkID, DockerName: name, IntentSha256: make([]byte, 32)}
	reference := workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)}
	evidence := workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("b", 64)}
	publish := func(executor workexecution.Identity) {
		t.Helper()
		view := networkObservationView(t, executor, environmentID, networkID, reference, now, store.revision)
		change, err := repository.prepareObservedApplied(ctx, view, input, false, false, now.Add(time.Second), evidence)
		if err != nil {
			t.Fatal(err)
		}
		store.apply(t, change)
	}
	publish(local)
	publish(first)
	before := store.values[AppliedKey(networkID)]
	publish(second)
	after := store.values[AppliedKey(networkID)]
	if before.ModRevision != after.ModRevision || string(before.Value) != string(after.Value) {
		t.Fatal("remote completion changed the legacy local receipt")
	}
	localReceipt, err := repository.ReadApplied(ctx, networkID, store.revision)
	if err != nil || localReceipt == nil || localReceipt.HostID != local.HostID {
		t.Fatalf("local reader lost its original owner: %v", err)
	}
	for _, executor := range []workexecution.Identity{first, second} {
		receipt, guards, err := repository.ReadRemoteApplied(ctx, executor, environmentID, networkID, store.revision)
		if err != nil || receipt.Revision == 0 || receipt.Record.Executor != executor ||
			receipt.Record.Input != reference || receipt.Record.Evidence != evidence {
			t.Fatalf("wrong exact destination receipt for %v: %v", executor, err)
		}
		if !store.matches(guards) {
			t.Fatal("destination read did not return usable publication guards")
		}
	}
	_, guards, err := repository.ReadRemoteApplied(ctx, first, environmentID, networkID, store.revision)
	if err != nil {
		t.Fatal(err)
	}
	replacement := first
	replacement.Generation++
	store.register(t, replacement)
	if store.matches(guards) {
		t.Fatal("previous Agent generation still satisfies the readiness guards")
	}
	if _, _, err := repository.ReadRemoteApplied(ctx, replacement, environmentID, networkID, store.revision); err == nil {
		t.Fatal("replacement Agent inherited an unobserved Network receipt")
	}
	publish(replacement)
	if _, _, err := repository.ReadRemoteApplied(ctx, replacement, environmentID, networkID, store.revision); err != nil {
		t.Fatalf("fresh observation did not qualify the replacement Agent: %v", err)
	}
}

func newNetworkExecutor() workexecution.Identity {
	return workexecution.Identity{Executor: workexecution.Agent, HostID: ids.New(ids.KindHost),
		AgentID: ids.New(ids.KindAgent), Generation: 1}
}

func networkObservationView(t *testing.T, executor workexecution.Identity, environmentID, networkID string,
	reference workexecution.Input, now time.Time, revision int64,
) workqueue.ResumeView {
	t.Helper()
	task := workexecution.Task{ID: ids.New(ids.KindTask), Action: workexecution.ActionDeploy,
		TargetID: ids.New(ids.KindService), Scope: workexecution.Scope{TenantID: ids.New(ids.KindTenant),
			ProjectID: ids.New(ids.KindProject), EnvironmentID: environmentID}}
	item := workexecution.Item{ID: ids.New(ids.KindWorkItem), TaskID: task.ID, Operation: workexecution.Network,
		TargetID: networkID, Placement: workexecution.Placement{Executor: workexecution.Agent, HostID: executor.HostID},
		Input: &reference, Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		Claims:    []workexecution.Claim{{HostID: executor.HostID, ScopeID: networkID}},
		CreatedAt: now, UpdatedAt: now, Deadline: now.Add(time.Minute)}
	item, attempt, err := workexecution.Assign(item, executor, ids.New(ids.KindAttempt), workexecution.Forward,
		now, item.Deadline)
	if err != nil {
		t.Fatal(err)
	}
	item, attempt, err = workexecution.Start(item, attempt, now)
	if err != nil {
		t.Fatal(err)
	}
	return workqueue.ResumeView{Work: workqueue.View{Task: workqueue.TaskSnapshot{Task: task},
		Item: workqueue.ItemSnapshot{Item: item}, ReadRevision: revision},
		Attempt: workqueue.AttemptSnapshot{Attempt: attempt}, ActiveAttempt: true, CurrentExecutor: true}
}

type appliedReceiptStore struct {
	Store
	revision int64
	values   map[string]keyvalue.KeyValue
}

func (store *appliedReceiptStore) GetMany(
	_ context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	values := make([]*keyvalue.KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		if entry, found := store.values[key]; found {
			entry.Value = append([]byte(nil), entry.Value...)
			values[index] = &entry
		}
	}
	return &keyvalue.GetManyResult{ReadRevision: store.revision, ResponseRevision: store.revision, Values: values}, nil
}

func (store *appliedReceiptStore) put(key string, raw []byte) {
	store.revision++
	store.values[key] = keyvalue.KeyValue{Key: key, ModRevision: store.revision, Value: raw}
}

func (store *appliedReceiptStore) matches(guards []keyvalue.Condition) bool {
	for _, guard := range guards {
		value, found := store.values[guard.Key]
		if !found && guard.ModRevision != 0 || found && !keyvalue.ConditionMatchesRead(guard, &value) {
			return false
		}
	}
	return true
}

func (store *appliedReceiptStore) apply(t *testing.T, change workqueue.Change) {
	t.Helper()
	if !store.matches(change.Conditions) {
		t.Fatal("fixture transaction has stale conditions")
	}
	store.revision++
	for _, mutation := range change.Mutations {
		if mutation.Type != keyvalue.MutationPut {
			t.Fatalf("unexpected fixture mutation type %v", mutation.Type)
		}
		store.values[mutation.Key] = keyvalue.KeyValue{
			Key: mutation.Key, ModRevision: store.revision, Value: mutation.Value,
		}
	}
}

func (store *appliedReceiptStore) enroll(t *testing.T, executor workexecution.Identity, now time.Time) {
	t.Helper()
	binding, err := hostbindings.PrepareCreation(
		hostbindings.Binding{HostID: executor.HostID, AgentID: executor.AgentID},
	)
	if err != nil {
		t.Fatal(err)
	}
	store.apply(t, binding)
	raw, err := recordcodec.Encode("remote-host", remotehost.Host{
		ID: executor.HostID, Name: "remote", Generation: 1,
		Agent: &remotehost.AgentCredential{AgentID: executor.AgentID,
			Certificate: remotehost.CertificateCredential{Digest: [32]byte{1}, ExpiresAt: now.Add(time.Hour)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	store.put(remotehosts.Key(executor.HostID), raw)
	store.register(t, executor)
}

func (store *appliedReceiptStore) register(t *testing.T, executor workexecution.Identity) {
	t.Helper()
	raw, err := workqueue.EncodeExecutor(workqueue.ExecutorRecord{
		Identity: executor, ProcessGeneration: [16]byte{byte(executor.Generation)}, Ready: true,
		Capacity: 1, MaxConcurrentTasks: 1, MaxParallelOperationsPerTask: 1,
		Operations: []workexecution.Operation{workexecution.Network},
	})
	if err != nil {
		t.Fatal(err)
	}
	store.put(workqueue.ExecutorKey(executor), raw)
}
