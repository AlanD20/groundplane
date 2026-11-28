package servingendpoint

import (
	"bytes"
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/servingendpoint"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	storage "github.com/AlanD20/groundplane/internal/infra/etcd/servingendpoints"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type memoryStore struct {
	entries  map[string]keyvalue.KeyValue
	revision int64
}

func (store *memoryStore) GetMany(_ context.Context, request keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error) {
	result := &keyvalue.GetManyResult{
		ReadRevision: store.revision,
		Values:       make([]*keyvalue.KeyValue, len(request.Keys)),
	}
	for index, key := range request.Keys {
		if entry, ok := store.entries[key]; ok {
			entry.Value = bytes.Clone(entry.Value)
			result.Values[index] = &entry
		}
	}
	return result, nil
}

func (store *memoryStore) Transact(
	_ context.Context, guards []keyvalue.Condition, mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	for _, guard := range guards {
		if store.entries[guard.Key].ModRevision != guard.ModRevision {
			return keyvalue.TransactionResult{Revision: store.revision}, nil
		}
	}
	store.revision++
	for _, mutation := range mutations {
		store.entries[mutation.Key] = keyvalue.KeyValue{
			Key: mutation.Key, Value: bytes.Clone(mutation.Value), ModRevision: store.revision,
		}
	}
	return keyvalue.TransactionResult{Revision: store.revision, Succeeded: true}, nil
}

type runtimeEvidence struct {
	now             time.Time
	guardRevision   int64
	endpoints       map[string]domain.Endpoint
	live            domain.EdgeAck
	fenced          domain.EdgeAck
	connectionCount uint32
	partitioned     bool
}

const runtimeKey = "/runtime"

func (evidence *runtimeEvidence) guard() []keyvalue.Condition {
	return []keyvalue.Condition{{Key: runtimeKey, ModRevision: evidence.guardRevision}}
}

func (evidence *runtimeEvidence) CaptureServing(_ context.Context, request ServingRequest) (ServingEvidence, error) {
	endpoint := evidence.endpoints[request.HostID]
	return ServingEvidence{
		Authority: domain.ServingAuthority{Target: request.Target, Endpoint: endpoint},
		Proof: domain.ServingProof{
			Target:     request.Target,
			Endpoint:   endpoint,
			ObservedAt: evidence.now,
			Ready:      true,
		},
		Conditions: evidence.guard(),
	}, nil
}

func (evidence *runtimeEvidence) CaptureApplied(_ context.Context, _ EdgeRequest) (AppliedEvidence, error) {
	if evidence.partitioned {
		return AppliedEvidence{}, errs.New(errs.KindAgentReadinessChanged, "edge Agent is offline")
	}
	return AppliedEvidence{Live: evidence.live, Conditions: evidence.guard()}, nil
}

func (evidence *runtimeEvidence) CaptureRestoration(
	ctx context.Context,
	request EdgeRequest,
) (RestorationEvidence, error) {
	applied, err := evidence.CaptureApplied(ctx, request)
	return RestorationEvidence{AppliedEvidence: applied, Fenced: evidence.fenced}, err
}

func (evidence *runtimeEvidence) CaptureDrain(
	_ context.Context,
	request EdgeRequest,
	drain domain.Drain,
) (DrainEvidence, error) {
	return DrainEvidence{Proof: domain.DrainProof{
		Target: request.Target, Endpoint: drain.Endpoint, Withdrawal: drain.Withdrawal,
		ObservedAt: evidence.now, RemainingConnections: &evidence.connectionCount,
	}, Conditions: evidence.guard()}, nil
}

func TestCutoverKeepsLastAppliedAcrossPartitionAndFencesWithdrawal(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2026, 11, 28, 10, 0, 0, 0, time.UTC)
	hostA, hostB := ids.NewAt(ids.KindHost, now, 1), ids.NewAt(ids.KindHost, now, 2)
	target := domain.Target{
		EnvironmentID: ids.NewAt(
			ids.KindEnvironment,
			now,
			1,
		), ServiceID: ids.NewAt(ids.KindService, now, 1), Port: 8080,
	}
	store := &memoryStore{
		revision: 1,
		entries:  map[string]keyvalue.KeyValue{runtimeKey: {Key: runtimeKey, ModRevision: 1}},
	}
	evidence := &runtimeEvidence{now: now, guardRevision: 1, endpoints: map[string]domain.Endpoint{
		hostA: {HostID: hostA, AgentID: ids.NewAt(ids.KindAgent, now, 1), AgentGeneration: 1,
			ReleaseID: ids.NewAt(ids.KindDeployment, now, 1), Address: netip.MustParseAddrPort("10.1.0.2:8080")},
		hostB: {HostID: hostB, AgentID: ids.NewAt(ids.KindAgent, now, 2), AgentGeneration: 2,
			ReleaseID: ids.NewAt(ids.KindDeployment, now, 2), Address: netip.MustParseAddrPort("10.1.0.3:8080")},
	}}
	newService := func() *Service {
		t.Helper()
		service, err := New(storage.NewRepository(store), evidence, evidence, hostA, func() time.Time {
			return evidence.now
		}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return service
	}
	service := newService()
	first, err := service.Publish(ctx, target, hostA)
	if err != nil || first.Applied.Number != 0 {
		t.Fatalf("publish applied before an edge receipt: %v, %v", first, err)
	}
	evidence.live = first.Desired.Witness(target)
	if _, err := service.Acknowledge(ctx, target); err != nil {
		t.Fatal(err)
	}
	replacement, err := service.Publish(ctx, target, hostB)
	if err != nil || len(replacement.Desired.Endpoints) != 2 || replacement.Applied.Number != first.Desired.Number {
		t.Fatalf("stage replacement: %v, %v", replacement, err)
	}
	evidence.partitioned = true
	if _, err := service.Acknowledge(ctx, target); err == nil {
		t.Fatal("unreachable edge acknowledged pending publication")
	}
	service = newService()
	recovered, err := service.Read(ctx, target)
	if err != nil || recovered.Applied.Number != first.Desired.Number ||
		recovered.Desired.Number != replacement.Desired.Number {
		t.Fatalf("reload after lost Agent: %v, %v", recovered, err)
	}
	if _, err := service.PrepareMove(ctx, target, hostA, hostB); err == nil {
		t.Fatal("unreachable edge permitted source withdrawal")
	}

	evidence.partitioned = false
	evidence.live = replacement.Desired.Witness(target)
	if _, err := service.Acknowledge(ctx, target); err != nil {
		t.Fatal(err)
	}
	prepared, err := service.PrepareMove(ctx, target, hostA, hostB)
	if err != nil {
		t.Fatal(err)
	}
	store.revision++
	entry := store.entries[runtimeKey]
	entry.ModRevision = store.revision
	store.entries[runtimeKey] = entry
	evidence.guardRevision = entry.ModRevision
	if _, err := service.BeginMove(ctx, prepared); err == nil {
		t.Fatal("changed runtime permitted a previously prepared source withdrawal")
	}
	prepared, err = service.PrepareMove(ctx, target, hostA, hostB)
	if err != nil {
		t.Fatal(err)
	}
	withdrawing, err := service.BeginMove(ctx, prepared)
	if err != nil || len(withdrawing.Applied.Endpoints) != 2 || len(withdrawing.Desired.Endpoints) != 1 {
		t.Fatalf("begin acknowledged cutover: %v, %v", withdrawing, err)
	}
	if _, err := service.FinishDrain(ctx, target, hostA); err == nil {
		t.Fatal("drain finished before the edge acknowledged source removal")
	}
	evidence.live = withdrawing.Desired.Witness(target)
	if _, err := service.Acknowledge(ctx, target); err != nil {
		t.Fatal(err)
	}
	evidence.connectionCount = 1
	if _, err := service.FinishDrain(ctx, target, hostA); err == nil {
		t.Fatal("old endpoint reservation released with a live connection")
	}
	evidence.connectionCount = 0
	settled, err := service.FinishDrain(ctx, target, hostA)
	if err != nil || len(settled.Draining) != 0 || len(settled.Applied.Endpoints) != 1 ||
		settled.Applied.Endpoints[0].HostID != hostB {
		t.Fatalf("finish cutover: %v, %v", settled, err)
	}
}
