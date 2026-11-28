package publicingress

import (
	"context"
	"net/netip"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	edge "github.com/AlanD20/groundplane/internal/core/publicingress"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	storage "github.com/AlanD20/groundplane/internal/infra/etcd/publicingress"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func TestPublicationFencesReviewedAuthorityAndRetainsLastGood(t *testing.T) {
	ctx := t.Context()
	host := ids.New(ids.KindHost)
	otherHost := ids.New(ids.KindHost)
	environment := ids.New(ids.KindEnvironment)
	release := ids.New(ids.KindDeployment)
	route := core.Route{
		ID: ids.New(ids.KindRoute), Host: "app.example.com", Path: "/",
		TargetServiceID: ids.New(ids.KindService), TargetPort: 8080, Exposure: "public",
	}
	bindAddress := netip.MustParseAddr("192.0.2.20")
	desired := Desired{Enabled: true, Addresses: []netip.Addr{bindAddress}, RouteIDs: []string{route.ID}}
	store := &memoryStore{revision: 1, records: map[string]keyvalue.KeyValue{}}
	service := New(
		storage.NewRepository(store),
		authorityFunc(func(_ context.Context, host string, desired Desired) (Capture, error) {
			input := edge.Input{HostID: host, Revision: store.revision, Addresses: desired.Addresses}
			if len(desired.RouteIDs) > 0 {
				input.Routes = []edge.Route{{
					Source: route, EnvironmentID: environment,
					Endpoint: edge.Endpoint{
						Kind: edge.EndpointServiceProxy, HostID: host, EnvironmentID: environment,
						RouteID: route.ID, ServiceID: route.TargetServiceID, ReleaseID: release,
						Address: netip.MustParseAddrPort("10.5.1.2:8080"), AcknowledgedRevision: 1,
					},
				}}
			}
			return Capture{Input: input, Guards: []keyvalue.Condition{{Key: "/tunnel-claim"}}}, nil
		}),
	)

	review, err := service.Preflight(ctx, host, desired)
	if err != nil {
		t.Fatal(err)
	}
	task := ids.New(ids.KindTask)
	pending, begin, err := service.PrepareBegin(ctx, host, task, review.Revision, review.ReviewToken, desired)
	if err != nil {
		t.Fatal(err)
	}
	commitChange(t, store, begin)
	if _, err := service.Preflight(ctx, otherHost, desired); err == nil {
		t.Fatal("a second Host obtained a reserved hostname")
	}

	wrong := storage.Receipt{RuntimeID: strings.Repeat("a", 64), Digest: pending.Digest, Revision: pending.Revision}
	if _, err := service.PrepareSettle(ctx, host, task, wrong); err == nil {
		t.Fatal("an unauthorized Agent receipt promoted a publication")
	}
	capturedRelease := release
	release = ids.New(ids.KindDeployment)
	if _, _, err := service.PrepareAuthorize(ctx, host, task); err == nil {
		t.Fatal("activation accepted a release that differed from review")
	}
	release = capturedRelease
	_, authorize, err := service.PrepareAuthorize(ctx, host, task)
	if err != nil {
		t.Fatal(err)
	}
	commitChange(t, store, authorize)
	if _, err := service.PrepareCancel(ctx, host, task); err == nil {
		t.Fatal("cancellation discarded an activation after Agent authorization")
	}
	settle, err := service.PrepareSettle(ctx, host, task, wrong)
	if err != nil {
		t.Fatal(err)
	}
	commitChange(t, store, settle)

	withoutRoute := Desired{Enabled: true, Addresses: desired.Addresses}
	review, err = service.Preflight(ctx, host, withoutRoute)
	if err != nil {
		t.Fatal(err)
	}
	withdraw := ids.New(ids.KindTask)
	_, begin, err = service.PrepareBegin(ctx, host, withdraw, review.Revision, review.ReviewToken, withoutRoute)
	if err != nil {
		t.Fatal(err)
	}
	commitChange(t, store, begin)
	if _, err := service.Preflight(ctx, otherHost, desired); err == nil {
		t.Fatal("pending withdrawal released a live hostname")
	}
	_, authorize, err = service.PrepareAuthorize(ctx, host, withdraw)
	if err != nil {
		t.Fatal(err)
	}
	commitChange(t, store, authorize)
	restore, err := service.PrepareRestore(ctx, host, withdraw, wrong)
	if err != nil {
		t.Fatal(err)
	}
	commitChange(t, store, restore)
	got, err := service.Read(ctx, host)
	if err != nil || got.Pending != nil || got.Active == nil || *got.Active != wrong {
		t.Fatalf("last-known-good was lost: state=%+v err=%v", got, err)
	}
}

type authorityFunc func(context.Context, string, Desired) (Capture, error)

func (capture authorityFunc) Capture(ctx context.Context, host string, desired Desired) (Capture, error) {
	return capture(ctx, host, desired)
}

type memoryStore struct {
	revision int64
	records  map[string]keyvalue.KeyValue
}

func (store *memoryStore) Get(_ context.Context, key string) (*keyvalue.GetResult, error) {
	return &keyvalue.GetResult{ReadRevision: store.revision, Entry: store.read(key)}, nil
}

func (store *memoryStore) GetMany(_ context.Context, request keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error) {
	reply := &keyvalue.GetManyResult{ReadRevision: store.revision}
	for _, key := range request.Keys {
		reply.Values = append(reply.Values, store.read(key))
	}
	return reply, nil
}

func (store *memoryStore) read(key string) *keyvalue.KeyValue {
	value, found := store.records[key]
	if !found {
		return nil
	}
	value.Value = slices.Clone(value.Value)
	return &value
}

func (store *memoryStore) Transact(
	_ context.Context,
	guards []keyvalue.Condition,
	updates []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	for _, guard := range guards {
		if store.records[guard.Key].ModRevision != guard.ModRevision {
			return keyvalue.TransactionResult{Revision: store.revision}, nil
		}
	}
	store.revision++
	for _, update := range updates {
		if update.Type == keyvalue.MutationDelete {
			delete(store.records, update.Key)
		} else {
			store.records[update.Key] = keyvalue.KeyValue{Key: update.Key, Value: slices.Clone(update.Value), ModRevision: store.revision}
		}
	}
	return keyvalue.TransactionResult{Revision: store.revision, Succeeded: true}, nil
}

func commitChange(t *testing.T, store *memoryStore, change workqueue.Change) {
	t.Helper()
	defer keyvalue.ClearMutationValues(change.Mutations)
	result, err := store.Transact(t.Context(), change.Conditions, change.Mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("expected CAS to succeed: result=%+v err=%v", result, err)
	}
}
