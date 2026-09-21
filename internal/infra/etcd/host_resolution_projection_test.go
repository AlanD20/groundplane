package etcd

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testhostresolution "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func TestHostResolutionProjectionPublicationUsesFixedInputRevisionAndCAS(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := testhostresolution.New(&releaseLogMemoryStore{store})
	if err != nil {
		t.Fatalf("newComponentRepository() error = %v", err)
	}
	routes := []testhostresolution.HostResolutionRouteRecord{{
		EnvironmentID:     ids.NewAt(ids.KindEnvironment, time.Unix(1_700_000_000, 0).UTC(), 1),
		DesiredRevisionID: ids.NewAt(ids.KindTask, time.Unix(1_700_000_000, 0).UTC(), 2), AppliedRevision: 17,
		RouteID: ids.NewAt(ids.KindRoute, time.Unix(1_700_000_000, 0).UTC(), 3),
		ServiceID: ids.NewAt(
			ids.KindService,
			time.Unix(1_700_000_000, 0).UTC(),
			4,
		), Hostname: "app.example.test", IPv4: "192.0.2.10",
	}}
	first, err := prepareHostResolutionProjectionPublication(nil, 41, routes)
	if err != nil {
		t.Fatalf("first publication: %v", err)
	}
	result, err := store.Transact(ctx, first.conditions, first.mutations)
	first.clear()
	if err != nil || !result.Succeeded {
		t.Fatalf("first transaction = %#v, %v", result, err)
	}
	stored, found, err := repository.GetHostResolutionProjection(ctx)
	if err != nil || !found || stored.Record.InputRevision != 41 {
		t.Fatalf("stored projection = %#v, %t, %v", stored, found, err)
	}
	current, err := store.Get(ctx, testhostresolution.StorageKey)
	if err != nil || current.Entry == nil {
		t.Fatalf("current projection = %#v, %v", current, err)
	}
	next, err := prepareHostResolutionProjectionPublication(current.Entry, 42, routes)
	if err != nil || next.record.InputRevision != 42 || next.record.InputSHA256 != stored.Record.InputSHA256 {
		t.Fatalf("next publication = %#v, %v", next.record, err)
	}
	if _, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: testhostresolution.StorageKey, Value: current.Entry.Value}}); err != nil {
		t.Fatalf("race projection: %v", err)
	}
	conflict, err := store.Transact(ctx, next.conditions, next.mutations)
	next.clear()
	if err != nil || conflict.Succeeded {
		t.Fatalf("stale publication = %#v, %v", conflict, err)
	}
	restarted, err := testhostresolution.New(&releaseLogMemoryStore{store})
	if err != nil {
		t.Fatalf("restart repository: %v", err)
	}
	after, found, err := restarted.GetHostResolutionProjection(ctx)
	if err != nil || !found || !reflect.DeepEqual(after.Record, stored.Record) {
		t.Fatalf("restart projection = %#v, %t, %v", after, found, err)
	}
}

func TestHostResolutionResolverPublicationPreservesRouteProvenance(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0).UTC()
	current := testhostresolution.HostResolutionProjectionRecord{
		Routes: []testhostresolution.HostResolutionRouteRecord{{
			EnvironmentID:     ids.NewAt(ids.KindEnvironment, now, 10),
			DesiredRevisionID: ids.NewAt(ids.KindTask, now, 11), AppliedRevision: 17,
			RouteID: ids.NewAt(ids.KindRoute, now, 12), ServiceID: ids.NewAt(ids.KindService, now, 13),
			Hostname: "app.example.test", IPv4: "192.0.2.10",
		}},
	}
	next := slices.Clone(current.Routes)
	next[0].DesiredRevisionID = ids.NewAt(ids.KindTask, now, 14)
	preserveHostResolutionDesiredRevisionIDs(&current, next)
	if next[0].DesiredRevisionID != current.Routes[0].DesiredRevisionID {
		t.Fatalf("resolver publication changed Route provenance: %#v", next[0])
	}
	next[0].IPv4 = "10.25.0.3"
	next[0].DesiredRevisionID = ids.NewAt(ids.KindTask, now, 15)
	preserveHostResolutionDesiredRevisionIDs(&current, next)
	if next[0].DesiredRevisionID == current.Routes[0].DesiredRevisionID {
		t.Fatalf("changed Route input retained stale provenance: %#v", next[0])
	}
}

func TestHostResolutionProjectionPublicationHasConstantTransactionSize(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0).UTC()
	routes := make([]testhostresolution.HostResolutionRouteRecord, 1024)
	for index := range routes {
		routes[index] = testhostresolution.HostResolutionRouteRecord{
			EnvironmentID:     ids.NewAt(ids.KindEnvironment, now, 20),
			DesiredRevisionID: ids.NewAt(ids.KindTask, now, 21), AppliedRevision: 17,
			RouteID:   ids.NewAt(ids.KindRoute, now, int64(100+index)),
			ServiceID: ids.NewAt(ids.KindService, now, 22),
			Hostname:  fmt.Sprintf("route-%04d.example.test", index), IPv4: "192.0.2.10",
		}
	}
	publication, err := prepareHostResolutionProjectionPublication(nil, 41, routes)
	if err != nil {
		t.Fatalf("prepareHostResolutionProjectionPublication(1024) error = %v", err)
	}
	defer publication.clear()
	if len(publication.conditions) != 1 || len(publication.mutations) != 1 {
		t.Fatalf(
			"projection operation count = %d/%d, want 1/1",
			len(publication.conditions),
			len(publication.mutations),
		)
	}
}

func TestHostResolutionProviderSnapshotHasConstantTransactionSize(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	providerIDs := make(map[string]struct{}, 1024)
	mutations := make([]testkeyvalue.Mutation, 0, 1025)
	for index := 0; index < 1024; index++ {
		record := componentRecordTestRecord(t, int64(10_000+index))
		value, encodeErr := testcomponents.EncodeRecord(record)
		if encodeErr != nil {
			t.Fatalf("encodeComponentRecord(%d) error = %v", index, encodeErr)
		}
		defer clear(value)
		providerIDs[record.Desired.ID] = struct{}{}
		mutations = append(
			mutations,
			testkeyvalue.Mutation{
				Type:  testkeyvalue.MutationPut,
				Key:   testcomponents.RecordKey(record.Desired.ID),
				Value: value,
			},
		)
	}
	mutations = append(mutations, testcomponents.WriteFenceMutation("seed"))
	seed, err := store.Transact(ctx, nil, mutations)
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed provider snapshot = %#v, %v", seed, err)
	}
	providers, conditions, err := repository.hostResolutionComponents(ctx, providerIDs, seed.Revision)
	if err != nil {
		t.Fatalf("hostResolutionComponents(1024) error = %v", err)
	}
	if len(providers) != len(providerIDs) || len(conditions) != 1 || conditions[0].Key != testcomponents.WriteFenceKey {
		t.Fatalf(
			"provider snapshot = %d providers / %#v conditions, want 1024 / one collection fence",
			len(providers),
			conditions,
		)
	}
}
