package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

func TestScriptAttachNetworksResolveBoundBackingProjection(t *testing.T) {
	// Rationale: hook networks come from intended Attach ownership and the
	// backing projection, not similarly named authored consumer networks.
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	attach, _ := testPendingAttach(t, scope, 31, "api-db", nil)
	zone := core.Zone{ID: attach.BackingNetworkID, Name: "postgres", Subnet: "10.33.10.0/24", Internal: true,
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: attach.BackingEnvironmentID}
	projection := zoneRepositoryTestProjection(t, attach.BackingEnvironmentID, 1001, zone)
	seedServiceRepositoryTestDesiredProjection(t, store.memoryHierarchyStore, projection)
	read, found, err := currentEnvironmentProjectionAtRevision(ctx, store, attach.BackingEnvironmentID, 0)
	if err != nil || !found {
		t.Fatalf("backing projection: %v", err)
	}
	intended := []Versioned[AttachRecord]{{Record: attach}}
	networks, err := resolveScriptAttachNetworks(
		ctx,
		store,
		attach.EnvironmentID,
		attach.ServiceID,
		intended,
		read.ReadRevision,
	)
	if err != nil || len(networks.Networks) != 1 || networks.Networks[0].Record.Desired != zone ||
		networks.Networks[0].Revision != read.Revision || networks.Networks[0].ReadRevision != read.ReadRevision {
		t.Fatalf("bound backing network: %v, %v", networks, err)
	}
	for _, inputs := range [][]Versioned[AttachRecord]{nil, {{Record: attach}, {Record: attach}}} {
		got, err := resolveScriptAttachNetworks(
			ctx,
			store,
			attach.EnvironmentID,
			attach.ServiceID,
			inputs,
			read.ReadRevision,
		)
		want := 0
		if len(inputs) != 0 {
			want = 1
		}
		if err != nil || len(got.Networks) != want {
			t.Fatalf("intended set selected %d networks, want %d: %v", len(got.Networks), want, err)
		}
	}
	sources := ScriptExecutionSources{Environment: scope.Environment, DesiredHead: scope.DesiredHead,
		DesiredProjection: scope.ComposeProjection, AttachSources: networks}
	conditions := scriptExecutionProjectionConditions(sources)
	backingHead := environmentBlueprintHeadKey(attach.BackingEnvironmentID)
	fenced := false
	for _, condition := range conditions {
		fenced = fenced || condition.Key == backingHead && condition.ModRevision == networks.Heads[0].Revision
	}
	if !fenced {
		t.Fatal("Script publication omitted backing topology compare")
	}
	unchanged, err := store.Transact(ctx, conditions, nil)
	if err != nil || !unchanged.Succeeded {
		t.Fatalf("unchanged captured sources must pass publication: %v, %v", unchanged, err)
	}
	changed, err := store.Transact(
		ctx,
		nil,
		[]Mutation{{Type: MutationPut, Key: backingHead, Value: []byte(projection.RevisionID)}},
	)
	if err != nil || !changed.Succeeded {
		t.Fatalf("change backing head: %v", err)
	}
	publication, err := store.Transact(
		ctx,
		conditions,
		[]Mutation{{Type: MutationPut, Key: "/test/stale-script-network", Value: []byte("forbidden")}},
	)
	if err != nil || publication.Succeeded {
		t.Fatalf("stale backing topology publication: %v, %v", publication, err)
	}
	intended[0].Record.BackingNetworkID = ids.NewAt(ids.KindNetwork, testAttachTime, 900)
	if _, err := resolveScriptAttachNetworks(ctx, store, attach.EnvironmentID, attach.ServiceID, intended, read.ReadRevision); err == nil {
		t.Fatal("accepted missing backing network")
	}
}
