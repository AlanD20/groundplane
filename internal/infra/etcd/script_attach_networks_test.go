package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

func TestScriptAttachSourceConditionsFenceBoundBackingProjection(t *testing.T) {
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
	read, found, err := testenvironmentqueries.NewProjectionReader(store).
		GetEnvironmentComposeProjection(ctx, attach.BackingEnvironmentID)
	if err != nil || !found {
		t.Fatalf("backing projection: %v", err)
	}
	intended := []testkeyvalue.Versioned[testattachments.Record]{{Record: attach}}
	bound, err := testenvironmentqueries.JoinZone(read, read.Record.DesiredZones[0])
	if err != nil || bound.Record.Desired != zone || bound.Revision != read.Revision ||
		bound.ReadRevision != read.ReadRevision {
		t.Fatalf("bound backing network: %#v, %v", bound, err)
	}
	networks := testscriptsourcequeries.ScriptAttachSources{
		Networks: []testkeyvalue.Versioned[testzones.Record]{bound},
		Heads: []testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintHead]{{
			Record: testblueprints.EnvironmentBlueprintHead{
				EnvironmentID: attach.BackingEnvironmentID, RevisionID: read.Record.RevisionID,
			},
			Revision: read.Revision, ReadRevision: read.ReadRevision,
		}},
		Attaches: intended,
	}
	sources := testscriptsourcequeries.ScriptExecutionSources{
		Environment:       scope.Environment,
		DesiredHead:       scope.DesiredHead,
		DesiredProjection: scope.ComposeProjection,
		AttachSources:     networks,
	}
	conditions := scriptExecutionProjectionConditions(sources)
	backingHead := testblueprints.EnvironmentBlueprintHeadKey(attach.BackingEnvironmentID)
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
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationPut, Key: backingHead, Value: []byte(projection.RevisionID)},
		},
	)
	if err != nil || !changed.Succeeded {
		t.Fatalf("change backing head: %v", err)
	}
	publication, err := store.Transact(
		ctx,
		conditions,
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationPut, Key: "/test/stale-script-network", Value: []byte("forbidden")},
		},
	)
	if err != nil || publication.Succeeded {
		t.Fatalf("stale backing topology publication: %v, %v", publication, err)
	}
}
