package etcd

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredauthoring"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// Rationale: first Entry/Zone authoring needs a durable predecessor, and direct
// metadata changes must survive export without retargeting an existing runtime.
func TestDirectDesiredPublicationPreservesRuntimeAndRejectsStaleCandidate(t *testing.T) {
	ctx := t.Context()
	_, store, environment, _ := componentRepositoryTestHierarchy(t)
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	marker := backupPolicyReplacementMarker(environment.Record.ID, "direct-baseline-0001")
	if err := hierarchy.InitializeEnvironmentDesiredState(ctx, environment.Record.ID, marker.Locator, marker.Intent, marker.CreatedAt); err != nil {
		t.Fatal(err)
	}
	baseline, found, err := blueprints.ReadCurrentProjection(ctx, store, environment.Record.ID, 0)
	if err != nil || !found {
		t.Fatalf("durable baseline = %v/%v", found, err)
	}
	if err := hierarchy.InitializeEnvironmentDesiredState(ctx, environment.Record.ID, marker.Locator, marker.Intent, marker.CreatedAt); err != nil {
		t.Fatal(err)
	}
	unchanged, _, err := blueprints.ReadCurrentProjection(ctx, store, environment.Record.ID, 0)
	if err != nil || unchanged.Record.RevisionID != baseline.Record.RevisionID {
		t.Fatal("initialization changed an existing head")
	}
	publication, err := prepareDirectDesiredPublication(ctx, store, environment.Record.ID, marker,
		func(input *core.BlueprintDesiredInput) error { input.NetworkPool = "10.48.0.0/16"; return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer clearRouteHeadPublication(publication)
	stale, err := prepareDirectDesiredPublication(ctx, store, environment.Record.ID, marker,
		func(input *core.BlueprintDesiredInput) error { input.NetworkPool = "10.49.0.0/16"; return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer clearRouteHeadPublication(stale)
	result, err := store.Transact(ctx, publication.conditions, publication.mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("publish = %v/%v", result.Succeeded, err)
	}
	result, err = store.Transact(ctx, stale.conditions, stale.mutations)
	if err != nil || result.Succeeded {
		t.Fatalf("stale candidate committed = %v/%v", result.Succeeded, err)
	}
	current, _, err := blueprints.ReadCurrentProjection(ctx, store, environment.Record.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if current.Record.RevisionID == baseline.Record.RevisionID || current.Record.RenderGeneration != baseline.Record.RenderGeneration || !bytes.Equal(current.Record.ComposeArtifact, baseline.Record.ComposeArtifact) {
		t.Fatal("metadata change altered runtime authority or failed to advance desired identity")
	}
	input, _, err := blueprints.ReadCurrentDesiredInput(ctx, store, environment.Record.ID, 0)
	if err != nil || input.Record.Input.NetworkPool != "10.48.0.0/16" {
		t.Fatalf("desired pool = %q/%v", input.Record.Input.NetworkPool, err)
	}
	prior, found, err := blueprints.ReadDesiredInputRevision(ctx, store, environment.Record.ID, baseline.Record.RevisionID, 0)
	if err != nil || !found || prior.Record.Input.NetworkPool != environment.Record.NetworkPool {
		t.Fatal("immutable predecessor was altered")
	}
	descriptorKey := stale.conditions[3].Key
	descriptorRead, err := store.Get(ctx, descriptorKey)
	if err != nil || descriptorRead.Entry == nil {
		t.Fatal("stale candidate lost its cleanup descriptor")
	}
	descriptor, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(descriptorRead.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	if removed, err := desiredauthoring.Cleanup(ctx, store, descriptor, descriptorRead.Entry.ModRevision, marker.CreatedAt); err != nil || removed {
		t.Fatalf("unexpired staging cleanup = %v/%v", removed, err)
	}
	if removed, err := desiredauthoring.Cleanup(ctx, store, descriptor, descriptorRead.Entry.ModRevision,
		marker.CreatedAt.Add(blueprints.EnvironmentBlueprintStageExpiry+time.Second)); err != nil || !removed {
		t.Fatalf("expired staging cleanup = %v/%v", removed, err)
	}
	remaining, err := store.Range(ctx, keyvalue.RangeRequest{Prefix: blueprints.EnvironmentBlueprintRevisionPrefixFinal(environment.Record.ID, descriptor.Claim.RevisionID), Limit: 32})
	if err != nil || len(remaining.Values) != 0 {
		t.Fatal("expired private content remains")
	}
	retained, _, err := blueprints.ReadCurrentProjection(ctx, store, environment.Record.ID, 0)
	if err != nil || retained.Record.RevisionID != current.Record.RevisionID {
		t.Fatal("cleanup changed the published revision")
	}
}

// Rationale: supported authored input can exceed one transaction; staging must
// keep every write bounded and leave the head unchanged until atomic promotion.
func TestDirectDesiredPublicationStagesLargeInputWithinTransactionLimits(t *testing.T) {
	_, memory, environment, _ := componentRepositoryTestHierarchy(t)
	store := &boundedDesiredStore{memoryHierarchyStore: memory}
	marker := backupPolicyReplacementMarker(environment.Record.ID, "large-desired-0001")
	publication, err := desiredauthoring.Prepare(t.Context(), store, environment.Record.ID, marker,
		func(input *core.BlueprintDesiredInput, _ *environmentprojection.EnvironmentComposeProjection) error {
			input.Scripts = make(map[string]core.ScriptSpec)
			for index := 0; index < 24; index++ {
				name := fmt.Sprintf("script-%d", index)
				input.Scripts[name] = core.ScriptSpec{Slug: name, Service: "app", When: core.ScriptManual, Script: "echo " + strings.Repeat("x", 60_000)}
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearMutationValues(publication.Mutations)
	_, found, err := blueprints.ReadCurrentDesiredInput(t.Context(), store, environment.Record.ID, 0)
	if err != nil || found {
		t.Fatal("staging exposed desired state before resource commit")
	}
	result, err := store.Transact(t.Context(), publication.Conditions, publication.Mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("large publication = %v/%v", result.Succeeded, err)
	}
	input, found, err := blueprints.ReadCurrentDesiredInput(t.Context(), store, environment.Record.ID, 0)
	if err != nil || !found || len(input.Record.Input.Scripts) != 24 {
		t.Fatalf("published large input lost Scripts: %v/%v", found, err)
	}
}

type boundedDesiredStore struct{ *memoryHierarchyStore }

func (store *boundedDesiredStore) Transact(ctx context.Context, conditions []keyvalue.Condition, mutations []keyvalue.Mutation) (keyvalue.TransactionResult, error) {
	budget, err := store.MeasureTransaction(ctx, conditions, mutations)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	if !budget.Fits() {
		return keyvalue.TransactionResult{}, fmt.Errorf("transaction exceeds limits: %+v", budget)
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}
