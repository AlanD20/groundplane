//go:build etcd_acceptance

package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestHierarchyDeletionRealEtcdProjectReplayIndexOwnerIsolationAndCorruption(t *testing.T) {
	// Rationale: the durable reverse index must survive target finalization and
	// reject a cross-linked marker instead of resolving the live target.
	ctx := context.Background()
	endpoint := hierarchyDeletionAcceptanceEndpoint(t)
	store := hierarchyDeletionAcceptanceStore(t, ctx, endpoint, hierarchyDeletionAcceptancePrefix(t, "project-replay"))
	repository, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("NewIdempotencyRepository() error = %v", err)
	}
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	tenantOne := ids.NewAt(ids.KindTenant, now, 801)
	tenantTwo := ids.NewAt(ids.KindTenant, now, 802)
	projectOne := ids.NewAt(ids.KindProject, now, 803)
	projectTwo := ids.NewAt(ids.KindProject, now, 804)
	key := "hierarchy-project-delete-key-0002"
	markerOne := hierarchyReplayTaskMarker(now, tenantOne, projectOne, key, ids.NewAt(ids.KindTask, now, 805))
	put := func(marker IdempotencyMarker) (string, error) {
		markerKey, err := idempotencyMarkerKey(marker.Locator)
		if err != nil {
			return "", err
		}
		markerValue, err := encodeIdempotencyMarker(marker)
		if err != nil {
			return "", err
		}
		targetKey, err := idempotencyReplayTargetKey(
			*marker.ReplayTarget,
			marker.Locator.Method,
			marker.Locator.Route,
			marker.Locator.Key,
		)
		if err != nil {
			return "", err
		}
		targetValue, err := encodeReplayTargetReference(markerKey)
		if err != nil {
			return "", err
		}
		result, err := store.Transact(ctx, nil, []Mutation{
			{Type: MutationPut, Key: markerKey, Value: markerValue},
			{Type: MutationPut, Key: targetKey, Value: targetValue},
		})
		if err != nil || !result.Succeeded {
			return "", err
		}
		return markerKey, nil
	}
	markerOneKey, err := put(markerOne)
	if err != nil {
		t.Fatalf("put first replay marker = %v", err)
	}

	// Same key under the same Tenant collides at the canonical marker owner,
	// even though the reverse target index has a different Project id.
	markerTwo := hierarchyReplayTaskMarker(now, tenantOne, projectTwo, key, ids.NewAt(ids.KindTask, now, 806))
	markerTwoKey, err := idempotencyMarkerKey(markerTwo.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey(second) error = %v", err)
	}
	markerTwoValue, err := encodeIdempotencyMarker(markerTwo)
	if err != nil {
		t.Fatalf("encode second marker = %v", err)
	}
	markerTwoTargetKey, err := idempotencyReplayTargetKey(
		*markerTwo.ReplayTarget,
		markerTwo.Locator.Method,
		markerTwo.Locator.Route,
		markerTwo.Locator.Key,
	)
	if err != nil {
		t.Fatalf("idempotencyReplayTargetKey(second) error = %v", err)
	}
	markerTwoTargetValue, err := encodeReplayTargetReference(markerTwoKey)
	if err != nil {
		t.Fatalf("encode second target reference = %v", err)
	}
	if result, err := store.Transact(ctx,
		[]Condition{{Key: markerTwoKey}, {Key: markerTwoTargetKey}},
		[]Mutation{{Type: MutationPut, Key: markerTwoKey, Value: markerTwoValue}, {Type: MutationPut, Key: markerTwoTargetKey, Value: markerTwoTargetValue}},
	); err != nil || result.Succeeded {
		t.Fatalf("same-owner replay claim = %#v/%v, want conflict", result, err)
	}

	// A distinct Tenant gets an independent owner-scoped marker for the same key.
	markerTwo.Locator.ScopeID = tenantTwo
	markerTwoKey, err = put(markerTwo)
	if err != nil {
		t.Fatalf("put distinct-owner replay marker = %v", err)
	}
	if markerTwoKey == markerOneKey {
		t.Fatal("distinct owners unexpectedly shared marker key")
	}

	// Simulate finalization removing the target primary while the retained
	// reverse locator and marker remain replayable.
	if result, err := store.Transact(ctx, nil, []Mutation{{Type: MutationDelete, Key: projectKey(projectOne)}}); err != nil ||
		!result.Succeeded {
		t.Fatalf("remove finalized Project primary = %#v/%v", result, err)
	}
	locator, revision, found, err := repository.ResolveReplayLocatorAtRevision(
		ctx, *markerOne.ReplayTarget, markerOne.Locator.Method, markerOne.Locator.Route, markerOne.Locator.Key,
	)
	if err != nil || !found || revision <= 0 || locator != markerOne.Locator {
		t.Fatalf("retained replay lookup = %#v/%d/%t/%v", locator, revision, found, err)
	}
	evidence, err := repository.ReadAtRevision(ctx, locator, revision)
	if err != nil || evidence == nil {
		t.Fatalf("retained replay evidence = %#v/%v", evidence, err)
	}
	readMarker, err := evidence.Marker()
	if err != nil || readMarker.TaskID != markerOne.TaskID {
		t.Fatalf("retained original Task = %#v/%v", readMarker, err)
	}

	// Repointing the Project-1 index at Project-2's marker is corruption, not
	// a valid replay or a fallback to the requested target primary.
	targetKey, err := idempotencyReplayTargetKey(
		*markerOne.ReplayTarget,
		markerOne.Locator.Method,
		markerOne.Locator.Route,
		markerOne.Locator.Key,
	)
	if err != nil {
		t.Fatalf("idempotencyReplayTargetKey(corrupt) error = %v", err)
	}
	badReference, err := encodeReplayTargetReference(markerTwoKey)
	if err != nil {
		t.Fatalf("encode corrupt target reference = %v", err)
	}
	if result, err := store.Transact(ctx, nil, []Mutation{{Type: MutationPut, Key: targetKey, Value: badReference}}); err != nil ||
		!result.Succeeded {
		t.Fatalf("write corrupt replay link = %#v/%v", result, err)
	}
	if _, _, _, err := repository.ResolveReplayLocatorAtRevision(
		ctx, *markerOne.ReplayTarget, markerOne.Locator.Method, markerOne.Locator.Route, markerOne.Locator.Key,
	); !isKind(err, errs.KindInternal) {
		t.Fatalf("corrupt replay link error = %v, want internal", err)
	}
}
