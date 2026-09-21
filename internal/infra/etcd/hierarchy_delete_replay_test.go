package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testhierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func TestHierarchyDeletionProjectReplayUsesCanonicalTenantScope(t *testing.T) {
	// Rationale: project idempotency ownership is the containing Tenant, not the
	// Project id, so sibling Projects must use one canonical replay scope.
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	tenantID := hierarchyTestID(ids.KindTenant, 701)
	projects := []testhierarchy.ProjectRecord{
		{
			ID:       hierarchyTestID(ids.KindProject, 702),
			TenantID: tenantID,
			Slug:     "one",
			Name:     "One",
			Kind:     testhierarchy.ProjectKindTenant,
		},
		{
			ID:       hierarchyTestID(ids.KindProject, 703),
			TenantID: tenantID,
			Slug:     "two",
			Name:     "Two",
			Kind:     testhierarchy.ProjectKindTenant,
		},
	}
	mutations := make([]testkeyvalue.Mutation, 0, len(projects))
	for _, project := range projects {
		value, err := testhierarchy.EncodeProject(project)
		if err != nil {
			t.Fatalf("encodeProject() error = %v", err)
		}
		mutations = append(
			mutations,
			testkeyvalue.Mutation{
				Type:  testkeyvalue.MutationPut,
				Key:   testhierarchy.ProjectKey(project.ID),
				Value: value,
			},
		)
	}
	if result, err := store.Transact(ctx, nil, mutations); err != nil || !result.Succeeded {
		t.Fatalf("seed Projects = %#v/%v", result, err)
	}
	repository, err := newHierarchyDeletionRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyDeletionRepository() error = %v", err)
	}
	for _, project := range projects {
		resolved, err := repository.ResolveDeletionTarget(
			ctx,
			testhierarchydeletion.HierarchyDeletionTargetProject,
			project.ID,
		)
		if err != nil {
			t.Fatalf("ResolveDeletionTarget(%s) error = %v", project.ID, err)
		}
		if resolved.TargetKind != testhierarchydeletion.HierarchyDeletionTargetProject ||
			resolved.ScopeKind != testidempotency.IdempotencyScopeTenant ||
			resolved.ScopeID != tenantID {
			t.Fatalf("ResolveDeletionTarget(%s) = %#v", project.ID, resolved)
		}
	}
}

func TestIdempotencyHierarchyReplayIndexPreservesOwnerIsolationAndOriginalTask(t *testing.T) {
	// Rationale: reverse target indexes must preserve both owner isolation and
	// the original accepted Task after the target has changed or been removed.
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("NewIdempotencyRepository() error = %v", err)
	}
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	tenantOne := hierarchyTestID(ids.KindTenant, 704)
	tenantTwo := hierarchyTestID(ids.KindTenant, 705)
	projectOne := hierarchyTestID(ids.KindProject, 706)
	projectTwo := hierarchyTestID(ids.KindProject, 707)
	key := "hierarchy-project-delete-key-0001"
	originalTask := ids.NewAt(ids.KindTask, now, 708)
	marker := hierarchyReplayTaskMarker(now, tenantOne, projectOne, key, originalTask)
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	markerValue, err := testidempotency.EncodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatalf("encodeIdempotencyMarker() error = %v", err)
	}
	targetKey, err := testidempotency.IdempotencyReplayTargetKey(
		*marker.ReplayTarget,
		marker.Locator.Method,
		marker.Locator.Route,
		marker.Locator.Key,
	)
	if err != nil {
		t.Fatalf("idempotencyReplayTargetKey() error = %v", err)
	}
	targetValue, err := testidempotency.EncodeReplayTargetReference(markerKey)
	if err != nil {
		t.Fatalf("encodeReplayTargetReference() error = %v", err)
	}
	if result, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: markerKey, Value: markerValue},
		{Type: testkeyvalue.MutationPut, Key: targetKey, Value: targetValue},
	}); err != nil || !result.Succeeded {
		t.Fatalf("seed replay marker = %#v/%v", result, err)
	}

	// A second Project under the same Tenant maps to the same owner-scoped
	// marker. It cannot claim the key, while another Tenant can.
	other := marker
	other.Locator.ScopeID = tenantOne
	other.ReplayTarget = &testidempotency.IdempotencyReplayTarget{
		Kind: testidempotency.IdempotencyReplayTargetProject,
		ID:   projectTwo,
	}
	otherValue, err := testidempotency.EncodeIdempotencyMarker(other)
	if err != nil {
		t.Fatalf("encode second marker = %v", err)
	}
	otherTargetKey, err := testidempotency.IdempotencyReplayTargetKey(
		*other.ReplayTarget,
		other.Locator.Method,
		other.Locator.Route,
		other.Locator.Key,
	)
	if err != nil {
		t.Fatalf("idempotencyReplayTargetKey(second) error = %v", err)
	}
	otherTargetValue, err := testidempotency.EncodeReplayTargetReference(markerKey)
	if err != nil {
		t.Fatalf("encode second target reference = %v", err)
	}
	if result, err := store.Transact(ctx,
		[]testkeyvalue.Condition{{Key: markerKey}, {Key: otherTargetKey}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: markerKey, Value: otherValue}, {Type: testkeyvalue.MutationPut, Key: otherTargetKey, Value: otherTargetValue}},
	); err != nil || result.Succeeded {
		t.Fatalf("same-owner key claim = %#v/%v, want conflict", result, err)
	}

	other.Locator.ScopeID = tenantTwo
	other.ReplayTarget = &testidempotency.IdempotencyReplayTarget{
		Kind: testidempotency.IdempotencyReplayTargetProject,
		ID:   projectTwo,
	}
	otherMarkerKey, err := testidempotency.IdempotencyMarkerKey(other.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey(second owner) error = %v", err)
	}
	otherValue, err = testidempotency.EncodeIdempotencyMarker(other)
	if err != nil {
		t.Fatalf("encode second-owner marker = %v", err)
	}
	otherTargetValue, err = testidempotency.EncodeReplayTargetReference(otherMarkerKey)
	if err != nil {
		t.Fatalf("encode second-owner target reference = %v", err)
	}
	if result, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: otherMarkerKey, Value: otherValue},
		{Type: testkeyvalue.MutationPut, Key: otherTargetKey, Value: otherTargetValue},
	}); err != nil || !result.Succeeded {
		t.Fatalf("different-owner key claim = %#v/%v", result, err)
	}

	locator, revision, found, err := repository.ResolveReplayLocatorAtRevision(
		ctx, *marker.ReplayTarget, marker.Locator.Method, marker.Locator.Route, marker.Locator.Key,
	)
	if err != nil || !found || locator != marker.Locator || revision <= 0 {
		t.Fatalf("ResolveReplayLocatorAtRevision() = %#v/%d/%t/%v", locator, revision, found, err)
	}
	evidence, err := repository.ReadAtRevision(ctx, locator, revision)
	if err != nil || evidence == nil {
		t.Fatalf("ReadAtRevision() = %#v/%v", evidence, err)
	}
	readMarker, err := evidence.Marker()
	if err != nil || readMarker.TaskID != originalTask {
		t.Fatalf("replayed marker TaskID = %#v/%v", readMarker, err)
	}
	var response struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal(readMarker.Response.Body, &response); err != nil || response.TaskID != originalTask {
		t.Fatalf("replayed original response = %#v/%v", response, err)
	}
}

func hierarchyReplayTaskMarker(
	now time.Time,
	tenantID, projectID, key, taskID string,
) testidempotency.IdempotencyMarker {
	ciphertext := []byte("hierarchy-protected-intent")
	digest := sha256.Sum256(ciphertext)
	body, _ := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: taskID})
	return testidempotency.IdempotencyMarker{
		Kind: testidempotency.IdempotencyMarkerTask, State: testidempotency.IdempotencyMarkerCompleted,
		Locator: testidempotency.IdempotencyLocator{
			ScopeKind: testidempotency.IdempotencyScopeTenant,
			ScopeID:   tenantID,
			Method:    http.MethodDelete,
			Route:     "/projects/{id}",
			Key:       key,
		},
		ReplayTarget: &testidempotency.IdempotencyReplayTarget{
			Kind: testidempotency.IdempotencyReplayTargetProject,
			ID:   projectID,
		},
		Intent: testidempotency.ProtectedIntentRecord{
			EnvelopeVersion:  1,
			Cipher:           "age-x25519",
			DigestAlgorithm:  "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]),
			Ciphertext:       ciphertext,
		},
		Response: testidempotency.IdempotencyResponse{
			Status:      http.StatusAccepted,
			ContentKind: "application/json",
			Body:        body,
		},
		TaskID: taskID, CreatedAt: now, UpdatedAt: now, TerminalAt: now, RetainUntil: now.Add(testidempotency.MarkerRetention),
	}
}
