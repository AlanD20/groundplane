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
)

func TestHierarchyDeletionProjectReplayUsesCanonicalTenantScope(t *testing.T) {
	// Rationale: project idempotency ownership is the containing Tenant, not the
	// Project id, so sibling Projects must use one canonical replay scope.
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	tenantID := hierarchyTestID(ids.KindTenant, 701)
	projects := []ProjectRecord{
		{
			ID:       hierarchyTestID(ids.KindProject, 702),
			TenantID: tenantID,
			Slug:     "one",
			Name:     "One",
			Kind:     ProjectKindTenant,
		},
		{
			ID:       hierarchyTestID(ids.KindProject, 703),
			TenantID: tenantID,
			Slug:     "two",
			Name:     "Two",
			Kind:     ProjectKindTenant,
		},
	}
	mutations := make([]Mutation, 0, len(projects))
	for _, project := range projects {
		value, err := encodeProject(project)
		if err != nil {
			t.Fatalf("encodeProject() error = %v", err)
		}
		mutations = append(mutations, Mutation{Type: MutationPut, Key: projectKey(project.ID), Value: value})
	}
	if result, err := store.Transact(ctx, nil, mutations); err != nil || !result.Succeeded {
		t.Fatalf("seed Projects = %#v/%v", result, err)
	}
	repository, err := newHierarchyDeletionRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyDeletionRepository() error = %v", err)
	}
	for _, project := range projects {
		resolved, err := repository.ResolveDeletionTarget(ctx, HierarchyDeletionTargetProject, project.ID)
		if err != nil {
			t.Fatalf("ResolveDeletionTarget(%s) error = %v", project.ID, err)
		}
		if resolved.TargetKind != HierarchyDeletionTargetProject || resolved.ScopeKind != IdempotencyScopeTenant ||
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
	repository, err := newIdempotencyRepository(store)
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
	markerKey, err := idempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	markerValue, err := encodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatalf("encodeIdempotencyMarker() error = %v", err)
	}
	targetKey, err := idempotencyReplayTargetKey(
		*marker.ReplayTarget,
		marker.Locator.Method,
		marker.Locator.Route,
		marker.Locator.Key,
	)
	if err != nil {
		t.Fatalf("idempotencyReplayTargetKey() error = %v", err)
	}
	targetValue, err := encodeReplayTargetReference(markerKey)
	if err != nil {
		t.Fatalf("encodeReplayTargetReference() error = %v", err)
	}
	if result, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: markerKey, Value: markerValue},
		{Type: MutationPut, Key: targetKey, Value: targetValue},
	}); err != nil || !result.Succeeded {
		t.Fatalf("seed replay marker = %#v/%v", result, err)
	}

	// A second Project under the same Tenant maps to the same owner-scoped
	// marker. It cannot claim the key, while another Tenant can.
	other := marker
	other.Locator.ScopeID = tenantOne
	other.ReplayTarget = &IdempotencyReplayTarget{Kind: IdempotencyReplayTargetProject, ID: projectTwo}
	otherValue, err := encodeIdempotencyMarker(other)
	if err != nil {
		t.Fatalf("encode second marker = %v", err)
	}
	otherTargetKey, err := idempotencyReplayTargetKey(
		*other.ReplayTarget,
		other.Locator.Method,
		other.Locator.Route,
		other.Locator.Key,
	)
	if err != nil {
		t.Fatalf("idempotencyReplayTargetKey(second) error = %v", err)
	}
	otherTargetValue, err := encodeReplayTargetReference(markerKey)
	if err != nil {
		t.Fatalf("encode second target reference = %v", err)
	}
	if result, err := store.Transact(ctx,
		[]Condition{{Key: markerKey}, {Key: otherTargetKey}},
		[]Mutation{{Type: MutationPut, Key: markerKey, Value: otherValue}, {Type: MutationPut, Key: otherTargetKey, Value: otherTargetValue}},
	); err != nil || result.Succeeded {
		t.Fatalf("same-owner key claim = %#v/%v, want conflict", result, err)
	}

	other.Locator.ScopeID = tenantTwo
	other.ReplayTarget = &IdempotencyReplayTarget{Kind: IdempotencyReplayTargetProject, ID: projectTwo}
	otherMarkerKey, err := idempotencyMarkerKey(other.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey(second owner) error = %v", err)
	}
	otherValue, err = encodeIdempotencyMarker(other)
	if err != nil {
		t.Fatalf("encode second-owner marker = %v", err)
	}
	otherTargetValue, err = encodeReplayTargetReference(otherMarkerKey)
	if err != nil {
		t.Fatalf("encode second-owner target reference = %v", err)
	}
	if result, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: otherMarkerKey, Value: otherValue},
		{Type: MutationPut, Key: otherTargetKey, Value: otherTargetValue},
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

func hierarchyReplayTaskMarker(now time.Time, tenantID, projectID, key, taskID string) IdempotencyMarker {
	ciphertext := []byte("hierarchy-protected-intent")
	digest := sha256.Sum256(ciphertext)
	body, _ := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: taskID})
	return IdempotencyMarker{
		Kind: IdempotencyMarkerTask, State: IdempotencyMarkerCompleted,
		Locator: IdempotencyLocator{
			ScopeKind: IdempotencyScopeTenant,
			ScopeID:   tenantID,
			Method:    http.MethodDelete,
			Route:     "/projects/{id}",
			Key:       key,
		},
		ReplayTarget: &IdempotencyReplayTarget{Kind: IdempotencyReplayTargetProject, ID: projectID},
		Intent: ProtectedIntentRecord{
			EnvelopeVersion:  1,
			Cipher:           "age-x25519",
			DigestAlgorithm:  "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]),
			Ciphertext:       ciphertext,
		},
		Response: IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json", Body: body},
		TaskID:   taskID, CreatedAt: now, UpdatedAt: now, TerminalAt: now, RetainUntil: now.Add(markerRetention),
	}
}
