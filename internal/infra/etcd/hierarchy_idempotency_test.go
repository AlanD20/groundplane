package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestHierarchyCreationPublishesDeletionCoordination(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenant := TenantRecord{ID: hierarchyTestID(ids.KindTenant, 191), Slug: "deletion", Name: "Deletion"}
	if _, err := hierarchy.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project := ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, 192), TenantID: tenant.ID,
		Slug: "deletion", Name: "Deletion", Kind: ProjectKindTenant,
	}
	if _, err := hierarchy.CreateProject(ctx, project); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	now := time.Date(2026, 8, 28, 17, 30, 0, 0, time.UTC)
	environment := EnvironmentRecord{
		ID: hierarchyTestID(ids.KindEnvironment, 193), ProjectID: project.ID,
		Name: "deletion", NetworkPool: "10.120.253.0/24",
		VolumeDir: "/var/lib/groundplane/vol/" + tenant.ID + "/" + project.ID + "/" + hierarchyTestID(
			ids.KindEnvironment,
			193,
		),
		ProvisioningState: EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, now, 194),
		CreatedAt:         now,
	}
	if _, err := hierarchy.CreateEnvironment(ctx, environment); err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	journal, err := newHierarchyDeletionRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyDeletionRepository() error = %v", err)
	}
	begin := hierarchyDeletionCreationTestBegin(
		now,
		HierarchyDeletionTargetEnvironment,
		environment.ID,
		HierarchyDeletionOperationEnvironment,
		"e",
	)
	result, err := journal.Begin(ctx, begin)
	if err != nil {
		t.Fatalf("Begin(Environment deletion) error = %v", err)
	}
	if result.Operation.Tombstone.TargetID != environment.ID || result.Operation.Tombstone.DeletionEpoch != 2 {
		t.Fatalf("Begin(Environment deletion) = %#v", result.Operation.Tombstone)
	}
}

func hierarchyDeletionCreationTestBegin(
	now time.Time,
	targetKind HierarchyDeletionTargetKind,
	targetID string,
	operationKind HierarchyDeletionOperationKind,
	digestDigit string,
) HierarchyDeletionBegin {
	taskID := ids.NewAt(ids.KindTask, now, 195)
	taskOperationID := ids.NewAt(ids.KindOperation, now, 196)
	key := "hierarchy-creation-deletion-key"
	ciphertext := []byte("protected-" + key)
	digest := sha256.Sum256(ciphertext)
	body := []byte(`{"task_id":"` + taskID + `"}`)
	marker := IdempotencyMarker{
		Kind: IdempotencyMarkerTask, State: IdempotencyMarkerPending,
		Locator: IdempotencyLocator{
			ScopeKind: IdempotencyScopeKind(targetKind), ScopeID: targetID,
			Method: http.MethodDelete, Route: "/" + string(targetKind) + "/{id}", Key: key,
		},
		Intent: ProtectedIntentRecord{
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
		},
		Response: IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json", Body: body},
		TaskID:   taskID, CreatedAt: now, UpdatedAt: now,
	}
	idempotencyDigest := sha256.Sum256([]byte(key))
	return HierarchyDeletionBegin{
		OperationID: "del_" + strings.Repeat(digestDigit, 32), TaskOperationID: taskOperationID,
		OperationKind: operationKind, TargetKind: targetKind, TargetID: targetID,
		TaskID: taskID, IdempotencyHash: hex.EncodeToString(idempotencyDigest[:]), Marker: marker,
		CreatedAt: now, DeadlineAt: now.Add(hierarchyDeletionAttemptTimeout),
	}
}

func TestHierarchyCreateTenantIdempotentCommitsResourceIndexesAndMarker(t *testing.T) {
	t.Parallel()

	repository, err := newHierarchyRepository(newMemoryHierarchyStore())
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	record := TenantRecord{
		ID: hierarchyTestID(ids.KindTenant, 201), Slug: "acme", Name: "Acme",
		Description: "Production workloads",
	}
	marker := testDirectMarker()
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopePlatform, ScopeID: "-", Method: http.MethodPost,
		Route: "/tenants", Key: "tenant-create-key-0001",
	}
	marker.Response = IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json", Body: []byte(`{"id":"tenant"}`),
	}

	result, err := repository.CreateTenantIdempotent(context.Background(), record, marker)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("CreateTenantIdempotent() = %#v, %v", result, err)
	}
	stored, err := repository.GetTenant(context.Background(), record.ID)
	if err != nil || stored.Record != record {
		t.Fatalf("GetTenant() = %#v, %v", stored, err)
	}
	assertHierarchyCoordinationRecord(
		t,
		repository.store.(*memoryHierarchyStore),
		HierarchyDeletionTargetTenant,
		record.ID,
		stored.Revision,
	)
	replayed, err := repository.CreateTenantIdempotent(context.Background(), record, marker)
	if err != nil || replayed.kind != idempotencyTransactionExisting {
		t.Fatalf("CreateTenantIdempotent(replay) = %#v, %v", replayed, err)
	}

	conflictingRecord := record
	conflictingRecord.ID = hierarchyTestID(ids.KindTenant, 202)
	conflictingMarker := marker
	conflictingMarker.Locator.Key = "tenant-create-key-0002"
	conflict, err := repository.CreateTenantIdempotent(
		context.Background(), conflictingRecord, conflictingMarker,
	)
	if err != nil || conflict.kind != idempotencyTransactionConflict ||
		!isKind(conflict.conflict, errs.KindSlugConflict) {
		t.Fatalf("CreateTenantIdempotent(conflict) = %#v, %v", conflict, err)
	}
	if _, err := repository.GetTenant(
		context.Background(),
		conflictingRecord.ID,
	); !isKind(
		err,
		errs.KindTenantNotFound,
	) {
		t.Fatalf("conflicting Tenant persisted: %v", err)
	}
}

func TestHierarchyMutateTenantIdempotentCommitsEditsAndSlugMoves(t *testing.T) {
	t.Parallel()

	repository, err := newHierarchyRepository(newMemoryHierarchyStore())
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	record := TenantRecord{
		ID: hierarchyTestID(ids.KindTenant, 211), Slug: "acme", Name: "Acme",
	}
	created, err := repository.CreateTenant(context.Background(), record)
	if err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	editMarker := testDirectMarker()
	editMarker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopePlatform, ScopeID: "-", Method: http.MethodPatch,
		Route: "/tenants/{id}", Key: "tenant-edit-key-0001",
	}
	editMarker.Response = IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"name":"Acme Inc"}`),
	}
	editedRecord := record
	editedRecord.Name = "Acme Inc"
	edited, err := repository.MutateTenantIdempotent(context.Background(), created, editedRecord, editMarker)
	if err != nil || edited.kind != idempotencyTransactionApplied {
		t.Fatalf("MutateTenantIdempotent(edit) = %#v, %v", edited, err)
	}
	current, err := repository.GetTenant(context.Background(), record.ID)
	if err != nil || current.Record != editedRecord {
		t.Fatalf("GetTenant(edited) = %#v, %v", current, err)
	}

	renameMarker := testDirectMarker()
	renameMarker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopePlatform, ScopeID: "-", Method: http.MethodPost,
		Route: "/tenants/{id}/rename", Key: "tenant-rename-key-0001",
	}
	renameMarker.Response = IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"slug":"acme-inc"}`),
	}
	renamedRecord := editedRecord
	renamedRecord.Slug = "acme-inc"
	renamed, err := repository.MutateTenantIdempotent(context.Background(), current, renamedRecord, renameMarker)
	if err != nil || renamed.kind != idempotencyTransactionApplied {
		t.Fatalf("MutateTenantIdempotent(rename) = %#v, %v", renamed, err)
	}
	resolved, err := repository.ResolveTenant(context.Background(), "acme-inc")
	if err != nil || resolved.Record != renamedRecord {
		t.Fatalf("ResolveTenant(new slug) = %#v, %v", resolved, err)
	}
	if _, err := repository.ResolveTenant(context.Background(), "acme"); !isKind(err, errs.KindTenantNotFound) {
		t.Fatalf("ResolveTenant(old slug) error = %v", err)
	}
	replayed, err := repository.MutateTenantIdempotent(
		context.Background(), current, renamedRecord, renameMarker,
	)
	if err != nil || replayed.kind != idempotencyTransactionExisting {
		t.Fatalf("MutateTenantIdempotent(replay) = %#v, %v", replayed, err)
	}
}
