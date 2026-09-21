package hierarchy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

const projectCreationTestTenantID = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"

func TestProjectCreationServicePersistsEffectiveInputAndExactResponse(t *testing.T) {
	t.Parallel()
	repository := &fakeProjectCreationRepository{}
	idempotency := &fakeProjectCreationIdempotency{
		evidence:   projectCreationTestEvidence(),
		resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied},
	}
	service, err := NewProjectCreationService(repository, idempotency)
	if err != nil {
		t.Fatalf("NewProjectCreationService() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 13, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	response, err := service.CreateProject(context.Background(), CreateProjectInput{
		TenantID: projectCreationTestTenantID, Slug: "console", Description: "Operator interface",
	}, "project-create-key-0001")
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	var project apiTypes.Project
	if err := json.Unmarshal(response.Body, &project); err != nil {
		t.Fatalf("CreateProject() body = %s, %v", response.Body, err)
	}
	if response.Status != http.StatusCreated || response.ContentKind != "application/json" ||
		project.ID == "" || project.TenantID != projectCreationTestTenantID || project.Slug != "console" ||
		project.Name != "console" || project.Description != "Operator interface" || project.Kind != "tenant" {
		t.Fatalf("CreateProject() response/Project = %#v/%#v", response, project)
	}
	if repository.record != (testhierarchy.ProjectRecord{
		ID: project.ID, TenantID: project.TenantID, Slug: project.Slug,
		Name: project.Name, Description: project.Description, Kind: testhierarchy.ProjectKindTenant,
	}) {
		t.Fatalf("persisted Project = %#v", repository.record)
	}
	if idempotency.project.ID != project.ID || idempotency.project.Name != "console" {
		t.Fatalf("canonical Project = %#v", idempotency.project)
	}
	marker := repository.marker
	if marker.Locator.ScopeKind != testidempotency.IdempotencyScopeTenant ||
		marker.Locator.ScopeID != projectCreationTestTenantID ||
		marker.Locator.Method != http.MethodPost ||
		marker.Locator.Route != projectCreationRoute ||
		marker.Locator.Key != "project-create-key-0001" ||
		marker.CreatedAt != now ||
		marker.RetainUntil != now.Add(90*24*time.Hour) ||
		!reflect.DeepEqual(marker.Response, response) {
		t.Fatalf("persisted marker = %#v", marker)
	}
}

func TestProjectCreationServiceReplaysBeforeRepositoryAccess(t *testing.T) {
	t.Parallel()
	want := testidempotency.IdempotencyResponse{
		Status:      http.StatusCreated,
		ContentKind: "application/json",
		Body: []byte(
			`{"id":"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV","tenant_id":"` + projectCreationTestTenantID + `","slug":"console","name":"Console","description":"","kind":"tenant"}`,
		),
	}
	repository := &fakeProjectCreationRepository{}
	service, err := NewProjectCreationService(repository, &fakeProjectCreationIdempotency{
		evidence: projectCreationTestEvidence(), existing: true,
		resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionReplay, Response: want},
	})
	if err != nil {
		t.Fatalf("NewProjectCreationService() error = %v", err)
	}
	got, err := service.CreateProject(context.Background(), CreateProjectInput{
		TenantID: projectCreationTestTenantID, Slug: "console",
	}, "project-create-key-0002")
	if err != nil || !reflect.DeepEqual(got, want) || repository.calls != 0 {
		t.Fatalf("CreateProject(replay) = %#v, %v, repository calls %d", got, err, repository.calls)
	}
}

type fakeProjectCreationRepository struct {
	record testhierarchy.ProjectRecord
	marker testidempotency.IdempotencyMarker
	calls  int
}

func (repository *fakeProjectCreationRepository) CreateProjectIdempotent(
	_ context.Context,
	record testhierarchy.ProjectRecord,
	marker testidempotency.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.calls++
	repository.record = record
	repository.marker = marker
	repository.marker.Intent.Ciphertext = append([]byte(nil), marker.Intent.Ciphertext...)
	repository.marker.Response.Body = append([]byte(nil), marker.Response.Body...)
	return etcd.IdempotencyTransactionResult{}, nil
}

type fakeProjectCreationIdempotency struct {
	evidence   projectCreationEvidence
	project    core.Project
	resolution idempotentintent.Resolution
	existing   bool
}

func (idempotency *fakeProjectCreationIdempotency) Prepare(
	_ context.Context,
	project core.Project,
) (projectCreationEvidence, error) {
	idempotency.project = project
	return idempotency.evidence, nil
}

func (idempotency *fakeProjectCreationIdempotency) ResolveExisting(
	context.Context, testidempotency.IdempotencyLocator,

	projectCreationEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotency.resolution, idempotency.existing, nil
}

func (idempotency *fakeProjectCreationIdempotency) ResolveKnown(
	context.Context,
	projectCreationEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func (idempotency *fakeProjectCreationIdempotency) ResolveUnknown(
	context.Context, testidempotency.IdempotencyLocator,

	projectCreationEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func projectCreationTestEvidence() projectCreationEvidence {
	ciphertext := []byte("protected-project-create-intent")
	digest := sha256.Sum256(ciphertext)
	return projectCreationEvidence{durable: testidempotency.ProtectedIntentRecord{
		EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
	}}
}
