package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/hierarchy"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

const projectChangeTestID = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"

func TestProjectChangeServiceEditsNameAndRenamesSlugWithoutMovingIdentity(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		change   func(*projectChangeService) (etcd.IdempotencyResponse, error)
		wantName string
		wantSlug string
		method   string
		route    string
	}{
		{
			name: "edit name", wantName: "Operator Console", wantSlug: "console",
			method: http.MethodPatch, route: projectEditRoute,
			change: func(service *projectChangeService) (etcd.IdempotencyResponse, error) {
				name := "Operator Console"
				return service.EditProject(context.Background(), projectChangeTestID,
					hierarchy.EditProjectInput{Name: &name}, "project-edit-key-0001")
			},
		},
		{
			name: "rename slug", wantName: "Console", wantSlug: "operator-console",
			method: http.MethodPost, route: projectRenameRoute,
			change: func(service *projectChangeService) (etcd.IdempotencyResponse, error) {
				return service.RenameProject(context.Background(), projectChangeTestID,
					hierarchy.RenameProjectInput{Slug: "operator-console"}, "project-rename-key-0001")
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			repository := &fakeProjectChangeRepository{current: projectChangeCurrent()}
			idempotency := &fakeProjectChangeIdempotency{
				evidence:   projectChangeTestEvidence(),
				resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied},
			}
			service, err := newProjectChangeService(repository, idempotency)
			if err != nil {
				t.Fatalf("newProjectChangeService() error = %v", err)
			}
			now := time.Date(2026, 8, 22, 14, 0, 0, 0, time.UTC)
			service.now = func() time.Time { return now }
			response, err := test.change(service)
			if err != nil {
				t.Fatalf("Project change error = %v", err)
			}
			var project apiTypes.Project
			if err := json.Unmarshal(response.Body, &project); err != nil ||
				project.ID != projectChangeTestID || project.TenantID != projectCreationTestTenantID ||
				project.Name != test.wantName || project.Slug != test.wantSlug ||
				project.Description != "Create-time summary" || project.Kind != "tenant" {
				t.Fatalf("Project response = %#v, %v", project, err)
			}
			if repository.replacement.Name != test.wantName || repository.replacement.Slug != test.wantSlug ||
				repository.replacement.Description != "Create-time summary" ||
				repository.replacement.TenantID != projectCreationTestTenantID {
				t.Fatalf("replacement = %#v", repository.replacement)
			}
			if repository.marker.Locator.ScopeKind != etcd.IdempotencyScopeProject ||
				repository.marker.Locator.ScopeID != projectChangeTestID ||
				repository.marker.Locator.Method != test.method || repository.marker.Locator.Route != test.route ||
				repository.marker.CreatedAt != now || response.Status != http.StatusOK {
				t.Fatalf("marker/response = %#v/%#v", repository.marker, response)
			}
		})
	}
}

func TestProjectChangeServiceReplaysBeforeProjectLookup(t *testing.T) {
	t.Parallel()
	want := etcd.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"id":"` + projectChangeTestID + `"}`),
	}
	repository := &fakeProjectChangeRepository{}
	service, err := newProjectChangeService(repository, &fakeProjectChangeIdempotency{
		evidence: projectChangeTestEvidence(), existing: true,
		resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionReplay, Response: want},
	})
	if err != nil {
		t.Fatalf("newProjectChangeService() error = %v", err)
	}
	got, err := service.RenameProject(context.Background(), projectChangeTestID,
		hierarchy.RenameProjectInput{Slug: "operator-console"}, "project-rename-key-0002")
	if err != nil || !reflect.DeepEqual(got, want) || repository.getCalls != 0 || repository.mutateCalls != 0 {
		t.Fatalf("RenameProject(replay) = %#v, %v, calls %d/%d", got, err, repository.getCalls, repository.mutateCalls)
	}
}

type fakeProjectChangeRepository struct {
	current     etcd.Versioned[etcd.ProjectRecord]
	replacement etcd.ProjectRecord
	marker      etcd.IdempotencyMarker
	getCalls    int
	mutateCalls int
}

func (repository *fakeProjectChangeRepository) GetProject(
	context.Context,
	string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	repository.getCalls++
	return repository.current, nil
}

func (repository *fakeProjectChangeRepository) MutateProjectIdempotent(
	_ context.Context,
	_ etcd.Versioned[etcd.ProjectRecord],
	replacement etcd.ProjectRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.mutateCalls++
	repository.replacement = replacement
	repository.marker = marker
	repository.marker.Intent.Ciphertext = append([]byte(nil), marker.Intent.Ciphertext...)
	repository.marker.Response.Body = append([]byte(nil), marker.Response.Body...)
	return etcd.IdempotencyTransactionResult{}, nil
}

type fakeProjectChangeIdempotency struct {
	evidence   projectChangeEvidence
	resolution idempotentintent.Resolution
	existing   bool
}

func (idempotency *fakeProjectChangeIdempotency) PrepareEdit(
	context.Context,
	string,
	hierarchy.EditProjectInput,
) (projectChangeEvidence, error) {
	return idempotency.evidence, nil
}

func (idempotency *fakeProjectChangeIdempotency) PrepareRename(
	context.Context,
	string,
	hierarchy.RenameProjectInput,
) (projectChangeEvidence, error) {
	return idempotency.evidence, nil
}

func (idempotency *fakeProjectChangeIdempotency) ResolveExisting(
	context.Context,
	etcd.IdempotencyLocator,
	projectChangeEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotency.resolution, idempotency.existing, nil
}

func (idempotency *fakeProjectChangeIdempotency) ResolveKnown(
	context.Context,
	projectChangeEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func (idempotency *fakeProjectChangeIdempotency) ResolveUnknown(
	context.Context,
	etcd.IdempotencyLocator,
	projectChangeEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func projectChangeCurrent() etcd.Versioned[etcd.ProjectRecord] {
	return etcd.Versioned[etcd.ProjectRecord]{
		Record: etcd.ProjectRecord{
			ID: projectChangeTestID, TenantID: projectCreationTestTenantID,
			Slug: "console", Name: "Console", Description: "Create-time summary", Kind: etcd.ProjectKindTenant,
		},
		Revision: 10, ReadRevision: 10,
	}
}

func projectChangeTestEvidence() projectChangeEvidence {
	ciphertext := []byte("protected-project-change-intent")
	digest := sha256.Sum256(ciphertext)
	return projectChangeEvidence{durable: etcd.ProtectedIntentRecord{
		EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
	}}
}
