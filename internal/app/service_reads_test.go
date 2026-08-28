package app

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type fakeServiceReadRepository struct {
	serviceReadRepository
	environment etcd.Versioned[etcd.EnvironmentRecord]
	project     etcd.Versioned[etcd.ProjectRecord]
	tenant      etcd.Versioned[etcd.TenantRecord]
	head        etcd.Versioned[etcd.EnvironmentBlueprintHead]
	revision    etcd.Versioned[etcd.EnvironmentBlueprintRevision]
	page        etcd.Page[etcd.ServiceRecord]
	wantRequest etcd.PageRequest
	listed      bool
}

func (fake *fakeServiceReadRepository) GetProject(
	context.Context,
	string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return fake.project, nil
}

func (fake *fakeServiceReadRepository) GetTenant(
	context.Context,
	string,
) (etcd.Versioned[etcd.TenantRecord], error) {
	return fake.tenant, nil
}

func (fake *fakeServiceReadRepository) GetEnvironmentBlueprintHead(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentBlueprintHead], bool, error) {
	return fake.head, fake.head.Record.RevisionID != "", nil
}

func (fake *fakeServiceReadRepository) GetEnvironmentBlueprintRevision(
	context.Context,
	string,
	string,
) (etcd.Versioned[etcd.EnvironmentBlueprintRevision], bool, error) {
	return fake.revision, fake.revision.Record.RevisionID != "", nil
}

func (fake *fakeServiceReadRepository) GetEnvironment(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return fake.environment, nil
}

func (fake *fakeServiceReadRepository) ListServices(
	_ context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ServiceRecord], error) {
	fake.listed = environmentID == fake.environment.Record.ID && request == fake.wantRequest
	return fake.page, nil
}

// Rationale: Service labels are scoped to an Environment, so a collection read must verify that
// owner and preserve the repository's opaque cursor tuple instead of treating a missing owner as empty.
func TestServiceListVerifiesOwnerAndPreservesPagination(t *testing.T) {
	t.Parallel()
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	request := etcd.PageRequest{Limit: 23, Cursor: "opaque-cursor"}
	want := etcd.Page[etcd.ServiceRecord]{NextCursor: "next-cursor", Revision: 91}
	repository := &fakeServiceReadRepository{
		environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record: etcd.EnvironmentRecord{
				NetworkPool: "10.40.0.0/16",
				ID:          environmentID,
			},
			Revision:     12,
			ReadRevision: 12,
		},
		page: want, wantRequest: request,
	}
	service, err := newServiceReadService(repository)
	if err != nil {
		t.Fatalf("newServiceReadService() error = %v", err)
	}
	got, err := service.ListServices(context.Background(), environmentID, request)
	if err != nil || !reflect.DeepEqual(got, want) || !repository.listed {
		t.Fatalf("ListServices() = %#v, %v, listed %t", got, err, repository.listed)
	}
}

func TestServiceDetailProjectsCanonicalNativeComposeFromDesiredHead(t *testing.T) {
	// Rationale: operator detail must read the immutable desired revision so
	// native Compose fields never depend on the intentionally smaller flat record.
	t.Parallel()
	at := time.Date(2026, time.August, 28, 10, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, at, 1)
	projectID := ids.NewAt(ids.KindProject, at, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 3)
	revisionID := ids.NewAt(ids.KindTask, at, 4)
	repository := &fakeServiceReadRepository{
		tenant: etcd.Versioned[etcd.TenantRecord]{Record: etcd.TenantRecord{ID: tenantID, Slug: "acme"}},
		project: etcd.Versioned[etcd.ProjectRecord]{Record: etcd.ProjectRecord{
			ID: projectID, TenantID: tenantID, Slug: "shop", Kind: etcd.ProjectKindTenant,
		}},
		environment: etcd.Versioned[etcd.EnvironmentRecord]{Record: etcd.EnvironmentRecord{
			ID: environmentID, ProjectID: projectID, Name: "production",
		}},
		head: etcd.Versioned[etcd.EnvironmentBlueprintHead]{Record: etcd.EnvironmentBlueprintHead{
			EnvironmentID: environmentID, RevisionID: revisionID,
		}},
		revision: etcd.Versioned[etcd.EnvironmentBlueprintRevision]{Record: etcd.EnvironmentBlueprintRevision{
			EnvironmentID: environmentID, RevisionID: revisionID, RootPath: "blueprint.yaml",
			ComposeSources: []string{"blueprint.yaml"},
			Files: []etcd.EnvironmentBlueprintFile{{Path: "blueprint.yaml", Content: []byte(`kind: environment
schema: 1
metadata: {tenant: acme, project: shop, environment: production}
services:
  api:
    image: example/api:1
    command: [serve, --http]
    environment: {APP_ENV: production}
    labels: {example.role: api}
`)}},
		}},
	}
	service, err := newServiceReadService(repository)
	if err != nil {
		t.Fatalf("newServiceReadService() error = %v", err)
	}
	native, err := service.GetServiceNativeCompose(context.Background(), environmentID, "api")
	if err != nil {
		t.Fatalf("GetServiceNativeCompose() error = %v", err)
	}
	for _, fragment := range []string{"services:", "api:", "image: example/api:1", "APP_ENV: production", "example.role: api"} {
		if !strings.Contains(native, fragment) {
			t.Fatalf("native Compose = %q, missing %q", native, fragment)
		}
	}
}
