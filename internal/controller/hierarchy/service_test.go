package hierarchy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestServiceCreatesTenantAndNormalProjectWithStableIDs(t *testing.T) {
	// Rationale: callers choose labels but never durable identity or project kind;
	// the Controller must generate canonical ids and close this facade to tenant projects.
	t.Parallel()

	repository := &repositoryStub{}
	repository.createTenant = func(_ context.Context, record core.Tenant) (Versioned[core.Tenant], error) {
		if err := ids.Validate(ids.KindTenant, record.ID); err != nil {
			t.Fatalf("tenant id = %q: %v", record.ID, err)
		}
		if record.Slug != "acme" || record.Name != "Acme" {
			t.Fatalf("tenant record = %+v", record)
		}
		return Versioned[core.Tenant]{Record: record, Revision: 11, ReadRevision: 11}, nil
	}
	repository.createProject = func(_ context.Context, record core.Project) (Versioned[core.Project], error) {
		if err := ids.Validate(ids.KindProject, record.ID); err != nil {
			t.Fatalf("project id = %q: %v", record.ID, err)
		}
		if record.TenantID != testTenantID || record.Kind != core.ProjectKindTenant ||
			record.Slug != "console" || record.Name != "Console" {
			t.Fatalf("project record = %+v", record)
		}
		return Versioned[core.Project]{Record: record, Revision: 12, ReadRevision: 12}, nil
	}
	service := mustService(t, repository)

	tenantName := "Acme"
	tenant, err := service.CreateTenant(
		context.Background(), CreateTenantInput{Slug: "acme", Name: &tenantName},
	)
	if err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	if tenant.Revision != 11 || tenant.Record.ID == "" {
		t.Fatalf("CreateTenant() = %+v", tenant)
	}
	projectName := "Console"
	project, err := service.CreateProject(context.Background(), CreateProjectInput{
		TenantID: testTenantID,
		Slug:     "console",
		Name:     &projectName,
	})
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	if project.Revision != 12 || project.Record.Kind != core.ProjectKindTenant {
		t.Fatalf("CreateProject() = %+v", project)
	}
}

func TestServiceReadsResolvesAndListsFixedRevisionHierarchy(t *testing.T) {
	// Rationale: hierarchy reads must preserve the repository's exact MVCC page
	// and opaque cursor while exposing only normal projects in their tenant scope.
	t.Parallel()

	tenant := core.Tenant{ID: testTenantID, Slug: "acme", Name: "Acme"}
	project := core.Project{
		ID: testProjectID, TenantID: testTenantID, Slug: "console", Name: "Console", Kind: core.ProjectKindTenant,
	}
	repository := &repositoryStub{
		getTenant: func(_ context.Context, id string) (Versioned[core.Tenant], error) {
			if id != testTenantID {
				t.Fatalf("GetTenant id = %q", id)
			}
			return Versioned[core.Tenant]{Record: tenant, Revision: 21, ReadRevision: 30}, nil
		},
		resolveTenant: func(_ context.Context, slug string) (Versioned[core.Tenant], error) {
			if slug != "acme" {
				t.Fatalf("ResolveTenant slug = %q", slug)
			}
			return Versioned[core.Tenant]{Record: tenant, Revision: 21, ReadRevision: 31}, nil
		},
		listTenants: func(_ context.Context, request PageRequest) (Page[core.Tenant], error) {
			if request != (PageRequest{Limit: 1, Cursor: "tenant-cursor"}) {
				t.Fatalf("ListTenants request = %+v", request)
			}
			return Page[core.Tenant]{
				Items:      []Versioned[core.Tenant]{{Record: tenant, Revision: 21, ReadRevision: 32}},
				NextCursor: "tenant-next",
				Revision:   32,
			}, nil
		},
		getProject: func(_ context.Context, id string) (Versioned[core.Project], error) {
			if id != testProjectID {
				t.Fatalf("GetProject id = %q", id)
			}
			return Versioned[core.Project]{Record: project, Revision: 22, ReadRevision: 33}, nil
		},
		resolveTenantProject: func(
			_ context.Context,
			tenantID string,
			slug string,
		) (Versioned[core.Project], error) {
			if tenantID != testTenantID || slug != "console" {
				t.Fatalf("ResolveTenantProject scope = %q/%q", tenantID, slug)
			}
			return Versioned[core.Project]{Record: project, Revision: 22, ReadRevision: 34}, nil
		},
		listTenantProjects: func(
			_ context.Context,
			tenantID string,
			request PageRequest,
		) (Page[core.Project], error) {
			if tenantID != testTenantID || request != (PageRequest{Limit: 2, Cursor: "project-cursor"}) {
				t.Fatalf("ListTenantProjects scope/request = %q/%+v", tenantID, request)
			}
			return Page[core.Project]{
				Items:      []Versioned[core.Project]{{Record: project, Revision: 22, ReadRevision: 35}},
				NextCursor: "project-next",
				Revision:   35,
			}, nil
		},
	}
	service := mustService(t, repository)

	gotTenant, err := service.GetTenant(context.Background(), testTenantID)
	if err != nil || gotTenant.Record != tenant || gotTenant.ReadRevision != 30 {
		t.Fatalf("GetTenant() = %+v, %v", gotTenant, err)
	}
	resolvedTenant, err := service.ResolveTenant(context.Background(), "acme")
	if err != nil || resolvedTenant.Record != tenant || resolvedTenant.ReadRevision != 31 {
		t.Fatalf("ResolveTenant() = %+v, %v", resolvedTenant, err)
	}
	tenantPage, err := service.ListTenants(
		context.Background(),
		PageRequest{Limit: 1, Cursor: "tenant-cursor"},
	)
	if err != nil || tenantPage.Revision != 32 || tenantPage.NextCursor != "tenant-next" ||
		len(tenantPage.Items) != 1 {
		t.Fatalf("ListTenants() = %+v, %v", tenantPage, err)
	}

	gotProject, err := service.GetProject(context.Background(), testProjectID)
	if err != nil || gotProject.Record != project || gotProject.ReadRevision != 33 {
		t.Fatalf("GetProject() = %+v, %v", gotProject, err)
	}
	resolvedProject, err := service.ResolveProject(context.Background(), testTenantID, "console")
	if err != nil || resolvedProject.Record != project || resolvedProject.ReadRevision != 34 {
		t.Fatalf("ResolveProject() = %+v, %v", resolvedProject, err)
	}
	projectPage, err := service.ListProjects(
		context.Background(),
		testTenantID,
		PageRequest{Limit: 2, Cursor: "project-cursor"},
	)
	if err != nil || projectPage.Revision != 35 || projectPage.NextCursor != "project-next" ||
		len(projectPage.Items) != 1 {
		t.Fatalf("ListProjects() = %+v, %v", projectPage, err)
	}
}

func TestServiceFailsClosedOnProjectScopeOrKindMismatch(t *testing.T) {
	// Rationale: the normal-project facade must never expose a backing project
	// or trust a scoped repository result whose tenant owner does not match the query.
	t.Parallel()

	backing := core.Project{
		ID: testProjectID, Slug: "postgres", Name: "Postgres", Kind: core.ProjectKindBacking,
	}
	wrongOwner := core.Project{
		ID: testProjectID, TenantID: otherTenantID, Slug: "console", Name: "Console",
		Kind: core.ProjectKindTenant,
	}
	repository := &repositoryStub{
		getProject: func(context.Context, string) (Versioned[core.Project], error) {
			return Versioned[core.Project]{Record: backing, Revision: 60, ReadRevision: 60}, nil
		},
		resolveTenantProject: func(context.Context, string, string) (Versioned[core.Project], error) {
			return Versioned[core.Project]{Record: wrongOwner, Revision: 61, ReadRevision: 61}, nil
		},
		listTenantProjects: func(context.Context, string, PageRequest) (Page[core.Project], error) {
			return Page[core.Project]{
				Items:    []Versioned[core.Project]{{Record: wrongOwner, Revision: 61, ReadRevision: 61}},
				Revision: 61,
			}, nil
		},
	}
	service := mustService(t, repository)

	if _, err := service.GetProject(context.Background(), testProjectID); !hasKind(err, errs.KindProjectNotFound) {
		t.Fatalf("GetProject(backing) error = %v, want project.not_found", err)
	}
	if _, err := service.ResolveProject(
		context.Background(), testTenantID, "console",
	); !hasKind(err, errs.KindInternal) {
		t.Fatalf("ResolveProject(wrong owner) error = %v, want internal", err)
	}
	if _, err := service.ListProjects(
		context.Background(), testTenantID, PageRequest{},
	); !hasKind(err, errs.KindInternal) {
		t.Fatalf("ListProjects(wrong owner) error = %v, want internal", err)
	}
}

func TestServiceFailsClosedOnNoncanonicalDurableSlug(t *testing.T) {
	// Rationale: a durable record bypassing boundary validation is corrupt and
	// must never expose a slug that cannot round-trip through the public contract.
	t.Parallel()

	repository := &repositoryStub{
		getTenant: func(context.Context, string) (Versioned[core.Tenant], error) {
			return Versioned[core.Tenant]{
				Record:       core.Tenant{ID: testTenantID, Slug: "Not/Canonical", Name: "Tenant"},
				Revision:     1,
				ReadRevision: 1,
			}, nil
		},
	}
	service := mustService(t, repository)
	if _, err := service.GetTenant(context.Background(), testTenantID); !hasKind(err, errs.KindInternal) {
		t.Fatalf("GetTenant(noncanonical slug) error = %v, want internal", err)
	}
}

func TestServiceSanitizesDependenciesAndUsesOnlyCallerCancellation(t *testing.T) {
	// Rationale: infrastructure diagnostics and dependency-owned cancellation
	// are private failures; only cancellation observed on the caller's context may cross unchanged.
	t.Parallel()

	privateCause := errors.New("private etcd endpoint and credential")
	rawService := mustService(t, &repositoryStub{
		getTenant: func(context.Context, string) (Versioned[core.Tenant], error) {
			return Versioned[core.Tenant]{}, privateCause
		},
	})
	_, err := rawService.GetTenant(context.Background(), testTenantID)
	if !hasKind(err, errs.KindInternal) || !errors.Is(err, privateCause) {
		t.Fatalf("raw repository error = %v, want private internal cause", err)
	}
	var domainError *errs.Error
	if !errors.As(err, &domainError) || strings.Contains(domainError.ToProblem().Detail, "credential") {
		t.Fatalf("public problem leaked private cause: %#v", domainError)
	}

	classified := errs.New(errs.KindSlugConflict, "slug is already in use")
	classifiedService := mustService(t, &repositoryStub{
		createTenant: func(context.Context, core.Tenant) (Versioned[core.Tenant], error) {
			return Versioned[core.Tenant]{}, classified
		},
	})
	classifiedName := "Acme"
	if _, err := classifiedService.CreateTenant(
		context.Background(), CreateTenantInput{Slug: "acme", Name: &classifiedName},
	); !errors.Is(err, classified) {
		t.Fatalf("classified repository error = %v, want preserved", err)
	}

	dependencyCanceledService := mustService(t, &repositoryStub{
		getTenant: func(context.Context, string) (Versioned[core.Tenant], error) {
			return Versioned[core.Tenant]{}, context.Canceled
		},
	})
	if _, err := dependencyCanceledService.GetTenant(
		context.Background(), testTenantID,
	); !hasKind(err, errs.KindInternal) || !errors.Is(err, context.Canceled) {
		t.Fatalf("dependency cancellation error = %v, want privately wrapped internal", err)
	}

	var calls atomic.Int64
	callerCanceledService := mustService(t, &repositoryStub{
		getTenant: func(context.Context, string) (Versioned[core.Tenant], error) {
			calls.Add(1)
			return Versioned[core.Tenant]{}, nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := callerCanceledService.GetTenant(ctx, testTenantID); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled caller error = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("repository calls = %d, want zero", calls.Load())
	}

	ctx, cancel = context.WithCancel(context.Background())
	cancelDuringCallService := mustService(t, &repositoryStub{
		getTenant: func(context.Context, string) (Versioned[core.Tenant], error) {
			cancel()
			return Versioned[core.Tenant]{}, privateCause
		},
	})
	if _, err := cancelDuringCallService.GetTenant(ctx, testTenantID); !errors.Is(err, context.Canceled) {
		t.Fatalf("in-flight caller cancellation error = %v", err)
	}
}

func TestServiceGeneratesUniqueIDsConcurrently(t *testing.T) {
	// Rationale: one application-scoped hierarchy service may serve concurrent
	// creates, so stable-id generation must remain race-free and collision-free.

	const operations = 64
	seen := make(map[string]struct{}, operations)
	var mutex sync.Mutex
	repository := &repositoryStub{
		createTenant: func(_ context.Context, record core.Tenant) (Versioned[core.Tenant], error) {
			mutex.Lock()
			defer mutex.Unlock()
			if _, exists := seen[record.ID]; exists {
				return Versioned[core.Tenant]{}, errors.New("duplicate stable id")
			}
			seen[record.ID] = struct{}{}
			return Versioned[core.Tenant]{Record: record, Revision: 1, ReadRevision: 1}, nil
		},
	}
	service := mustService(t, repository)
	errorsByOperation := make(chan error, operations)
	var workers sync.WaitGroup
	workers.Add(operations)
	for index := range operations {
		go func() {
			defer workers.Done()
			name := "Tenant"
			_, err := service.CreateTenant(context.Background(), CreateTenantInput{
				Slug: fmt.Sprintf("tenant-%02d", index),
				Name: &name,
			})
			if err != nil {
				errorsByOperation <- err
			}
		}()
	}
	workers.Wait()
	close(errorsByOperation)
	for err := range errorsByOperation {
		t.Fatalf("concurrent CreateTenant() error = %v", err)
	}
	if len(seen) != operations {
		t.Fatalf("generated ids = %d, want %d", len(seen), operations)
	}
}

func TestNewServiceRequiresRepository(t *testing.T) {
	// Rationale: missing durable hierarchy wiring is a startup fault and must
	// fail closed before a request can invent or lose identity state.
	t.Parallel()

	if _, err := NewService(nil); !hasKind(err, errs.KindInternal) {
		t.Fatalf("NewService(nil) error = %v, want internal", err)
	}
	var typedNil *repositoryStub
	if _, err := NewService(typedNil); !hasKind(err, errs.KindInternal) {
		t.Fatalf("NewService(typed nil) error = %v, want internal", err)
	}
}

func TestServiceDefaultsNamesAndValidatesSlugs(t *testing.T) {
	// Rationale: hierarchy slugs are URL labels with one server-side grammar,
	// while an omitted display name has the deterministic slug default.
	t.Parallel()

	repository := &repositoryStub{
		createTenant: func(_ context.Context, record core.Tenant) (Versioned[core.Tenant], error) {
			if record.Name != "acme" {
				t.Fatalf("default tenant name = %q", record.Name)
			}
			return Versioned[core.Tenant]{Record: record, Revision: 1, ReadRevision: 1}, nil
		},
	}
	service := mustService(t, repository)
	if _, err := service.CreateTenant(context.Background(), CreateTenantInput{Slug: "acme"}); err != nil {
		t.Fatalf("CreateTenant(default name) error = %v", err)
	}
	for _, slug := range []string{"", "Upper", "two--parts", "-start", "end-", "has/slash", "naive cafe"} {
		if _, err := service.ResolveTenant(context.Background(), slug); !hasKind(err, errs.KindValidationFailed) {
			t.Fatalf("ResolveTenant(%q) error = %v, want validation", slug, err)
		}
	}
}

func TestServiceRejectsInvalidWriteMetadataAndOversizedPages(t *testing.T) {
	// Rationale: a repository cannot fabricate a successful no-op CAS or return
	// more records than the bounded request contract permits.
	t.Parallel()

	tenant := core.Tenant{ID: testTenantID, Slug: "acme", Name: "Acme"}
	repository := &repositoryStub{
		createTenant: func(_ context.Context, record core.Tenant) (Versioned[core.Tenant], error) {
			return Versioned[core.Tenant]{Record: record, Revision: 5, ReadRevision: 6}, nil
		},
		listTenants: func(context.Context, PageRequest) (Page[core.Tenant], error) {
			return Page[core.Tenant]{
				Items: []Versioned[core.Tenant]{
					{Record: tenant, Revision: 5, ReadRevision: 5},
					{Record: core.Tenant{ID: otherTenantID, Slug: "other", Name: "Other"}, Revision: 5, ReadRevision: 5},
				},
				Revision: 5,
			}, nil
		},
	}
	service := mustService(t, repository)
	if _, err := service.CreateTenant(
		context.Background(), CreateTenantInput{Slug: "created"},
	); !hasKind(err, errs.KindInternal) {
		t.Fatalf("CreateTenant(stale write metadata) error = %v, want internal", err)
	}
	if _, err := service.ListTenants(context.Background(), PageRequest{Limit: 1}); !hasKind(err, errs.KindInternal) {
		t.Fatalf("ListTenants(over-return) error = %v, want internal", err)
	}
}

const (
	testTenantID  = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	otherTenantID = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	testProjectID = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAX"
)

type repositoryStub struct {
	createTenant         func(context.Context, core.Tenant) (Versioned[core.Tenant], error)
	getTenant            func(context.Context, string) (Versioned[core.Tenant], error)
	resolveTenant        func(context.Context, string) (Versioned[core.Tenant], error)
	listTenants          func(context.Context, PageRequest) (Page[core.Tenant], error)
	createProject        func(context.Context, core.Project) (Versioned[core.Project], error)
	getProject           func(context.Context, string) (Versioned[core.Project], error)
	resolveTenantProject func(context.Context, string, string) (Versioned[core.Project], error)
	listTenantProjects   func(context.Context, string, PageRequest) (Page[core.Project], error)
}

func (repository *repositoryStub) ready() bool {
	return repository != nil
}

func (repository *repositoryStub) CreateTenant(
	ctx context.Context,
	record core.Tenant,
) (Versioned[core.Tenant], error) {
	return repository.createTenant(ctx, record)
}

func (repository *repositoryStub) GetTenant(
	ctx context.Context,
	id string,
) (Versioned[core.Tenant], error) {
	return repository.getTenant(ctx, id)
}

func (repository *repositoryStub) ResolveTenant(
	ctx context.Context,
	slug string,
) (Versioned[core.Tenant], error) {
	return repository.resolveTenant(ctx, slug)
}

func (repository *repositoryStub) ListTenants(
	ctx context.Context,
	request PageRequest,
) (Page[core.Tenant], error) {
	return repository.listTenants(ctx, request)
}

func (repository *repositoryStub) CreateProject(
	ctx context.Context,
	record core.Project,
) (Versioned[core.Project], error) {
	return repository.createProject(ctx, record)
}

func (repository *repositoryStub) GetProject(
	ctx context.Context,
	id string,
) (Versioned[core.Project], error) {
	return repository.getProject(ctx, id)
}

func (repository *repositoryStub) ResolveTenantProject(
	ctx context.Context,
	tenantID string,
	slug string,
) (Versioned[core.Project], error) {
	return repository.resolveTenantProject(ctx, tenantID, slug)
}

func (repository *repositoryStub) ListTenantProjects(
	ctx context.Context,
	tenantID string,
	request PageRequest,
) (Page[core.Project], error) {
	return repository.listTenantProjects(ctx, tenantID, request)
}

func mustService(t *testing.T, repository Repository) *Service {
	t.Helper()
	service, err := NewService(repository)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return service
}

func hasKind(err error, kind errs.Kind) bool {
	return errors.Is(err, errs.New(kind, ""))
}
