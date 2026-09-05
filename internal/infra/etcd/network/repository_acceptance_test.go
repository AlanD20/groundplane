//go:build c07_network_l2

package network

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type acceptanceFactResolver struct{}

func (acceptanceFactResolver) ResolveRemovalDatabase(
	context.Context,
	etcd.Versioned[etcd.AttachRecord],
	func(string) error,
) error {
	return errors.New("acceptance fact resolver is intentionally unused")
}

func acceptanceRepository(
	t *testing.T,
	store etcd.Store,
) (*Repository, *etcd.HierarchyRepository, *etcd.ServiceRepository, etcd.Versioned[etcd.EnvironmentRecord],
	etcd.Versioned[etcd.ProjectRecord]) {
	t.Helper()
	repository, hierarchy, services, _, _ := acceptanceRepositoryForExisting(t, store)
	ctx := context.Background()
	tenant := etcd.TenantRecord{ID: ids.New(ids.KindTenant), Slug: "c07-tenant", Name: "C07 Tenant"}
	if _, err := hierarchy.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("create acceptance Tenant: %v", err)
	}
	projectRecord := etcd.ProjectRecord{
		ID: ids.New(ids.KindProject), TenantID: tenant.ID,
		Slug: "c07-project", Name: "C07 Project", Kind: etcd.ProjectKindTenant,
	}
	project, err := hierarchy.CreateProject(ctx, projectRecord)
	if err != nil {
		t.Fatalf("create acceptance Project: %v", err)
	}
	environmentRecord, err := etcd.NewProvisioningEnvironment(
		environmentpath.DefaultVolumeRoot, projectRecord, ids.New(ids.KindEnvironment), "c07-environment",
		"10.200.0.0/16", ids.New(ids.KindTask), time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("construct acceptance Environment: %v", err)
	}
	environment, err := hierarchy.CreateEnvironment(ctx, environmentRecord)
	if err != nil {
		t.Fatalf("create acceptance Environment: %v", err)
	}
	return repository, hierarchy, services, environment, project
}

func acceptanceRepositoryForExisting(
	t *testing.T,
	store etcd.Store,
) (*Repository, *etcd.HierarchyRepository, *etcd.ServiceRepository, *etcd.ZoneRepository, *etcd.RouteRepository) {
	t.Helper()
	hierarchy, err := etcd.NewHierarchyRepository(store)
	if err != nil {
		t.Fatalf("construct Hierarchy repository: %v", err)
	}
	services, err := etcd.NewServiceRepository(store)
	if err != nil {
		t.Fatalf("construct Service repository: %v", err)
	}
	zones, err := etcd.NewZoneRepository(store)
	if err != nil {
		t.Fatalf("construct Zone repository: %v", err)
	}
	routes, err := etcd.NewRouteRepository(store)
	if err != nil {
		t.Fatalf("construct Route repository: %v", err)
	}
	attaches, err := etcd.NewAttachRepository(store)
	if err != nil {
		t.Fatalf("construct Attach repository: %v", err)
	}
	tasks, err := etcd.NewTaskRepository(store)
	if err != nil {
		t.Fatalf("construct Task repository: %v", err)
	}
	idempotency, err := etcd.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("construct Idempotency repository: %v", err)
	}
	repository, err := NewRepository(
		hierarchy, services, zones, routes, attaches, tasks, idempotency, acceptanceFactResolver{},
	)
	if err != nil {
		t.Fatalf("construct Network adapter: %v", err)
	}
	return repository, hierarchy, services, zones, routes
}
