package app

import (
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/config"
	environmentcapability "github.com/AlanD20/groundplane/internal/controller/environment"
	"github.com/AlanD20/groundplane/internal/controller/handlers"
	hierarchycontroller "github.com/AlanD20/groundplane/internal/controller/hierarchy"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type controllerHierarchyMutations struct {
	tenantMutations      handlers.TenantMutator
	tenantChanges        handlers.TenantChanger
	projectMutations     handlers.ProjectMutator
	projectChanges       handlers.ProjectChanger
	environmentMutations handlers.EnvironmentMutator
	environmentChanges   handlers.EnvironmentChanger
}

func newControllerHierarchyMutations(
	cfg config.ControllerConfig,
	hierarchyRecords *etcd.HierarchyRepository,
	zoneRecords *etcd.ZoneRepository,
	intentCoordinator *requestidempotency.Coordinator,
	idempotency *etcd.IdempotencyRepository,
) (*controllerHierarchyMutations, error) {
	tenantCreationIdempotency, err := hierarchycontroller.NewTenantCreationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Tenant creation idempotency: %w", err)
	}
	tenantMutations, err := hierarchycontroller.NewTenantCreationService(hierarchyRecords, tenantCreationIdempotency)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Tenant creation service: %w", err)
	}
	tenantChangeIdempotency, err := hierarchycontroller.NewTenantChangeIdempotency(intentCoordinator, idempotency)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Tenant change idempotency: %w", err)
	}
	tenantChanges, err := hierarchycontroller.NewTenantChangeService(hierarchyRecords, tenantChangeIdempotency)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Tenant change service: %w", err)
	}
	projectCreationIdempotency, err := hierarchycontroller.NewProjectCreationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Project creation idempotency: %w", err)
	}
	projectMutations, err := hierarchycontroller.NewProjectCreationService(hierarchyRecords, projectCreationIdempotency)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Project creation service: %w", err)
	}
	projectChangeIdempotency, err := hierarchycontroller.NewProjectChangeIdempotency(intentCoordinator, idempotency)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Project change idempotency: %w", err)
	}
	projectChanges, err := hierarchycontroller.NewProjectChangeService(hierarchyRecords, projectChangeIdempotency)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Project change service: %w", err)
	}
	environmentChangeIdempotency, err := environmentcapability.NewDurableChangeIdempotency(
		intentCoordinator,
		idempotency,
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Environment change idempotency: %w", err)
	}
	environmentChanges, err := environmentcapability.NewChangeService(
		cfg.EnvironmentPool,
		hierarchyRecords,
		zoneRecords,
		environmentChangeIdempotency,
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Environment change service: %w", err)
	}
	environmentCreationIdempotency, err := environmentcapability.NewDurableCreationIdempotency(
		intentCoordinator,
		idempotency,
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Environment creation idempotency: %w", err)
	}
	environmentMutations, err := environmentcapability.NewCreationService(
		cfg.Storage.VolumeRoot,
		cfg.EnvironmentPool,
		hierarchyRecords,
		environmentCreationIdempotency,
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Environment creation service: %w", err)
	}
	return &controllerHierarchyMutations{
		tenantMutations:      tenantMutations,
		tenantChanges:        tenantChanges,
		projectMutations:     projectMutations,
		projectChanges:       projectChanges,
		environmentMutations: environmentMutations,
		environmentChanges:   environmentChanges,
	}, nil
}
