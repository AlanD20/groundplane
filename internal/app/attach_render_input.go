package app

import (
	"slices"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func buildAttachTaskRenderInput(
	scope etcd.AttachCreateScope,
	record etcd.AttachRecord,
	attaches []etcd.Versioned[etcd.AttachRecord],
	task etcd.TaskRecord,
	artifactID string,
) (etcd.AttachTaskRenderInput, error) {
	if scope.Tenant.Revision <= 0 || scope.Project.Revision <= 0 || scope.Environment.Revision <= 0 ||
		scope.DesiredHead.Revision <= 0 || scope.ComposeProjection.Revision <= 0 ||
		scope.BackingProject.Revision <= 0 || scope.BackingEnvironment.Revision <= 0 ||
		scope.BackingService.Revision <= 0 {
		return etcd.AttachTaskRenderInput{}, errs.New(
			errs.KindValidationFailed,
			"Attach render scope records must be versioned",
		)
	}
	if ids.Validate(ids.KindPlan, task.PlanID) != nil || ids.Validate(ids.KindConfig, artifactID) != nil ||
		task.Target != record.ID || task.RenderGeneration <= 0 ||
		uint64(task.RenderGeneration) != scope.ComposeProjection.Record.RenderGeneration {
		return etcd.AttachTaskRenderInput{}, errs.New(
			errs.KindValidationFailed,
			"Attach render Task identity is invalid",
		)
	}
	if scope.Tenant.Record.ID != scope.Project.Record.TenantID ||
		scope.Project.Record.Kind != etcd.ProjectKindTenant ||
		scope.Environment.Record.ProjectID != scope.Project.Record.ID ||
		record.EnvironmentID != scope.Environment.Record.ID ||
		scope.DesiredHead.Record.EnvironmentID != record.EnvironmentID ||
		scope.ComposeProjection.Record.EnvironmentID != record.EnvironmentID ||
		scope.ComposeProjection.Record.RevisionID != scope.DesiredHead.Record.RevisionID ||
		scope.BackingProject.Record.Kind != etcd.ProjectKindBacking ||
		scope.BackingEnvironment.Record.ProjectID != scope.BackingProject.Record.ID ||
		scope.BackingService.Record.EnvironmentID != scope.BackingEnvironment.Record.ID ||
		record.BackingProjectID != scope.BackingProject.Record.ID ||
		record.BackingEnvironmentID != scope.BackingEnvironment.Record.ID ||
		record.BackingServiceID != scope.BackingService.Record.Desired.ID ||
		record.BackingNetworkID != scope.BackingService.Record.BackingNetworkID {
		return etcd.AttachTaskRenderInput{}, errs.New(
			errs.KindScopeUnauthorized,
			"Attach render hierarchy is invalid",
		)
	}

	excludedAttachID := ""
	unionRecords := attaches
	switch task.Type {
	case etcd.TaskAttach:
		if record.Status != core.AttachPending || record.Operation != etcd.AttachOperationProvision ||
			record.TaskID != task.ID {
			return etcd.AttachTaskRenderInput{}, errs.New(
				errs.KindValidationFailed,
				"Attach create render requires its pending provision record",
			)
		}
		unionRecords = append(
			append([]etcd.Versioned[etcd.AttachRecord](nil), attaches...),
			etcd.Versioned[etcd.AttachRecord]{
				Record: record,
			},
		)
	case etcd.TaskDetach:
		if record.Status != core.AttachDetaching || record.Operation != etcd.AttachOperationDetach ||
			record.TaskID != task.ID {
			return etcd.AttachTaskRenderInput{}, errs.New(
				errs.KindValidationFailed,
				"Attach detach render requires its detaching record",
			)
		}
		excludedAttachID = record.ID
	default:
		return etcd.AttachTaskRenderInput{}, errs.New(
			errs.KindValidationFailed,
			"Attach render Task type is invalid",
		)
	}

	joins, err := resolveAttachNetworkJoins(
		record.EnvironmentID,
		scope.ComposeProjection.Record,
		unionRecords,
		excludedAttachID,
	)
	if err != nil {
		return etcd.AttachTaskRenderInput{}, err
	}
	return etcd.AttachTaskRenderInput{
		PlanID: task.PlanID, AttachID: record.ID,
		AttachName: record.Name,
		TenantID:   scope.Tenant.Record.ID, TenantSlug: scope.Tenant.Record.Slug,
		ProjectID: scope.Project.Record.ID, ProjectSlug: scope.Project.Record.Slug,
		EnvironmentID: record.EnvironmentID, EnvironmentName: scope.Environment.Record.Name,
		AuthorizedVolumeDir:    scope.Environment.Record.VolumeDir,
		BackingServiceID:       record.BackingServiceID,
		BackingProjectID:       record.BackingProjectID,
		AdapterKey:             scope.BackingService.Record.Desired.Adapter,
		BlueprintRevisionID:    scope.DesiredHead.Record.RevisionID,
		ArtifactID:             artifactID,
		RenderGeneration:       scope.ComposeProjection.Record.RenderGeneration,
		Services:               attachTaskServiceSnapshots(scope.ComposeProjection.Record.DesiredServices),
		Networks:               attachTaskOwnedNetworkSnapshots(scope.ComposeProjection.Record.DesiredZones),
		Volumes:                slices.Clone(scope.ComposeProjection.Record.Volumes),
		VolumeMounts:           slices.Clone(scope.ComposeProjection.Record.VolumeMounts),
		NetworkJoins:           joins,
		ConsumerServiceIDs:     []string{record.ServiceID},
		GrantAttachIDs:         append([]string(nil), record.GrantAttachIDs...),
		ServiceDependencyPlans: scope.ComposeProjection.Record.ServiceDependencyPlans.Clone(),
	}, nil
}

func resolveAttachNetworkJoins(
	environmentID string,
	projection etcd.EnvironmentComposeProjection,
	attaches []etcd.Versioned[etcd.AttachRecord],
	excludedAttachID string,
) ([]etcd.AttachTaskNetworkJoin, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || projection.EnvironmentID != environmentID {
		return nil, errs.New(errs.KindValidationFailed, "Attach network union Environment is invalid")
	}
	allowedServices := make(map[string]struct{}, len(projection.DesiredServices))
	for _, service := range projection.DesiredServices {
		allowedServices[service.Desired.ID] = struct{}{}
	}
	ownedNetworks := make(map[string]struct{}, len(projection.DesiredZones))
	for _, network := range projection.DesiredZones {
		ownedNetworks[network.Desired.ID] = struct{}{}
	}

	seenAttaches := make(map[string]struct{}, len(attaches))
	union := make(map[string]map[string]struct{})
	for _, current := range attaches {
		record := current.Record
		if _, duplicate := seenAttaches[record.ID]; duplicate {
			return nil, errs.New(errs.KindInternal, "Attach network union contains a duplicate Attach")
		}
		seenAttaches[record.ID] = struct{}{}
		if record.EnvironmentID != environmentID {
			return nil, errs.New(errs.KindScopeUnauthorized, "Attach network union crosses Environments")
		}
		if record.ID == excludedAttachID || record.Operation == etcd.AttachOperationDetach ||
			record.Status == core.AttachDetached {
			continue
		}
		if record.Operation != etcd.AttachOperationProvision {
			return nil, errs.New(errs.KindInternal, "Attach network union contains an invalid operation")
		}
		if ids.Validate(ids.KindNetwork, record.BackingNetworkID) != nil {
			return nil, errs.New(errs.KindInternal, "Attach network union contains an invalid network")
		}
		if _, owned := ownedNetworks[record.BackingNetworkID]; owned {
			return nil, errs.New(
				errs.KindStateConflict,
				"Attach backing network is owned by the consumer Environment",
			)
		}
		services := union[record.BackingNetworkID]
		if services == nil {
			services = make(map[string]struct{})
			union[record.BackingNetworkID] = services
		}
		if _, exists := allowedServices[record.ServiceID]; !exists {
			return nil, errs.New(
				errs.KindStateConflict,
				"Attach network consumer is absent from the current Compose projection",
			)
		}
		services[record.ServiceID] = struct{}{}
	}

	networkIDs := make([]string, 0, len(union))
	for networkID := range union {
		networkIDs = append(networkIDs, networkID)
	}
	sort.Strings(networkIDs)
	joins := make([]etcd.AttachTaskNetworkJoin, len(networkIDs))
	for index, networkID := range networkIDs {
		serviceIDs := make([]string, 0, len(union[networkID]))
		for serviceID := range union[networkID] {
			serviceIDs = append(serviceIDs, serviceID)
		}
		sort.Strings(serviceIDs)
		joins[index] = etcd.AttachTaskNetworkJoin{NetworkID: networkID, ServiceIDs: serviceIDs}
	}
	return joins, nil
}

func attachTaskServiceSnapshots(
	values []etcd.EnvironmentServiceProjection,
) []etcd.AttachTaskServiceSnapshot {
	snapshots := make([]etcd.AttachTaskServiceSnapshot, len(values))
	for index, value := range values {
		snapshots[index] = etcd.AttachTaskServiceSnapshot{ID: value.Desired.ID, Name: value.Desired.Name}
	}
	return snapshots
}

func attachTaskOwnedNetworkSnapshots(
	values []etcd.EnvironmentZoneProjection,
) []etcd.AttachTaskOwnedNetworkSnapshot {
	snapshots := make([]etcd.AttachTaskOwnedNetworkSnapshot, len(values))
	for index, value := range values {
		snapshots[index] = etcd.AttachTaskOwnedNetworkSnapshot{ID: value.Desired.ID, Name: value.Desired.Name}
	}
	return snapshots
}
