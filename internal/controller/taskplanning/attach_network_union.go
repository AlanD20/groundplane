package taskplanning

import (
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"sort"

	composetypes "github.com/compose-spec/compose-go/v2/types"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ProjectEnvironmentAttachNetworks owns the authored-runtime binding step:
// validate the intended union against topology before changing Compose.
func ProjectEnvironmentAttachNetworks(
	project *composetypes.Project,
	environmentID string,
	zones []projectionrecord.EnvironmentZoneProjection,
	services []servicerecord.EnvironmentServiceProjection,
	attaches []etcdstore.Versioned[attachrecord.Record],
) ([]composeidentity.Resource, error) {
	projection := projectionrecord.EnvironmentComposeProjection{
		EnvironmentID: environmentID, DesiredZones: zones, DesiredServices: services,
	}
	joins, err := ResolveAttachNetworkJoins(environmentID, projection, attaches, "")
	if err != nil {
		return nil, err
	}
	return ProjectAttachNetworks(project, projection, joins)
}

// ResolveAttachNetworkJoins computes the complete intended binding union.
// Entry and Attach mutations use the same filtering and ownership checks.
func ResolveAttachNetworkJoins(
	environmentID string,
	projection projectionrecord.EnvironmentComposeProjection,
	attaches []etcdstore.Versioned[attachrecord.Record],
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
		if record.ID == excludedAttachID || record.Operation == attachrecord.AttachOperationDetach ||
			record.Status == core.AttachDetached {
			continue
		}
		if record.Operation != attachrecord.AttachOperationProvision {
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
