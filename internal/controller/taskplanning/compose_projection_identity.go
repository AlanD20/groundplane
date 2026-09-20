package taskplanning

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"sort"
)

func ComposeIdentitySnapshotFromProjection(
	projection etcd.EnvironmentComposeProjection,
) (composeidentity.Snapshot, error) {
	componentOwners := make(map[string]string)
	for _, component := range projection.Components {
		for _, serviceID := range component.Runtime.GeneratedServices {
			if owner, exists := componentOwners[serviceID]; exists && owner != component.Desired.ID {
				return composeidentity.Snapshot{}, errs.New(
					errs.KindValidationFailed, "generated Service has multiple Component owners",
				)
			}
			componentOwners[serviceID] = component.Desired.ID
		}
	}
	services := make([]composeidentity.Resource, 0, len(projection.DesiredServices))
	usedServiceIDs := make(map[string]struct{}, cap(services))
	usedServiceNames := make(map[string]struct{}, cap(services))
	desiredServiceNames := make(map[string]string, len(projection.DesiredServices))
	desiredNames := make(map[string]struct{}, len(projection.DesiredServices))
	for _, desired := range projection.DesiredServices {
		service := desired.Desired
		if ids.Validate(ids.KindService, service.ID) != nil || service.Name == "" {
			return composeidentity.Snapshot{}, errs.New(errs.KindInternal, "desired Service render identity is invalid")
		}
		if _, duplicate := desiredServiceNames[service.ID]; duplicate {
			return composeidentity.Snapshot{}, errs.New(
				errs.KindInternal,
				"desired Service render identity is duplicated",
			)
		}
		if _, duplicate := desiredNames[service.Name]; duplicate {
			return composeidentity.Snapshot{}, errs.New(errs.KindInternal, "desired Service render name is duplicated")
		}
		desiredServiceNames[service.ID] = service.Name
		desiredNames[service.Name] = struct{}{}
		if _, generated := componentOwners[service.ID]; generated {
			continue
		}
		usedServiceIDs[service.ID] = struct{}{}
		usedServiceNames[service.Name] = struct{}{}
		services = append(services, composeidentity.Resource{ID: service.ID, Name: service.Name})
	}
	if len(componentOwners) != 0 {
		artifact := &agentpb.ComposeArtifact{}
		if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(projection.ComposeArtifact, artifact); err != nil {
			return composeidentity.Snapshot{}, errs.New(
				errs.KindInternal,
				"Component generated Service render metadata is corrupt",
			)
		}
		for _, service := range artifact.GetServices() {
			componentID, generated := componentOwners[service.GetServiceId()]
			if !generated {
				continue
			}
			// Generated Services are owned by the Component and its pinned
			// artifact, not the native desired-Service collection.
			if ids.Validate(ids.KindService, service.GetServiceId()) != nil || service.GetComposeName() == "" {
				return composeidentity.Snapshot{}, errs.New(
					errs.KindInternal,
					"Component generated Service render name is missing",
				)
			}
			if desiredName, present := desiredServiceNames[service.GetServiceId()]; present &&
				desiredName != service.GetComposeName() {
				return composeidentity.Snapshot{}, errs.New(
					errs.KindInternal,
					"Component generated Service render name diverges",
				)
			}
			_, duplicateID := usedServiceIDs[service.GetServiceId()]
			_, duplicateName := usedServiceNames[service.GetComposeName()]
			if duplicateID || duplicateName {
				return composeidentity.Snapshot{}, errs.New(
					errs.KindInternal,
					"Component generated Service render identity or name is duplicated",
				)
			}
			identity, err := pinnedComponentServiceIdentity(service, componentID)
			if err != nil {
				return composeidentity.Snapshot{}, err
			}
			usedServiceIDs[identity.ID], usedServiceNames[identity.Name] = struct{}{}, struct{}{}
			services = append(services, identity)
		}
		for serviceID := range componentOwners {
			if _, found := usedServiceIDs[serviceID]; !found {
				return composeidentity.Snapshot{}, errs.New(
					errs.KindInternal,
					"Component generated Service render metadata is missing",
				)
			}
		}
	}
	sort.Slice(services, func(left, right int) bool { return services[left].Name < services[right].Name })
	return composeidentity.Snapshot{
		Services: services,
		Networks: desiredZoneResourceIdentities(projection.DesiredZones),
		Volumes:  composeVolumeResourceIdentities(projection.Volumes),
	}, nil
}
