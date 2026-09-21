package taskplanning

import (
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"sort"
)

type entryRemovalServiceIdentity struct {
	ID   string
	Name string
}

func entryRemovalEnvironmentServiceIdentities(
	projection etcd.EnvironmentComposeProjection,
) ([]entryRemovalServiceIdentity, error) {
	identities := make([]entryRemovalServiceIdentity, 0, len(projection.DesiredServices))
	seenIDs := make(map[string]struct{}, len(projection.DesiredServices))
	seenNames := make(map[string]struct{}, len(projection.DesiredServices))
	add := func(identity entryRemovalServiceIdentity) error {
		if identity.ID == "" || identity.Name == "" {
			return errs.New(errs.KindInternal, "entry removal service identity is incomplete")
		}
		if _, duplicate := seenIDs[identity.ID]; duplicate {
			return errs.New(errs.KindInternal, "entry removal service identity id is duplicated")
		}
		if _, duplicate := seenNames[identity.Name]; duplicate {
			return errs.New(errs.KindInternal, "entry removal service identity name is duplicated")
		}
		seenIDs[identity.ID] = struct{}{}
		seenNames[identity.Name] = struct{}{}
		identities = append(identities, identity)
		return nil
	}
	generatedIDs := make(map[string]string)
	for _, component := range projection.Components {
		for _, serviceID := range component.Runtime.GeneratedServices {
			if serviceID == "" {
				return nil, errs.New(errs.KindInternal, "entry removal generated service identity is incomplete")
			}
			if _, duplicate := generatedIDs[serviceID]; duplicate {
				return nil, errs.New(errs.KindInternal, "entry removal generated service identity is duplicated")
			}
			generatedIDs[serviceID] = component.Desired.ID
		}
	}
	for _, service := range projection.DesiredServices {
		if _, generated := generatedIDs[service.Desired.ID]; generated {
			continue
		}
		if err := add(entryRemovalServiceIdentity{ID: service.Desired.ID, Name: service.Desired.Name}); err != nil {
			return nil, err
		}
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil {
		return nil, errs.New(errs.KindInternal, "entry removal Compose artifact is corrupt")
	}
	artifactServices := make(map[string]*agentpb.ComposeService, len(generatedIDs))
	for _, service := range artifact.GetServices() {
		if service == nil {
			return nil, errs.New(errs.KindInternal, "entry removal Compose artifact service metadata is invalid")
		}
		if _, generated := generatedIDs[service.GetServiceId()]; !generated {
			continue
		}
		if service.GetComposeName() == "" || service.GetOwnerComponentId() == "" ||
			service.GetOwnerComponentId() != generatedIDs[service.GetServiceId()] {
			return nil, errs.New(errs.KindInternal, "entry removal Compose artifact service metadata is invalid")
		}
		if _, duplicate := artifactServices[service.GetServiceId()]; duplicate {
			return nil, errs.New(errs.KindInternal, "entry removal Compose artifact service metadata is duplicated")
		}
		artifactServices[service.GetServiceId()] = service
	}
	generatedServiceIDs := make([]string, 0, len(generatedIDs))
	for serviceID := range generatedIDs {
		generatedServiceIDs = append(generatedServiceIDs, serviceID)
	}
	sort.Strings(generatedServiceIDs)
	for _, serviceID := range generatedServiceIDs {
		service, found := artifactServices[serviceID]
		if !found {
			return nil, errs.New(errs.KindInternal, "entry removal generated service metadata is missing")
		}
		if err := add(entryRemovalServiceIdentity{ID: service.GetServiceId(), Name: service.GetComposeName()}); err != nil {
			return nil, err
		}
	}
	sort.Slice(identities, func(left, right int) bool {
		if identities[left].Name == identities[right].Name {
			return identities[left].ID < identities[right].ID
		}
		return identities[left].Name < identities[right].Name
	})
	return identities, nil
}

func entryExposesService(entry core.EnvEntry, serviceName string) bool {
	for _, exposure := range entry.Exposure {
		if exposure == serviceName {
			return true
		}
	}
	return false
}
