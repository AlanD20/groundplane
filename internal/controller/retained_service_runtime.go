package controller

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// RenderRetainedServiceRuntime reconstructs only the sealed serving workload,
// its stable proxy, and any captured inactive blue/green workload.
func (resolver *TaskPlanResolver) RenderRetainedServiceRuntime(
	ctx context.Context,
	source etcd.ServiceLifecycleRelease,
) ([]*agentpb.ComposeArtifact, error) {
	return resolver.renderServiceLifecycleArtifacts(ctx, etcd.ServiceLifecycleRenderInput{Release: source}, "")
}

// RetainBlueprintNativeRuntimeSources merges all physical members before
// replacing a Service, so retaining an inactive slot cannot erase its active slot.
func RetainBlueprintNativeRuntimeSources(
	current *agentpb.ComposeArtifact,
	sources []*agentpb.ComposeArtifact,
	serviceIDs []string,
) (*agentpb.ComposeArtifact, error) {
	if current == nil || len(sources) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint retained runtime sources are absent")
	}
	combined := proto.CloneOf(current)
	combined.Services, combined.Networks, combined.Volumes = nil, nil, nil
	root := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	services := ensureMappingValue(root, "services")
	for _, source := range sources {
		if source == nil || source.OwnerKind != current.OwnerKind || source.OwnerId != current.OwnerId ||
			source.ProjectName != current.ProjectName ||
			source.AuthorizedVolumeDir != current.AuthorizedVolumeDir {
			return nil, errs.New(errs.KindStateConflict, "Blueprint retained source ownership changed")
		}
		var document yaml.Node
		if yaml.Unmarshal(source.CanonicalYaml, &document) != nil || len(document.Content) != 1 ||
			document.Content[0].Kind != yaml.MappingNode {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint retained source YAML is invalid")
		}
		mapping, err := serviceArtifactMapping(document.Content[0])
		if err != nil {
			return nil, err
		}
		references := make(map[string]map[string]bool)
		for _, service := range source.Services {
			index := mappingIndex(mapping, service.ComposeName)
			if service.OwnerComponentId != "" ||
				service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED ||
				index < 0 ||
				mappingIndex(services, service.ComposeName) >= 0 {
				return nil, errs.New(
					errs.KindStateConflict,
					"Blueprint retained physical member is duplicated or invalid",
				)
			}
			for _, existing := range combined.Services {
				if existing.ServiceId == service.ServiceId && existing.Role == service.Role &&
					service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
					return nil, errs.New(errs.KindStateConflict, "Blueprint retained physical role is duplicated")
				}
			}
			appendMappingValue(services, service.ComposeName, mapping.Content[index+1])
			if err := retainedServiceResourceReferences(mapping.Content[index+1], references); err != nil {
				return nil, err
			}
			combined.Services = append(combined.Services, proto.CloneOf(service))
		}
		for _, section := range []string{"networks", "volumes", "configs", "secrets"} {
			index := mappingIndex(document.Content[0], section)
			if index < 0 {
				continue
			}
			prior := document.Content[0].Content[index+1]
			if prior.Kind != yaml.MappingNode {
				return nil, errs.New(errs.KindValidationFailed, "Blueprint retained resource is invalid")
			}
			next := ensureMappingValue(root, section)
			for entry := 0; entry < len(prior.Content); entry += 2 {
				name, value := prior.Content[entry].Value, prior.Content[entry+1]
				if !references[section][name] {
					continue
				}
				existing := mappingIndex(next, name)
				if existing < 0 {
					appendMappingValue(next, name, value)
					continue
				}
				before, e1 := yaml.Marshal(value)
				after, e2 := yaml.Marshal(next.Content[existing+1])
				if e1 != nil || e2 != nil || !bytes.Equal(before, after) {
					return nil, errs.New(errs.KindStateConflict, "Blueprint retained resource sources disagree")
				}
			}
		}
		combined.Networks = append(combined.Networks, source.Networks...)
		combined.Volumes = append(combined.Volumes, source.Volumes...)
	}
	var err error
	combined.CanonicalYaml, err = yaml.Marshal(root)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return RetainBlueprintNativeRuntime(current, combined, serviceIDs)
}
