package taskplanning

import (
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// RetainBlueprintNativeRuntimeSources merges all physical members before
// replacing a Service. Only acknowledged bytes supply the complete replica set,
// stable proxy and retained slot; immutable Release inputs cannot reconstruct it.
func RetainBlueprintNativeRuntimeSources(
	current *agentpb.ComposeArtifact,
	runtimes []serviceruntimerecord.Record,
	serviceIDs []string,
) (*agentpb.ComposeArtifact, error) {
	if current == nil || len(runtimes) == 0 || len(runtimes) != len(serviceIDs) {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint retained runtime sources are absent")
	}
	sources, err := retainedRuntimeArtifacts(current.OwnerId, runtimes, serviceIDs)
	if err != nil {
		return nil, err
	}
	combined := proto.CloneOf(current)
	combined.Services, combined.Networks, combined.Volumes = nil, nil, nil
	root := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	services := composerender.EnsureMappingValue(root, "services")
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
		mapping, err := composerender.ServiceArtifactMapping(document.Content[0])
		if err != nil {
			return nil, err
		}
		references := make(map[string]map[string]bool)
		for _, service := range source.Services {
			index := composerender.MappingIndex(mapping, service.ComposeName)
			if service.OwnerComponentId != "" ||
				service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED ||
				index < 0 ||
				composerender.MappingIndex(services, service.ComposeName) >= 0 {
				return nil, errs.New(
					errs.KindStateConflict,
					"Blueprint retained physical member is duplicated or invalid",
				)
			}
			for _, existing := range combined.Services {
				if existing.ServiceId == service.ServiceId && existing.Role == service.Role &&
					service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT &&
					(service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA ||
						existing.RollingReplicaOrdinal == service.RollingReplicaOrdinal) {
					return nil, errs.New(errs.KindStateConflict, "Blueprint retained physical role is duplicated")
				}
			}
			composerender.AppendMappingValue(services, service.ComposeName, mapping.Content[index+1])
			if err := composerender.RetainedServiceResourceReferences(mapping.Content[index+1], references); err != nil {
				return nil, err
			}
			combined.Services = append(combined.Services, proto.CloneOf(service))
		}
		for _, section := range []string{"networks", "volumes", "configs", "secrets"} {
			index := composerender.MappingIndex(document.Content[0], section)
			if index < 0 {
				continue
			}
			prior := document.Content[0].Content[index+1]
			if prior.Kind != yaml.MappingNode {
				return nil, errs.New(errs.KindValidationFailed, "Blueprint retained resource is invalid")
			}
			next := composerender.EnsureMappingValue(root, section)
			for entry := 0; entry < len(prior.Content); entry += 2 {
				name, value := prior.Content[entry].Value, prior.Content[entry+1]
				if !references[section][name] {
					continue
				}
				existing := composerender.MappingIndex(next, name)
				if existing < 0 {
					composerender.AppendMappingValue(next, name, value)
					continue
				}
				if !composerender.SameComponentRuntimeNode(value, next.Content[existing+1]) {
					return nil, errs.New(errs.KindStateConflict, "Blueprint retained resource sources disagree")
				}
			}
		}
		combined.Networks = append(combined.Networks, source.Networks...)
		combined.Volumes = append(combined.Volumes, source.Volumes...)
	}
	combined.CanonicalYaml, err = yaml.Marshal(root)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return composerender.RetainBlueprintNativeRuntime(current, combined, serviceIDs)
}

func retainedRuntimeArtifacts(
	environmentID string,
	runtimes []serviceruntimerecord.Record,
	serviceIDs []string,
) ([]*agentpb.ComposeArtifact, error) {
	selected := make(map[string]bool, len(serviceIDs))
	for _, id := range serviceIDs {
		if selected[id] {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint retained Service is duplicated")
		}
		selected[id] = true
	}
	artifacts := make([]*agentpb.ComposeArtifact, 0, len(runtimes)*2)
	for _, source := range runtimes {
		if source.EnvironmentID != environmentID || !selected[source.Runtime.ServiceID] ||
			serviceruntimerecord.Validate(source) != nil {
			return nil, errs.New(errs.KindStateConflict, "Blueprint retained acknowledged runtime is invalid")
		}
		delete(selected, source.Runtime.ServiceID)
		for _, encoded := range [][]byte{source.Runtime.CurrentArtifact, source.Runtime.RetainedPriorArtifact} {
			if len(encoded) == 0 {
				continue
			}
			artifact := &agentpb.ComposeArtifact{}
			if proto.Unmarshal(encoded, artifact) != nil {
				return nil, errs.New(errs.KindStateConflict, "Blueprint retained acknowledged artifact is invalid")
			}
			artifacts = append(artifacts, artifact)
		}
	}
	return artifacts, nil
}
