package taskplanning

import (
	"crypto/sha256"
	"sort"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// RetainBlueprintNativeRuntime copies only captured native runtime entries into
// the current managed artifact. It never resolves images or guesses ownership.
func RetainBlueprintNativeRuntime(
	current, applied *agentpb.ComposeArtifact,
	serviceIDs []string,
) (*agentpb.ComposeArtifact, error) {
	if current == nil || applied == nil || current.OwnerKind != applied.OwnerKind ||
		current.OwnerId != applied.OwnerId ||
		current.ProjectName != applied.ProjectName ||
		current.AuthorizedVolumeDir != applied.AuthorizedVolumeDir {
		return nil, errs.New(errs.KindStateConflict, "retained Blueprint artifact ownership changed")
	}
	owned := proto.CloneOf(current)
	var next, prior yaml.Node
	if yaml.Unmarshal(current.CanonicalYaml, &next) != nil || yaml.Unmarshal(applied.CanonicalYaml, &prior) != nil ||
		len(
			next.Content,
		) != 1 || len(prior.Content) != 1 || next.Content[0].Kind != yaml.MappingNode || prior.Content[0].Kind != yaml.MappingNode {
		return nil, errs.New(errs.KindValidationFailed, "retained Blueprint artifact YAML is invalid")
	}
	selected := make(map[string]bool, len(serviceIDs))
	for _, id := range serviceIDs {
		if selected[id] {
			return nil, errs.New(errs.KindValidationFailed, "retained Blueprint Service is duplicated")
		}
		selected[id] = true
	}
	nextServices, err := serviceArtifactMapping(next.Content[0])
	if err != nil {
		return nil, err
	}
	priorServices, err := serviceArtifactMapping(prior.Content[0])
	if err != nil {
		return nil, err
	}
	kept := make([]*agentpb.ComposeService, 0, len(owned.Services)+len(applied.Services))
	for _, service := range owned.Services {
		if selected[service.ServiceId] {
			if service.OwnerComponentId != "" {
				return nil, errs.New(errs.KindStateConflict, "retained Blueprint native Service changed owner")
			}
			removeMappingValue(nextServices, service.ComposeName)
			continue
		}
		kept = append(kept, service)
	}
	found := make(map[string]bool, len(selected))
	references := make(map[string]map[string]bool)
	for _, service := range applied.Services {
		if !selected[service.ServiceId] {
			continue
		}
		index := mappingIndex(priorServices, service.ComposeName)
		if service.OwnerComponentId != "" ||
			service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED ||
			index < 0 ||
			mappingIndex(nextServices, service.ComposeName) >= 0 {
			return nil, errs.New(errs.KindStateConflict, "retained Blueprint physical Service authority changed")
		}
		appendMappingValue(nextServices, service.ComposeName, priorServices.Content[index+1])
		if err := retainedServiceResourceReferences(priorServices.Content[index+1], references); err != nil {
			return nil, err
		}
		kept = append(kept, proto.CloneOf(service))
		found[service.ServiceId] = true
	}
	if len(found) != len(selected) {
		return nil, errs.New(errs.KindStateConflict, "retained Blueprint native runtime is absent")
	}
	// Persistent resources keep stable ownership. A configuration edit that
	// would change a retained resource is not silently applied around its users.
	for _, section := range []string{"networks", "volumes", "configs", "secrets"} {
		priorIndex := mappingIndex(prior.Content[0], section)
		if priorIndex < 0 {
			continue
		}
		priorMapping := prior.Content[0].Content[priorIndex+1]
		nextMapping := ensureMappingValue(next.Content[0], section)
		if priorMapping.Kind != yaml.MappingNode || nextMapping == nil {
			return nil, errs.New(errs.KindValidationFailed, "retained Blueprint resource mapping is invalid")
		}
		for index := 0; index < len(priorMapping.Content); index += 2 {
			name, value := priorMapping.Content[index].Value, priorMapping.Content[index+1]
			if !references[section][name] {
				continue
			}
			currentIndex := mappingIndex(nextMapping, name)
			if currentIndex >= 0 {
				if !sameComponentRuntimeNode(value, nextMapping.Content[currentIndex+1]) {
					return nil, errs.Newf(errs.KindStateConflict,
						"retained Blueprint %s resource %q configuration changed", section, name)
				}
			} else if section == "configs" || section == "secrets" {
				appendMappingValue(nextMapping, name, value)
			} else {
				return nil, errs.New(errs.KindStateConflict, "retained Blueprint persistent resource disappeared")
			}
		}
		sortMapping(nextMapping)
	}
	for _, network := range applied.Networks {
		if !references["networks"][network.ComposeName] {
			continue
		}
		matched := false
		for _, candidate := range owned.Networks {
			matched = matched || proto.Equal(network, candidate)
		}
		if !matched {
			return nil, errs.New(errs.KindStateConflict, "retained Blueprint network ownership changed")
		}
	}
	for _, volume := range applied.Volumes {
		if !references["volumes"][volume.ComposeName] {
			continue
		}
		matched := false
		for _, candidate := range owned.Volumes {
			matched = matched || proto.Equal(volume, candidate)
		}
		if !matched {
			return nil, errs.New(errs.KindStateConflict, "retained Blueprint volume ownership changed")
		}
	}
	sort.Slice(kept, func(i, j int) bool {
		if kept[i].ServiceId != kept[j].ServiceId {
			return kept[i].ServiceId < kept[j].ServiceId
		}
		return kept[i].ComposeName < kept[j].ComposeName
	})
	owned.Services = kept
	sortMapping(nextServices)
	owned.CanonicalYaml, err = yaml.Marshal(&next)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(owned.CanonicalYaml)
	owned.YamlSha256 = digest[:]
	return owned, nil
}

func retainedServiceResourceReferences(service *yaml.Node, references map[string]map[string]bool) error {
	for _, section := range []string{"networks", "volumes", "configs", "secrets"} {
		index := mappingIndex(service, section)
		if index < 0 {
			continue
		}
		if references[section] == nil {
			references[section] = make(map[string]bool)
		}
		entries := service.Content[index+1]
		if section == "networks" && entries.Kind == yaml.MappingNode {
			for index := 0; index < len(entries.Content); index += 2 {
				references[section][entries.Content[index].Value] = true
			}
			continue
		}
		if entries.Kind != yaml.SequenceNode {
			return errs.New(errs.KindValidationFailed, "retained Blueprint resource references are invalid")
		}
		for _, entry := range entries.Content {
			if entry.Kind == yaml.ScalarNode && section != "volumes" {
				references[section][entry.Value] = true
				continue
			}
			if entry.Kind != yaml.MappingNode {
				return errs.New(errs.KindValidationFailed, "retained Blueprint resource reference is not canonical")
			}
			if section == "volumes" {
				typeIndex := mappingIndex(entry, "type")
				if typeIndex < 0 {
					return errs.New(errs.KindValidationFailed, "retained Blueprint volume type is absent")
				}
				if entry.Content[typeIndex+1].Value != "volume" {
					continue
				}
			}
			sourceIndex := mappingIndex(entry, "source")
			if sourceIndex < 0 {
				return errs.New(errs.KindValidationFailed, "retained Blueprint resource source is absent")
			}
			references[section][entry.Content[sourceIndex+1].Value] = true
		}
	}
	return nil
}
