package composerender

import (
	"bytes"
	"crypto/sha256"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
	"strconv"
)

// RetainEnvironmentComponentRuntime retains equivalent effective Component
// runtime from a CAS-fenced applied artifact or its frozen candidate result.
// It never infers ownership from live Docker or an unacknowledged desired head.
func RetainEnvironmentComponentRuntime(
	current *agentpb.ComposeArtifact,
	source []byte,
) (*agentpb.ComposeArtifact, error) {
	if current == nil {
		return nil, errs.New(errs.KindInternal, "Component runtime artifact is absent")
	}
	hasComponent := false
	for _, service := range current.Services {
		hasComponent = hasComponent || service.GetOwnerComponentId() != ""
	}
	if len(source) == 0 || !hasComponent {
		return current, nil
	}
	prior := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(source, prior) != nil || executionplan.RejectUnknown(prior) != nil || current == nil ||
		prior.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT || current.OwnerKind != prior.OwnerKind ||
		current.OwnerId != prior.OwnerId || current.ProjectName != prior.ProjectName || current.AuthorizedVolumeDir != prior.AuthorizedVolumeDir {
		return nil, errs.New(errs.KindStateConflict, "Component retained runtime ownership is invalid")
	}
	nextRoot, err := componentRetentionDocument(current)
	if err != nil {
		return nil, err
	}
	priorRoot, err := componentRetentionDocument(prior)
	if err != nil {
		return nil, err
	}
	nextServices, err := ServiceArtifactMapping(nextRoot)
	if err != nil {
		return nil, err
	}
	priorServices, err := ServiceArtifactMapping(priorRoot)
	if err != nil {
		return nil, err
	}
	owned, changed := proto.CloneOf(current), false
	for index, service := range owned.Services {
		if service.GetOwnerComponentId() == "" {
			continue
		}
		var selected *agentpb.ComposeService
		for _, candidate := range prior.Services {
			if candidate.GetServiceId() == service.ServiceId && candidate.GetComposeName() == service.ComposeName {
				if selected != nil {
					return nil, errs.New(errs.KindStateConflict, "Component retained runtime is duplicated")
				}
				selected = candidate
			}
		}
		if selected == nil || !sameComponentRuntimeMetadata(service, selected) {
			continue
		}
		nextNode, priorNode := componentRetentionValue(
			nextServices,
			service.ComposeName,
		), componentRetentionValue(
			priorServices,
			service.ComposeName,
		)
		if nextNode == nil || priorNode == nil {
			return nil, errs.New(errs.KindStateConflict, "Component retained runtime is absent")
		}
		matches, err := sameComponentRuntimeService(nextNode, priorNode, service, selected)
		if err != nil {
			return nil, err
		}
		if !matches {
			continue
		}
		matches, err = sameComponentRuntimeResources(nextNode, nextRoot, priorRoot)
		if err != nil {
			return nil, err
		}
		if !matches {
			continue
		}
		owned.Services[index] = proto.CloneOf(selected)
		nextServices.Content[MappingIndex(nextServices, service.ComposeName)+1] = priorNode
		changed = true
	}
	if !changed {
		return current, nil
	}
	owned.CanonicalYaml, err = yaml.Marshal(nextRoot)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(owned.CanonicalYaml)
	owned.YamlSha256 = digest[:]
	return owned, nil
}

func sameComponentRuntimeResources(service, current, prior *yaml.Node) (bool, error) {
	references := make(map[string]map[string]bool)
	if err := RetainedServiceResourceReferences(service, references); err != nil {
		return false, err
	}
	for section, names := range references {
		for name := range names {
			next := componentRetentionValue(componentRetentionValue(current, section), name)
			previous := componentRetentionValue(componentRetentionValue(prior, section), name)
			if next == nil || previous == nil || !SameComponentRuntimeNode(next, previous) {
				return false, nil
			}
		}
	}
	return true, nil
}

func componentRetentionDocument(artifact *agentpb.ComposeArtifact) (*yaml.Node, error) {
	digest := sha256.Sum256(artifact.CanonicalYaml)
	var document yaml.Node
	if !bytes.Equal(digest[:], artifact.YamlSha256) || yaml.Unmarshal(artifact.CanonicalYaml, &document) != nil ||
		len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return nil, errs.New(errs.KindStateConflict, "Component retained runtime digest or YAML is invalid")
	}
	return document.Content[0], nil
}

func componentRetentionValue(mapping *yaml.Node, key string) *yaml.Node {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return nil
	}
	index := MappingIndex(mapping, key)
	if index < 0 {
		return nil
	}
	return mapping.Content[index+1]
}

func sameComponentRuntimeMetadata(current, prior *agentpb.ComposeService) bool {
	left, right := proto.CloneOf(current), proto.CloneOf(prior)
	left.ExpectedLabels = componentRuntimeStableLabels(left.ExpectedLabels)
	right.ExpectedLabels = componentRuntimeStableLabels(right.ExpectedLabels)
	return proto.Equal(left, right)
}

func componentRuntimeStableLabels(labels []*agentpb.LabelPair) []*agentpb.LabelPair {
	result := make([]*agentpb.LabelPair, 0, len(labels))
	for _, label := range labels {
		if label.GetKey() != composeLabelPlanID && label.GetKey() != composeLabelRenderGen {
			result = append(result, label)
		}
	}
	return result
}

func sameComponentRuntimeService(next, prior *yaml.Node, current, previous *agentpb.ComposeService) (bool, error) {
	priorLabels, nextLabels := componentRetentionValue(prior, "labels"), componentRetentionValue(next, "labels")
	if priorLabels == nil || nextLabels == nil {
		return false, errs.New(errs.KindStateConflict, "Component runtime labels are absent")
	}
	for _, binding := range []struct {
		node    *yaml.Node
		service *agentpb.ComposeService
	}{{nextLabels, current}, {priorLabels, previous}} {
		values := make(map[string]string)
		for _, label := range binding.service.ExpectedLabels {
			values[label.GetKey()] = label.GetValue()
		}
		generation, err := strconv.ParseUint(values[composeLabelRenderGen], 10, 64)
		if ids.Validate(ids.KindPlan, values[composeLabelPlanID]) != nil || err != nil || generation == 0 ||
			strconv.FormatUint(generation, 10) != values[composeLabelRenderGen] {
			return false, errs.New(errs.KindStateConflict, "Component runtime ownership is malformed")
		}
		for _, key := range []string{composeLabelPlanID, composeLabelRenderGen} {
			node := componentRetentionValue(binding.node, key)
			if node == nil || node.Kind != yaml.ScalarNode || node.Value != values[key] {
				return false, errs.New(errs.KindStateConflict, "Component runtime label and metadata disagree")
			}
		}
	}
	priorContent, nextContent := priorLabels.Content, nextLabels.Content
	defer func() { priorLabels.Content, nextLabels.Content = priorContent, nextContent }()
	priorLabels.Content, nextLabels.Content = append(
		[]*yaml.Node(nil),
		priorContent...), append(
		[]*yaml.Node(nil),
		nextContent...)
	for _, key := range []string{composeLabelPlanID, composeLabelRenderGen} {
		RemoveMappingValue(priorLabels, key)
		RemoveMappingValue(nextLabels, key)
	}
	return SameComponentRuntimeNode(next, prior), nil
}

func SameComponentRuntimeNode(left, right *yaml.Node) bool {
	if left == nil || right == nil {
		return left == right
	}
	canonicalizeComponentRuntimeNode(left)
	canonicalizeComponentRuntimeNode(right)
	first, firstErr := yaml.Marshal(left)
	second, secondErr := yaml.Marshal(right)
	return firstErr == nil && secondErr == nil && bytes.Equal(first, second)
}

func canonicalizeComponentRuntimeNode(node *yaml.Node) {
	if node.Kind == yaml.MappingNode {
		sortMapping(node)
	}
	for _, child := range node.Content {
		canonicalizeComponentRuntimeNode(child)
	}
}
