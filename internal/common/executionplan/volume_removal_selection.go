package executionplan

import (
	"slices"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

func dependencyFreeServiceSelection(
	plan *agentpb.ExecutionPlan, step *agentpb.ExecutionStep, artifacts map[string]*agentpb.ComposeArtifact,
) bool {
	if blueprintManagedServiceSelection(plan.Operation, step.GetComposeApply(), artifacts) ||
		routeManagedServiceSelection(plan, step, artifacts) {
		return true
	}
	if plan.GetEntryMutationProcedure() != nil {
		return validateEntryMutationPlan(plan) == nil
	}
	if _, selected, err := AttachMutationServices(plan, step.GetStepId()); selected {
		return err == nil
	}
	_, selected, err := VolumeRemovalServices(plan, step.StepId)
	return selected && err == nil
}

// VolumeRemovalServices selects runtime names from the sealed baseline mounts,
// not every proxy/slot sharing a logical Service id. The boolean distinguishes
// unrelated operations from a removal with no deployed consumers.
func VolumeRemovalServices(plan *agentpb.ExecutionPlan, stepID string) ([]string, bool, error) {
	if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_REMOVE ||
		ids.Validate(ids.KindVolume, plan.GetTargetId()) != nil {
		return nil, false, nil
	}
	invalid := func() ([]string, bool, error) {
		return nil, true, errs.New(errs.KindValidationFailed, "Volume removal consumer selection is invalid")
	}
	if len(plan.Steps) != 3 || len(plan.Artifacts) != 2 || plan.Steps[0].GetStepId() != stepID {
		return invalid()
	}
	apply := plan.Steps[0].GetComposeApply()
	remove := plan.Steps[1].GetManagedVolumeRemove()
	directory := plan.Steps[2].GetManagedVolumeDirectoryRemove()
	if apply == nil || apply.FullReconcile || apply.ForceRecreate || !apply.NoDependencies ||
		remove.GetVolumeId() != plan.TargetId || directory.GetVolumeId() != plan.TargetId {
		return invalid()
	}
	var baseline, candidate *agentpb.ComposeArtifact
	for _, artifact := range plan.Artifacts {
		if artifact.GetArtifactId() == directory.ArtifactId {
			baseline = artifact
		}
		if artifact.GetArtifactId() == apply.ArtifactId {
			candidate = artifact
		}
	}
	if baseline == nil || candidate == nil || baseline == candidate ||
		baseline.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		candidate.OwnerKind != baseline.OwnerKind || candidate.OwnerId != baseline.OwnerId ||
		candidate.ProjectName != baseline.ProjectName || candidate.AuthorizedVolumeDir != baseline.AuthorizedVolumeDir ||
		len(candidate.Services) != len(baseline.Services) {
		return invalid()
	}
	found := false
	for _, volume := range baseline.Volumes {
		if volume.GetVolumeId() == plan.TargetId && volume.GetComposeName() == directory.ComposeKey &&
			volume.GetDockerName() == remove.DockerName {
			found = true
		}
	}
	for _, volume := range candidate.Volumes {
		if volume.GetVolumeId() == plan.TargetId || volume.GetComposeName() == directory.ComposeKey {
			return invalid()
		}
	}
	if !found {
		return invalid()
	}
	priorMounts, err := volumeRemovalMounts(baseline, directory.ComposeKey)
	if err != nil {
		return nil, true, err
	}
	nextMounts, err := volumeRemovalMounts(candidate, directory.ComposeKey)
	if err != nil {
		return nil, true, err
	}
	var names []string
	for index, service := range baseline.Services {
		if service == nil || !proto.Equal(service, candidate.Services[index]) || nextMounts[service.ComposeName] {
			return invalid()
		}
		if priorMounts[service.ComposeName] {
			if !slices.Contains(apply.ServiceIds, service.ServiceId) {
				return invalid()
			}
			names = append(names, service.ComposeName)
		}
	}
	sort.Strings(names)
	return names, true, nil
}

func volumeRemovalMounts(artifact *agentpb.ComposeArtifact, key string) (map[string]bool, error) {
	var document struct {
		Services map[string]struct {
			Volumes []yaml.Node `yaml:"volumes"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(artifact.CanonicalYaml, &document); err != nil {
		return nil, errs.Wrap(errs.KindValidationFailed, err)
	}
	if len(document.Services) != len(artifact.Services) {
		return nil, errs.New(errs.KindValidationFailed, "Volume removal runtime mapping is incomplete")
	}
	result := make(map[string]bool, len(document.Services))
	for _, service := range artifact.Services {
		entry, ok := document.Services[service.GetComposeName()]
		if !ok {
			return nil, errs.New(errs.KindValidationFailed, "Volume removal runtime Service is absent")
		}
		for _, mount := range entry.Volumes {
			var source, kind string
			switch mount.Kind {
			case yaml.ScalarNode:
				source, _, _ = strings.Cut(mount.Value, ":")
			case yaml.MappingNode:
				var value struct {
					Source string `yaml:"source"`
					Type   string `yaml:"type"`
				}
				if err := mount.Decode(&value); err != nil {
					return nil, errs.Wrap(errs.KindValidationFailed, err)
				}
				source, kind = value.Source, value.Type
			default:
				return nil, errs.New(errs.KindValidationFailed, "Volume removal mount is invalid")
			}
			if source == key && (kind == "" || kind == "volume") {
				result[service.ComposeName] = true
			}
		}
	}
	return result, nil
}
