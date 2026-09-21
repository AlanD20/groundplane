package blueprint

import (
	"github.com/AlanD20/groundplane/internal/common/slug"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"

	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"path"
	"sort"
)

func managedEnvironmentVolumeIDs(current []composeidentity.Resource) []string {
	result := make([]string, 0, len(current))
	for _, volume := range current {
		result = append(result, volume.ID)
	}
	sort.Strings(result)
	return result
}

func environmentBlueprintVolumeSlugs(
	project *composetypes.Project,
	previous projectionrecord.EnvironmentComposeProjection,
	hasPrevious bool,
) (map[string]string, error) {
	if project == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Compose project is missing")
	}
	previousByKey := make(map[string]string, len(previous.Volumes))
	if hasPrevious {
		for _, volume := range previous.Volumes {
			previousByKey[volume.Key] = volume.Slug
		}
	}
	result := make(map[string]string, len(project.Volumes))
	seenSlugs := make(map[string]struct{}, len(project.Volumes))
	for key, volume := range project.Volumes {
		volumeSlug := ""
		if authored, exists := volume.Extensions["x-gp-slug"]; exists {
			var ok bool
			volumeSlug, ok = authored.(string)
			if !ok {
				return nil, errs.New(errs.KindValidationFailed, "Blueprint volume x-gp-slug must be a string")
			}
		} else if retained, exists := previousByKey[key]; exists {
			volumeSlug = retained
		} else {
			volumeSlug = key
		}
		if err := slug.Validate("Blueprint volume x-gp-slug", volumeSlug); err != nil {
			return nil, err
		}
		if _, duplicate := seenSlugs[volumeSlug]; duplicate {
			return nil, errs.New(errs.KindNameConflict, "Blueprint volume slug is already in use")
		}
		seenSlugs[volumeSlug] = struct{}{}
		result[key] = volumeSlug
	}
	return result, nil
}

func environmentBlueprintVolumeMounts(
	project *composetypes.Project,
	identities composeidentity.Snapshot,
) ([]projectionrecord.EnvironmentServiceVolumeMount, error) {
	if project == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Compose project is missing")
	}
	services := make(map[string]string, len(identities.Services))
	for _, service := range identities.Services {
		services[service.Name] = service.ID
	}
	volumes := make(map[string]string, len(identities.Volumes))
	for _, volume := range identities.Volumes {
		volumes[volume.Name] = volume.ID
	}
	mounts := make([]projectionrecord.EnvironmentServiceVolumeMount, 0)
	for serviceName, service := range project.Services {
		serviceID, exists := services[serviceName]
		if !exists {
			return nil, errs.New(errs.KindInternal, "Blueprint Service identity is missing")
		}
		seenTargets := make(map[string]struct{}, len(service.Volumes))
		for _, mount := range service.Volumes {
			if mount.Type != composetypes.VolumeTypeVolume {
				continue
			}
			volumeID, exists := volumes[mount.Source]
			if !exists {
				return nil, errs.New(
					errs.KindValidationFailed,
					"Blueprint Service references an unknown managed Volume",
				)
			}
			if mount.Target == "" || !path.IsAbs(mount.Target) || path.Clean(mount.Target) != mount.Target {
				return nil, errs.New(
					errs.KindValidationFailed,
					"Blueprint Volume mount target must be a clean absolute path",
				)
			}
			if _, duplicate := seenTargets[mount.Target]; duplicate {
				return nil, errs.New(errs.KindValidationFailed, "Blueprint Service repeats a Volume mount target")
			}
			seenTargets[mount.Target] = struct{}{}
			mounts = append(mounts, projectionrecord.EnvironmentServiceVolumeMount{
				ServiceID: serviceID, VolumeID: volumeID, Target: mount.Target, ReadOnly: mount.ReadOnly,
			})
		}
	}
	sort.Slice(mounts, func(left int, right int) bool {
		leftKey := mounts[left].ServiceID + "\x00" + mounts[left].Target
		rightKey := mounts[right].ServiceID + "\x00" + mounts[right].Target
		return leftKey < rightKey
	})
	return mounts, nil
}
