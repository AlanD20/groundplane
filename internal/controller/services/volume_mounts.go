package services

import (
	"path"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func replaceServiceVolumeMounts(
	projection *projectionrecord.EnvironmentComposeProjection,
	serviceID string,
	replacement *[]core.Mount,
) (*[]composerender.ServiceArtifactVolumeMount, error) {
	if replacement == nil {
		return nil, nil
	}
	volumes := make(map[string]string, len(projection.Volumes))
	for _, volume := range projection.Volumes {
		volumes[volume.ID] = volume.Key
	}
	seen := make(map[string]bool, len(*replacement))
	rendered := make([]composerender.ServiceArtifactVolumeMount, 0, len(*replacement))
	kept := make([]projectionrecord.EnvironmentServiceVolumeMount, 0, len(projection.VolumeMounts))
	for _, mount := range projection.VolumeMounts {
		if mount.ServiceID != serviceID {
			kept = append(kept, mount)
		}
	}
	for _, mount := range *replacement {
		key, exists := volumes[mount.Volume]
		if !exists || mount.File != "" {
			return nil, errs.New(errs.KindValidationFailed, "Select a managed Volume in this Environment")
		}
		if !path.IsAbs(mount.Mount) || path.Clean(mount.Mount) != mount.Mount || !utf8.ValidString(mount.Mount) ||
			strings.ContainsAny(mount.Mount, "\x00\r\n") {
			return nil, errs.New(errs.KindValidationFailed, "Mount target must be a clean absolute container path")
		}
		if seen[mount.Mount] {
			return nil, errs.New(errs.KindValidationFailed, "Mount targets must be unique")
		}
		seen[mount.Mount] = true
		kept = append(
			kept,
			projectionrecord.EnvironmentServiceVolumeMount{
				ServiceID: serviceID,
				VolumeID:  mount.Volume,
				Target:    mount.Mount,
				ReadOnly:  mount.RO,
			},
		)
		rendered = append(
			rendered,
			composerender.ServiceArtifactVolumeMount{Key: key, Target: mount.Mount, ReadOnly: mount.RO},
		)
	}
	sort.Slice(kept, func(i, j int) bool {
		return kept[i].ServiceID+"\x00"+kept[i].Target < kept[j].ServiceID+"\x00"+kept[j].Target
	})
	projection.VolumeMounts = kept
	return &rendered, nil
}
