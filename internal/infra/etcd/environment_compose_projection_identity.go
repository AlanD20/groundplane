package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/slug"
	"github.com/AlanD20/groundplane/internal/common/volumeidentity"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateEnvironmentComposeProjectionAdvanceAllowingVolumeRemoval(
	previous EnvironmentComposeProjection,
	hasPrevious bool,
	next EnvironmentComposeProjection,
	removedVolumeID string,
) error {
	if err := validateEnvironmentComposeProjection(next); err != nil {
		return err
	}
	if !hasPrevious {
		if next.RenderGeneration != 1 {
			return errs.New(errs.KindStateConflict, "first Environment render generation must be one")
		}
		return nil
	}
	if previous.EnvironmentID != next.EnvironmentID || next.RenderGeneration != previous.RenderGeneration+1 {
		return errs.New(errs.KindStateConflict, "Environment render generation did not advance exactly once")
	}
	return preserveEnvironmentVolumeIdentities(previous.Volumes, next.Volumes, removedVolumeID)
}

func validateEnvironmentVolumeIdentities(values []EnvironmentVolumeIdentity) error {
	previousKey := ""
	idsSeen := make(map[string]struct{}, len(values))
	slugsSeen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if recordcodec.ValidateID(ids.KindVolume, value.ID) != nil || value.Key <= previousKey ||
			slug.Validate("volume slug", value.Slug) != nil || len(value.Key) > 255 ||
			volumeidentity.ValidateKey(value.Key) != nil {
			return errs.New(errs.KindValidationFailed, "Environment Volume identities are invalid or unsorted")
		}
		if _, duplicate := idsSeen[value.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment Volume identity id is duplicated")
		}
		if _, duplicate := slugsSeen[value.Slug]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment Volume identity slug is duplicated")
		}
		idsSeen[value.ID] = struct{}{}
		slugsSeen[value.Slug] = struct{}{}
		previousKey = value.Key
	}
	return nil
}

func validateEnvironmentServiceVolumeMounts(projection EnvironmentComposeProjection) error {
	serviceIDs := make(map[string]struct{}, len(projection.DesiredServices))
	for _, service := range projection.DesiredServices {
		serviceIDs[service.Desired.ID] = struct{}{}
	}
	volumeIDs := make(map[string]struct{}, len(projection.Volumes))
	for _, volume := range projection.Volumes {
		volumeIDs[volume.ID] = struct{}{}
	}
	previous := ""
	for _, mount := range projection.VolumeMounts {
		ordering := mount.ServiceID + "\x00" + mount.Target
		if ordering <= previous || recordcodec.ValidateID(ids.KindService, mount.ServiceID) != nil ||
			recordcodec.ValidateID(ids.KindVolume, mount.VolumeID) != nil ||
			mount.Target == "" || !strings.HasPrefix(mount.Target, "/") ||
			!utf8.ValidString(mount.Target) || strings.IndexByte(mount.Target, 0) >= 0 {
			return errs.New(errs.KindValidationFailed, "Environment Volume mounts are invalid or unsorted")
		}
		if _, exists := serviceIDs[mount.ServiceID]; !exists {
			return errs.New(errs.KindValidationFailed, "Environment Volume mount Service is absent")
		}
		if _, exists := volumeIDs[mount.VolumeID]; !exists {
			return errs.New(errs.KindValidationFailed, "Environment Volume mount Volume is absent")
		}
		previous = ordering
	}
	return nil
}

func preserveEnvironmentVolumeIdentities(
	previous []EnvironmentVolumeIdentity,
	next []EnvironmentVolumeIdentity,
	removedVolumeID string,
) error {
	byKey := make(map[string]string, len(next))
	for _, identity := range next {
		byKey[identity.Key] = identity.ID
	}
	for _, identity := range previous {
		nextID, exists := byKey[identity.Key]
		if !exists {
			if identity.ID == removedVolumeID {
				continue
			}
			return errs.Newf(
				errs.KindResourceInUse,
				"Blueprint omits existing volume %s; remove it explicitly before apply",
				identity.Key,
			)
		}
		if nextID != identity.ID {
			return errs.New(errs.KindStateConflict, "Environment Volume stable identity changed")
		}
	}
	if removedVolumeID != "" {
		for _, identity := range next {
			if identity.ID == removedVolumeID {
				return errs.New(errs.KindStateConflict, "Volume removal did not remove the selected identity")
			}
		}
	}
	return nil
}

func corruptEnvironmentComposeProjection() error {
	return errs.New(errs.KindInternal, "Environment Compose projection is corrupt")
}
