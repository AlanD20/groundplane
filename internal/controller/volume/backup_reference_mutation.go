package volume

import (
	"github.com/AlanD20/groundplane/internal/core"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// updateAuthoredBackupVolumeReferences advances only references owned by the
// selected stable Volume identity. Recovery Points and durable source history
// are separate from this replayable authored input.
func updateAuthoredBackupVolumeReferences(
	input *core.BlueprintDesiredInput,
	current projectionrecord.EnvironmentComposeProjection,
	request volumeMutationRequest,
) error {
	if request.action == volumeMutationActionAdd || input.Backup == nil {
		return nil
	}
	var currentSlug string
	for _, volume := range current.Volumes {
		if volume.ID == request.volumeID && volume.Key == request.key {
			currentSlug = volume.Slug
			break
		}
	}
	if currentSlug == "" {
		return errs.New(errs.KindInternal, "Volume Backup reference identity is inconsistent")
	}
	if request.action == volumeMutationActionEdit {
		for index := range input.Backup.Sources {
			source := &input.Backup.Sources[index]
			if source.Kind == core.BackupSourceVolume && source.Ref == currentSlug {
				source.Ref = request.slug
			}
		}
		return nil
	}
	if request.action != volumeMutationActionRemove {
		return errs.New(errs.KindInternal, "Volume Backup reference mutation action is invalid")
	}
	retained := input.Backup.Sources[:0]
	for _, source := range input.Backup.Sources {
		if source.Kind == core.BackupSourceVolume && source.Ref == currentSlug {
			continue
		}
		retained = append(retained, source)
	}
	input.Backup.Sources = retained
	if len(retained) == 0 {
		input.Backup.Enabled = false
	}
	return nil
}
