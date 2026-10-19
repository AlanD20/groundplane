package backupwork

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// CaptureCheckpointEvidence reads completed Volume transfer evidence at the
// same revision as admission. An Agent-supplied manifest digest alone is not
// sufficient to publish an archive or grant an upload.
func (repository *InputRepository) CaptureCheckpointEvidence(ctx context.Context, view workqueue.ResumeView,
	admitted CheckpointAdmission,
) (CheckpointEvidence, error) {
	prepared := admitted.Request.GetArtifactPrepared()
	if prepared.GetVolume() == nil {
		return CheckpointEvidence{}, nil
	}
	binding, err := backupvolumetransfer.Bind(admitted.Assignment, view.Attempt.Attempt.Executor,
		admitted.Request.SourceOrdinal, agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE)
	if err != nil {
		return CheckpointEvidence{}, err
	}
	owner := backupvolumemanifest.Owner{Binding: binding}
	complete, err := backupvolumemanifest.ReadComplete(ctx, repository.store, owner, view.Work.ReadRevision)
	if err != nil {
		return CheckpointEvidence{}, err
	}
	archive, err := backupvolumetransfer.ArchiveEvidenceFromWire(prepared.GetVolume())
	if err != nil || archive != complete.Archive || complete.Source == nil ||
		complete.Source.SizeBytes != prepared.Evidence.SourceSizeBytes ||
		!bytes.Equal(complete.Source.SHA256[:], prepared.Evidence.SourceSha256) ||
		complete.Start.PointId != prepared.PointId || complete.Start.RestoreGenerationId != "" ||
		complete.Start.Role != agentpb.BackupVolumeManifestRole_BACKUP_VOLUME_MANIFEST_ROLE_CAPTURED {
		return CheckpointEvidence{}, conflict("Volume archive differs from its completed capture manifest")
	}
	return CheckpointEvidence{VolumeManifest: proto.CloneOf(prepared.GetVolume()),
		VolumeReference: backupruntime.BackupVolumeManifestReference{
			Binding:        binding,
			CursorRevision: complete.Revision,
		},
		Conditions: []keyvalue.Condition{
			{Key: backupvolumemanifest.CursorKey(binding), ModRevision: complete.Revision},
		}}, nil
}
