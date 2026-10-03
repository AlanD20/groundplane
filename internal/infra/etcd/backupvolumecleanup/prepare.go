package backupvolumecleanup

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const Prefix = "/v1/runtime/backup-volume-manifest-cleanup/"

type Intent struct {
	PointID        string                     `json:"point_id"`
	Owner          backupvolumemanifest.Owner `json:"owner"`
	CursorRevision int64                      `json:"cursor_revision"`
}

type cursorReader interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

// Prepare joins exact Point/orphan retirement. The independent manifest intent
// survives the deleting Task and remains held until native Task retention ends.
func Prepare(ctx context.Context, reader cursorReader,
	point backupruntime.BackupRecoveryPointSnapshot, revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	if point.SourceKind != backupruntime.BackupRuntimeSourceVolume {
		return nil, nil, nil
	}
	ref := point.VolumeArchive.Manifest
	if !ref.Valid() {
		return nil, nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	binding := backupvolumetransfer.Binding{TaskID: ref.TaskID, AssignmentID: ref.AssignmentID,
		StepID: ref.StepID, TransferID: ref.TransferID,
		Direction: agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE}
	if _, err := hex.Decode(binding.AuthorityDigest[:], []byte(ref.AuthoritySHA256)); err != nil {
		return nil, nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	owner := backupvolumemanifest.Owner{Binding: binding, AgentID: ref.AgentID,
		AgentGeneration: ref.AgentGeneration, AssignmentGeneration: ref.AssignmentGeneration}
	cursor, found, err := backupvolumemanifest.ReadCursor(ctx, reader, owner, revision)
	if err != nil {
		return nil, nil, err
	}
	if !found || cursor.Revision != ref.CursorRevision || !cursor.Record.Complete || cursor.Record.PointID != point.ID {
		return nil, nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	intent := Intent{PointID: point.ID, Owner: owner, CursorRevision: ref.CursorRevision}
	encoded, err := recordcodec.Encode("backup-volume-manifest-cleanup", intent)
	if err != nil {
		return nil, nil, err
	}
	key := Prefix + point.ID
	return []keyvalue.Condition{{Key: key}, {Key: backupruntime.BackupOrphanKey(point.ID)},
			{Key: backupvolumemanifest.CursorKey(binding), ModRevision: ref.CursorRevision}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: encoded}}, nil
}
