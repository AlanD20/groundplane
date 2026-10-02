package agent

import (
	"bytes"
	"context"
	"path"

	"github.com/AlanD20/groundplane/internal/infra/agentvolumejournal"
	"github.com/AlanD20/groundplane/internal/infra/backupvolumefs"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type volumeRestoreCheckpointJournal struct {
	local        *agentvolumejournal.Journal
	publisher    *backupStepCheckpoint
	pointID      string
	generationID string
	newManifest  [32]byte
}

func (journal *volumeRestoreCheckpointJournal) Intent(ctx context.Context,
	mutation backupvolumefs.Mutation,
) error {
	if err := journal.local.Intent(ctx, mutation); err != nil {
		return err
	}
	checkpoint, err := journal.checkpoint(mutation, false)
	if err != nil {
		return err
	}
	return journal.publisher.publish(ctx, checkpoint)
}

func (journal *volumeRestoreCheckpointJournal) Completed(ctx context.Context,
	mutation backupvolumefs.Mutation,
) error {
	if err := journal.local.Completed(ctx, mutation); err != nil {
		return err
	}
	checkpoint, err := journal.checkpoint(mutation, true)
	if err != nil {
		return err
	}
	return journal.publisher.publish(ctx, checkpoint)
}

func (journal *volumeRestoreCheckpointJournal) checkpoint(mutation backupvolumefs.Mutation,
	completed bool,
) (*agentpb.BackupCheckpointRequest, error) {
	volume := &agentpb.BackupVolumeCheckpoint{}
	switch mutation.Kind {
	case backupvolumefs.MutationConstruction:
		if mutation.Ordinal == 0 {
			return nil, invalidAgentStaging()
		}
		intent := &agentpb.BackupVolumeConstructionIntent{PointId: journal.pointID,
			RestoreGenerationId:          journal.generationID,
			NewTreeContentManifestSha256: append([]byte(nil), journal.newManifest[:]...),
			ConstructionCursorBefore:     mutation.Ordinal - 1, ArchiveOrdinal: mutation.Ordinal,
			LogicalPath: append([]byte(nil), mutation.Entry.Path...),
			Kind:        agentpb.BackupVolumeEntryKind(mutation.Entry.Kind), Mode: mutation.Entry.Mode,
			Uid: mutation.Entry.UID, Gid: mutation.Entry.GID, SizeBytes: mutation.Entry.SizeBytes,
			ContentSha256: append([]byte(nil), mutation.Entry.ContentSHA256[:]...)}
		if completed {
			volume.Checkpoint = &agentpb.BackupVolumeCheckpoint_ConstructionCompleted{
				ConstructionCompleted: &agentpb.BackupVolumeConstructionCompleted{
					Intent: intent, ConstructionCursorAfter: mutation.Ordinal}}
		} else {
			volume.Checkpoint = &agentpb.BackupVolumeCheckpoint_ConstructionIntent{ConstructionIntent: intent}
		}
	case backupvolumefs.MutationFinalization:
		if mutation.Ordinal == 0 {
			return nil, invalidAgentStaging()
		}
		intent := &agentpb.BackupVolumeFinalizationIntent{PointId: journal.pointID,
			RestoreGenerationId:          journal.generationID,
			NewTreeContentManifestSha256: append([]byte(nil), journal.newManifest[:]...),
			FinalizationCursorBefore:     mutation.Ordinal - 1, FinalizationOrdinal: mutation.Ordinal,
			LogicalPath: append([]byte(nil), mutation.Entry.Path...), FinalUid: mutation.Entry.UID,
			FinalGid: mutation.Entry.GID, FinalMode: mutation.Entry.Mode}
		if completed {
			volume.Checkpoint = &agentpb.BackupVolumeCheckpoint_FinalizationCompleted{
				FinalizationCompleted: &agentpb.BackupVolumeFinalizationCompleted{
					Intent: intent, FinalizationCursorAfter: mutation.Ordinal}}
		} else {
			volume.Checkpoint = &agentpb.BackupVolumeCheckpoint_FinalizationIntent{FinalizationIntent: intent}
		}
	case backupvolumefs.MutationExchange:
		if mutation.OldTreeSHA == ([32]byte{}) || mutation.NewTreeSHA == ([32]byte{}) {
			return nil, invalidAgentStaging()
		}
		if completed {
			volume.Checkpoint = &agentpb.BackupVolumeCheckpoint_TreeExchanged{
				TreeExchanged: &agentpb.BackupVolumeTreeExchanged{PointId: journal.pointID,
					RestoreGenerationId: journal.generationID,
					OldFullTreeSha256:   append([]byte(nil), mutation.OldTreeSHA[:]...),
					NewFullTreeSha256:   append([]byte(nil), mutation.NewTreeSHA[:]...)}}
		} else {
			volume.Checkpoint = &agentpb.BackupVolumeCheckpoint_ExchangeIntent{
				ExchangeIntent: &agentpb.BackupVolumeExchangeIntent{PointId: journal.pointID,
					RestoreGenerationId: journal.generationID,
					OldFullTreeSha256:   append([]byte(nil), mutation.OldTreeSHA[:]...),
					NewFullTreeSha256:   append([]byte(nil), mutation.NewTreeSHA[:]...)}}
		}
	case backupvolumefs.MutationDelete:
		if mutation.Ordinal == 0 {
			return nil, invalidAgentStaging()
		}
		parent := []byte(nil)
		if !bytes.Equal(mutation.Entry.Path, []byte(".")) {
			parent = []byte(path.Dir(string(mutation.Entry.Path)))
		}
		if completed {
			volume.Checkpoint = &agentpb.BackupVolumeCheckpoint_VolumeReplacedPathCleaned{
				VolumeReplacedPathCleaned: &agentpb.BackupVolumeReplacedPathCleaned{PointId: journal.pointID,
					RestoreGenerationId:  journal.generationID,
					OldFullTreeSha256:    append([]byte(nil), mutation.OldTreeSHA[:]...),
					NewFullTreeSha256:    append([]byte(nil), mutation.NewTreeSHA[:]...),
					DeletionCursorBefore: mutation.Ordinal - 1, DeletionOrdinal: mutation.Ordinal,
					LogicalPath: append([]byte(nil), mutation.Entry.Path...),
					Kind:        agentpb.BackupVolumeEntryKind(mutation.Entry.Kind), ParentLogicalPath: parent}}
		} else {
			volume.Checkpoint = &agentpb.BackupVolumeCheckpoint_ReplacedPathDeleteIntent{
				ReplacedPathDeleteIntent: &agentpb.BackupVolumeDeleteIntent{PointId: journal.pointID,
					RestoreGenerationId:  journal.generationID,
					OldFullTreeSha256:    append([]byte(nil), mutation.OldTreeSHA[:]...),
					NewFullTreeSha256:    append([]byte(nil), mutation.NewTreeSHA[:]...),
					DeletionCursorBefore: mutation.Ordinal - 1, DeletionOrdinal: mutation.Ordinal,
					LogicalPath: append([]byte(nil), mutation.Entry.Path...),
					Kind:        agentpb.BackupVolumeEntryKind(mutation.Entry.Kind), ParentLogicalPath: parent}}
		}
	default:
		return nil, errs.New(errs.KindInternal, "Volume Restore mutation kind is unsupported")
	}
	return &agentpb.BackupCheckpointRequest{Checkpoint: &agentpb.BackupCheckpointRequest_Volume{Volume: volume}}, nil
}
