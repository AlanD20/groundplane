package agent

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/infra/agentvolumejournal"
	"github.com/AlanD20/groundplane/internal/infra/backupvolumefs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// The local journal is fsynced before each Controller publication. On an
// ambiguous response, at most its last intent or completion can be ahead of
// the accepted native projection. Replay that exact receipt before touching
// the filesystem again; never synthesize a new ordinal from a live listing.
func reconcileVolumeRestoreJournal(ctx context.Context, state agentvolumejournal.State,
	journal *volumeRestoreCheckpointJournal, resume *agentpb.BackupRestoreResume,
) error {
	if state.RecordCount == 0 {
		return nil
	}
	progress := resume.GetVolumeProgress()
	if progress == nil {
		return invalidAgentStaging()
	}
	mutation := state.Pending
	completed := false
	if mutation == nil {
		if len(state.Completed) == 0 {
			return invalidAgentStaging()
		}
		mutation = &state.Completed[len(state.Completed)-1]
		completed = true
	}
	checkpoint, err := journal.checkpoint(*mutation, completed)
	if err != nil {
		return err
	}
	local := checkpoint.GetVolume()
	acknowledged, missing := false, false
	switch mutation.Kind {
	case backupvolumefs.MutationConstruction:
		if completed {
			acknowledged = progress.ConstructionCursor == mutation.Ordinal
			missing = progress.ConstructionCursor+1 == mutation.Ordinal &&
				proto.Equal(progress.PendingConstruction, local.GetConstructionCompleted().GetIntent())
		} else {
			acknowledged = proto.Equal(progress.PendingConstruction, local.GetConstructionIntent())
			missing = progress.ConstructionCursor+1 == mutation.Ordinal && progress.PendingConstruction == nil
		}
	case backupvolumefs.MutationFinalization:
		if completed {
			acknowledged = progress.FinalizationCursor == mutation.Ordinal
			missing = progress.FinalizationCursor+1 == mutation.Ordinal &&
				proto.Equal(progress.PendingFinalization, local.GetFinalizationCompleted().GetIntent())
		} else {
			acknowledged = proto.Equal(progress.PendingFinalization, local.GetFinalizationIntent())
			missing = progress.FinalizationCursor+1 == mutation.Ordinal && progress.PendingFinalization == nil
		}
	case backupvolumefs.MutationExchange:
		if completed {
			acknowledged = progress.PendingExchange == nil &&
				bytes.Equal(progress.OldFullTreeSha256, mutation.OldTreeSHA[:]) &&
				bytes.Equal(progress.NewFullTreeSha256, mutation.NewTreeSHA[:])
			missing = progress.PendingExchange != nil &&
				bytes.Equal(progress.PendingExchange.OldFullTreeSha256, mutation.OldTreeSHA[:]) &&
				bytes.Equal(progress.PendingExchange.NewFullTreeSha256, mutation.NewTreeSHA[:])
		} else {
			acknowledged = proto.Equal(progress.PendingExchange, local.GetExchangeIntent())
			missing = progress.PendingExchange == nil && len(progress.OldFullTreeSha256) == 0
		}
	case backupvolumefs.MutationDelete:
		if completed {
			acknowledged = progress.DeletionCursor == mutation.Ordinal
			missing = progress.DeletionCursor+1 == mutation.Ordinal &&
				progress.PendingDelete != nil &&
				bytes.Equal(progress.PendingDelete.LogicalPath, mutation.Entry.Path)
		} else {
			acknowledged = progress.PendingDelete != nil &&
				bytes.Equal(progress.PendingDelete.LogicalPath, mutation.Entry.Path) &&
				progress.PendingDelete.DeletionOrdinal == mutation.Ordinal
			missing = progress.DeletionCursor+1 == mutation.Ordinal && progress.PendingDelete == nil
		}
	default:
		return invalidAgentStaging()
	}
	if acknowledged {
		return nil
	}
	if !missing {
		return invalidAgentStaging()
	}
	return journal.publisher.publish(ctx, checkpoint)
}
