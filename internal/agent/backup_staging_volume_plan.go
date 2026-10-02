package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/agentvolumejournal"
	"github.com/AlanD20/groundplane/internal/infra/agentvolumemanifest"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (state *backupStagingState) applyVolumeRecoveryPlan(ctx context.Context,
	plan *agentpb.BackupStagingRecoveryPlan,
) error {
	for _, disposition := range plan.VolumeDispositions {
		entry, exists := state.volumes[string(disposition.RecoveryKeySha256)]
		switch {
		case disposition.GetResume() != nil:
			if !exists || entry.wire.JournalPartial || entry.wire.JournalRetiring ||
				entry.wire.ReceiverRetiring {
				return invalidAgentStaging()
			}
		case disposition.GetHold() != nil:
			// A terminal native RecoveryRequired receipt retains the private
			// bytes without granting execution or deletion.
			if !exists {
				return invalidAgentStaging()
			}
		case disposition.GetCleanup() != nil:
			if !exists {
				continue // Exact previously journaled cleanup is idempotent.
			}
			if err := cleanupRecoveredVolumeRestore(ctx, entry); err != nil {
				return err
			}
			delete(state.volumes, string(disposition.RecoveryKeySha256))
		default:
			return invalidAgentStaging()
		}
	}
	return nil
}

func cleanupRecoveredVolumeRestore(ctx context.Context, entry recoveredVolumeRestore) error {
	if row := entry.journal; row != nil {
		if row.Retiring {
			if row.Discarding {
				if err := agentvolumejournal.ResumeDiscard(ctx, row.Config); err != nil {
					return err
				}
			} else if err := agentvolumejournal.ResumeCleanup(ctx, row.Config); err != nil {
				return err
			}
		} else if row.State.RootDeleted {
			journal, err := agentvolumejournal.Open(ctx, row.Config)
			if err != nil {
				return err
			}
			cleanupErr := journal.Cleanup(ctx)
			closeErr := journal.Close()
			if cleanupErr != nil {
				return cleanupErr
			}
			if closeErr != nil {
				return closeErr
			}
		} else if err := agentvolumejournal.Discard(ctx, row.Config); err != nil {
			return err
		}
	}
	if row := entry.receiver; row != nil {
		if row.Retiring {
			if row.Discarding {
				if err := agentvolumemanifest.ResumeDiscard(ctx, row.Config); err != nil {
					return err
				}
			} else if err := agentvolumemanifest.ResumeCleanup(ctx, row.Config); err != nil {
				return err
			}
		} else if row.Complete && !row.Partial {
			journal, err := agentvolumemanifest.Open(ctx, row.Config)
			if err != nil {
				return err
			}
			cleanupErr := journal.Cleanup(ctx)
			closeErr := journal.Close()
			if cleanupErr != nil {
				return cleanupErr
			}
			if closeErr != nil {
				return closeErr
			}
		} else if err := agentvolumemanifest.Discard(ctx, row.Config); err != nil {
			return err
		}
	}
	return nil
}
