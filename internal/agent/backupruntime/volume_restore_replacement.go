package backupruntime

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/infra/agentvolumejournal"
	"github.com/AlanD20/groundplane/internal/infra/backupvolumefs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// resumeVolumeReplacement executes the captured filesystem journal. Forward and
// recovery owners supply their own checkpoint delivery and execution authority.
func resumeVolumeReplacement(ctx context.Context, volume *backupvolumefs.Volume,
	journalConfig agentvolumejournal.Config, journal *agentvolumejournal.Journal,
	state agentvolumejournal.State, existingJournal bool, oldTree backupvolumefs.Tree,
	newEntries []backupvolume.Entry, artifact *volumeRestoreArtifact,
	mutationJournal *volumeRestoreCheckpointJournal,
) (resultErr error) {
	var err error
	if existingJournal && !state.Exchanged &&
		(state.Pending == nil || state.Pending.Kind != backupvolumefs.MutationExchange) {
		oldTree, err = volume.Snapshot(ctx)
		if err != nil || oldTree.FullTreeSHA256 != journalConfig.OldFullTreeSHA256 {
			return errs.New(errs.KindStateConflict, "Volume Restore filesystem differs from its captured journal")
		}
	}
	var replacement *backupvolumefs.Replacement
	openedExchanged := false
	switch {
	case state.RootDeleted:
		oldTree, err = reconstructVolumeOldTree(nil, state, journalConfig)
	case state.Exchanged || state.Pending != nil && state.Pending.Kind == backupvolumefs.MutationExchange:
		replacement, err = volume.OpenExchanged(ctx, journalConfig.SiblingName,
			state.ExchangeOldInode, state.ExchangeNewInode)
		openedExchanged = err == nil
		if err != nil && state.Exchanged && state.Pending != nil &&
			state.Pending.Kind == backupvolumefs.MutationDelete &&
			bytes.Equal(state.Pending.Entry.Path, []byte(".")) {
			oldTree, err = reconstructVolumeOldTree(nil, state, journalConfig)
			if err == nil {
				err = volume.ResumeRootDeletion(ctx, journalConfig.SiblingName,
					state.ExchangeOldInode, state.ExchangeNewInode, state.Pending.Ordinal,
					oldTree.FullTreeSHA256, journalConfig.NewFullTreeSHA256,
					oldTree.Entries[0], mutationJournal)
			}
			if err == nil {
				state, err = journal.ReadState(ctx)
			}
		}
		if err != nil && state.Pending != nil && state.Pending.Kind == backupvolumefs.MutationExchange {
			// An exchange intent can precede the physical rename. The exact
			// pre-exchange placement is checked by OpenReplacement/Exchange.
			replacement, err = volume.OpenReplacement(ctx, journalConfig.SiblingName, state.RootNewInode)
			openedExchanged = false
		}
		if err == nil && replacement != nil && openedExchanged {
			remaining, snapshotErr := replacement.SnapshotOld(ctx)
			if snapshotErr != nil {
				return snapshotErr
			}
			oldTree, err = reconstructVolumeOldTree(&remaining, state, journalConfig)
		} else if err == nil && replacement != nil {
			oldTree, err = volume.Snapshot(ctx)
			if err == nil && oldTree.FullTreeSHA256 != journalConfig.OldFullTreeSHA256 {
				err = errs.New(errs.KindStateConflict, "Volume Restore filesystem differs from its captured journal")
			}
		}
	case state.ConstructionCursor == 0 && state.Pending == nil:
		replacement, err = volume.CreateReplacement(ctx, journalConfig.SiblingName, newEntries[0], mutationJournal)
	case state.ConstructionCursor == 0 && state.Pending.Kind == backupvolumefs.MutationConstruction && state.Pending.Ordinal == 1:
		replacement, err = volume.ResumeCreatedReplacement(
			ctx,
			journalConfig.SiblingName,
			newEntries[0],
			mutationJournal,
		)
	default:
		replacement, err = volume.OpenReplacement(ctx, journalConfig.SiblingName, state.RootNewInode)
	}
	if err != nil {
		return err
	}
	if replacement != nil {
		defer func() {
			if closeErr := replacement.Close(); closeErr != nil {
				resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
			}
		}()
	}
	state, err = journal.ReadState(ctx)
	if err != nil {
		return err
	}
	if !state.Exchanged && (state.Pending == nil || state.Pending.Kind != backupvolumefs.MutationExchange) {
		reader, err := artifact.source.OpenPrefix(ctx)
		if err != nil {
			return err
		}
		if err := replacement.ConstructArchive(ctx, reader, artifact.validated, artifact.manifest,
			max(uint64(2), state.ConstructionCursor+1),
			state.Pending != nil && state.Pending.Kind == backupvolumefs.MutationConstruction,
			mutationJournal); err != nil {
			_ = reader.Close()
			return err
		}
		if err := reader.Close(); err != nil {
			return err
		}
		state, err = journal.ReadState(ctx)
		if err != nil {
			return err
		}
		order, err := backupvolumefs.FinalizationOrder(newEntries)
		if err != nil {
			return err
		}
		for ordinal := state.FinalizationCursor + 1; ordinal <= uint64(len(order)); ordinal++ {
			if err := replacement.Finalize(ctx, ordinal, order[ordinal-1],
				state.Pending != nil && state.Pending.Kind == backupvolumefs.MutationFinalization &&
					state.Pending.Ordinal == ordinal, mutationJournal); err != nil {
				return err
			}
		}
	}
	verified := artifact.validated.Evidence.Archive.FullTreeSHA256
	if replacement != nil {
		actual, verifyErr := replacement.Verify(ctx, newEntries)
		if verifyErr != nil || actual != verified {
			return errs.New(errs.KindStateConflict, "Volume Restore filesystem differs from its captured journal")
		}
	}
	state, err = journal.ReadState(ctx)
	if err != nil {
		return err
	}
	if !state.Exchanged {
		if err := replacement.Exchange(ctx, oldTree, newEntries,
			state.Pending != nil && state.Pending.Kind == backupvolumefs.MutationExchange, mutationJournal); err != nil {
			return err
		}
	}
	state, err = journal.ReadState(ctx)
	if err != nil {
		return err
	}
	deletions, err := backupvolumefs.DeletionOrder(oldTree)
	if err != nil {
		return err
	}
	for ordinal := state.DeletionCursor + 1; ordinal <= uint64(len(deletions)); ordinal++ {
		if replacement == nil {
			return errs.New(errs.KindStateConflict, "Volume Restore filesystem differs from its captured journal")
		}
		if err := replacement.DeleteOld(ctx, ordinal, deletions[ordinal-1], oldTree.FullTreeSHA256,
			verified, state.Pending != nil && state.Pending.Kind == backupvolumefs.MutationDelete &&
				state.Pending.Ordinal == ordinal, mutationJournal); err != nil {
			return err
		}
	}
	return nil
}
