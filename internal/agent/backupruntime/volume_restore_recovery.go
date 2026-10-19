package backupruntime

import (
	"bytes"
	"context"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/infra/agentvolumejournal"
	"github.com/AlanD20/groundplane/internal/infra/backupvolumefs"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// An existing private journal is selected only by its sealed, exact
// assignment identity. A new journal may be created only before the first
// filesystem mutation intent; otherwise losing local state is fail-closed.
func findVolumeRestoreJournal(ctx context.Context, expected agentvolumejournal.Config,
	resume *agentpb.BackupRestoreResume,
) (agentvolumejournal.Config, bool, error) {
	row, found, err := agentvolumejournal.Inspect(ctx, agentvolumejournal.AgentRoot,
		expected.RestoreGenerationID)
	if err != nil {
		return expected, false, err
	}
	if found {
		if row.Unbound || row.Retiring || row.State.UncommittedPrefix ||
			row.Config.JournalRoot != expected.JournalRoot || row.Config.Binding != expected.Binding ||
			row.Config.PointID != expected.PointID ||
			row.Config.RestoreGenerationID != expected.RestoreGenerationID ||
			row.Config.NewManifestSHA256 != expected.NewManifestSHA256 ||
			row.Config.NewFullTreeSHA256 != expected.NewFullTreeSHA256 ||
			row.Config.NewEntryCount != expected.NewEntryCount ||
			row.Config.VolumeRoot != expected.VolumeRoot ||
			row.Config.AuthorizedVolumeDir != expected.AuthorizedVolumeDir ||
			row.Config.ComposeKey != expected.ComposeKey ||
			row.Config.SiblingName != expected.SiblingName {
			return expected, false, errs.New(
				errs.KindStateConflict,
				"Volume Restore journal differs from its acknowledged execution",
			)
		}
		expected = row.Config
	}
	if !found {
		progress := resume.GetVolumeProgress()
		if progress != nil && (progress.ConstructionCursor != 0 || progress.PendingConstruction != nil ||
			progress.FinalizationCursor != 0 || progress.PendingFinalization != nil ||
			progress.PendingExchange != nil || len(progress.OldFullTreeSha256) != 0 ||
			progress.DeletionCursor != 0 || progress.PendingDelete != nil) {
			return expected, false, errs.New(
				errs.KindStateConflict,
				"Volume Restore journal differs from its acknowledged execution",
			)
		}
	}
	return expected, found, nil
}

func captureVolumeJournalBaseline(ctx context.Context, volume *backupvolumefs.Volume,
	config agentvolumejournal.Config,
) (agentvolumejournal.Config, backupvolumefs.Tree, error) {
	tree, err := volume.Snapshot(ctx)
	if err != nil {
		return config, backupvolumefs.Tree{}, err
	}
	manifest, err := backupvolumetransfer.EncodeEntries(tree.Entries)
	if err != nil {
		return config, backupvolumefs.Tree{}, err
	}
	config.OldManifestSHA256, err = backupvolume.ContentManifestSHA256(manifest)
	if err != nil {
		return config, backupvolumefs.Tree{}, err
	}
	config.OldFullTreeSHA256, config.OldEntryCount = tree.FullTreeSHA256, uint64(len(tree.Entries))
	return config, tree, nil
}

// Rebuild the original old-tree proof from the surviving sibling and durable
// deletion receipts. A completed deletion still visible in the sibling is a
// contradiction; a pending deletion may be present or absent.
func reconstructVolumeOldTree(observed *backupvolumefs.Tree,
	state agentvolumejournal.State, config agentvolumejournal.Config,
) (backupvolumefs.Tree, error) {
	entries := make(map[string]backupvolume.Entry, config.OldEntryCount)
	if observed != nil {
		for _, entry := range observed.Entries {
			entries[string(entry.Path)] = entry
		}
	}
	for _, mutation := range state.Completed {
		if mutation.Kind != backupvolumefs.MutationDelete {
			continue
		}
		key := string(mutation.Entry.Path)
		if _, present := entries[key]; present {
			return backupvolumefs.Tree{}, errs.New(
				errs.KindStateConflict,
				"Volume Restore journal differs from its acknowledged execution",
			)
		}
		entries[key] = mutation.Entry
	}
	if pending := state.Pending; pending != nil && pending.Kind == backupvolumefs.MutationDelete {
		key := string(pending.Entry.Path)
		if present, exists := entries[key]; exists {
			if !sameVolumeRestoreEntry(present, pending.Entry) {
				return backupvolumefs.Tree{}, errs.New(
					errs.KindStateConflict,
					"Volume Restore journal differs from its acknowledged execution",
				)
			}
		} else {
			entries[key] = pending.Entry
		}
	}
	if uint64(len(entries)) != config.OldEntryCount {
		return backupvolumefs.Tree{}, errs.New(
			errs.KindStateConflict,
			"Volume Restore journal differs from its acknowledged execution",
		)
	}
	ordered := make([]backupvolume.Entry, 0, len(entries))
	for _, entry := range entries {
		ordered = append(ordered, entry)
	}
	sort.Slice(ordered, func(i, j int) bool { return bytes.Compare(ordered[i].Path, ordered[j].Path) < 0 })
	tree, err := backupvolumefs.TreeFromEntries(ordered)
	if err != nil || tree.FullTreeSHA256 != config.OldFullTreeSHA256 {
		return backupvolumefs.Tree{}, errs.New(
			errs.KindStateConflict,
			"Volume Restore journal differs from its acknowledged execution",
		)
	}
	manifestEntries, err := backupvolumetransfer.EncodeEntries(ordered)
	if err != nil {
		return backupvolumefs.Tree{}, err
	}
	manifest, err := backupvolume.ContentManifestSHA256(manifestEntries)
	if err != nil || manifest != config.OldManifestSHA256 {
		return backupvolumefs.Tree{}, errs.New(
			errs.KindStateConflict,
			"Volume Restore journal differs from its acknowledged execution",
		)
	}
	return tree, nil
}

func sameVolumeRestoreEntry(left, right backupvolume.Entry) bool {
	return bytes.Equal(left.Path, right.Path) && left.Kind == right.Kind && left.Mode == right.Mode &&
		left.UID == right.UID && left.GID == right.GID && left.SizeBytes == right.SizeBytes &&
		left.ContentSHA256 == right.ContentSHA256
}
