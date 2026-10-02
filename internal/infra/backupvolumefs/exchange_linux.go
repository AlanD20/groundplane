//go:build linux

package backupvolumefs

import (
	"bytes"
	"context"
	"errors"
	"sort"

	"golang.org/x/sys/unix"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
)

// Exchange resolves a pending rename by comparing both durable inode
// identities. If it already happened, only the pending intent may complete it.
// A different tree at either name is never overwritten.
func (replacement *Replacement) Exchange(ctx context.Context, old Tree,
	newEntries []backupvolume.Entry, pending bool, journal Journal,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if replacement == nil || replacement.fd < 0 || journal == nil ||
		(replacement.exchanged && !pending) || (replacement.exchanged && replacement.oldFD < 0) ||
		(!replacement.exchanged && replacement.oldFD >= 0) {
		return invalid("replacement Volume exchange authority is invalid")
	}
	if err := backupvolume.ValidateEntries(newEntries); err != nil {
		return err
	}
	newDigest, err := backupvolume.FullTreeSHA256(newEntries)
	if err != nil {
		return err
	}
	oldProof, err := TreeFromEntries(old.Entries)
	if err != nil || oldProof.FullTreeSHA256 != old.FullTreeSHA256 {
		return invalid("old managed Volume snapshot is invalid")
	}
	mut := Mutation{Kind: MutationExchange, OldTreeSHA: old.FullTreeSHA256, NewTreeSHA: newDigest,
		OldInode: replacement.oldIno, NewInode: replacement.ino}
	if replacement.exchanged {
		if err := replacement.verifyExchangedPlacement(); err != nil {
			return err
		}
		if err := unix.Fsync(replacement.volume.parentFD); err != nil {
			return system("sync exchanged managed Volume parent", err)
		}
		if err := replacement.verifyExchangeTrees(ctx, oldProof, newEntries); err != nil {
			return err
		}
		return journal.Completed(ctx, mut)
	}
	if !pending {
		if err := journal.Intent(ctx, mut); err != nil {
			return err
		}
	}
	live, sibling, err := replacement.placement()
	if err != nil {
		return err
	}
	switch {
	case live == replacement.oldIno && sibling == replacement.ino:
		if replacement.exchanged {
			return invalid("replacement Volume exchange state is contradictory")
		}
		current, err := replacement.volume.Snapshot(ctx)
		if err != nil {
			return err
		}
		if !sameTree(old, current) {
			return invalid("stopped live Volume changed before exchange")
		}
		if _, err := replacement.Verify(ctx, newEntries); err != nil {
			return err
		}
		if err := unix.Renameat2(replacement.volume.parentFD, replacement.volume.name,
			replacement.volume.parentFD, replacement.name, unix.RENAME_EXCHANGE); err != nil {
			return system("atomically exchange managed Volume trees", err)
		}
		if err := unix.Fsync(replacement.volume.parentFD); err != nil {
			return system("sync exchanged managed Volume parent", err)
		}
	case live == replacement.ino && sibling == replacement.oldIno:
		if !pending {
			return invalid("managed Volume exchange happened without a pending intent")
		}
		if err := unix.Fsync(replacement.volume.parentFD); err != nil {
			return system("sync exchanged managed Volume parent", err)
		}
	default:
		return invalid("managed Volume exchange placement is ambiguous")
	}
	replacement.oldFD, err = unix.Dup(replacement.volume.liveFD)
	if err != nil {
		return system("retain replaced managed Volume descriptor", err)
	}
	replacement.exchanged = true
	if err := replacement.verifyExchangedPlacement(); err != nil {
		return err
	}
	if err := replacement.verifyExchangeTrees(ctx, oldProof, newEntries); err != nil {
		return err
	}
	return journal.Completed(ctx, mut)
}

func (replacement *Replacement) verifyExchangeTrees(ctx context.Context, old Tree,
	newEntries []backupvolume.Entry,
) error {
	if _, err := replacement.Verify(ctx, newEntries); err != nil {
		return err
	}
	oldAfter, err := snapshotAt(ctx, replacement.oldFD, replacement.volume.dev)
	if err != nil {
		return err
	}
	if !sameTree(old, oldAfter) {
		return invalid("replaced Volume changed during exchange")
	}
	return nil
}

// OpenExchanged resumes a durable exchange or a pending exchange whose rename
// already happened. oldInode and newInode come from the sealed private journal.
func (volume *Volume) OpenExchanged(ctx context.Context, sibling string,
	oldInode, newInode uint64,
) (*Replacement, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validSibling(sibling, volume.name) || oldInode == 0 || newInode == 0 || oldInode == newInode ||
		volume.ino != newInode {
		return nil, invalid("exchanged managed Volume identity is invalid")
	}
	oldFD, err := openDirectory(volume.parentFD, sibling)
	if err != nil {
		return nil, system("open replaced managed Volume sibling", err)
	}
	newFD, err := unix.Dup(volume.liveFD)
	if err != nil {
		_ = unix.Close(oldFD)
		return nil, system("retain restored managed Volume descriptor", err)
	}
	replacement := &Replacement{volume: volume, name: sibling, fd: newFD, oldFD: oldFD,
		ino: newInode, oldIno: oldInode, exchanged: true}
	if err := replacement.verifyExchangedPlacementWithOld(oldInode); err != nil {
		_ = replacement.Close()
		return nil, err
	}
	return replacement, nil
}

func (replacement *Replacement) placement() (uint64, uint64, error) {
	var live, sibling unix.Stat_t
	if err := unix.Fstatat(
		replacement.volume.parentFD,
		replacement.volume.name,
		&live,
		unix.AT_SYMLINK_NOFOLLOW,
	); err != nil {
		return 0, 0, system("inspect live Volume exchange name", err)
	}
	if err := unix.Fstatat(
		replacement.volume.parentFD,
		replacement.name,
		&sibling,
		unix.AT_SYMLINK_NOFOLLOW,
	); err != nil {
		return 0, 0, system("inspect replacement Volume exchange name", err)
	}
	if live.Mode&unix.S_IFMT != unix.S_IFDIR || sibling.Mode&unix.S_IFMT != unix.S_IFDIR ||
		uint64(live.Dev) != replacement.volume.dev || uint64(sibling.Dev) != replacement.volume.dev {
		return 0, 0, invalid("managed Volume exchange path crossed a mount or changed kind")
	}
	return live.Ino, sibling.Ino, nil
}

func (replacement *Replacement) verifyExchangedPlacement() error {
	return replacement.verifyExchangedPlacementWithOld(replacement.oldIno)
}

func (replacement *Replacement) verifyExchangedPlacementWithOld(oldInode uint64) error {
	live, sibling, err := replacement.placement()
	if err != nil {
		return err
	}
	if live != replacement.ino || sibling != oldInode {
		return invalid("exchanged managed Volume placement changed")
	}
	var old unix.Stat_t
	if err := unix.Fstat(replacement.oldFD, &old); err != nil {
		return system("inspect retained old Volume", err)
	}
	if old.Ino != oldInode || uint64(old.Dev) != replacement.volume.dev {
		return invalid("retained old Volume identity changed")
	}
	return nil
}

// SnapshotOld observes the retained sibling through its pinned descriptor.
// It returns the current remaining tree: after any deletion it is not the
// original full-tree manifest. Recovery must merge the journal's completed and
// pending deletions with these entries, then compare the reconstructed original
// tree digest to the sealed old-tree evidence before advancing deletion.
func (replacement *Replacement) SnapshotOld(ctx context.Context) (Tree, error) {
	if ctx == nil || replacement == nil || replacement.volume == nil ||
		!replacement.exchanged || replacement.fd < 0 || replacement.oldFD < 0 {
		return Tree{}, invalid("replaced Volume snapshot authority is invalid")
	}
	if err := ctx.Err(); err != nil {
		return Tree{}, err
	}
	if err := replacement.verifyExchangedPlacement(); err != nil {
		return Tree{}, err
	}
	before, err := snapshotAt(ctx, replacement.oldFD, replacement.volume.dev)
	if err != nil {
		return Tree{}, err
	}
	after, err := snapshotAt(ctx, replacement.oldFD, replacement.volume.dev)
	if err != nil {
		return Tree{}, err
	}
	if !sameTree(before, after) {
		return Tree{}, invalid("replaced Volume changed during snapshot")
	}
	if err := replacement.verifyExchangedPlacement(); err != nil {
		return Tree{}, err
	}
	return before, nil
}

// DeletionOrder is children before parents, with the root last. The caller
// retains this exact order and cursor in the private journal/checkpoint.
func DeletionOrder(tree Tree) ([]backupvolume.Entry, error) {
	if err := backupvolume.ValidateEntries(tree.Entries); err != nil {
		return nil, err
	}
	ordered := make([]backupvolume.Entry, len(tree.Entries))
	for i := range tree.Entries {
		ordered[i] = cloneEntry(tree.Entries[i])
	}
	sort.Slice(ordered, func(i, j int) bool {
		left, right := bytes.Count(ordered[i].Path, []byte{'/'}), bytes.Count(ordered[j].Path, []byte{'/'})
		if left != right {
			return left > right
		}
		return bytes.Compare(ordered[i].Path, ordered[j].Path) > 0
	})
	return ordered, nil
}

// FinalizationOrder applies ownership and mode to children before parents so
// restrictive directory modes never block construction of descendants.
func FinalizationOrder(entries []backupvolume.Entry) ([]backupvolume.Entry, error) {
	tree, err := TreeFromEntries(entries)
	if err != nil {
		return nil, err
	}
	return DeletionOrder(tree)
}

// DeleteOld removes exactly one path from the retained old tree. A missing
// path is accepted only for a replayed pending delete; the parent is synced
// before the completion callback. Call only after exchange is durable.
func (replacement *Replacement) DeleteOld(ctx context.Context, ordinal uint64,
	entry backupvolume.Entry, old, newSHA [32]byte, pending bool, journal Journal,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if replacement == nil || !replacement.exchanged || replacement.oldFD < 0 || journal == nil ||
		ordinal == 0 || errEntry(entry) != nil {
		return invalid("replaced Volume deletion authority is invalid")
	}
	if old == ([32]byte{}) || newSHA == ([32]byte{}) {
		return invalid("replaced Volume deletion tree identity is missing")
	}
	mut := Mutation{Kind: MutationDelete, Ordinal: ordinal, Entry: cloneEntry(entry),
		OldTreeSHA: old, NewTreeSHA: newSHA, OldInode: replacement.oldIno, NewInode: replacement.ino}
	if !pending {
		if err := journal.Intent(ctx, mut); err != nil {
			return err
		}
	}
	if bytes.Equal(entry.Path, []byte(".")) {
		if err := replacement.deleteOldRoot(pending); err != nil {
			return err
		}
		return journal.Completed(ctx, mut)
	}
	if err := replacement.verifyExchangedPlacement(); err != nil {
		return err
	}
	parent, leaf := parentAndName(string(entry.Path))
	parentFD, err := openDirectory(replacement.oldFD, parent)
	if err != nil {
		return system("open replaced Volume path parent", err)
	}
	defer unix.Close(parentFD)
	var stat unix.Stat_t
	err = unix.Fstatat(parentFD, leaf, &stat, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) && pending {
		if err := unix.Fsync(parentFD); err != nil {
			return system("sync replaced Volume parent", err)
		}
		return journal.Completed(ctx, mut)
	}
	if err != nil {
		return system("inspect replaced Volume path", err)
	}
	if uint64(stat.Dev) != replacement.volume.dev || !sameKind(stat.Mode, entry.Kind) ||
		stat.Mode&0o7777 != entry.Mode || stat.Uid != entry.UID || stat.Gid != entry.GID ||
		(entry.Kind == backupvolume.EntryRegular && uint64(stat.Size) != entry.SizeBytes) {
		return invalid("replaced Volume path changed before cleanup")
	}
	if entry.Kind == backupvolume.EntryDirectory {
		fd, err := openDirectory(parentFD, leaf)
		if err != nil {
			return system("open replaced Volume directory before cleanup", err)
		}
		var opened unix.Stat_t
		if err := unix.Fstat(fd, &opened); err != nil {
			_ = unix.Close(fd)
			return system("inspect replaced Volume directory before cleanup", err)
		}
		_ = unix.Close(fd)
		if opened.Ino != stat.Ino || uint64(opened.Dev) != replacement.volume.dev {
			return invalid("replaced Volume directory identity changed")
		}
	} else {
		fd, err := openRegular(parentFD, leaf)
		if err != nil {
			return system("open replaced Volume file before cleanup", err)
		}
		var opened unix.Stat_t
		if err := unix.Fstat(fd, &opened); err != nil {
			_ = unix.Close(fd)
			return system("inspect replaced Volume file before cleanup", err)
		}
		if opened.Ino != stat.Ino || uint64(opened.Dev) != replacement.volume.dev {
			_ = unix.Close(fd)
			return invalid("replaced Volume file identity changed")
		}
		observed := make([]observedEntry, 0, 1)
		err = appendRegular(ctx, fd, string(entry.Path), opened, &observed)
		_ = unix.Close(fd)
		if err != nil {
			return err
		}
		if !sameEntry(observed[0].entry, entry) {
			return invalid("replaced Volume file content changed before cleanup")
		}
	}
	var current unix.Stat_t
	if err := unix.Fstatat(parentFD, leaf, &current, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return system("reinspect replaced Volume path before cleanup", err)
	}
	if current.Ino != stat.Ino || uint64(current.Dev) != replacement.volume.dev || !sameKind(current.Mode, entry.Kind) {
		return invalid("replaced Volume path identity changed before cleanup")
	}
	flags := 0
	if entry.Kind == backupvolume.EntryDirectory {
		flags = unix.AT_REMOVEDIR
	}
	if err := unix.Unlinkat(parentFD, leaf, flags); err != nil {
		return system("remove replaced Volume path", err)
	}
	if err := unix.Fsync(parentFD); err != nil {
		return system("sync replaced Volume parent", err)
	}
	return journal.Completed(ctx, mut)
}

func (replacement *Replacement) deleteOldRoot(pending bool) error {
	var stat unix.Stat_t
	err := unix.Fstatat(replacement.volume.parentFD, replacement.name, &stat, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) && pending {
		if err := replacement.volume.liveHasInode(replacement.ino); err != nil {
			return err
		}
		if err := unix.Fsync(replacement.volume.parentFD); err != nil {
			return system("sync replaced Volume parent", err)
		}
		return nil
	}
	if err != nil {
		return system("inspect replaced Volume root", err)
	}
	live, sibling, err := replacement.placement()
	if err != nil {
		return err
	}
	if live != replacement.ino || sibling != replacement.oldIno {
		return invalid("replaced Volume root identity changed")
	}
	if err := unix.Unlinkat(replacement.volume.parentFD, replacement.name, unix.AT_REMOVEDIR); err != nil {
		return system("remove replaced Volume root", err)
	}
	if err := unix.Fsync(replacement.volume.parentFD); err != nil {
		return system("sync replaced Volume parent", err)
	}
	return nil
}

// ResumeRootDeletion completes only an already journaled root deletion whose
// sibling is absent. It is usable after restart, when no old descriptor can be
// reopened. It never initiates a new deletion.
func (volume *Volume) ResumeRootDeletion(ctx context.Context, sibling string,
	oldInode, newInode uint64, ordinal uint64, old, newSHA [32]byte,
	root backupvolume.Entry, journal Journal,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if journal == nil || !validSibling(sibling, volume.name) || oldInode == 0 ||
		newInode == 0 || oldInode == newInode || ordinal == 0 ||
		old == ([32]byte{}) || newSHA == ([32]byte{}) ||
		root.Kind != backupvolume.EntryDirectory || !bytes.Equal(root.Path, []byte(".")) {
		return invalid("replaced Volume root deletion receipt is invalid")
	}
	if err := volume.liveHasInode(newInode); err != nil {
		return err
	}
	var stat unix.Stat_t
	err := unix.Fstatat(volume.parentFD, sibling, &stat, unix.AT_SYMLINK_NOFOLLOW)
	if !errors.Is(err, unix.ENOENT) {
		if err != nil {
			return system("inspect deleted Volume sibling", err)
		}
		return invalid("replaced Volume root still exists")
	}
	if err := unix.Fsync(volume.parentFD); err != nil {
		return system("sync deleted Volume sibling parent", err)
	}
	return journal.Completed(ctx, Mutation{Kind: MutationDelete, Ordinal: ordinal,
		Entry: cloneEntry(root), OldTreeSHA: old, NewTreeSHA: newSHA,
		OldInode: oldInode, NewInode: newInode})
}

func (volume *Volume) liveHasInode(expected uint64) error {
	var stat unix.Stat_t
	if err := unix.Fstatat(volume.parentFD, volume.name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return system("inspect restored live Volume", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || uint64(stat.Dev) != volume.dev || stat.Ino != expected {
		return invalid("restored live Volume identity changed")
	}
	return nil
}
