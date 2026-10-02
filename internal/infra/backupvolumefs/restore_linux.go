//go:build linux

package backupvolumefs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path"
	"sort"

	"golang.org/x/sys/unix"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
)

type MutationKind string

const (
	MutationConstruction MutationKind = "construction"
	MutationFinalization MutationKind = "finalization"
	MutationExchange     MutationKind = "exchange"
	MutationDelete       MutationKind = "delete"
)

// Mutation is the exact intended effect. The Agent maps it to the accepted
// typed checkpoint and its private write-ahead journal; calls must be durable.
type Mutation struct {
	Kind       MutationKind
	Ordinal    uint64
	Entry      backupvolume.Entry
	OldTreeSHA [sha256.Size]byte
	NewTreeSHA [sha256.Size]byte
	OldInode   uint64
	NewInode   uint64
}

type Journal interface {
	Intent(context.Context, Mutation) error
	Completed(context.Context, Mutation) error
}

// Replacement is a hidden sibling of the live tree. Its name must be sealed in
// the assignment, and must be unique within the authorized Environment.
type Replacement struct {
	volume    *Volume
	name      string
	fd        int
	oldFD     int
	oldIno    uint64
	ino       uint64
	exchanged bool
}

// CreateReplacement journals the root construction before creating the hidden
// sibling. After a lost acknowledgement, ResumeCreatedReplacement may inspect
// and complete only that exact pending root intent.
func (volume *Volume) CreateReplacement(ctx context.Context, sibling string, root backupvolume.Entry,
	journal Journal,
) (*Replacement, error) {
	if err := validateReplacementRequest(ctx, volume, sibling, root, journal); err != nil {
		return nil, err
	}
	if err := volume.liveIdentity(); err != nil {
		return nil, err
	}
	mutation := Mutation{Kind: MutationConstruction, Ordinal: 1, Entry: cloneEntry(root)}
	if err := journal.Intent(ctx, mutation); err != nil {
		return nil, err
	}
	if err := unix.Mkdirat(volume.parentFD, sibling, 0o700); err != nil {
		return nil, system("create replacement Volume sibling", err)
	}
	if err := unix.Fsync(volume.parentFD); err != nil {
		return nil, system("sync replacement Volume parent", err)
	}
	replacement, err := volume.openReplacement(sibling, 0)
	if err != nil {
		return nil, err
	}
	mutation.NewInode = replacement.ino
	if err := journal.Completed(ctx, mutation); err != nil {
		_ = replacement.Close()
		return nil, err
	}
	return replacement, nil
}

func (volume *Volume) ResumeCreatedReplacement(ctx context.Context, sibling string, root backupvolume.Entry,
	journal Journal,
) (*Replacement, error) {
	if err := validateReplacementRequest(ctx, volume, sibling, root, journal); err != nil {
		return nil, err
	}
	var existing unix.Stat_t
	err := unix.Fstatat(volume.parentFD, sibling, &existing, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		if err := unix.Mkdirat(volume.parentFD, sibling, 0o700); err != nil {
			return nil, system("resume replacement Volume root creation", err)
		}
		if err := unix.Fsync(volume.parentFD); err != nil {
			return nil, system("sync replacement Volume parent", err)
		}
	} else if err != nil {
		return nil, system("inspect pending replacement Volume root", err)
	}
	replacement, err := volume.openReplacement(sibling, 0)
	if err != nil {
		return nil, err
	}
	if err := replacement.empty(); err != nil {
		_ = replacement.Close()
		return nil, err
	}
	if err := unix.Fsync(volume.parentFD); err != nil {
		_ = replacement.Close()
		return nil, system("sync pending replacement Volume parent", err)
	}
	mut := Mutation{Kind: MutationConstruction, Ordinal: 1, Entry: cloneEntry(root)}
	mut.NewInode = replacement.ino
	if err := journal.Completed(ctx, mut); err != nil {
		_ = replacement.Close()
		return nil, err
	}
	return replacement, nil
}

// OpenReplacement resumes from a durable completed root-construction record.
// The exact inode is supplied by the private Agent journal, not inferred from a
// name that could have been reused.
func (volume *Volume) OpenReplacement(ctx context.Context, sibling string, rootInode uint64) (*Replacement, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validSibling(sibling, volume.name) || rootInode == 0 {
		return nil, invalid("replacement Volume identity is invalid")
	}
	return volume.openReplacement(sibling, rootInode)
}

func validateReplacementRequest(ctx context.Context, volume *Volume, sibling string,
	root backupvolume.Entry, journal Journal,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if journal == nil || !validSibling(sibling, volume.name) ||
		root.Kind != backupvolume.EntryDirectory || !bytes.Equal(root.Path, []byte(".")) {
		return invalid("replacement Volume root authority is invalid")
	}
	return backupvolume.ValidateEntries([]backupvolume.Entry{root})
}

func validSibling(sibling, live string) bool {
	return sibling != live && len(sibling) > len(".gp-restore-") &&
		len(sibling) <= 255 && len(sibling) >= 12 && sibling[:12] == ".gp-restore-" && validName(sibling)
}

func (volume *Volume) openReplacement(sibling string, expectedInode uint64) (*Replacement, error) {
	fd, err := openDirectory(volume.parentFD, sibling)
	if err != nil {
		return nil, system("open replacement Volume sibling", err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		_ = unix.Close(fd)
		return nil, system("inspect replacement Volume sibling", err)
	}
	if uint64(stat.Dev) != volume.dev || stat.Mode&unix.S_IFMT != unix.S_IFDIR ||
		(expectedInode == 0 && (stat.Mode&0o7777 != 0o700 || stat.Uid != 0)) ||
		(expectedInode != 0 && stat.Ino != expectedInode) {
		_ = unix.Close(fd)
		return nil, invalid("replacement Volume sibling identity is invalid")
	}
	return &Replacement{volume: volume, name: sibling, fd: fd, oldFD: -1,
		oldIno: volume.ino, ino: stat.Ino}, nil
}

func (replacement *Replacement) Close() error {
	if replacement == nil || replacement.volume == nil || replacement.fd < 0 {
		return nil
	}
	err := unix.Close(replacement.fd)
	replacement.fd = -1
	if replacement.oldFD >= 0 {
		oldErr := unix.Close(replacement.oldFD)
		replacement.oldFD = -1
		if err == nil {
			err = oldErr
		}
	}
	if err != nil {
		return system("close replacement Volume", err)
	}
	return nil
}

func (replacement *Replacement) empty() error {
	dup, err := unix.Dup(replacement.fd)
	if err != nil {
		return system("inspect pending replacement Volume root", err)
	}
	directory := os.NewFile(uintptr(dup), "pending-replacement-volume")
	names, readErr := directory.Readdirnames(-1)
	closeErr := directory.Close()
	if readErr != nil {
		return system("read pending replacement Volume root", readErr)
	}
	if closeErr != nil {
		return system("close pending replacement Volume root", closeErr)
	}
	if len(names) != 0 {
		return invalid("pending replacement Volume root has unexpected content")
	}
	return nil
}

// Construct journals one already validated archive entry. Callers supply the
// exact archive file stream for a regular entry, nil for a directory. A pending
// construction may be replayed only before exchange; a partial regular file is
// replaced in place and verified before its completion is recorded.
func (replacement *Replacement) Construct(ctx context.Context, ordinal uint64,
	entry backupvolume.Entry, content io.Reader, pending bool, journal Journal,
) error {
	if err := replacement.ready(ctx, journal); err != nil {
		return err
	}
	if ordinal < 2 || ordinal > backupvolume.MaxEntries || errEntry(entry) != nil ||
		bytes.Equal(entry.Path, []byte(".")) {
		return invalid("replacement Volume construction entry is invalid")
	}
	if (entry.Kind == backupvolume.EntryRegular) != (content != nil) {
		return invalid("replacement Volume content does not match entry kind")
	}
	mut := Mutation{Kind: MutationConstruction, Ordinal: ordinal, Entry: cloneEntry(entry)}
	if !pending {
		if err := journal.Intent(ctx, mut); err != nil {
			return err
		}
	}
	parent, leaf := parentAndName(string(entry.Path))
	parentFD, err := openDirectory(replacement.fd, parent)
	if err != nil {
		return system("open replacement Volume entry parent", err)
	}
	defer unix.Close(parentFD)
	if entry.Kind == backupvolume.EntryDirectory {
		err = constructDirectory(parentFD, leaf, pending)
	} else {
		err = constructFile(ctx, parentFD, leaf, entry, content, pending, replacement.volume.dev)
	}
	if err != nil {
		return err
	}
	if err := unix.Fsync(parentFD); err != nil {
		return system("sync replacement Volume entry parent", err)
	}
	return journal.Completed(ctx, mut)
}

func constructDirectory(parentFD int, leaf string, pending bool) error {
	err := unix.Mkdirat(parentFD, leaf, 0o700)
	if err != nil && (!pending || !errors.Is(err, unix.EEXIST)) {
		return system("create replacement Volume directory", err)
	}
	fd, err := openDirectory(parentFD, leaf)
	if err != nil {
		return system("open replacement Volume directory", err)
	}
	defer unix.Close(fd)
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return system("inspect replacement Volume directory", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Mode&0o7777 != 0o700 || stat.Uid != 0 {
		return invalid("replacement Volume directory state is ambiguous")
	}
	if err := unix.Fsync(fd); err != nil {
		return system("sync replacement Volume directory", err)
	}
	return nil
}

func constructFile(ctx context.Context, parentFD int, leaf string, entry backupvolume.Entry,
	content io.Reader, pending bool, device uint64,
) error {
	flags := uint64(unix.O_WRONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK | unix.O_CREAT)
	if !pending {
		flags |= unix.O_EXCL
	} else {
		var named unix.Stat_t
		err := unix.Fstatat(parentFD, leaf, &named, unix.AT_SYMLINK_NOFOLLOW)
		if err != nil && !errors.Is(err, unix.ENOENT) {
			return system("inspect pending replacement Volume file", err)
		}
		if err == nil && (named.Mode&unix.S_IFMT != unix.S_IFREG ||
			uint64(named.Dev) != device || named.Nlink != 1) {
			return invalid("pending replacement Volume file kind changed")
		}
	}
	fd, err := unix.Openat2(parentFD, leaf, &unix.OpenHow{Flags: flags, Mode: 0o600, Resolve: confined})
	if err != nil {
		return system("create replacement Volume file", err)
	}
	defer unix.Close(fd)
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return system("inspect replacement Volume file", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || uint64(stat.Dev) != device || stat.Nlink != 1 {
		return invalid("replacement Volume file identity is invalid")
	}
	if pending {
		if err := unix.Ftruncate(fd, 0); err != nil {
			return system("restart pending replacement Volume file", err)
		}
	}
	digest := sha256.New()
	remaining := entry.SizeBytes
	buffer := make([]byte, 32*1024)
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		want := uint64(len(buffer))
		if want > remaining {
			want = remaining
		}
		count, err := io.ReadFull(content, buffer[:int(want)])
		if err != nil || count != int(want) {
			return invalid("replacement Volume file content is truncated")
		}
		_, _ = digest.Write(buffer[:count])
		for offset := 0; offset < count; {
			written, writeErr := unix.Write(fd, buffer[offset:count])
			if writeErr != nil {
				return system("write replacement Volume file", writeErr)
			}
			if written == 0 {
				return invalid("replacement Volume file write made no progress")
			}
			offset += written
		}
		remaining -= uint64(count)
	}
	var extra [1]byte
	count, readErr := content.Read(extra[:])
	if count != 0 || !errors.Is(readErr, io.EOF) {
		return invalid("replacement Volume file content has trailing bytes")
	}
	if !bytes.Equal(digest.Sum(nil), entry.ContentSHA256[:]) {
		return invalid("replacement Volume file content digest does not match")
	}
	if err := unix.Fsync(fd); err != nil {
		return system("sync replacement Volume file", err)
	}
	return nil
}

// Finalize applies ownership and mode only after all descendants have been
// constructed. The caller invokes entries in reverse depth order and journals
// every ordinal. Replaying a pending finalization is idempotent.
func (replacement *Replacement) Finalize(ctx context.Context, ordinal uint64,
	entry backupvolume.Entry, pending bool, journal Journal,
) error {
	if err := replacement.ready(ctx, journal); err != nil {
		return err
	}
	if ordinal == 0 || ordinal > backupvolume.MaxEntries || errEntry(entry) != nil {
		return invalid("replacement Volume finalization entry is invalid")
	}
	mut := Mutation{Kind: MutationFinalization, Ordinal: ordinal, Entry: cloneEntry(entry)}
	if !pending {
		if err := journal.Intent(ctx, mut); err != nil {
			return err
		}
	}
	flags := uint64(unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK)
	if entry.Kind == backupvolume.EntryDirectory {
		flags |= unix.O_DIRECTORY
	}
	fd, err := unix.Openat2(replacement.fd, string(entry.Path), &unix.OpenHow{Flags: flags, Resolve: confined})
	if err != nil {
		return system("open replacement Volume entry for finalization", err)
	}
	defer unix.Close(fd)
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return system("inspect replacement Volume finalization entry", err)
	}
	if uint64(stat.Dev) != replacement.volume.dev || !sameKind(stat.Mode, entry.Kind) ||
		(entry.Kind == backupvolume.EntryRegular && stat.Nlink != 1) {
		return invalid("replacement Volume finalization target changed")
	}
	if err := unix.Fchown(fd, int(entry.UID), int(entry.GID)); err != nil {
		return system("set replacement Volume ownership", err)
	}
	if err := unix.Fchmod(fd, entry.Mode); err != nil {
		return system("set replacement Volume mode", err)
	}
	if err := unix.Fsync(fd); err != nil {
		return system("sync finalized replacement Volume entry", err)
	}
	return journal.Completed(ctx, mut)
}

func (replacement *Replacement) Verify(ctx context.Context, expected []backupvolume.Entry) ([sha256.Size]byte, error) {
	if err := ctx.Err(); err != nil {
		return [sha256.Size]byte{}, err
	}
	if err := backupvolume.ValidateEntries(expected); err != nil {
		return [sha256.Size]byte{}, err
	}
	observed := make([]observedEntry, 0, len(expected))
	if err := walk(ctx, replacement.fd, replacement.volume.dev, ".", &observed); err != nil {
		return [sha256.Size]byte{}, err
	}
	sort.Slice(
		observed,
		func(i, j int) bool { return bytes.Compare(observed[i].entry.Path, observed[j].entry.Path) < 0 },
	)
	if len(observed) != len(expected) {
		return [sha256.Size]byte{}, invalid("replacement Volume entry count does not match archive")
	}
	for i := range expected {
		if !sameEntry(observed[i].entry, expected[i]) {
			return [sha256.Size]byte{}, invalid("replacement Volume tree does not match archive")
		}
	}
	return backupvolume.FullTreeSHA256(expected)
}

func (replacement *Replacement) ready(ctx context.Context, journal Journal) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if replacement == nil || replacement.fd < 0 || replacement.exchanged || journal == nil {
		return invalid("replacement Volume mutation authority is invalid")
	}
	return replacement.namedIdentity(replacement.ino)
}

func (replacement *Replacement) namedIdentity(ino uint64) error {
	var stat unix.Stat_t
	if err := unix.Fstatat(replacement.volume.parentFD, replacement.name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return system("inspect replacement Volume name", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || uint64(stat.Dev) != replacement.volume.dev || stat.Ino != ino {
		return invalid("replacement Volume name changed")
	}
	return nil
}

func errEntry(entry backupvolume.Entry) error {
	if len(entry.Path) == 0 || len(entry.Path) > backupvolume.MaxPathBytes ||
		entry.Path[0] == '/' || bytes.IndexByte(entry.Path, 0) >= 0 || entry.Mode > 0o7777 {
		return invalid("replacement Volume entry path or mode is invalid")
	}
	if string(entry.Path) != path.Clean(string(entry.Path)) || bytes.Contains(entry.Path, []byte("//")) ||
		bytes.Equal(entry.Path, []byte("..")) || bytes.HasPrefix(entry.Path, []byte("../")) {
		return invalid("replacement Volume entry path is unsafe")
	}
	if entry.Kind != backupvolume.EntryDirectory && entry.Kind != backupvolume.EntryRegular {
		return invalid("replacement Volume entry kind is invalid")
	}
	if entry.Kind == backupvolume.EntryDirectory &&
		(entry.SizeBytes != 0 || entry.ContentSHA256 != ([sha256.Size]byte{})) {
		return invalid("replacement Volume directory content is invalid")
	}
	return nil
}

func sameKind(mode uint32, kind backupvolume.EntryKind) bool {
	return kind == backupvolume.EntryDirectory && mode&unix.S_IFMT == unix.S_IFDIR ||
		kind == backupvolume.EntryRegular && mode&unix.S_IFMT == unix.S_IFREG
}

func sameEntry(a, b backupvolume.Entry) bool {
	return bytes.Equal(a.Path, b.Path) && a.Kind == b.Kind && a.Mode == b.Mode &&
		a.UID == b.UID && a.GID == b.GID && a.SizeBytes == b.SizeBytes && a.ContentSHA256 == b.ContentSHA256
}

func cloneEntry(entry backupvolume.Entry) backupvolume.Entry {
	entry.Path = append([]byte(nil), entry.Path...)
	return entry
}
