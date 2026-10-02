//go:build linux

package backupvolumefs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"sort"

	"golang.org/x/sys/unix"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
)

type fingerprint struct {
	dev, ino, size, nlink uint64
	mode, uid, gid        uint32
	mtime, ctime          unix.Timespec
}

type observedEntry struct {
	entry backupvolume.Entry
	stamp fingerprint
}

// Tree is a complete, byte-sorted observation. Each Snapshot reopens every
// descendant through openat2 and hashes every regular file from its descriptor.
type Tree struct {
	Entries        []backupvolume.Entry
	FullTreeSHA256 [sha256.Size]byte
	observed       []observedEntry
}

// TreeFromEntries reconstructs authenticated manifest evidence after process
// restart. The caller must have verified the manifest and its sealed digest.
func TreeFromEntries(entries []backupvolume.Entry) (Tree, error) {
	if err := backupvolume.ValidateEntries(entries); err != nil {
		return Tree{}, err
	}
	copyEntries := make([]backupvolume.Entry, len(entries))
	for i := range entries {
		copyEntries[i] = entries[i]
		copyEntries[i].Path = append([]byte(nil), entries[i].Path...)
	}
	digest, err := backupvolume.FullTreeSHA256(copyEntries)
	if err != nil {
		return Tree{}, err
	}
	return Tree{Entries: copyEntries, FullTreeSHA256: digest}, nil
}

func (volume *Volume) Snapshot(ctx context.Context) (Tree, error) {
	if err := ctx.Err(); err != nil {
		return Tree{}, err
	}
	if err := volume.liveIdentity(); err != nil {
		return Tree{}, err
	}
	tree, err := snapshotAt(ctx, volume.liveFD, volume.dev)
	if err != nil {
		return Tree{}, err
	}
	if err := volume.liveIdentity(); err != nil {
		return Tree{}, err
	}
	return tree, nil
}

func snapshotAt(ctx context.Context, fd int, device uint64) (Tree, error) {
	observed := make([]observedEntry, 0, 32)
	if err := walk(ctx, fd, device, ".", &observed); err != nil {
		return Tree{}, err
	}
	sort.Slice(observed, func(i, j int) bool {
		return bytes.Compare(observed[i].entry.Path, observed[j].entry.Path) < 0
	})
	entries := make([]backupvolume.Entry, len(observed))
	for i := range observed {
		entries[i] = observed[i].entry
	}
	digest, err := backupvolume.FullTreeSHA256(entries)
	if err != nil {
		return Tree{}, err
	}
	return Tree{Entries: entries, FullTreeSHA256: digest, observed: observed}, nil
}

// Capture writes the canonical archive using schema-owner manifest bytes. A
// second complete observation must match path, metadata, identity, timestamps,
// and content before the result is usable. The destination remains untrusted
// until the caller receives a nil error and commits its prepared evidence.
func (volume *Volume) Capture(ctx context.Context, destination io.Writer,
	manifestFor func([]backupvolume.Entry) ([]backupvolume.ManifestEntryBytes, error),
) (Tree, []backupvolume.ManifestEntryBytes, backupvolume.ArtifactEvidence, error) {
	if destination == nil || manifestFor == nil {
		return Tree{}, nil, backupvolume.ArtifactEvidence{}, invalid(
			"volume capture destination or manifest encoder is missing",
		)
	}
	before, err := volume.Snapshot(ctx)
	if err != nil {
		return Tree{}, nil, backupvolume.ArtifactEvidence{}, err
	}
	manifestInput := make([]backupvolume.Entry, len(before.Entries))
	for i := range before.Entries {
		manifestInput[i] = cloneEntry(before.Entries[i])
	}
	manifest, err := manifestFor(manifestInput)
	if err != nil {
		return Tree{}, nil, backupvolume.ArtifactEvidence{}, err
	}
	if err := backupvolume.ValidateManifest(manifest, len(before.Entries)); err != nil {
		return Tree{}, nil, backupvolume.ArtifactEvidence{}, err
	}
	contents := make([]io.Reader, len(before.observed))
	readers := make([]*stableFile, 0, len(before.observed))
	for index, entry := range before.observed {
		if entry.entry.Kind != backupvolume.EntryRegular {
			continue
		}
		reader := &stableFile{ctx: ctx, rootFD: volume.liveFD, observed: entry}
		contents[index] = reader
		readers = append(readers, reader)
	}
	defer func() {
		for _, reader := range readers {
			_ = reader.close()
		}
	}()
	evidence, err := backupvolume.Write(ctx, destination, before.Entries, contents, manifest)
	if err != nil {
		return Tree{}, nil, backupvolume.ArtifactEvidence{}, err
	}
	after, err := volume.Snapshot(ctx)
	if err != nil {
		return Tree{}, nil, backupvolume.ArtifactEvidence{}, err
	}
	if !sameTree(before, after) {
		return Tree{}, nil, backupvolume.ArtifactEvidence{}, invalid("managed Volume changed during capture")
	}
	return before, manifest, evidence, nil
}

func sameTree(left, right Tree) bool {
	if len(left.Entries) != len(right.Entries) || left.FullTreeSHA256 != right.FullTreeSHA256 {
		return false
	}
	for i := range left.Entries {
		if !sameEntry(left.Entries[i], right.Entries[i]) {
			return false
		}
	}
	if len(left.observed) != 0 {
		if len(left.observed) != len(right.observed) {
			return false
		}
		for i := range left.observed {
			if left.observed[i].stamp != right.observed[i].stamp {
				return false
			}
		}
	}
	return true
}

func walk(ctx context.Context, fd int, device uint64, relative string, result *[]observedEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(*result) >= backupvolume.MaxEntries {
		return invalid("managed Volume exceeds its entry limit")
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return system("inspect managed Volume entry", err)
	}
	if uint64(stat.Dev) != device || stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return invalid("managed Volume contains a mount crossing or non-directory")
	}
	if err := supportedMetadata(fd); err != nil {
		return err
	}
	*result = append(*result, observedEntry{entry: backupvolume.Entry{
		Path: []byte(relative), Kind: backupvolume.EntryDirectory,
		Mode: stat.Mode & 0o7777, UID: stat.Uid, GID: stat.Gid,
	}, stamp: fingerprintOf(stat)})
	// Dup shares the directory offset with the pinned descriptor. A fresh
	// confined open is required because capture and recovery snapshot the same
	// tree more than once.
	duplicate, err := unix.Openat2(fd, ".", &unix.OpenHow{
		Flags: uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: confined,
	})
	if err != nil {
		return system("reopen managed Volume directory for snapshot", err)
	}
	directory := os.NewFile(uintptr(duplicate), "managed-volume-snapshot")
	names, readErr := directory.Readdirnames(backupvolume.MaxEntries + 1)
	closeErr := directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return system("read managed Volume directory", readErr)
	}
	if closeErr != nil {
		return system("close managed Volume directory stream", closeErr)
	}
	if len(names) > backupvolume.MaxEntries {
		return invalid("managed Volume directory exceeds its entry limit")
	}
	sort.Strings(names)
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return err
		}
		if name == "." || name == ".." || len(name) == 0 {
			return invalid("managed Volume contains an unsafe path")
		}
		child := name
		if relative != "." {
			child = relative + "/" + name
		}
		if len(child) > backupvolume.MaxPathBytes {
			return invalid("managed Volume path exceeds archive limit")
		}
		var named unix.Stat_t
		if err := unix.Fstatat(fd, name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return system("inspect managed Volume descendant name", err)
		}
		if named.Mode&unix.S_IFMT != unix.S_IFDIR && named.Mode&unix.S_IFMT != unix.S_IFREG {
			return invalid("managed Volume contains a link or special file")
		}
		entryFD, openErr := unix.Openat2(fd, name, &unix.OpenHow{
			Flags: uint64(unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK), Resolve: confined,
		})
		if openErr != nil {
			if errors.Is(openErr, unix.EXDEV) || errors.Is(openErr, unix.ELOOP) {
				return invalid("managed Volume contains a mount crossing or link")
			}
			return system("open managed Volume descendant", openErr)
		}
		var before unix.Stat_t
		if err := unix.Fstat(entryFD, &before); err != nil {
			_ = unix.Close(entryFD)
			return system("inspect managed Volume descendant", err)
		}
		if uint64(before.Dev) != device || before.Ino != named.Ino ||
			before.Mode&unix.S_IFMT != named.Mode&unix.S_IFMT {
			_ = unix.Close(entryFD)
			return invalid("managed Volume descendant identity or mount changed")
		}
		switch before.Mode & unix.S_IFMT {
		case unix.S_IFDIR:
			err = walk(ctx, entryFD, device, child, result)
		case unix.S_IFREG:
			err = appendRegular(ctx, entryFD, child, before, result)
		default:
			err = invalid("managed Volume contains a link or special file")
		}
		closeErr := unix.Close(entryFD)
		if err != nil {
			return err
		}
		if closeErr != nil {
			return system("close managed Volume descendant", closeErr)
		}
	}
	var after unix.Stat_t
	if err := unix.Fstat(fd, &after); err != nil {
		return system("reinspect managed Volume directory", err)
	}
	if fingerprintOf(stat) != fingerprintOf(after) {
		return invalid("managed Volume directory changed during capture")
	}
	return nil
}

func appendRegular(ctx context.Context, fd int, relative string, before unix.Stat_t, result *[]observedEntry) error {
	if len(*result) >= backupvolume.MaxEntries {
		return invalid("managed Volume exceeds its entry limit")
	}
	if before.Nlink != 1 {
		return invalid("managed Volume contains a hard-linked file")
	}
	if err := supportedMetadata(fd); err != nil {
		return err
	}
	digest := sha256.New()
	buffer := make([]byte, 32*1024)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		count, err := unix.Read(fd, buffer)
		if count > 0 {
			_, _ = digest.Write(buffer[:count])
		}
		if count == 0 && err == nil {
			break
		}
		if err != nil {
			return system("read managed Volume file", err)
		}
		if count == 0 {
			return invalid("managed Volume file read made no progress")
		}
	}
	var after unix.Stat_t
	if err := unix.Fstat(fd, &after); err != nil {
		return system("reinspect managed Volume file", err)
	}
	if fingerprintOf(before) != fingerprintOf(after) {
		return invalid("managed Volume file changed during capture")
	}
	var sum [sha256.Size]byte
	copy(sum[:], digest.Sum(nil))
	*result = append(*result, observedEntry{entry: backupvolume.Entry{
		Path: []byte(relative), Kind: backupvolume.EntryRegular, Mode: before.Mode & 0o7777,
		UID: before.Uid, GID: before.Gid, SizeBytes: uint64(before.Size), ContentSHA256: sum,
	}, stamp: fingerprintOf(before)})
	return nil
}

func supportedMetadata(fd int) error {
	var extended unix.Statx_t
	if err := unix.Statx(fd, "", unix.AT_EMPTY_PATH|unix.AT_SYMLINK_NOFOLLOW,
		unix.STATX_BASIC_STATS, &extended); err != nil {
		return system("inspect managed Volume inode attributes", err)
	}
	if extended.Attributes&extended.Attributes_mask != 0 {
		return invalid("managed Volume contains unsupported inode attributes")
	}
	length, err := unix.Flistxattr(fd, nil)
	if err != nil {
		return system("inspect managed Volume extended metadata", err)
	}
	if length != 0 {
		return invalid("managed Volume contains unsupported extended metadata")
	}
	return nil
}

func fingerprintOf(stat unix.Stat_t) fingerprint {
	return fingerprint{dev: uint64(stat.Dev), ino: stat.Ino, size: uint64(stat.Size), nlink: uint64(stat.Nlink),
		mode: stat.Mode, uid: stat.Uid, gid: stat.Gid, mtime: stat.Mtim, ctime: stat.Ctim}
}

func (volume *Volume) liveIdentity() error {
	var stat unix.Stat_t
	if err := unix.Fstatat(volume.parentFD, volume.name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return system("inspect live managed Volume name", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || uint64(stat.Dev) != volume.dev || stat.Ino != volume.ino {
		return invalid("live managed Volume identity changed")
	}
	return nil
}

type stableFile struct {
	ctx      context.Context
	rootFD   int
	observed observedEntry
	fd       int
	file     *os.File
}

func (reader *stableFile) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	if reader.file == nil {
		fd, err := openRegular(reader.rootFD, string(reader.observed.entry.Path))
		if err != nil {
			return 0, system("reopen managed Volume file", err)
		}
		var stat unix.Stat_t
		if err := unix.Fstat(fd, &stat); err != nil {
			_ = unix.Close(fd)
			return 0, system("reinspect managed Volume file", err)
		}
		if fingerprintOf(stat) != reader.observed.stamp || stat.Mode&unix.S_IFMT != unix.S_IFREG {
			_ = unix.Close(fd)
			return 0, invalid("managed Volume file changed before archive write")
		}
		reader.fd, reader.file = fd, os.NewFile(uintptr(fd), "managed-volume-archive")
	}
	count, err := reader.file.Read(buffer)
	if errors.Is(err, io.EOF) {
		var stat unix.Stat_t
		if statErr := unix.Fstat(reader.fd, &stat); statErr != nil {
			return count, system("reinspect archived Volume file", statErr)
		}
		if fingerprintOf(stat) != reader.observed.stamp {
			return count, invalid("managed Volume file changed during archive write")
		}
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return count, system("read archived Volume file", err)
	}
	return count, err
}

func (reader *stableFile) close() error {
	if reader.file == nil {
		return nil
	}
	err := reader.file.Close()
	reader.file = nil
	if err != nil {
		return system("close archived Volume file", err)
	}
	return nil
}
