package postgres16helper

import (
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	grounderrs "github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// inspectReleaseFile measures the opened inode, rather than a path that may be
// substituted between metadata inspection and execution. The descriptor must
// stay open until the supervisor has passed it to the private gate.
func inspectReleaseFile(path string) (*os.File, postgres16protocol.ConfinementFileIdentity, error) {
	if !strings.HasPrefix(path, "/") || path == "/" || strings.ContainsRune(path, 0) {
		return nil, postgres16protocol.ConfinementFileIdentity{}, releaseFileError()
	}
	rootFD, err := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, postgres16protocol.ConfinementFileIdentity{}, releaseFileError()
	}
	defer unix.Close(rootFD)
	fd, err := unix.Openat2(rootFD, strings.TrimPrefix(path, "/"), &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
	})
	if err != nil {
		return nil, postgres16protocol.ConfinementFileIdentity{}, releaseFileError()
	}
	file := os.NewFile(uintptr(fd), path)
	closeOnError := func() (*os.File, postgres16protocol.ConfinementFileIdentity, error) {
		_ = file.Close()
		return nil, postgres16protocol.ConfinementFileIdentity{}, releaseFileError()
	}
	var before unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil || before.Mode&unix.S_IFMT != unix.S_IFREG ||
		before.Size <= 0 || before.Uid != 0 || before.Gid != 0 || before.Mode&0o6000 != 0 {
		return closeOnError()
	}
	var capability [1]byte
	_, err = unix.Fgetxattr(fd, "security.capability", capability[:])
	if !errors.Is(err, unix.ENODATA) {
		return closeOnError()
	}
	hash := sha256.New()
	n, err := io.Copy(hash, file)
	if err != nil || n != before.Size {
		return closeOnError()
	}
	var after unix.Stat_t
	if err := unix.Fstat(fd, &after); err != nil || after.Dev != before.Dev ||
		after.Ino != before.Ino || after.Size != before.Size ||
		after.Mtim != before.Mtim || after.Ctim != before.Ctim ||
		after.Mode != before.Mode || after.Uid != before.Uid || after.Gid != before.Gid {
		return closeOnError()
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return closeOnError()
	}
	var digest postgres16protocol.Digest
	copy(digest[:], hash.Sum(nil))
	identity := postgres16protocol.ConfinementFileIdentity{
		Path: path, Device: uint64(before.Dev), Inode: before.Ino,
		SizeBytes: uint64(before.Size), SHA256: digest,
		UID: before.Uid, GID: before.Gid, Mode: before.Mode & 0o7777,
		Regular: true, SetUIDAbsent: true, SetGIDAbsent: true, FileCapabilitiesAbsent: true,
	}
	if err := identity.Validate(); err != nil {
		return closeOnError()
	}
	return file, identity, nil
}

func releaseFileError() error {
	return grounderrs.New(grounderrs.KindInternal, "postgres helper release file is unavailable")
}
