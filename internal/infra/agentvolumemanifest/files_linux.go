//go:build linux

package agentvolumemanifest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"golang.org/x/sys/unix"
)

const (
	authorityName = "authority.bin"
	logName       = "receive.bin"
	markerName    = "retire.bin"
	discardName   = "discard.bin"
	resolveLocal  = unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV
)

func openRoot(ctx context.Context, create bool) (int, bool, error) {
	current, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, false, storage(err)
	}
	parts := strings.Split(strings.TrimPrefix(AgentRoot, "/"), "/")
	for index, part := range parts {
		if err := ctx.Err(); err != nil {
			_ = unix.Close(current)
			return -1, false, err
		}
		final := index == len(parts)-1
		var before unix.Stat_t
		statErr := unix.Fstatat(current, part, &before, unix.AT_SYMLINK_NOFOLLOW)
		if errors.Is(statErr, unix.ENOENT) && !create {
			_ = unix.Close(current)
			return -1, false, nil
		}
		if errors.Is(statErr, unix.ENOENT) && final && create {
			if err := unix.Mkdirat(current, part, 0o700); err != nil {
				_ = unix.Close(current)
				return -1, false, storage(err)
			}
			if err := unix.Fsync(current); err != nil {
				_ = unix.Close(current)
				return -1, false, storage(err)
			}
			statErr = unix.Fstatat(current, part, &before, unix.AT_SYMLINK_NOFOLLOW)
		}
		if statErr != nil || before.Mode&unix.S_IFMT != unix.S_IFDIR || before.Uid != 0 ||
			(final && before.Mode&0o7777 != 0o700) || (!final && before.Mode&0o022 != 0) {
			_ = unix.Close(current)
			if statErr != nil {
				return -1, false, storage(statErr)
			}
			return -1, false, invalid("Volume manifest receiver root ancestry is unsafe")
		}
		resolve := uint64(unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS)
		if final {
			resolve = resolveLocal
		}
		next, err := unix.Openat2(current, part, &unix.OpenHow{
			Flags: uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: resolve,
		})
		_ = unix.Close(current)
		if err != nil {
			return -1, false, storage(err)
		}
		var after unix.Stat_t
		if err := unix.Fstat(next, &after); err != nil || after.Ino != before.Ino || after.Dev != before.Dev {
			_ = unix.Close(next)
			if err != nil {
				return -1, false, storage(err)
			}
			return -1, false, invalid("Volume manifest receiver root ancestor changed")
		}
		current = next
	}
	return current, true, nil
}

func validateStageFilesystem(fd int) error {
	stage, err := unix.Open(backupstage.AgentRoot,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return storage(err)
	}
	defer unix.Close(stage)
	var first, second unix.Stat_t
	if err := unix.Fstat(stage, &first); err != nil {
		return storage(err)
	}
	if err := unix.Fstat(fd, &second); err != nil {
		return storage(err)
	}
	if first.Dev != second.Dev {
		return conflict("Volume manifest receiver and reserved staging filesystem differ")
	}
	return nil
}

func openDirectory(rootFD int, name string, create bool) (int, error) {
	var named unix.Stat_t
	err := unix.Fstatat(rootFD, name, &named, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) && create {
		if err := unix.Mkdirat(rootFD, name, 0o700); err != nil {
			return -1, storage(err)
		}
		if err := unix.Fsync(rootFD); err != nil {
			return -1, storage(err)
		}
		err = unix.Fstatat(rootFD, name, &named, unix.AT_SYMLINK_NOFOLLOW)
	}
	if err != nil {
		return -1, storage(err)
	}
	if named.Mode&unix.S_IFMT != unix.S_IFDIR || named.Mode&0o7777 != 0o700 || named.Uid != 0 {
		return -1, invalid("Volume manifest receiver directory is unsafe")
	}
	fd, err := unix.Openat2(rootFD, name, &unix.OpenHow{
		Flags: uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: resolveLocal,
	})
	if err != nil {
		return -1, storage(err)
	}
	var opened unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil || opened.Dev != named.Dev || opened.Ino != named.Ino ||
		opened.Mode&unix.S_IFMT != unix.S_IFDIR || opened.Mode&0o7777 != 0o700 || opened.Uid != 0 {
		_ = unix.Close(fd)
		if err != nil {
			return -1, storage(err)
		}
		return -1, invalid("Volume manifest receiver directory changed")
	}
	return fd, nil
}

func openLeaf(directoryFD int, name string, flags int, create bool, maximum int64) (*os.File, error) {
	var named unix.Stat_t
	err := unix.Fstatat(directoryFD, name, &named, unix.AT_SYMLINK_NOFOLLOW)
	if create {
		if !errors.Is(err, unix.ENOENT) {
			if err != nil {
				return nil, storage(err)
			}
			return nil, conflict("Volume manifest receiver leaf already exists")
		}
		flags |= unix.O_CREAT | unix.O_EXCL
	} else if err != nil {
		return nil, storage(err)
	} else if named.Mode&unix.S_IFMT != unix.S_IFREG || named.Mode&0o7777 != 0o600 ||
		named.Uid != 0 || named.Nlink != 1 || named.Size < 0 || named.Size > maximum {
		return nil, invalid("Volume manifest receiver leaf is unsafe")
	}
	mode := uint64(0)
	if create {
		mode = 0o600
	}
	fd, err := unix.Openat2(directoryFD, name, &unix.OpenHow{
		Flags: uint64(flags | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK), Mode: mode, Resolve: resolveLocal,
	})
	if err != nil {
		return nil, storage(err)
	}
	var opened unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil || opened.Mode&unix.S_IFMT != unix.S_IFREG ||
		opened.Mode&0o7777 != 0o600 || opened.Uid != 0 || opened.Nlink != 1 ||
		opened.Size < 0 || opened.Size > maximum ||
		(!create && (opened.Ino != named.Ino || opened.Dev != named.Dev)) {
		_ = unix.Close(fd)
		if err != nil {
			return nil, storage(err)
		}
		return nil, invalid("Volume manifest receiver leaf changed")
	}
	return os.NewFile(uintptr(fd), name), nil
}

func readLeaf(fd int, name string, maximum int64) ([]byte, error) {
	file, err := openLeaf(fd, name, unix.O_RDONLY, false, maximum)
	if err != nil {
		return nil, err
	}
	info, statErr := file.Stat()
	if statErr != nil || info.Size() > maximum {
		_ = file.Close()
		if statErr != nil {
			return nil, storage(statErr)
		}
		return nil, invalid("Volume manifest receiver leaf exceeds its bound")
	}
	raw, readErr := io.ReadAll(io.NewSectionReader(file, 0, info.Size()))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return nil, storage(errors.Join(readErr, closeErr))
	}
	if int64(len(raw)) != info.Size() {
		return nil, invalid("Volume manifest receiver leaf changed during read")
	}
	return raw, nil
}

func writeAuthority(ctx context.Context, fd int, expected []byte) error {
	file, err := openLeaf(fd, authorityName, unix.O_RDWR, false, maximumAuthority)
	if errors.Is(err, unix.ENOENT) {
		file, err = openLeaf(fd, authorityName, unix.O_RDWR, true, maximumAuthority)
	}
	if err != nil {
		return err
	}
	current, readErr := io.ReadAll(io.LimitReader(file, maximumAuthority+1))
	if readErr != nil || len(current) > len(expected) || !bytes.Equal(current, expected[:len(current)]) {
		_ = file.Close()
		if readErr != nil {
			return storage(readErr)
		}
		return conflict("Volume manifest receiver authority differs from sealed assignment")
	}
	if err := ctx.Err(); err != nil {
		_ = file.Close()
		return err
	}
	written, writeErr := file.WriteAt(expected[len(current):], int64(len(current)))
	if writeErr == nil && written != len(expected)-len(current) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		return storage(errors.Join(writeErr, closeErr))
	}
	return storage(unix.Fsync(fd))
}

func directoryNames(fd int) ([]string, error) {
	fresh, err := unix.Openat2(fd, ".", &unix.OpenHow{
		Flags: uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: resolveLocal,
	})
	if err != nil {
		return nil, storage(err)
	}
	directory := os.NewFile(uintptr(fresh), "volume-manifest-directory")
	names, readErr := directory.Readdirnames(4)
	closeErr := directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) || closeErr != nil {
		return nil, storage(errors.Join(readErr, closeErr))
	}
	if len(names) > 3 {
		return nil, invalid("Volume manifest receiver contains unexpected leaves")
	}
	return names, nil
}
