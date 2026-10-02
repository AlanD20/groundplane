//go:build linux

package agentvolumejournal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func openLeaf(directoryFD int, name string, flags int, create bool, maximum int64) (*os.File, error) {
	var named unix.Stat_t
	statErr := unix.Fstatat(directoryFD, name, &named, unix.AT_SYMLINK_NOFOLLOW)
	if create {
		if !errors.Is(statErr, unix.ENOENT) {
			if statErr != nil {
				return nil, storage(statErr)
			}
			return nil, conflict("Volume journal leaf already exists")
		}
		flags |= unix.O_CREAT | unix.O_EXCL
	} else if statErr != nil {
		return nil, storage(statErr)
	} else if named.Mode&unix.S_IFMT != unix.S_IFREG || named.Uid != 0 || named.Nlink != 1 ||
		named.Mode&0o7777 != 0o600 || named.Size < 0 || named.Size > maximum {
		return nil, invalid("Volume journal leaf metadata is unsafe")
	}
	mode := uint64(0)
	if create {
		mode = 0o600
	}
	fd, err := unix.Openat2(directoryFD, name, &unix.OpenHow{
		Flags: uint64(flags | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK),
		Mode:  mode, Resolve: resolveLocal,
	})
	if err != nil {
		return nil, storage(err)
	}
	var opened unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil || opened.Mode&unix.S_IFMT != unix.S_IFREG ||
		opened.Uid != 0 || opened.Nlink != 1 || opened.Mode&0o7777 != 0o600 ||
		opened.Size < 0 || opened.Size > maximum ||
		(!create && (opened.Ino != named.Ino || opened.Dev != named.Dev)) {
		_ = unix.Close(fd)
		if err != nil {
			return nil, storage(err)
		}
		return nil, invalid("Volume journal leaf changed or is unsafe")
	}
	return os.NewFile(uintptr(fd), name), nil
}

func writeImmutableAuthority(ctx context.Context, directoryFD int, expected []byte) error {
	file, err := openLeaf(directoryFD, authorityName, unix.O_RDWR, false, maximumAuthorityBytes)
	if errors.Is(err, unix.ENOENT) {
		file, err = openLeaf(directoryFD, authorityName, unix.O_RDWR, true, maximumAuthorityBytes)
	}
	if err != nil {
		return err
	}
	current, readErr := io.ReadAll(io.LimitReader(file, maximumAuthorityBytes+1))
	if readErr != nil || len(current) > len(expected) || !bytes.Equal(current, expected[:len(current)]) {
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			return storage(errors.Join(readErr, closeErr))
		}
		return conflict("Volume journal authority differs from sealed assignment")
	}
	if err := ctx.Err(); err != nil {
		_ = file.Close()
		return err
	}
	remaining := expected[len(current):]
	written, writeErr := file.WriteAt(remaining, int64(len(current)))
	if writeErr == nil && written != len(remaining) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		return storage(errors.Join(writeErr, closeErr))
	}
	if err := unix.Fsync(directoryFD); err != nil {
		return storage(err)
	}
	return nil
}

func readAuthority(ctx context.Context, directoryFD int) (Config, error) {
	if err := ctx.Err(); err != nil {
		return Config{}, err
	}
	file, err := openLeaf(directoryFD, authorityName, unix.O_RDONLY, false, maximumAuthorityBytes)
	if err != nil {
		return Config{}, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, maximumAuthorityBytes+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return Config{}, storage(errors.Join(readErr, closeErr))
	}
	return decodeAuthority(raw)
}

func openOrCreateLog(directoryFD int) (*os.File, error) {
	file, err := openLeaf(directoryFD, logName, unix.O_RDWR, false, MaximumJournalBytes)
	if errors.Is(err, unix.ENOENT) {
		file, err = openLeaf(directoryFD, logName, unix.O_RDWR, true, MaximumJournalBytes)
		if err == nil {
			err = file.Sync()
		}
		if err == nil {
			err = unix.Fsync(directoryFD)
		}
		if err != nil {
			if file != nil {
				_ = file.Close()
			}
			return nil, storage(err)
		}
	}
	return file, err
}

func readLogBytes(file *os.File) ([]byte, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, storage(err)
	}
	if info.Size() < 0 || info.Size() > MaximumJournalBytes {
		return nil, invalid("Volume journal length exceeds its bound")
	}
	raw, err := io.ReadAll(io.NewSectionReader(file, 0, info.Size()))
	if err != nil {
		return nil, storage(err)
	}
	if int64(len(raw)) != info.Size() {
		return nil, invalid("Volume journal file changed during replay")
	}
	return raw, nil
}

func directoryNames(directoryFD int) ([]string, error) {
	dup, err := unix.Openat2(directoryFD, ".", &unix.OpenHow{
		Flags: uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: resolveLocal,
	})
	if err != nil {
		return nil, storage(err)
	}
	directory := os.NewFile(uintptr(dup), "volume-journal-directory")
	names, readErr := directory.Readdirnames(4)
	closeErr := directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) || closeErr != nil {
		return nil, storage(errors.Join(readErr, closeErr))
	}
	if len(names) > 3 {
		return nil, invalid("Volume journal contains unexpected leaves")
	}
	return names, nil
}
