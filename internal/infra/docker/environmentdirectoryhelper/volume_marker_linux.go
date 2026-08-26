//go:build linux

package environmentdirectoryhelper

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	volumeCreationMarker = ".gp-volume-create-marker"
	volumeMarkerBytes    = 4096
)

func writeExclusiveMarker(directoryFD int, marker []byte) error {
	markerFD, err := unix.Openat(directoryFD, volumeCreationMarker,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("create managed volume ownership marker: %w", err))
	}
	file := os.NewFile(uintptr(markerFD), "managed-volume-marker")
	if file == nil {
		_ = unix.Close(markerFD)
		return errs.New(errs.KindInternal, "create managed volume ownership marker file failed")
	}
	if err := writeBytes(file, marker); err != nil {
		_ = file.Close()
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := unix.Fsync(markerFD); err != nil {
		_ = file.Close()
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := file.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func writeBytes(writer io.Writer, value []byte) error {
	for len(value) != 0 {
		written, err := writer.Write(value)
		if err != nil {
			return err
		}
		if written <= 0 {
			return errors.New("managed volume marker writer made no progress")
		}
		value = value[written:]
	}
	return nil
}

func verifyMarker(directoryFD int, expected []byte) error {
	markerFD, err := unix.Openat(directoryFD, volumeCreationMarker, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(markerFD), "managed-volume-marker")
	if file == nil {
		_ = unix.Close(markerFD)
		return errors.New("open managed volume ownership marker failed")
	}
	actual, readErr := io.ReadAll(io.LimitReader(file, volumeMarkerBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if len(actual) > volumeMarkerBytes || !bytes.Equal(actual, expected) {
		return errors.New("managed volume ownership marker does not match")
	}
	return nil
}

func finalizePublishedVolume(leafFD, environmentFD int) error {
	if err := unix.Unlinkat(leafFD, volumeCreationMarker, 0); err != nil && !errors.Is(err, syscall.ENOENT) {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("remove managed volume ownership marker: %w", err))
	}
	if err := unix.Fsync(leafFD); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := unix.Fsync(environmentFD); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func removePrivateSibling(environmentFD int, sibling string, marker []byte) error {
	siblingFD, err := openDirectoryAt(environmentFD, sibling)
	if errors.Is(err, syscall.ENOENT) {
		return nil
	}
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := verifyMarker(siblingFD, marker); err != nil {
		_ = unix.Close(siblingFD)
		return errs.New(errs.KindValidationFailed, "managed volume private sibling is not task-owned")
	}
	if err := unix.Unlinkat(siblingFD, volumeCreationMarker, 0); err != nil && !errors.Is(err, syscall.ENOENT) {
		_ = unix.Close(siblingFD)
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := unix.Close(siblingFD); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := unix.Unlinkat(environmentFD, sibling, unix.AT_REMOVEDIR); err != nil && !errors.Is(err, syscall.ENOENT) {
		return errs.Wrap(errs.KindInternal, err)
	}
	return unix.Fsync(environmentFD)
}
