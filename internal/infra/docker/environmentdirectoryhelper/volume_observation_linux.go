package environmentdirectoryhelper

import (
	"context"
	"errors"
	"fmt"
	"syscall"

	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

func (Creator) InspectManagedVolume(ctx context.Context, request ManagedVolumeInspectRequest) (ManagedVolumeObservation, error) {
	if ctx == nil {
		return ManagedVolumeObservation{}, errs.New(errs.KindInternal, "Volume directory inspection requires a context")
	}
	if err := ctx.Err(); err != nil {
		return ManagedVolumeObservation{}, err
	}
	if err := validateManagedVolumeIdentity(ManagedVolume{ID: request.VolumeID, Key: request.ComposeKey}); err != nil {
		return ManagedVolumeObservation{}, err
	}
	parentFD, environmentFD, err := openEnvironmentDirectory(request.VolumeRoot, request.VolumeDir, 0)
	if err != nil {
		// Missing/unreadable ancestors are not proof of a specific Volume's
		// absence under its still-owned Environment directory.
		return ManagedVolumeObservation{}, err
	}
	defer unix.Close(parentFD)
	defer unix.Close(environmentFD)
	leafFD, err := unix.Openat2(environmentFD, request.ComposeKey, &unix.OpenHow{
		Flags:   uint64(unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV,
	})
	if errors.Is(err, syscall.ENOENT) {
		if err := unix.Fsync(environmentFD); err != nil {
			return ManagedVolumeObservation{}, errs.Wrap(errs.KindInternal, err)
		}
		if err := ctx.Err(); err != nil {
			return ManagedVolumeObservation{}, err
		}
		return ManagedVolumeObservation{Absent: true}, nil
	}
	if err != nil {
		return ManagedVolumeObservation{}, errs.Wrap(errs.KindStateConflict,
			fmt.Errorf("inspect managed Volume directory without following links: %w", err))
	}
	defer unix.Close(leafFD)
	var marker unix.Stat_t
	err = unix.Fstatat(leafFD, volumeCreationMarker, &marker, unix.AT_SYMLINK_NOFOLLOW)
	if err == nil {
		// Inspection never finishes an interrupted publication or removes its
		// evidence. The owning operation must account for that effect first.
		return ManagedVolumeObservation{}, errs.New(errs.KindStateConflict, "Volume directory creation has not finished publishing")
	}
	if !errors.Is(err, syscall.ENOENT) {
		return ManagedVolumeObservation{}, errs.Wrap(errs.KindStateConflict,
			fmt.Errorf("inspect managed Volume creation marker: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return ManagedVolumeObservation{}, err
	}
	return ManagedVolumeObservation{Ready: true}, nil
}
