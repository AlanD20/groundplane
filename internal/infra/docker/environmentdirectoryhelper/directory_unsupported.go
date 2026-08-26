//go:build !linux

package environmentdirectoryhelper

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type Creator struct{}

func (Creator) Create(context.Context, string, string) error {
	return errs.New(errs.KindNotImplemented, "Environment directory creation requires Linux")
}

func (Creator) EnsureManagedVolumes(context.Context, ManagedVolumeEnsureRequest) error {
	return errs.New(errs.KindNotImplemented, "Managed volume directory creation requires Linux")
}

func (Creator) RemoveManagedVolume(context.Context, ManagedVolumeDirectoryRemoveRequest) (ManagedVolumeDirectoryRemoveResult, error) {
	return ManagedVolumeDirectoryRemoveResult{}, errs.New(errs.KindNotImplemented, "Managed volume directory removal requires Linux")
}

func (Creator) Remove(context.Context, string, string) error {
	return errs.New(errs.KindNotImplemented, "Environment directory removal requires Linux")
}
