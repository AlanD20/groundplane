//go:build linux

package entrymaterializer

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"golang.org/x/sys/unix"
)

type digestHasher interface {
	Write([]byte) (int, error)
	Verify(entrymaterialization.Digest) bool
	Destroy()
}

type linuxOps struct {
	close     func(int) error
	fsync     func(int) error
	openat2   func(int, string, *unix.OpenHow) (int, error)
	renameat  func(int, string, int, string) error
	unlinkat  func(int, string, int) error
	newHasher func() digestHasher
}

func productionLinuxOps() linuxOps {
	return linuxOps{
		close:    unix.Close,
		fsync:    unix.Fsync,
		openat2:  unix.Openat2,
		renameat: unix.Renameat,
		unlinkat: unix.Unlinkat,
		newHasher: func() digestHasher {
			return entrymaterialization.NewHasher()
		},
	}
}

func validateLinuxOps(ops linuxOps) error {
	if ops.close == nil || ops.fsync == nil || ops.openat2 == nil ||
		ops.renameat == nil || ops.unlinkat == nil || ops.newHasher == nil {
		return errors.New("incomplete Linux operation table")
	}
	return nil
}
