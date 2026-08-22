//go:build !linux

package environmentroot

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type Identity struct {
	Device uint64
	Inode  uint64
}

func Validate(context.Context, string) (Identity, error) {
	return Identity{}, errs.New(errs.KindInternal, "environment volume roots require Linux openat2")
}
