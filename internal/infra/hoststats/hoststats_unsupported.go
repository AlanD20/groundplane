//go:build !linux

package hoststats

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (collector *Collector) Snapshot(ctx context.Context) (Snapshot, error) {
	if ctx == nil {
		return Snapshot{}, errs.New(errs.KindInternal, "host stats context is required")
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	return Snapshot{}, errs.New(errs.KindNotImplemented, "host stats require Linux")
}
