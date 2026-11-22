//go:build !linux

package hoststats

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (*Collector) Measure(context.Context) (UsageSnapshot, error) {
	return UsageSnapshot{}, errs.New(errs.KindStorageUnavailable, "host monitoring requires Linux")
}
