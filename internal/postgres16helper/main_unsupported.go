//go:build !linux

package postgres16helper

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func Main(context.Context, []string) (postgres16protocol.ExitCode, error) {
	return postgres16protocol.ExitEnvironmentInvalid,
		errs.New(errs.KindInternal, "managed PostgreSQL execution requires Linux")
}
