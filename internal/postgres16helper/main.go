package postgres16helper

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	grounderrs "github.com/AlanD20/groundplane/pkg/errs"
)

// Main fails closed until the root supervisor and private client-gate runtime
// implement the confinement contracts in this package.
func Main(_ context.Context, _ []string) (postgres16protocol.ExitCode, error) {
	return postgres16protocol.ExitInternalFailure,
		grounderrs.New(grounderrs.KindInternal, "postgres helper confinement runtime is unavailable")
}
