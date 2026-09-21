package backupconfig

import (
	"context"
	"fmt"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func archiveError(message string) error {
	return errs.New(errs.KindInternal, fmt.Sprintf("backup Config archive: %s", message))
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return archiveError("operation context is nil")
	}
	select {
	case <-ctx.Done():
		return archiveCause("operation was canceled", ctx.Err())
	default:
		return nil
	}
}

func archiveCause(message string, cause error) error {
	return errs.Wrap(errs.KindInternal, fmt.Errorf("backup Config archive: %s: %w", message, cause))
}

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
