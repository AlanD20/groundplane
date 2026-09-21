package backupstage

import (
	"context"
	"errors"
	"fmt"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

func joinPrivate(primary error, secondary ...error) error {
	causes := make([]error, 0, 1+len(secondary))
	if primary != nil {
		causes = append(causes, primary)
	}
	for _, cause := range secondary {
		if cause != nil {
			causes = append(causes, cause)
		}
	}
	if len(causes) == 0 {
		return nil
	}
	if len(causes) == 1 {
		if _, ok := errs.KindOf(causes[0]); ok {
			return causes[0]
		}
		return errs.Wrap(errs.KindInternal, causes[0])
	}
	kind := errs.KindInternal
	if value, ok := errs.KindOf(causes[0]); ok {
		kind = value
	}
	return errs.WrapJoined(kind, causes[0], causes[1:]...)
}

func wrapPrivateCauses(operation string, causes ...error) error {
	filtered := make([]error, 0, len(causes))
	for _, cause := range causes {
		if cause != nil {
			filtered = append(filtered, cause)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	primary := fmt.Errorf("backup stage: %s: %w", operation, filtered[0])
	if len(filtered) == 1 {
		return errs.Wrap(errs.KindInternal, primary)
	}
	return errs.WrapJoined(errs.KindInternal, primary, filtered[1:]...)
}

func rawOperationError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("backup stage: %s: %w", operation, err)
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return internalError("context is required")
	}
	return ctx.Err()
}

func validationError(format string, args ...any) error {
	return errs.Newf(errs.KindValidationFailed, "backup stage: "+format, args...)
}

func stateConflictError(message string) error {
	return errs.New(errs.KindStateConflict, "backup stage: "+message)
}

func requiredLinuxError(syscall string) error {
	return errs.Newf(errs.KindNotImplemented, "backup stage requires Linux %s support", syscall)
}

func internalError(message string) error {
	return errs.New(errs.KindInternal, "backup stage: "+message)
}

func systemError(operation string, err error) error {
	return errs.Wrap(errs.KindInternal, fmt.Errorf("backup stage: %s: %w", operation, err))
}

func storageSystemError(operation string, err error) error {
	if errors.Is(err, unix.ENOSPC) || errors.Is(err, unix.EDQUOT) {
		return errs.Wrap(errs.KindStorageUnavailable, fmt.Errorf("backup stage: %s: %w", operation, err))
	}
	return systemError(operation, err)
}

func storageOperationError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return storageSystemError(operation, err)
}
