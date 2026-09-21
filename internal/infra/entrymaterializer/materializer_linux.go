//go:build linux

package entrymaterializer

import (
	"context"
	"crypto/rand"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"sync"
)

const (
	directoryMode      = uint32(0o700)
	temporaryMode      = uint32(0o600)
	temporaryRandomLen = 16
	temporaryAttempts  = 16
	contentBufferSize  = 32 * 1024
	maxEmptyReads      = 100
)

var beneathPolicy = uint64(
	unix.RESOLVE_BENEATH |
		unix.RESOLVE_NO_SYMLINKS |
		unix.RESOLVE_NO_MAGICLINKS |
		unix.RESOLVE_NO_XDEV,
)

type materializer struct {
	lifecycleMu sync.Mutex
	rootFD      int
	randomness  io.Reader
	helperUID   uint32
	helperGID   uint32
	ops         linuxOps
}

func openMaterializer(ctx context.Context) (*materializer, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rootFD, err := unix.Open(
		rootPath,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return nil, wrapSystemError("open fixed root", err)
	}
	ops := productionLinuxOps()
	materializer, createErr := newOwnedMaterializer(ctx, rootFD, rand.Reader, 0, 0, ops)
	if createErr != nil {
		return nil, rejectRoot(context.WithoutCancel(ctx), ops, rootFD, createErr)
	}
	return materializer, nil
}

func newMaterializer(
	ctx context.Context,
	root *os.File,
	randomness io.Reader,
	helperUID uint32,
	helperGID uint32,
	ops linuxOps,
) (*materializer, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if root == nil || randomness == nil {
		return nil, internalError("root descriptor and randomness are required")
	}
	if err := validateLinuxOps(ops); err != nil {
		return nil, internalError("Linux operation table is required")
	}
	rootFD, err := unix.Dup(int(root.Fd()))
	if err != nil {
		return nil, wrapSystemError("duplicate root descriptor", err)
	}
	unix.CloseOnExec(rootFD)
	materializer, createErr := newOwnedMaterializer(
		ctx,
		rootFD,
		randomness,
		helperUID,
		helperGID,
		ops,
	)
	if createErr != nil {
		return nil, rejectRoot(context.WithoutCancel(ctx), ops, rootFD, createErr)
	}
	return materializer, nil
}

func rejectRoot(
	ctx context.Context,
	ops linuxOps,
	rootFD int,
	operationErr error,
) error {
	if err := contextError(ctx); err != nil {
		return preferCleanupError(operationErr, err)
	}
	closeErr := ops.close(rootFD)
	if closeErr != nil {
		closeErr = wrapSystemError("close rejected root", closeErr)
	}
	return preferCleanupError(operationErr, closeErr)
}

func newOwnedMaterializer(
	ctx context.Context,
	rootFD int,
	randomness io.Reader,
	helperUID uint32,
	helperGID uint32,
	ops linuxOps,
) (*materializer, error) {
	if err := verifyDirectory(ctx, rootFD, helperUID, helperGID); err != nil {
		return nil, err
	}
	probeFD, err := openDirectoryAt(ctx, ops, rootFD, ".")
	if err != nil {
		return nil, wrapSystemError("probe secure path resolution", err)
	}
	if err := ops.close(probeFD); err != nil {
		return nil, wrapSystemError("close path-resolution probe", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &materializer{
		rootFD:     rootFD,
		randomness: randomness,
		helperUID:  helperUID,
		helperGID:  helperGID,
		ops:        ops,
	}, nil
}

func (materializer *materializer) close(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	materializer.lifecycleMu.Lock()
	defer materializer.lifecycleMu.Unlock()
	if materializer.rootFD < 0 {
		return internalError("root descriptor is already closed")
	}
	rootFD := materializer.rootFD
	materializer.rootFD = -1
	if err := materializer.ops.close(rootFD); err != nil {
		return wrapSystemError("close root descriptor", err)
	}
	return ctx.Err()
}

func (materializer *materializer) materialize(
	ctx context.Context,
	header entrymaterialization.Header,
	content io.Reader,
) error {
	publication, err := materializer.prepare(ctx, header, content)
	if err != nil {
		return err
	}
	return publication.publish(ctx)
}

func Run(ctx context.Context, source io.ReadCloser, limits entrymaterialization.Limits) error {
	return run(ctx, source, limits, openMaterializer)
}

func run(
	ctx context.Context,
	source io.ReadCloser,
	limits entrymaterialization.Limits,
	open func(context.Context) (*materializer, error),
) (result error) {
	var materializer *materializer
	var publication *publication
	_, decodeErr := entrymaterialization.Decode(
		ctx,
		source,
		limits,
		func(callbackCtx context.Context, header entrymaterialization.Header, content io.Reader) error {
			if open == nil {
				return internalError("materializer opener is required")
			}
			opened, err := open(callbackCtx)
			if err != nil {
				return err
			}
			materializer = opened
			publication, err = materializer.prepare(callbackCtx, header, content)
			return err
		},
	)
	if publication != nil {
		if decodeErr != nil {
			result = publication.abort(context.WithoutCancel(ctx), decodeErr)
		} else {
			result = publication.publish(ctx)
		}
	} else {
		result = decodeErr
		if result == nil {
			result = internalError("decoder returned without a publication")
		}
	}
	if materializer != nil {
		closeErr := materializer.close(context.WithoutCancel(ctx))
		result = preferCleanupError(result, closeErr)
	}
	return result
}
