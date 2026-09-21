package entrymaterializer

import (
	"context"
	"encoding/hex"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"golang.org/x/sys/unix"
	"io"
)

func (materializer *materializer) prepareTemporary(
	ctx context.Context,
	parentFD int,
	header entrymaterialization.Header,
	content io.Reader,
) (*temporary, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	temporaryFile, err := materializer.createTemporary(ctx, parentFD)
	if err != nil {
		return nil, err
	}
	fail := func(operationErr error) error {
		cleanupErr := cleanupTemporary(
			context.WithoutCancel(ctx),
			materializer.ops,
			parentFD,
			temporaryFile,
		)
		return preferCleanupError(operationErr, cleanupErr)
	}

	if err := writeDeclaredContent(
		ctx,
		materializer.ops,
		temporaryFile.fd,
		header,
		content,
	); err != nil {
		return nil, fail(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, fail(err)
	}
	if err := materializer.ops.fsync(temporaryFile.fd); err != nil {
		return nil, fail(wrapSystemError("sync temporary content", err))
	}
	if err := ctx.Err(); err != nil {
		return nil, fail(err)
	}
	if err := unix.Fchown(temporaryFile.fd, int(header.UID()), int(header.GID())); err != nil {
		return nil, fail(wrapSystemError("set temporary ownership", err))
	}
	if err := ctx.Err(); err != nil {
		return nil, fail(err)
	}
	if err := unix.Fchmod(temporaryFile.fd, uint32(header.Mode())); err != nil {
		return nil, fail(wrapSystemError("set temporary mode", err))
	}
	if err := verifyTemporary(ctx, materializer.ops, temporaryFile.fd, header); err != nil {
		return nil, fail(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, fail(err)
	}
	if err := materializer.ops.close(temporaryFile.fd); err != nil {
		temporaryFile.open = false
		return nil, fail(wrapSystemError("close temporary before publication", err))
	}
	temporaryFile.open = false
	return temporaryFile, nil
}

type temporary struct {
	fd      int
	name    string
	open    bool
	present bool
}

func (materializer *materializer) createTemporary(
	ctx context.Context,
	parentFD int,
) (*temporary, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	randomBytes := make([]byte, temporaryRandomLen)
	defer clear(randomBytes)
	for range temporaryAttempts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(materializer.randomness, randomBytes); err != nil {
			return nil, internalError("temporary randomness failed")
		}
		name := entrymaterialization.TemporaryPrefix + hex.EncodeToString(randomBytes)
		fd, err := unix.Openat(
			parentFD,
			name,
			unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW,
			temporaryMode,
		)
		if errors.Is(err, unix.EEXIST) {
			continue
		}
		if err != nil {
			return nil, wrapSystemError("create exclusive temporary", err)
		}
		temporaryFile := &temporary{fd: fd, name: name, open: true, present: true}
		if err := setCreatedTemporary(
			ctx,
			fd,
			materializer.helperUID,
			materializer.helperGID,
		); err != nil {
			return nil, preferCleanupError(
				err,
				cleanupTemporary(
					context.WithoutCancel(ctx),
					materializer.ops,
					parentFD,
					temporaryFile,
				),
			)
		}
		return temporaryFile, nil
	}
	return nil, internalError("exclusive temporary allocation failed")
}
