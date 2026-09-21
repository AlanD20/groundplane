package entrymaterializer

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"golang.org/x/sys/unix"
)

func verifyTemporary(
	ctx context.Context,
	ops linuxOps,
	fd int,
	header entrymaterialization.Header,
) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return wrapSystemError("inspect temporary", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Uid != header.UID() ||
		stat.Gid != header.GID() || stat.Mode&0o7777 != uint32(header.Mode()) ||
		stat.Size != int64(header.Length()) {
		return internalError("temporary metadata mismatch")
	}
	return verifyDescriptorDigest(ctx, ops, fd, stat.Size, header.Digest())
}

func verifyDescriptorDigest(
	ctx context.Context,
	ops linuxOps,
	fd int,
	length int64,
	expected entrymaterialization.Digest,
) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	buffer := make([]byte, contentBufferSize)
	defer clear(buffer)
	contentHash := ops.newHasher()
	if contentHash == nil {
		return internalError("verification hasher is required")
	}
	defer contentHash.Destroy()
	var offset int64
	for offset < length {
		if err := ctx.Err(); err != nil {
			return err
		}
		readSize := int64(len(buffer))
		if remaining := length - offset; remaining < readSize {
			readSize = remaining
		}
		count, err := unix.Pread(fd, buffer[:int(readSize)], offset)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return wrapSystemError("read temporary for verification", err)
		}
		if count <= 0 || count > int(readSize) {
			return internalError("temporary verification made no progress")
		}
		written, hashErr := contentHash.Write(buffer[:count])
		clear(buffer[:count])
		if hashErr != nil || written != count {
			return internalError("temporary verification hash failed")
		}
		offset += int64(count)
	}
	if !contentHash.Verify(expected) {
		return internalError("temporary digest mismatch")
	}
	return nil
}

func inspectDestination(
	ctx context.Context,
	ops linuxOps,
	parentFD int,
	destination string,
	header entrymaterialization.Header,
) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	fd, err := openPathAt(ctx, ops, parentFD, destination)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return wrapSystemError("open existing destination", err)
	}
	var stat unix.Stat_t
	statErr := unix.Fstat(fd, &stat)
	closeErr := ops.close(fd)
	var operationErr error
	if statErr != nil {
		operationErr = wrapSystemError("inspect existing destination", statErr)
	}
	var cleanupErr error
	if closeErr != nil {
		cleanupErr = wrapSystemError("close existing destination", closeErr)
	}
	if result := preferCleanupError(operationErr, cleanupErr); result != nil {
		return result
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Uid != header.UID() ||
		stat.Gid != header.GID() || stat.Mode&0o7777 != uint32(header.Mode()) {
		return internalError("existing destination metadata mismatch")
	}
	return nil
}
