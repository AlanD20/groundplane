package entrymaterializer

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"golang.org/x/sys/unix"
	"io"
)

func verifyRemovalContent(
	ctx context.Context,
	ops linuxOps,
	header entrymaterialization.Header,
	content io.Reader,
) error {
	if header.Length() != 0 || !header.OutputKind().Removes() {
		return internalError("removal content metadata is invalid")
	}
	if err := requireStreamEnd(ctx, content); err != nil {
		return err
	}
	hasher := ops.newHasher()
	if hasher == nil {
		return internalError("content hasher is required")
	}
	defer hasher.Destroy()
	if !hasher.Verify(header.Digest()) {
		return internalError("content digest mismatch")
	}
	return nil
}

func writeDeclaredContent(
	ctx context.Context,
	ops linuxOps,
	fd int,
	header entrymaterialization.Header,
	content io.Reader,
) error {
	buffer := make([]byte, contentBufferSize)
	defer clear(buffer)
	contentHash := ops.newHasher()
	if contentHash == nil {
		return internalError("content hasher is required")
	}
	defer contentHash.Destroy()
	remaining := header.Length()
	emptyReads := 0
	ended := false
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		readSize := uint64(len(buffer))
		if remaining < readSize {
			readSize = remaining
		}
		count, readErr := content.Read(buffer[:int(readSize)])
		if count < 0 || count > int(readSize) {
			return internalError("content stream violated framing")
		}
		if count > 0 {
			emptyReads = 0
			if err := writeAll(ctx, fd, buffer[:count]); err != nil {
				clear(buffer[:count])
				return err
			}
			written, hashErr := contentHash.Write(buffer[:count])
			clear(buffer[:count])
			if hashErr != nil || written != count {
				return internalError("content hashing failed")
			}
			remaining -= uint64(count)
		} else {
			emptyReads++
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				if remaining != 0 {
					return internalError("content stream ended early")
				}
				ended = true
				break
			}
			return internalError("content stream failed")
		}
		if emptyReads >= maxEmptyReads {
			return internalError("content stream made no progress")
		}
	}
	if !ended {
		if err := requireStreamEnd(ctx, content); err != nil {
			return err
		}
	}
	if !contentHash.Verify(header.Digest()) {
		return internalError("content digest mismatch")
	}
	return nil
}

func requireStreamEnd(ctx context.Context, content io.Reader) error {
	buffer := []byte{0}
	defer clear(buffer)
	for emptyReads := 0; emptyReads < maxEmptyReads; emptyReads++ {
		if err := contextError(ctx); err != nil {
			return err
		}
		count, readErr := content.Read(buffer)
		if count < 0 || count > len(buffer) {
			return internalError("content stream violated framing")
		}
		if count != 0 {
			return internalError("content stream has extra bytes")
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return internalError("content stream failed")
		}
	}
	return internalError("content stream made no progress")
}

func writeAll(ctx context.Context, fd int, content []byte) error {
	for len(content) > 0 {
		if err := contextError(ctx); err != nil {
			return err
		}
		count, err := unix.Write(fd, content)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return wrapSystemError("write temporary content", err)
		}
		if count <= 0 || count > len(content) {
			return internalError("temporary write made no progress")
		}
		content = content[count:]
	}
	return nil
}
