//go:build linux

package entrymaterializer

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// Verify independently reads the selected file without creating, repairing or
// removing anything. The complete input frame is checked before root access.
// StateConflict means that the selected postcondition is not satisfied; unsafe
// paths, malformed authority and I/O failures are not treated as mere drift.
func Verify(ctx context.Context, source io.ReadCloser, limits entrymaterialization.Limits) error {
	return verify(ctx, source, limits, openMaterializer)
}

func verify(
	ctx context.Context,
	source io.ReadCloser,
	limits entrymaterialization.Limits,
	open func(context.Context) (*materializer, error),
) error {
	if open == nil {
		if source != nil {
			return preferCleanupError(internalError("verification root opener is required"), source.Close())
		}
		return internalError("verification root opener is required")
	}
	header, err := entrymaterialization.Decode(ctx, source, limits,
		func(ctx context.Context, _ entrymaterialization.Header, content io.Reader) error {
			buffer := make([]byte, contentBufferSize)
			defer clear(buffer)
			for {
				if err := contextError(ctx); err != nil {
					return err
				}
				_, err := content.Read(buffer)
				clear(buffer)
				if errors.Is(err, io.EOF) {
					return nil
				}
				if err != nil {
					return err
				}
			}
		})
	if err != nil {
		return err
	}
	materializer, err := open(ctx)
	if err != nil {
		return err
	}
	if materializer == nil {
		return internalError("verification root is absent")
	}
	verificationErr := materializer.verify(ctx, header)
	return preferCleanupError(verificationErr, materializer.close(context.WithoutCancel(ctx)))
}

func (materializer *materializer) verify(ctx context.Context, header entrymaterialization.Header) (result error) {
	rootFD, err := materializer.duplicateRoot(ctx)
	if err != nil {
		return err
	}
	defer func() {
		result = closeBeforeReturn(
			context.WithoutCancel(ctx),
			materializer.ops,
			rootFD,
			result,
			"close verification root",
		)
	}()
	components := strings.Split(header.Destination(), "/")
	parentFD, missing, err := materializer.openExistingParents(ctx, rootFD, components[:len(components)-1])
	if err != nil {
		return err
	}
	if missing {
		return verificationPresence(header, false)
	}
	defer func() {
		result = closeBeforeReturn(
			context.WithoutCancel(ctx),
			materializer.ops,
			parentFD,
			result,
			"close verification parent",
		)
	}()
	fd, err := materializer.ops.openat2(parentFD, components[len(components)-1], &unix.OpenHow{
		Flags: unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK, Resolve: beneathPolicy,
	})
	if errors.Is(err, unix.ENOENT) {
		return verificationPresence(header, false)
	}
	if err != nil {
		return wrapSystemError("open verification file", err)
	}
	file := os.NewFile(uintptr(fd), "materialization verification")
	defer func() { result = preferCleanupError(result, file.Close()) }()
	var before unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil {
		return wrapSystemError("inspect verification file", err)
	}
	if before.Mode&unix.S_IFMT != unix.S_IFREG || before.Nlink != 1 {
		return internalError("verification target is not an exclusively owned regular file")
	}
	if err := verificationPresence(header, true); err != nil {
		return err
	}
	if before.Uid != header.UID() || before.Gid != header.GID() ||
		before.Mode&0o7777 != uint32(header.Mode()) || before.Size < 0 || uint64(before.Size) != header.Length() {
		return configurationMismatch()
	}
	hasher := materializer.ops.newHasher()
	defer hasher.Destroy()
	buffer := make([]byte, contentBufferSize)
	defer clear(buffer)
	reader := io.LimitReader(file, int64(header.Length())+1)
	var length uint64
	for {
		if err := contextError(ctx); err != nil {
			return err
		}
		n, readErr := reader.Read(buffer)
		if n > 0 {
			length += uint64(n)
			written, hashErr := hasher.Write(buffer[:n])
			clear(buffer[:n])
			if hashErr != nil || written != n {
				return internalError("verification digest failed")
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return wrapSystemError("read verification file", readErr)
		}
	}
	var after unix.Stat_t
	if err := unix.Fstat(fd, &after); err != nil {
		return wrapSystemError("reinspect verification file", err)
	}
	if !sameVerificationStat(before, after) || length != header.Length() || !hasher.Verify(header.Digest()) {
		return configurationMismatch()
	}
	// Verify the destination still names the descriptor we read, not an inode
	// atomically replaced while it was being hashed.
	pathFD, err := openPathAt(ctx, materializer.ops, parentFD, components[len(components)-1])
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return configurationMismatch()
		}
		return wrapSystemError("reopen verification destination", err)
	}
	var named unix.Stat_t
	statErr := unix.Fstat(pathFD, &named)
	closeErr := materializer.ops.close(pathFD)
	if statErr != nil || closeErr != nil {
		return wrapSystemError("reinspect verification destination", errors.Join(statErr, closeErr))
	}
	if !sameVerificationStat(after, named) {
		return configurationMismatch()
	}
	return nil
}

func sameVerificationStat(left, right unix.Stat_t) bool {
	return left.Dev == right.Dev && left.Ino == right.Ino && left.Mode == right.Mode && left.Nlink == right.Nlink &&
		left.Uid == right.Uid && left.Gid == right.Gid && left.Size == right.Size &&
		left.Mtim == right.Mtim && left.Ctim == right.Ctim
}

func verificationPresence(header entrymaterialization.Header, present bool) error {
	if present == header.OutputKind().Removes() {
		return configurationMismatch()
	}
	return nil
}

func configurationMismatch() error {
	return errs.New(errs.KindStateConflict, "entry materializer: pinned configuration is not restored")
}
