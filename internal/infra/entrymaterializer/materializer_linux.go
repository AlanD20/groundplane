//go:build linux

package entrymaterializer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
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

func (materializer *materializer) prepare(
	ctx context.Context,
	header entrymaterialization.Header,
	content io.Reader,
) (*publication, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	if header.TaskID() == "" {
		return nil, internalError("validated header is required")
	}
	if content == nil {
		return nil, internalError("content stream is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rootFD, err := materializer.duplicateRoot(ctx)
	if err != nil {
		return nil, err
	}
	if err := verifyDirectory(
		ctx,
		rootFD,
		materializer.helperUID,
		materializer.helperGID,
	); err != nil {
		return nil, closeBeforeReturn(
			context.WithoutCancel(ctx),
			materializer.ops,
			rootFD,
			err,
			"close rejected operation root",
		)
	}

	components := strings.Split(header.Destination(), "/")
	parentFD, err := materializer.ensureParents(ctx, rootFD, components[:len(components)-1])
	if err != nil {
		return nil, closeBeforeReturn(
			context.WithoutCancel(ctx),
			materializer.ops,
			rootFD,
			err,
			"close operation root after traversal failure",
		)
	}
	if err := materializer.reconcileOrphans(ctx, parentFD); err != nil {
		return nil, closeOperationDescriptors(
			context.WithoutCancel(ctx),
			materializer.ops,
			rootFD,
			parentFD,
			err,
		)
	}
	if err := inspectDestination(
		ctx,
		materializer.ops,
		parentFD,
		components[len(components)-1],
		header,
	); err != nil {
		return nil, closeOperationDescriptors(
			context.WithoutCancel(ctx),
			materializer.ops,
			rootFD,
			parentFD,
			err,
		)
	}
	temporaryFile, err := materializer.prepareTemporary(ctx, parentFD, header, content)
	if err != nil {
		return nil, closeOperationDescriptors(
			context.WithoutCancel(ctx),
			materializer.ops,
			rootFD,
			parentFD,
			err,
		)
	}
	return &publication{
		rootFD:      rootFD,
		parentFD:    parentFD,
		temporary:   temporaryFile,
		destination: components[len(components)-1],
		ops:         materializer.ops,
	}, nil
}

func (materializer *materializer) duplicateRoot(ctx context.Context) (int, error) {
	if err := contextError(ctx); err != nil {
		return -1, err
	}
	materializer.lifecycleMu.Lock()
	defer materializer.lifecycleMu.Unlock()
	if materializer.rootFD < 0 {
		return -1, internalError("root descriptor is closed")
	}
	rootFD, err := unix.Dup(materializer.rootFD)
	if err != nil {
		return -1, wrapSystemError("duplicate operation root", err)
	}
	unix.CloseOnExec(rootFD)
	return rootFD, nil
}

func (materializer *materializer) ensureParents(
	ctx context.Context,
	rootFD int,
	components []string,
) (int, error) {
	if err := contextError(ctx); err != nil {
		return -1, err
	}
	currentFD, err := unix.Dup(rootFD)
	if err != nil {
		return -1, wrapSystemError("duplicate parent root", err)
	}
	unix.CloseOnExec(currentFD)
	cleanupCtx := context.WithoutCancel(ctx)
	for _, component := range components {
		if err := ctx.Err(); err != nil {
			return -1, closeBeforeReturn(
				cleanupCtx,
				materializer.ops,
				currentFD,
				err,
				"close cancelled parent",
			)
		}
		childFD, openErr := openDirectoryAt(ctx, materializer.ops, currentFD, component)
		created := false
		if errors.Is(openErr, unix.ENOENT) {
			if mkdirErr := unix.Mkdirat(currentFD, component, directoryMode); mkdirErr != nil {
				if !errors.Is(mkdirErr, unix.EEXIST) {
					return -1, closeBeforeReturn(
						cleanupCtx,
						materializer.ops,
						currentFD,
						wrapSystemError("create destination parent", mkdirErr),
						"close parent after create failure",
					)
				}
			} else {
				created = true
			}
			childFD, openErr = openDirectoryAt(ctx, materializer.ops, currentFD, component)
		}
		if openErr != nil {
			return -1, closeBeforeReturn(
				cleanupCtx,
				materializer.ops,
				currentFD,
				wrapSystemError("open destination parent", openErr),
				"close rejected parent",
			)
		}
		if created {
			if err := setCreatedDirectory(
				ctx,
				childFD,
				materializer.helperUID,
				materializer.helperGID,
			); err != nil {
				return -1, closePairBeforeReturn(
					cleanupCtx,
					materializer.ops,
					currentFD,
					childFD,
					err,
				)
			}
		}
		if err := verifyDirectory(
			ctx,
			childFD,
			materializer.helperUID,
			materializer.helperGID,
		); err != nil {
			return -1, closePairBeforeReturn(
				cleanupCtx,
				materializer.ops,
				currentFD,
				childFD,
				err,
			)
		}
		if created {
			if err := materializer.ops.fsync(currentFD); err != nil {
				return -1, closePairBeforeReturn(
					cleanupCtx,
					materializer.ops,
					currentFD,
					childFD,
					wrapSystemError("sync created parent", err),
				)
			}
		}
		if err := materializer.ops.close(currentFD); err != nil {
			return -1, closeBeforeReturn(
				cleanupCtx,
				materializer.ops,
				childFD,
				wrapSystemError("close traversed parent", err),
				"close child after parent close failure",
			)
		}
		currentFD = childFD
	}
	return currentFD, nil
}

func (materializer *materializer) reconcileOrphans(ctx context.Context, parentFD int) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	readFD, err := openDirectoryAt(ctx, materializer.ops, parentFD, ".")
	if err != nil {
		return wrapSystemError("open destination directory for reconciliation", err)
	}
	directory := os.NewFile(uintptr(readFD), "entry-materializer-directory")
	entries, readErr := directory.ReadDir(-1)
	closeErr := directory.Close()
	var operationErr error
	if readErr != nil {
		operationErr = wrapSystemError("read destination directory", readErr)
	}
	var cleanupErr error
	if closeErr != nil {
		cleanupErr = wrapSystemError("close reconciliation directory", closeErr)
	}
	if result := preferCleanupError(operationErr, cleanupErr); result != nil {
		return result
	}

	removed := false
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), entrymaterialization.TemporaryPrefix) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return finishDirectoryMutation(
				context.WithoutCancel(ctx),
				materializer.ops,
				parentFD,
				removed,
				err,
			)
		}
		if err := inspectOrphan(ctx, materializer.ops, parentFD, entry.Name()); err != nil {
			return finishDirectoryMutation(
				context.WithoutCancel(ctx),
				materializer.ops,
				parentFD,
				removed,
				err,
			)
		}
		if err := materializer.ops.unlinkat(parentFD, entry.Name(), 0); err != nil {
			return finishDirectoryMutation(
				context.WithoutCancel(ctx),
				materializer.ops,
				parentFD,
				removed,
				wrapSystemError("remove orphaned temporary", err),
			)
		}
		removed = true
	}
	return finishDirectoryMutation(
		context.WithoutCancel(ctx),
		materializer.ops,
		parentFD,
		removed,
		ctx.Err(),
	)
}

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

type publication struct {
	rootFD      int
	parentFD    int
	temporary   *temporary
	destination string
	ops         linuxOps
}

func (publication *publication) publish(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return publication.abort(context.WithoutCancel(ctx), err)
	}
	if err := publication.ops.renameat(
		publication.parentFD,
		publication.temporary.name,
		publication.parentFD,
		publication.destination,
	); err != nil {
		return publication.abort(
			context.WithoutCancel(ctx),
			wrapSystemError("publish temporary", err),
		)
	}
	publication.temporary.present = false
	result := error(nil)
	if err := publication.ops.fsync(publication.parentFD); err != nil {
		result = wrapSystemError("sync published destination", err)
	} else {
		result = ctx.Err()
	}
	return closeOperationDescriptors(
		context.WithoutCancel(ctx),
		publication.ops,
		publication.rootFD,
		publication.parentFD,
		result,
	)
}

func (publication *publication) abort(ctx context.Context, operationErr error) error {
	result := preferCleanupError(
		operationErr,
		cleanupTemporary(ctx, publication.ops, publication.parentFD, publication.temporary),
	)
	return closeOperationDescriptors(
		ctx,
		publication.ops,
		publication.rootFD,
		publication.parentFD,
		result,
	)
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

func inspectOrphan(ctx context.Context, ops linuxOps, parentFD int, name string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	fd, err := openPathAt(ctx, ops, parentFD, name)
	if err != nil {
		return wrapSystemError("open orphaned temporary", err)
	}
	var stat unix.Stat_t
	statErr := unix.Fstat(fd, &stat)
	closeErr := ops.close(fd)
	var operationErr error
	if statErr != nil {
		operationErr = wrapSystemError("inspect orphaned temporary", statErr)
	}
	var cleanupErr error
	if closeErr != nil {
		cleanupErr = wrapSystemError("close orphaned temporary", closeErr)
	}
	if result := preferCleanupError(operationErr, cleanupErr); result != nil {
		return result
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 {
		return internalError("orphaned temporary is unsafe")
	}
	return nil
}

func setCreatedDirectory(ctx context.Context, fd int, uid, gid uint32) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := unix.Fchown(fd, int(uid), int(gid)); err != nil {
		return wrapSystemError("set created parent ownership", err)
	}
	if err := unix.Fchmod(fd, directoryMode); err != nil {
		return wrapSystemError("set created parent mode", err)
	}
	return nil
}

func setCreatedTemporary(ctx context.Context, fd int, uid, gid uint32) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := unix.Fchown(fd, int(uid), int(gid)); err != nil {
		return wrapSystemError("set created temporary ownership", err)
	}
	if err := unix.Fchmod(fd, temporaryMode); err != nil {
		return wrapSystemError("set created temporary mode", err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return wrapSystemError("inspect created temporary", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Uid != uid ||
		stat.Gid != gid ||
		stat.Mode&0o7777 != temporaryMode ||
		stat.Size != 0 {
		return internalError("created temporary metadata mismatch")
	}
	return nil
}

func verifyDirectory(ctx context.Context, fd int, uid, gid uint32) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return wrapSystemError("inspect directory", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Uid != uid || stat.Gid != gid ||
		stat.Mode&0o7777 != directoryMode {
		return internalError("directory metadata mismatch")
	}
	return nil
}

func openDirectoryAt(ctx context.Context, ops linuxOps, parentFD int, name string) (int, error) {
	if err := contextError(ctx); err != nil {
		return -1, err
	}
	return ops.openat2(parentFD, name, &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW,
		Resolve: beneathPolicy,
	})
}

func openPathAt(ctx context.Context, ops linuxOps, parentFD int, name string) (int, error) {
	if err := contextError(ctx); err != nil {
		return -1, err
	}
	return ops.openat2(parentFD, name, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_CLOEXEC | unix.O_NOFOLLOW,
		Resolve: beneathPolicy,
	})
}

func cleanupTemporary(
	ctx context.Context,
	ops linuxOps,
	parentFD int,
	temporaryFile *temporary,
) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	var cleanupErrors []error
	if temporaryFile.open {
		if err := ops.close(temporaryFile.fd); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("close temporary: %w", err))
		}
		temporaryFile.open = false
	}
	if temporaryFile.present {
		if err := ops.unlinkat(parentFD, temporaryFile.name, 0); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("remove temporary: %w", err))
		} else {
			temporaryFile.present = false
			if err := ops.fsync(parentFD); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("sync temporary removal: %w", err))
			}
		}
	}
	if len(cleanupErrors) != 0 {
		return wrapSystemError("plaintext cleanup failed", errors.Join(cleanupErrors...))
	}
	return nil
}

func finishDirectoryMutation(
	ctx context.Context,
	ops linuxOps,
	parentFD int,
	mutated bool,
	operationErr error,
) error {
	if err := contextError(ctx); err != nil {
		return preferCleanupError(operationErr, err)
	}
	if mutated {
		if err := ops.fsync(parentFD); err != nil {
			return preferCleanupError(
				operationErr,
				wrapSystemError("sync orphan reconciliation", err),
			)
		}
	}
	return operationErr
}

func closeBeforeReturn(
	ctx context.Context,
	ops linuxOps,
	fd int,
	operationErr error,
	closeOperation string,
) error {
	if err := contextError(ctx); err != nil {
		return preferCleanupError(operationErr, err)
	}
	if err := ops.close(fd); err != nil {
		return preferCleanupError(operationErr, wrapSystemError(closeOperation, err))
	}
	return operationErr
}

func closePairBeforeReturn(
	ctx context.Context,
	ops linuxOps,
	firstFD int,
	secondFD int,
	operationErr error,
) error {
	result := closeBeforeReturn(
		ctx,
		ops,
		firstFD,
		operationErr,
		"close parent after traversal failure",
	)
	return closeBeforeReturn(ctx, ops, secondFD, result, "close child after traversal failure")
}

func closeOperationDescriptors(
	ctx context.Context,
	ops linuxOps,
	rootFD int,
	parentFD int,
	operationErr error,
) error {
	result := closeBeforeReturn(ctx, ops, parentFD, operationErr, "close destination directory")
	return closeBeforeReturn(ctx, ops, rootFD, result, "close operation root")
}

func preferCleanupError(operationErr, cleanupErr error) error {
	if cleanupErr == nil {
		return operationErr
	}
	if errors.Is(cleanupErr, context.Canceled) ||
		errors.Is(cleanupErr, context.DeadlineExceeded) {
		return internalError("mandatory cleanup failed")
	}
	if operationErr == nil {
		return cleanupErr
	}
	if errors.Is(operationErr, context.Canceled) ||
		errors.Is(operationErr, context.DeadlineExceeded) {
		return cleanupErr
	}
	return errs.Wrap(errs.KindInternal, errors.Join(cleanupErr, operationErr))
}

func wrapSystemError(operation string, err error) error {
	return errs.Wrap(errs.KindInternal, fmt.Errorf("entry materializer: %s: %w", operation, err))
}
