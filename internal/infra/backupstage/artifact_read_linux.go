package backupstage

import (
	"context"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"sync"
)

// Open returns a dup/pread reader over the retained inode; no name lookup occurs.
func (artifact *Artifact) Open(ctx context.Context) (io.ReadCloser, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if artifact == nil || artifact.stage == nil {
		return nil, internalError("artifact is required")
	}
	stage := artifact.stage
	stage.mu.Lock()
	defer stage.mu.Unlock()
	if artifact.state != artifactPublished || artifact.file == nil || stage.closed || stage.cleaned {
		return nil, internalError("artifact is not published")
	}
	if err := validateRegularFD(ctx, int(artifact.file.Fd()), artifact.size, 1); err != nil {
		return nil, joinPrivate(internalError("published artifact metadata is invalid"), err)
	}
	fd, err := unix.FcntlInt(artifact.file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, systemError("duplicate published artifact descriptor", err)
	}
	if artifact.readers == nil {
		artifact.readers = make(map[*preadCloser]struct{})
	}
	reader := &preadCloser{fd: fd, size: artifact.size, ctx: ctx, artifact: artifact}
	artifact.readers[reader] = struct{}{}
	return reader, nil
}

type preadCloser struct {
	mu       sync.Mutex
	fd       int
	offset   int64
	size     int64
	closed   bool
	ctx      context.Context
	artifact *Artifact
}

// Read implements io.Reader; the fixed signature is the standards exception
// to ctx-first, so it uses the context captured by Artifact.Open.
func (reader *preadCloser) Read(content []byte) (int, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.closed {
		return 0, systemError("read closed published artifact", os.ErrClosed)
	}
	if err := contextError(reader.ctx); err != nil {
		return 0, err
	}
	if reader.offset >= reader.size {
		return 0, io.EOF
	}
	if int64(len(content)) > reader.size-reader.offset {
		content = content[:reader.size-reader.offset]
	}
	n, err := unix.Pread(reader.fd, content, reader.offset)
	reader.offset += int64(n)
	if contextErr := contextError(reader.ctx); contextErr != nil {
		return n, contextErr
	}
	if err != nil {
		return n, systemError("read published artifact", err)
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

// Close implements io.Closer and therefore cannot take context. It follows
// the stage-mutex then reader-mutex order shared with cleanup.
func (reader *preadCloser) Close() error {
	if reader == nil {
		return nil
	}
	stage := reader.artifact.stage
	stage.mu.Lock()
	defer stage.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), stage.owner.ops.cleanupTimeout)
	defer cancel()
	return reader.closeLocked(ctx)
}

func (reader *preadCloser) closeLocked(ctx context.Context) error {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.closed {
		return nil
	}
	reader.closed = true
	delete(reader.artifact.readers, reader)
	if err := unix.Close(reader.fd); err != nil {
		return systemError("close published artifact reader", err)
	}
	reader.fd = -1
	return nil
}
