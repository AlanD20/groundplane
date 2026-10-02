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
	return artifact.openReader(ctx, false)
}

// OpenPrefix returns a dup/pread reader over the currently written bytes of
// an open or published artifact. Later appends cannot extend this reader.
func (artifact *Artifact) OpenPrefix(ctx context.Context) (*ArtifactReader, error) {
	return artifact.openReader(ctx, true)
}

func (artifact *Artifact) openReader(ctx context.Context, allowPartial bool) (*ArtifactReader, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if artifact == nil || artifact.stage == nil {
		return nil, internalError("artifact is required")
	}
	stage := artifact.stage
	stage.mu.Lock()
	defer stage.mu.Unlock()
	if (artifact.state != artifactPublished && !(allowPartial && artifact.state == artifactOpen)) ||
		artifact.file == nil || stage.closed || stage.cleaned {
		return nil, internalError("artifact is not readable")
	}
	if err := validateRegularFD(ctx, int(artifact.file.Fd()), artifact.written, 1); err != nil {
		return nil, joinPrivate(internalError("artifact metadata is invalid"), err)
	}
	fd, err := unix.FcntlInt(artifact.file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, systemError("duplicate artifact descriptor", err)
	}
	if artifact.readers == nil {
		artifact.readers = make(map[*preadCloser]struct{})
	}
	reader := &preadCloser{fd: fd, size: artifact.written, ctx: ctx, artifact: artifact}
	artifact.readers[reader] = struct{}{}
	return &ArtifactReader{reader: reader}, nil
}

// ArtifactReader reads only the retained prefix captured when opened. ReadAt
// does not change the sequential Read position.
type ArtifactReader struct {
	reader *preadCloser
}

func (reader *ArtifactReader) Read(content []byte) (int, error) {
	if reader == nil || reader.reader == nil {
		return 0, internalError("artifact reader is required")
	}
	return reader.reader.Read(content)
}

func (reader *ArtifactReader) ReadAt(content []byte, offset int64) (int, error) {
	if reader == nil || reader.reader == nil {
		return 0, internalError("artifact reader is required")
	}
	reader.reader.mu.Lock()
	defer reader.reader.mu.Unlock()
	if offset < 0 {
		return 0, validationError("artifact read offset cannot be negative")
	}
	wanted := len(content)
	n, err := reader.reader.readAtLocked(content, offset)
	if err != nil {
		return n, err
	}
	if n != wanted {
		return n, io.EOF
	}
	return n, nil
}

func (reader *ArtifactReader) Close() error {
	if reader == nil || reader.reader == nil {
		return nil
	}
	return reader.reader.Close()
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
// to ctx-first, so it uses the context captured when the artifact was opened.
func (reader *preadCloser) Read(content []byte) (int, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	n, err := reader.readAtLocked(content, reader.offset)
	reader.offset += int64(n)
	return n, err
}

func (reader *preadCloser) readAtLocked(content []byte, offset int64) (int, error) {
	if reader.closed {
		return 0, systemError("read closed staged artifact", os.ErrClosed)
	}
	if err := contextError(reader.ctx); err != nil {
		return 0, err
	}
	if len(content) == 0 {
		return 0, nil
	}
	if offset >= reader.size {
		return 0, io.EOF
	}
	if int64(len(content)) > reader.size-offset {
		content = content[:reader.size-offset]
	}
	read := 0
	for read < len(content) {
		if err := contextError(reader.ctx); err != nil {
			return read, err
		}
		n, err := unix.Pread(reader.fd, content[read:], offset+int64(read))
		if n < 0 || n > len(content)-read {
			return read, internalError("staged artifact pread returned an invalid count")
		}
		read += n
		if contextErr := contextError(reader.ctx); contextErr != nil {
			return read, contextErr
		}
		if err != nil {
			return read, systemError("read staged artifact", err)
		}
		if n == 0 {
			return read, io.EOF
		}
	}
	return read, nil
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
		return systemError("close staged artifact reader", err)
	}
	reader.fd = -1
	return nil
}
