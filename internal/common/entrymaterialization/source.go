package entrymaterialization

import (
	"context"
	"io"
	"sync"
)

// OwnBytes transfers value into an io.ReadCloser that clears bytes as they are
// read and clears every unread byte when closed.
func OwnBytes(value []byte) io.ReadCloser {
	return &byteSource{value: value}
}

type byteSource struct {
	mu     sync.Mutex
	value  []byte
	offset int
	closed bool
}

func (source *byteSource) Read(target []byte) (int, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.closed {
		return 0, io.ErrClosedPipe
	}
	if source.offset == len(source.value) {
		return 0, io.EOF
	}
	count := copy(target, source.value[source.offset:])
	clear(source.value[source.offset : source.offset+count])
	source.offset += count
	return count, nil
}

func (source *byteSource) Close() error {
	source.mu.Lock()
	defer source.mu.Unlock()
	clear(source.value)
	source.value = nil
	source.offset = 0
	source.closed = true
	return nil
}

type sourceGuard struct {
	source   io.ReadCloser
	stop     chan struct{}
	done     chan struct{}
	close    sync.Once
	closeErr error
}

func newSourceGuard(ctx context.Context, source io.ReadCloser) *sourceGuard {
	guard := &sourceGuard{
		source: source,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go func() {
		defer close(guard.done)
		select {
		case <-ctx.Done():
			guard.closeSource()
		case <-guard.stop:
		}
	}()
	return guard
}

func (guard *sourceGuard) closeSource() {
	guard.close.Do(func() {
		guard.closeErr = guard.source.Close()
	})
}

func (guard *sourceGuard) finish(result error) error {
	close(guard.stop)
	<-guard.done
	guard.closeSource()
	if guard.closeErr != nil {
		return protocolError("content source close failed")
	}
	return result
}

func writeAll(ctx context.Context, destination io.Writer, value []byte) (int, error) {
	written := 0
	for written < len(value) {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		count, err := destination.Write(value[written:])
		if count < 0 || count > len(value)-written {
			return written, protocolError("invalid writer result")
		}
		written += count
		if err != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return written, contextErr
			}
			return written, protocolError("payload write failed")
		}
		if count == 0 {
			return written, protocolError("short payload write")
		}
	}
	return written, nil
}
