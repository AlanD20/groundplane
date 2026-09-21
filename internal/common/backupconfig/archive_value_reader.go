package backupconfig

import (
	"context"
	"io"
	"sync"
)

type authenticatedValueReader struct {
	mu       sync.Mutex
	ctx      context.Context
	value    []byte
	offset   int
	closed   bool
	consumed bool
	onClose  func(bool)
}

func (reader *authenticatedValueReader) Read(destination []byte) (int, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.closed {
		return 0, io.EOF
	}
	if err := checkContext(reader.ctx); err != nil {
		reader.closeLocked()
		return 0, err
	}
	if reader.offset == len(reader.value) {
		reader.consumed = true
		clearBytes(reader.value)
		return 0, io.EOF
	}
	count := copy(destination, reader.value[reader.offset:])
	reader.offset += count
	if reader.offset == len(reader.value) {
		reader.consumed = true
		clearBytes(reader.value)
	}
	return count, nil
}

func (reader *authenticatedValueReader) Close() error {
	if reader == nil {
		return nil
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.closed {
		return nil
	}
	reader.closeLocked()
	return nil
}

func (reader *authenticatedValueReader) closeLocked() {
	clearBytes(reader.value)
	reader.closed = true
	reader.offset = len(reader.value)
	closed := reader.onClose
	reader.onClose = nil
	if closed != nil {
		closed(reader.consumed)
	}
}
