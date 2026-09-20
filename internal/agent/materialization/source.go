package materialization

import (
	"bytes"
	"errors"
	"github.com/AlanD20/groundplane/pkg/errs"
	"io"
	"sync"
)

type ownedMaterializationSource struct {
	mu      sync.Mutex
	content []byte
	reader  *bytes.Reader
	closed  bool
}

func (source *ownedMaterializationSource) Read(destination []byte) (int, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.closed {
		return 0, io.EOF
	}
	return source.reader.Read(destination)
}

func (source *ownedMaterializationSource) Close() error {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.closed {
		return nil
	}
	clear(source.content)
	source.content = nil
	source.reader = bytes.NewReader(nil)
	source.closed = true
	return nil
}

func CloseSourceWithError(source io.ReadCloser, message string) error {
	if source == nil {
		return errs.New(errs.KindInternal, message)
	}
	if err := source.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, errors.Join(errs.New(errs.KindInternal, message), err))
	}
	return errs.New(errs.KindInternal, message)
}
