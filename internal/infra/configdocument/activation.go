package configdocument

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Initialize creates only an absent, fixed bootstrap document; it never
// overwrites an operator file or follows a symlink.
func Initialize(ctx context.Context, path string, content []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !filepath.IsAbs(path) {
		return errs.New(errs.KindValidationFailed, "configuration path must be absolute")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, fs.ErrExist) {
		_, readErr := readDocument(path)
		return readErr
	}
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	written, writeErr := file.Write(content)
	if writeErr == nil && written != len(content) {
		writeErr = io.ErrShortWrite
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return errs.Wrap(errs.KindInternal, errors.Join(err, os.Remove(path)))
	}
	return syncDirectory(filepath.Dir(path))
}

// Replaced returns only a durably completed replacement receipt. Consumers use
// this to resume their own activation/rollback without repeating a failed edit.
func (s *Store) Replaced(ctx context.Context, key string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := readDocument(s.path)
	if err != nil {
		return false, err
	}
	if err := s.recoverPending(ctx, current); err != nil {
		return false, err
	}
	record, found, err := s.readReplay(key)
	return found && record.State == replayCompleted, err
}

// MarkApplied updates observation only after the consumer has verified the
// exact document is active. A newer saved revision remains pending.
func (s *Store) MarkApplied(content []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startupRevision = revision(content)
}
