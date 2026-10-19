package agentworkjournal

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"golang.org/x/sys/unix"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type Journal struct {
	mu        sync.Mutex
	root      *os.Root
	directory *os.File
	uid       uint32
	closed    bool
}

// Open creates only the supplied leaf, never its parents, and pins its identity.
func Open(path string) (*Journal, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return nil, invalid()
	}
	created := false
	if err := os.Mkdir(path, 0o700); err == nil {
		created = true
	} else if !errors.Is(err, fs.ErrExist) {
		return nil, fileError(err)
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, fileError(err)
	}
	if !before.IsDir() {
		return nil, invalid()
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, fileError(err)
	}
	directory, err := root.Open(".")
	if err != nil {
		_ = root.Close()
		return nil, fileError(err)
	} // Cleanup after failed root pin.
	after, err := directory.Stat()
	if err != nil || !os.SameFile(before, after) || !privateDirectory(after, uint32(os.Geteuid())) {
		_ = directory.Close()
		_ = root.Close() // Reject a raced or unsafe root.
		return nil, invalid()
	}
	if created {
		parent, openErr := os.OpenFile(
			filepath.Dir(path),
			unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
			0,
		)
		if openErr == nil {
			openErr = errors.Join(parent.Sync(), parent.Close())
		}
		if openErr != nil {
			_ = directory.Close()
			_ = root.Close()
			return nil, fileError(openErr)
		} // Cleanup after failed creation durability.
	}
	return &Journal{root: root, directory: directory, uid: uint32(os.Geteuid())}, nil
}

func privateDirectory(info fs.FileInfo, uid uint32) bool {
	if info == nil {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && info.IsDir() && stat.Uid == uid && stat.Mode&0o7777 == 0o700
}

func (journal *Journal) Close() error {
	if journal == nil {
		return nil
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return nil
	}
	journal.closed = true
	return fileError(errors.Join(journal.directory.Close(), journal.root.Close()))
}

// PrepareScript durably allocates one sequence before the transport sends it.
// Read/prepare/write share a cross-process lock, not only an in-memory mutex.
func (journal *Journal) PrepareScript(ctx context.Context, attempt workexecution.Attempt,
	request *agentpb.ScriptCheckpointRequest, at time.Time,
) (ScriptRecord, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return ScriptRecord{}, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return ScriptRecord{}, err
	}
	if state == nil {
		attempts, err := journal.attempts(ctx)
		if err != nil {
			return ScriptRecord{}, err
		}
		if _, reserved := attempts[attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
			return ScriptRecord{}, errs.New(
				errs.KindStateConflict,
				"Agent work journal is full; settle retained attempts before accepting more work",
			)
		}
		state = &attemptRecord{Attempt: attempt}
	}
	previous, err := state.script()
	if err != nil {
		return ScriptRecord{}, err
	}
	if state.ScriptCompletion != nil || state.ScriptFailure != nil {
		return ScriptRecord{}, conflict()
	}
	if !workexecution.SameAssignment(state.Attempt, attempt) || state.Sequence == ^uint64(0) {
		return ScriptRecord{}, conflict()
	}
	next, err := PrepareScript(attempt, previous, state.Sequence+1, request, at)
	if err != nil {
		return ScriptRecord{}, err
	}
	if previous != nil && next.Report.Sequence == previous.Report.Sequence {
		return next, nil
	}
	pending, err := state.pending()
	if err != nil {
		return ScriptRecord{}, err
	}
	if pending || at.Before(state.UpdatedAt) {
		return ScriptRecord{}, conflict()
	}
	if state.Release != nil && (!state.Release.Progress.InFlight ||
		state.Release.Progress.Phase != releaseexecution.PreHooks && state.Release.Progress.Phase != releaseexecution.PostHooks) {
		return ScriptRecord{}, conflict()
	}
	if state.Recovery != nil && (!state.Recovery.Report.Progress.InFlight ||
		state.Recovery.Report.Progress.Phase != releaseexecution.FailureHooks || state.Recovery.Report.Ack == nil) {
		return ScriptRecord{}, conflict()
	}
	state.Script, err = encodeScript(next)
	if err != nil {
		return ScriptRecord{}, err
	}
	state.Sequence, state.UpdatedAt = next.Report.Sequence, next.Report.RecordedAt.AsTime()
	if err := journal.writeRecord(ctx, *state); err != nil {
		return ScriptRecord{}, err
	}
	return next, nil
}

func (journal *Journal) AcknowledgeScript(ctx context.Context, ack *agentpb.ScriptCheckpointAck) error {
	if ack == nil {
		return invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, ack.AttemptId)
	if err != nil {
		return err
	}
	if state == nil {
		return conflict()
	}
	previous, err := state.script()
	if err != nil {
		return err
	}
	if previous == nil {
		return conflict()
	}
	next, err := AcknowledgeScript(*previous, ack)
	if err != nil {
		return err
	}
	state.Script, err = encodeScript(next)
	if err != nil {
		return err
	}
	return journal.writeRecord(ctx, *state)
}

// Script returns the persisted report for exact replay. Reading it never
// changes executor identity or permits a replacement attempt to execute.
func (journal *Journal) Script(ctx context.Context, attemptID string) (*ScriptRecord, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attemptID)
	if err != nil || state == nil {
		return nil, err
	}
	return state.script()
}

func (journal *Journal) readRecord(ctx context.Context, attemptID string) (*attemptRecord, error) {
	if ids.Validate(ids.KindAttempt, attemptID) != nil {
		return nil, invalid()
	}
	file, err := journal.openLeaf(ctx, attemptID+".json", unix.O_RDONLY, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, MaximumAttemptRecordBytes+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return nil, fileError(errors.Join(readErr, closeErr))
	}
	record, err := decodeRecord(raw)
	clear(raw)
	if err != nil {
		return nil, err
	}
	if record.Attempt.ID != attemptID {
		return nil, invalid()
	}
	return &record, nil
}

func (journal *Journal) writeRecord(ctx context.Context, record attemptRecord) error {
	raw, err := encodeRecord(record)
	if err != nil {
		return err
	}
	defer clear(raw)
	name := record.Attempt.ID + ".next"
	if existing, err := journal.openLeaf(ctx, name, unix.O_RDONLY, 0); err == nil {
		if err := existing.Close(); err != nil {
			return fileError(err)
		}
		if err := journal.root.Remove(name); err != nil {
			return fileError(err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	file, err := journal.openLeaf(ctx, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	written, writeErr := file.Write(raw)
	if writeErr == nil && written != len(raw) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		return fileError(errors.Join(writeErr, closeErr))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := journal.root.Rename(name, record.Attempt.ID+".json"); err != nil {
		return fileError(err)
	}
	return fileError(journal.directory.Sync())
}

func (journal *Journal) openLeaf(ctx context.Context, name string, flags int, mode uint32) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fd, err := unix.Openat(
		int(journal.directory.Fd()),
		name,
		flags|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC,
		mode,
	)
	if err != nil {
		return nil, fileError(&os.PathError{Op: "openat", Path: name, Err: err})
	}
	file := os.NewFile(uintptr(fd), name)
	info, err := file.Stat()
	if err == nil {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || !info.Mode().IsRegular() || stat.Mode&0o7777 != 0o600 || stat.Uid != journal.uid || stat.Nlink != 1 ||
			info.Size() > MaximumAttemptRecordBytes {
			err = invalid()
		}
	}
	if err != nil {
		_ = file.Close()
		return nil, fileError(err)
	} // Reject an unsafe descriptor before I/O.
	return file, nil
}

func (journal *Journal) lock(ctx context.Context) (func(), error) {
	if journal == nil || ctx == nil {
		return nil, invalid()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	journal.mu.Lock()
	if journal.closed {
		journal.mu.Unlock()
		return nil, errs.New(errs.KindStateConflict, "Agent work journal is closed")
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := unix.Flock(int(journal.directory.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return func() {
				_ = unix.Flock(
					int(journal.directory.Fd()),
					unix.LOCK_UN,
				) // Cleanup releases only this operation's advisory lock.
				journal.mu.Unlock()
			}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EINTR) {
			journal.mu.Unlock()
			return nil, fileError(err)
		}
		select {
		case <-ctx.Done():
			journal.mu.Unlock()
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func fileError(err error) error {
	if err == nil {
		return nil
	}
	return errs.Wrap(errs.KindStorageUnavailable, err)
}
