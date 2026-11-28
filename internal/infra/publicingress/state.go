package publicingress

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// StartupFilename is mounted read-only into the edge by a directory bind.
// The container must load this file instead of using its admin API autosave.
const StartupFilename = "startup.json"

const (
	stateFilename     = "activation.json"
	maximumStateBytes = 2*maximumConfigBytes + 4096
)

type recordedConfig struct {
	Receipt
	JSON json.RawMessage `json:"config"`
}

type activation struct {
	Current   recordedConfig  `json:"current"`
	Candidate *recordedConfig `json:"candidate,omitempty"`
	Restoring bool            `json:"restoring,omitempty"`
}

type stateDirectory struct {
	root *os.Root
	file *os.File
}

func (store *Store) open(ctx context.Context) (*stateDirectory, error) {
	if store == nil || ctx == nil {
		return nil, errs.New(errs.KindValidationFailed, "public ingress store or context is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fd, err := unix.Open(store.stateRoot, unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_RDONLY, 0)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	file := os.NewFile(uintptr(fd), store.stateRoot)
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		_ = file.Close()
		return nil, conflict("public ingress state directory is not private")
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, conflict("public ingress state directory is locked")
		}
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	root, err := os.OpenRoot(fmt.Sprintf("/proc/self/fd/%d", fd))
	if err != nil {
		_ = file.Close()
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return &stateDirectory{root: root, file: file}, nil
}

func (dir *stateDirectory) close() {
	// No writes depend on closing these handles. Closing the open directory
	// releases its exclusive lock after all explicit syncs above have settled.
	_ = dir.root.Close()
	_ = dir.file.Close()
}

func (dir *stateDirectory) requireState() (activation, error) {
	state, found, err := dir.load()
	if err != nil {
		return activation{}, err
	}
	if !found {
		return activation{}, conflict("public ingress startup ownership has not been recorded")
	}
	return state, nil
}

func (dir *stateDirectory) load() (activation, bool, error) {
	raw, found, err := dir.read(stateFilename, maximumStateBytes)
	if err != nil || !found {
		return activation{}, false, err
	}
	state, err := jcs.Decode[activation](raw)
	if err != nil || !recordedConfigValid(state.Current) ||
		(state.Candidate == nil && state.Restoring) ||
		(state.Candidate != nil &&
			(!recordedConfigValid(*state.Candidate) ||
				state.Candidate.RuntimeID != state.Current.RuntimeID ||
				state.Candidate.Revision <= state.Current.Revision ||
				state.Candidate.Digest == state.Current.Digest)) {
		return activation{}, false, conflict("public ingress activation receipt is invalid")
	}
	startup, found, err := dir.read(StartupFilename, maximumConfigBytes)
	if err != nil {
		return activation{}, false, err
	}
	// Between startup publication and the final receipt sync a crash can
	// leave either of the two sealed files as the only recoverable candidate.
	if !found || (!bytes.Equal(startup, state.Current.JSON) &&
		(state.Candidate == nil || !bytes.Equal(startup, state.Candidate.JSON))) {
		return activation{}, false, conflict("public ingress startup file has unknown ownership")
	}
	return state, true, nil
}

func recordedConfigValid(record recordedConfig) bool {
	captured, err := capture(Configuration{RuntimeID: record.RuntimeID, Revision: record.Revision, JSON: record.JSON})
	return err == nil && captured.Receipt == record.Receipt && bytes.Equal(captured.JSON, record.JSON)
}

func (dir *stateDirectory) commit(cfg recordedConfig) error {
	if err := dir.replace(StartupFilename, cfg.JSON); err != nil {
		return err
	}
	return dir.save(activation{Current: cfg})
}

func (dir *stateDirectory) save(state activation) error {
	encoded, err := json.Marshal(state)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	canonical, err := jcs.Canonicalize(encoded)
	if err != nil || len(canonical) > maximumStateBytes {
		return errs.New(errs.KindInternal, "public ingress activation receipt exceeds its storage bound")
	}
	return dir.replace(stateFilename, canonical)
}

func (dir *stateDirectory) read(name string, maximum int64) ([]byte, bool, error) {
	file, err := dir.root.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errs.Wrap(errs.KindInternal, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, false, errs.Wrap(errs.KindInternal, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() <= 0 || info.Size() > maximum {
		return nil, false, conflict("public ingress state file is unsafe or oversized")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, false, errs.Wrap(errs.KindInternal, err)
	}
	if int64(len(raw)) != info.Size() {
		return nil, false, conflict("public ingress state file changed during observation")
	}
	return raw, true, nil
}

func (dir *stateDirectory) replace(name string, value []byte) error {
	stage := ".activation." + ids.NewULID()
	file, err := dir.root.OpenFile(stage, os.O_CREATE|os.O_EXCL|os.O_WRONLY|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer dir.root.Remove(stage) // After publication this name is absent.
	_, writeErr := file.Write(value)
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := dir.root.Rename(stage, name); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := dir.file.Sync(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}
