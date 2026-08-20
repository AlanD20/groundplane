package agentcredential

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"
	"syscall"

	"github.com/AlanD20/groundplane/internal/infra/docker/agentcontainer"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const temporaryTokenName = ".token.tmp"

func (m *Manager) materializeToken(ctx context.Context, agentID string, token []byte) error {
	paths, err := agentcontainer.RuntimePathsForAgent(agentID)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(m.hostRoot)
	if err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: open host root: %w", err))
	}
	defer root.Close()

	directoryName := rootedName(paths.Directory)
	if _, err := prepareRuntimeDirectory(root, directoryName, m.expectedUID, true); err != nil {
		return err
	}

	directory, err := root.OpenRoot(directoryName)
	if err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: open runtime directory: %w", err))
	}
	defer directory.Close()
	if err := validateReplaceTarget(directory, "token", m.expectedUID); err != nil {
		return err
	}
	if err := removeStaleTemporary(directory, m.expectedUID); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	temporary, err := directory.OpenFile(temporaryTokenName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o400)
	if err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: create temporary token file: %w", err))
	}
	temporaryExists := true
	defer func() {
		// Best effort: the primary operation reports any actionable close failure.
		_ = temporary.Close()
		if temporaryExists {
			// Best effort: preserve the primary failure when temporary cleanup also fails.
			_ = directory.Remove(temporaryTokenName)
		}
	}()
	if err := temporary.Chmod(0o400); err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: set temporary token mode: %w", err))
	}
	temporaryInfo, err := temporary.Stat()
	if err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: inspect temporary token: %w", err))
	}
	if err := validateOwnership(temporaryInfo, m.expectedUID, "temporary token"); err != nil {
		return err
	}
	if _, err := temporary.Write(token); err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: write temporary token: %w", err))
	}
	if err := temporary.Sync(); err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: sync temporary token: %w", err))
	}
	if err := temporary.Close(); err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: close temporary token: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.rename(directory, temporaryTokenName, "token"); err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: replace token: %w", err))
	}
	temporaryExists = false
	if err := syncRoot(directory); err != nil {
		return err
	}
	return nil
}

// DeleteRuntime removes only the validated per-Agent runtime directory. The
// caller must revoke the token before invoking this method.
func (m *Manager) DeleteRuntime(ctx context.Context, agentID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	paths, err := agentcontainer.RuntimePathsForAgent(agentID)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(m.hostRoot)
	if err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: open host root: %w", err))
	}
	defer root.Close()

	directoryName := rootedName(paths.Directory)
	exists, err := prepareRuntimeDirectory(root, directoryName, m.expectedUID, false)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := root.RemoveAll(directoryName); err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: remove runtime directory: %w", err))
	}
	parent, err := root.OpenRoot(path.Dir(directoryName))
	if err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: open runtime parent: %w", err))
	}
	defer parent.Close()
	return syncRoot(parent)
}

func prepareRuntimeDirectory(root *os.Root, name string, expectedUID uint32, create bool) (bool, error) {
	current := ""
	for index, component := range strings.Split(name, "/") {
		current = path.Join(current, component)
		info, err := root.Lstat(current)
		if errors.Is(err, fs.ErrNotExist) {
			if !create {
				return false, nil
			}
			if err := root.Mkdir(current, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
				return false, errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: create runtime directory: %w", err))
			}
			info, err = root.Lstat(current)
		}
		if err != nil {
			return false, errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: inspect runtime path: %w", err))
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return false, errs.New(errs.CodeInternal, "agent credential: runtime path contains a non-directory or symlink")
		}
		if err := validateOwnership(info, expectedUID, "runtime path component"); err != nil {
			return false, err
		}
		if create && index > 0 {
			if err := root.Chmod(current, 0o700); err != nil {
				return false, errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: set managed runtime directory mode: %w", err))
			}
		}
	}
	return true, nil
}

func validateReplaceTarget(root *os.Root, name string, expectedUID uint32) error {
	info, err := root.Lstat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: inspect existing token: %w", err))
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errs.New(errs.CodeInternal, "agent credential: existing token is not a regular file")
	}
	return validateOwnership(info, expectedUID, "existing token")
}

func removeStaleTemporary(root *os.Root, expectedUID uint32) error {
	info, err := root.Lstat(temporaryTokenName)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: inspect stale temporary token: %w", err))
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errs.New(errs.CodeInternal, "agent credential: stale temporary token is not a regular file")
	}
	if err := validateOwnership(info, expectedUID, "stale temporary token"); err != nil {
		return err
	}
	if err := root.Remove(temporaryTokenName); err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: remove stale temporary token: %w", err))
	}
	return nil
}

func validateOwnership(info fs.FileInfo, expectedUID uint32, label string) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errs.Newf(errs.CodeInternal, "agent credential: cannot determine %s ownership", label)
	}
	if stat.Uid != expectedUID {
		return errs.Newf(errs.CodeInternal, "agent credential: %s is owned by uid %d, want %d", label, stat.Uid, expectedUID)
	}
	return nil
}

func syncRoot(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: open directory for sync: %w", err))
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent credential: sync directory: %w", err))
	}
	return nil
}

func rootedName(absolute string) string {
	return strings.TrimPrefix(absolute, "/")
}
