package agentcredential

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/infra/docker/agentcontainer"
	"github.com/AlanD20/groundplane/internal/infra/runtimepath"
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

const (
	temporaryConfigName = ".config.yaml.tmp"
	temporaryTokenName  = ".token.tmp"
)

type runtimeFile struct {
	name          string
	temporaryName string
	label         string
	mode          os.FileMode
	contents      []byte
}

// Materialize recreates the complete volatile Agent runtime from durable
// ciphertext and typed configuration. Token plaintext exists only in memory
// during this call and is cleared before the call returns.
func (m *Manager) Materialize(
	ctx context.Context,
	agentID string,
	encryptedToken EncryptedToken,
	runtimeConfig config.AgentConfig,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	paths, err := agentcontainer.RuntimePathsForAgent(agentID)
	if err != nil {
		return err
	}
	if runtimeConfig.AgentID != agentID {
		return errs.New(errs.KindInternal, "agent runtime: durable config agent id does not match runtime path")
	}
	if err := runtimeConfig.Validate(); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("agent runtime: invalid durable config: %w", err))
	}
	if len(encryptedToken.Ciphertext) == 0 {
		return errs.New(errs.KindInternal, "agent runtime: encrypted token is empty")
	}

	ciphertext := append([]byte(nil), encryptedToken.Ciphertext...)
	defer clear(ciphertext)
	rawToken, err := m.opener.Open(ctx, ciphertext)
	if err != nil {
		return errs.New(errs.KindInternal, "agent runtime: decrypt token")
	}
	defer clear(rawToken)
	if len(rawToken) != agentprotocol.RawTokenBytes {
		return errs.New(errs.KindInternal, "agent runtime: decrypted token has invalid length")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	encodedToken := make([]byte, agentprotocol.EncodedTokenBytes)
	defer clear(encodedToken)
	base64.RawURLEncoding.Encode(encodedToken, rawToken)
	configContents, err := yaml.Marshal(runtimeConfig)
	if err != nil {
		return errs.New(errs.KindInternal, "agent runtime: encode config")
	}

	root, err := os.OpenRoot(m.hostRoot)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("agent runtime: open host root: %w", err))
	}
	defer root.Close()

	directoryName := runtimepath.RootedName(paths.Directory)
	if _, err := runtimepath.PrepareDirectory(ctx, root, directoryName, m.expectedUID, 1, true); err != nil {
		return err
	}
	directory, err := root.OpenRoot(directoryName)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("agent runtime: open runtime directory: %w", err))
	}
	defer directory.Close()

	files := []runtimeFile{
		{
			name:          path.Base(paths.Config),
			temporaryName: temporaryConfigName,
			label:         "config",
			mode:          0o444,
			contents:      configContents,
		},
		{
			name:          path.Base(paths.Token),
			temporaryName: temporaryTokenName,
			label:         "token",
			mode:          0o400,
			contents:      encodedToken,
		},
	}
	for _, file := range files {
		if err := m.writeRuntimeFile(ctx, directory, file); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) writeRuntimeFile(ctx context.Context, directory *os.Root, file runtimeFile) error {
	if err := validateReplaceTarget(directory, file.name, file.label, m.expectedUID); err != nil {
		return err
	}
	if err := removeStaleTemporary(directory, file.temporaryName, file.label, m.expectedUID); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	temporary, err := directory.OpenFile(file.temporaryName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, file.mode)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("agent runtime: create temporary %s: %w", file.label, err))
	}
	temporaryOpen := true
	temporaryExists := true
	defer func() {
		if temporaryOpen {
			// Best effort: preserve the primary operation failure.
			_ = temporary.Close()
		}
		if temporaryExists {
			// Best effort: preserve the primary operation failure.
			_ = directory.Remove(file.temporaryName)
		}
	}()
	if err := temporary.Chmod(file.mode); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("agent runtime: set temporary %s mode: %w", file.label, err))
	}
	temporaryInfo, err := temporary.Stat()
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("agent runtime: inspect temporary %s: %w", file.label, err))
	}
	if err := runtimepath.ValidateOwnership(temporaryInfo, m.expectedUID, "temporary "+file.label); err != nil {
		return err
	}
	if _, err := temporary.Write(file.contents); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("agent runtime: write temporary %s: %w", file.label, err))
	}
	if err := temporary.Sync(); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("agent runtime: sync temporary %s: %w", file.label, err))
	}
	if err := temporary.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("agent runtime: close temporary %s: %w", file.label, err))
	}
	temporaryOpen = false
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.rename(directory, file.temporaryName, file.name); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("agent runtime: replace %s: %w", file.label, err))
	}
	temporaryExists = false
	return runtimepath.SyncDirectory(ctx, directory)
}

// Remove removes only the validated per-Agent runtime directory. The caller
// must revoke the token before invoking this method.
func (m *Manager) Remove(ctx context.Context, agentID string) error {
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
		return errs.Wrap(errs.KindInternal, fmt.Errorf("agent runtime: open host root: %w", err))
	}
	defer root.Close()

	directoryName := runtimepath.RootedName(paths.Directory)
	exists, err := runtimepath.PrepareDirectory(ctx, root, directoryName, m.expectedUID, 1, false)
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
		return errs.Wrap(errs.KindInternal, fmt.Errorf("agent runtime: remove runtime directory: %w", err))
	}
	parent, err := root.OpenRoot(path.Dir(directoryName))
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("agent runtime: open runtime parent: %w", err))
	}
	defer parent.Close()
	return runtimepath.SyncDirectory(ctx, parent)
}

func validateReplaceTarget(root *os.Root, name, label string, expectedUID uint32) error {
	info, err := root.Lstat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("agent runtime: inspect existing %s: %w", label, err))
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errs.Newf(errs.KindInternal, "agent runtime: existing %s is not a regular file", label)
	}
	return runtimepath.ValidateOwnership(info, expectedUID, "existing "+label)
}

func removeStaleTemporary(root *os.Root, name, label string, expectedUID uint32) error {
	info, err := root.Lstat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("agent runtime: inspect stale temporary %s: %w", label, err))
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errs.Newf(errs.KindInternal, "agent runtime: stale temporary %s is not a regular file", label)
	}
	if err := runtimepath.ValidateOwnership(info, expectedUID, "stale temporary "+label); err != nil {
		return err
	}
	if err := root.Remove(name); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("agent runtime: remove stale temporary %s: %w", label, err))
	}
	return nil
}
