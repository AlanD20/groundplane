// materializer.go: Agent-side writes to the host filesystem — env files
// at 0600, and the /etc/resolv.conf rewrite. "Everything host-level is
// actioned by the Agent (locked)" — no host-level hands anywhere but the
// Agent's. See mvp.md. Every function here does file I/O, so ctx is
// first per docs/standards.md, section 4.
package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// MaterializeEnvFile atomically writes an already-rendered env file at 0600.
// The path is relative to rootDir and cannot escape it through path elements
// or symbolic links. Values are never logged or re-serialized by the Agent.
func MaterializeEnvFile(ctx context.Context, rootDir, relPath string, content []byte) error {
	return materializeFile(ctx, rootDir, relPath, content)
}

// MaterializeFileSecret writes a file secret at 0600, at a path always
// relative to the environment's volume folder (never escapes it — see
// mvp.md, "File secret paths are volume-relative (locked)").
func MaterializeFileSecret(ctx context.Context, volumeDir, relPath string, content []byte) error {
	return materializeFile(ctx, volumeDir, relPath, content)
}

func materializeFile(ctx context.Context, rootDir, relPath string, content []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !filepath.IsLocal(relPath) || filepath.Clean(relPath) == "." {
		return errs.Newf(errs.KindValidationFailed, "materializer: path %q is not root-relative", relPath)
	}
	if err := os.MkdirAll(rootDir, 0o700); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("materializer: create root: %w", err))
	}

	root, err := os.OpenRoot(rootDir)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("materializer: open root: %w", err))
	}
	defer root.Close()

	cleanPath := filepath.Clean(relPath)
	parentPath := filepath.Dir(cleanPath)
	if err := root.MkdirAll(parentPath, 0o700); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("materializer: create parent: %w", err))
	}
	parent, err := root.OpenRoot(parentPath)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("materializer: open parent: %w", err))
	}
	defer parent.Close()

	return writeAtomic(ctx, parent, filepath.Base(cleanPath), content)
}

func writeAtomic(ctx context.Context, root *os.Root, name string, content []byte) error {
	temporary, temporaryName, err := createTemporary(root)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("materializer: create temporary file: %w", err))
	}
	keepTemporary := true
	defer func() {
		if keepTemporary {
			_ = root.Remove(temporaryName)
		}
	}()

	if err := writeAndClose(ctx, temporary, content); err != nil {
		return err
	}
	if err := root.Rename(temporaryName, name); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("materializer: replace destination: %w", err))
	}
	keepTemporary = false

	directory, err := root.Open(".")
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("materializer: open destination directory: %w", err))
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("materializer: sync destination directory: %w", err))
	}
	return nil
}

func createTemporary(root *os.Root) (*os.File, string, error) {
	for range 10 {
		var random [12]byte
		if _, err := rand.Read(random[:]); err != nil {
			return nil, "", err
		}
		name := ".groundplane-" + hex.EncodeToString(random[:]) + ".tmp"
		file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			return file, name, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return nil, "", err
		}
	}
	return nil, "", fmt.Errorf("temporary filename collision limit reached")
}

func writeAndClose(ctx context.Context, file *os.File, content []byte) error {
	if err := ctx.Err(); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		return errs.Wrap(errs.KindInternal, fmt.Errorf("materializer: write temporary file: %w", err))
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return errs.Wrap(errs.KindInternal, fmt.Errorf("materializer: sync temporary file: %w", err))
	}
	if err := file.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("materializer: close temporary file: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// RewriteResolvConf points /etc/resolv.conf at 127.0.0.1 as the final
// step of applying the DNS (CoreDNS) service, and restores the previous
// content if CoreDNS is removed.
//
// TODO: back up the previous /etc/resolv.conf content the first time
// this runs, so removal can restore it exactly (see mvp.md, "DNS
// resolver (locked)").
func RewriteResolvConf(ctx context.Context, pointAtLocalhost bool) error {
	return errs.New(errs.KindNotImplemented, "materializer: RewriteResolvConf not implemented")
}
