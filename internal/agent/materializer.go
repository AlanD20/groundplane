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
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/runtimepath"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const temporaryFilePrefix = ".groundplane-"

type fileMaterializer struct {
	mu          sync.Mutex
	hostRoot    string
	expectedUID uint32
	random      io.Reader
	rename      func(*os.Root, string, string) error
}

func newFileMaterializer(hostRoot string, expectedUID uint32, random io.Reader) *fileMaterializer {
	return &fileMaterializer{
		hostRoot:    hostRoot,
		expectedUID: expectedUID,
		random:      random,
		rename: func(root *os.Root, oldName, newName string) error {
			return root.Rename(oldName, newName)
		},
	}
}

// The accepted Agent container runs as root. Keeping that identity explicit
// makes a misconfigured non-root Agent fail closed rather than silently
// changing the ownership contract of materialized secrets.
var hostFileMaterializer = newFileMaterializer("/", 0, rand.Reader)

// MaterializeEnvFile atomically writes an already-rendered env file at 0600.
// The destination is relative to volumeDir and cannot escape it through path
// elements or symbolic links. The function consumes and clears content before
// returning; callers must not retain or reuse that plaintext buffer.
func MaterializeEnvFile(ctx context.Context, volumeDir, relPath string, content []byte) error {
	return hostFileMaterializer.materialize(ctx, volumeDir, relPath, content)
}

// MaterializeFileSecret writes a file secret at 0600, at a path always
// relative to the environment's volume folder (never escapes it — see
// mvp.md, "File secret paths are volume-relative (locked)"). The function
// consumes and clears content before returning. ADR 0020 must define explicit
// workload ownership before this public operation may publish a file secret.
func MaterializeFileSecret(ctx context.Context, _, _ string, content []byte) error {
	defer clear(content)
	if err := ctx.Err(); err != nil {
		return err
	}
	return errs.New(
		errs.KindNotImplemented,
		"materializer: file-secret ownership contract is not accepted",
	)
}

func (m *fileMaterializer) materialize(
	ctx context.Context,
	volumeDir, relPath string,
	content []byte,
) error {
	defer clear(content)

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	volumeName, cleanPath, err := validateMaterializationPaths(volumeDir, relPath)
	if err != nil {
		return err
	}
	if m.hostRoot == "" || m.random == nil || m.rename == nil {
		return errs.New(errs.KindInternal, "materializer: invalid runtime configuration")
	}

	host, err := os.OpenRoot(m.hostRoot)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("materializer: open host root: %w", err))
	}
	// Best effort: closing a traversal handle has no publication result to
	// replace, so preserve the materialization error or success.
	defer host.Close()
	if err := validateOwnedDirectory(host, m.expectedUID, "host root"); err != nil {
		return err
	}

	volume, err := prepareOwnedDirectory(ctx, host, volumeName, m.expectedUID)
	if err != nil {
		return err
	}
	// Best effort: preserve the materialization result after releasing the
	// descriptor-rooted volume handle.
	defer volume.Close()

	parentPath := path.Dir(cleanPath)
	parent := volume
	if parentPath != "." {
		parent, err = prepareOwnedDirectory(ctx, volume, parentPath, m.expectedUID)
		if err != nil {
			return err
		}
		// Best effort: preserve the materialization result after releasing the
		// descriptor-rooted destination-parent handle.
		defer parent.Close()
	}

	return m.writeAtomic(ctx, parent, path.Base(cleanPath), content)
}

func validateMaterializationPaths(volumeDir, relPath string) (string, string, error) {
	cleanVolume := path.Clean(volumeDir)
	components := strings.Split(volumeDir, "/")
	if volumeDir == "" || !path.IsAbs(volumeDir) || cleanVolume != volumeDir || len(components) != 6 ||
		components[0] != "" || components[1] != "infra" ||
		components[2] != "vol" ||
		ids.Validate(ids.KindTenant, components[3]) != nil ||
		ids.Validate(ids.KindProject, components[4]) != nil ||
		ids.Validate(ids.KindEnvironment, components[5]) != nil {
		return "", "", errs.Newf(
			errs.KindValidationFailed,
			"materializer: volume directory %q is not a canonical environment volume",
			volumeDir,
		)
	}
	cleanPath := path.Clean(relPath)
	if relPath == "" || path.IsAbs(relPath) || cleanPath == "." || cleanPath != relPath ||
		strings.HasPrefix(cleanPath, "../") || strings.IndexByte(relPath, 0) >= 0 {
		return "", "", errs.Newf(
			errs.KindValidationFailed,
			"materializer: path %q is not canonical and volume-relative",
			relPath,
		)
	}
	return strings.TrimPrefix(cleanVolume, "/"), cleanPath, nil
}

func prepareOwnedDirectory(
	ctx context.Context,
	root *os.Root,
	relative string,
	expectedUID uint32,
) (*os.Root, error) {
	current := ""
	for _, component := range strings.Split(relative, "/") {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if component == "" || component == "." || component == ".." {
			return nil, errs.New(errs.KindInternal, "materializer: invalid prepared directory")
		}
		current = path.Join(current, component)
		info, err := root.Lstat(current)
		if errors.Is(err, fs.ErrNotExist) {
			if err := root.Mkdir(current, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
				return nil, errs.Wrap(
					errs.KindInternal,
					fmt.Errorf("materializer: create directory: %w", err),
				)
			}
			if err := syncRootPath(root, path.Dir(current)); err != nil {
				return nil, err
			}
			info, err = root.Lstat(current)
		}
		if err != nil {
			return nil, errs.Wrap(
				errs.KindInternal,
				fmt.Errorf("materializer: inspect directory: %w", err),
			)
		}
		if err := validateOwnedDirectoryInfo(info, expectedUID, "directory component"); err != nil {
			return nil, err
		}
	}

	directory, err := root.OpenRoot(relative)
	if err != nil {
		return nil, errs.Wrap(
			errs.KindInternal,
			fmt.Errorf("materializer: open directory: %w", err),
		)
	}
	if err := validateOwnedDirectory(directory, expectedUID, "opened directory"); err != nil {
		// Best effort: the validation error is the actionable primary failure.
		directory.Close()
		return nil, err
	}
	return directory, nil
}

func validateOwnedDirectory(root *os.Root, expectedUID uint32, label string) error {
	info, err := root.Lstat(".")
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("materializer: inspect %s: %w", label, err))
	}
	return validateOwnedDirectoryInfo(info, expectedUID, label)
}

func validateOwnedDirectoryInfo(info fs.FileInfo, expectedUID uint32, label string) error {
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errs.Newf(errs.KindInternal, "materializer: %s is not a real directory", label)
	}
	if err := runtimepath.ValidateOwnership(info, expectedUID, label); err != nil {
		return err
	}
	if info.Mode().Perm()&0o022 != 0 {
		return errs.Newf(
			errs.KindInternal,
			"materializer: %s is writable by another identity",
			label,
		)
	}
	return nil
}

func (m *fileMaterializer) writeAtomic(
	ctx context.Context,
	root *os.Root,
	name string,
	content []byte,
) error {
	if err := validateReplaceTarget(root, name, m.expectedUID); err != nil {
		return err
	}
	temporary, temporaryName, err := m.createTemporary(root)
	if err != nil {
		return errs.Wrap(
			errs.KindInternal,
			fmt.Errorf("materializer: create temporary file: %w", err),
		)
	}
	temporaryOpen := true
	keepTemporary := true
	defer func() {
		if temporaryOpen {
			// Best effort: preserve the primary write or publication failure.
			_ = temporary.Close()
		}
		if keepTemporary {
			// Best effort: preserve the primary failure while attempting to erase
			// the unpublished plaintext artifact.
			_ = root.Remove(temporaryName)
		}
	}()

	if err := writeFile(ctx, temporary, content); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return errs.Wrap(
			errs.KindInternal,
			fmt.Errorf("materializer: close temporary file: %w", err),
		)
	}
	temporaryOpen = false
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateReplaceTarget(root, name, m.expectedUID); err != nil {
		return err
	}
	if err := m.rename(root, temporaryName, name); err != nil {
		return errs.Wrap(
			errs.KindInternal,
			fmt.Errorf("materializer: replace destination: %w", err),
		)
	}
	keepTemporary = false
	if err := syncDirectory(root); err != nil {
		return err
	}
	return ctx.Err()
}

func (m *fileMaterializer) createTemporary(root *os.Root) (*os.File, string, error) {
	for range 10 {
		var random [12]byte
		if _, err := io.ReadFull(m.random, random[:]); err != nil {
			return nil, "", err
		}
		name := temporaryFilePrefix + hex.EncodeToString(random[:]) + ".tmp"
		file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			if err := file.Chmod(0o600); err != nil {
				// Best effort: preserve the metadata failure during cleanup.
				file.Close()
				// Best effort: preserve the metadata failure while removing plaintext.
				_ = root.Remove(name)
				return nil, "", err
			}
			info, err := file.Stat()
			if err != nil {
				// Best effort: preserve the inspection failure during cleanup.
				file.Close()
				// Best effort: preserve the inspection failure while removing plaintext.
				_ = root.Remove(name)
				return nil, "", err
			}
			if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
				// Best effort: preserve the unsafe-inode failure during cleanup.
				file.Close()
				// Best effort: preserve the unsafe-inode failure while removing plaintext.
				_ = root.Remove(name)
				return nil, "", fmt.Errorf("temporary output is not a private regular file")
			}
			if err := runtimepath.ValidateOwnership(
				info,
				m.expectedUID,
				"temporary output",
			); err != nil {
				// Best effort: preserve the ownership failure during cleanup.
				file.Close()
				// Best effort: preserve the ownership failure while removing plaintext.
				_ = root.Remove(name)
				return nil, "", err
			}
			return file, name, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return nil, "", err
		}
	}
	return nil, "", fmt.Errorf("temporary filename collision limit reached")
}

func validateReplaceTarget(root *os.Root, name string, expectedUID uint32) error {
	info, err := root.Lstat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errs.Wrap(
			errs.KindInternal,
			fmt.Errorf("materializer: inspect destination: %w", err),
		)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errs.New(errs.KindInternal, "materializer: destination is not a regular file")
	}
	return runtimepath.ValidateOwnership(info, expectedUID, "existing destination")
}

func writeFile(ctx context.Context, file *os.File, content []byte) error {
	const writeChunk = 64 * 1024
	for offset := 0; offset < len(content); {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(offset+writeChunk, len(content))
		written, err := file.Write(content[offset:end])
		if err != nil {
			return errs.Wrap(
				errs.KindInternal,
				fmt.Errorf("materializer: write temporary file: %w", err),
			)
		}
		if written == 0 {
			return errs.Wrap(errs.KindInternal, io.ErrShortWrite)
		}
		offset += written
	}
	if err := file.Sync(); err != nil {
		return errs.Wrap(
			errs.KindInternal,
			fmt.Errorf("materializer: sync temporary file: %w", err),
		)
	}
	return nil
}

func syncRootPath(root *os.Root, relative string) error {
	directory := root
	if relative != "." {
		opened, err := root.OpenRoot(relative)
		if err != nil {
			return errs.Wrap(
				errs.KindInternal,
				fmt.Errorf("materializer: open created directory parent: %w", err),
			)
		}
		// Best effort: directory Sync above owns durability; preserve its result.
		defer opened.Close()
		directory = opened
	}
	return syncDirectory(directory)
}

func syncDirectory(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return errs.Wrap(
			errs.KindInternal,
			fmt.Errorf("materializer: open directory for sync: %w", err),
		)
	}
	// Best effort: the explicit Sync result, not descriptor close, owns the
	// materialization durability contract.
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("materializer: sync directory: %w", err))
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
