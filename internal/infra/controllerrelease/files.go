// Package controllerrelease owns the private native Controller release store.
// Public construction fixes its host path and root ownership; request values
// can select only validated digest leaves, never arbitrary filesystem paths.
package controllerrelease

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	Directory                = "/var/lib/groundplane/controller-updates"
	BinaryDirectory          = "/usr/local/libexec/groundplane"
	MaximumBinaryBytes int64 = 256 << 20
)

type Store struct {
	root *os.Root
	uid  uint32
}

// Open requires bootstrap to have installed the private root-owned directory.
// Absence is not repaired silently by an operator update request.
func Open(ctx context.Context) (*Store, error) {
	return openStore(ctx, Directory, 0)
}

func openStore(ctx context.Context, path string, uid uint32) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errs.New(
			errs.KindInternal,
			"controller release root must be an absolute clean path",
		)
	}
	root, err := os.OpenRoot("/")
	if err != nil {
		return nil, fileError(err)
	}
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		next, openErr := openDirectory(ctx, root, part)
		closeErr := root.Close()
		if openErr != nil {
			return nil, openErr
		}
		if closeErr != nil {
			_ = next.Close() // Preserve the failed parent close as the primary error.
			return nil, fileError(closeErr)
		}
		root = next
	}
	if err := privateDirectory(ctx, root, uid); err != nil {
		_ = root.Close() // Cleanup only; retain the validation error.
		return nil, err
	}
	return &Store{root: root, uid: uid}, nil
}

func (store *Store) Close() error {
	return store.root.Close()
}

func openDirectory(ctx context.Context, parent *os.Root, name string) (*os.Root, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	before, err := parent.Lstat(name)
	if err != nil {
		return nil, fileError(err)
	}
	if !before.IsDir() {
		return nil, unsafeFile()
	}
	root, err := parent.OpenRoot(name)
	if err != nil {
		return nil, fileError(err)
	}
	after, err := root.Stat(".")
	if err != nil || !os.SameFile(before, after) {
		_ = root.Close() // Discard a raced/invalid directory handle.
		return nil, unsafeFile()
	}
	return root, nil
}

func privateDirectory(ctx context.Context, root *os.Root, uid uint32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := root.Stat(".")
	if err != nil {
		return fileError(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uid || !info.IsDir() || info.Mode().Perm() != 0o700 {
		return unsafeFile()
	}
	return nil
}

func (store *Store) releaseDirectory(ctx context.Context, id upgrade.Digest) (*os.Root, error) {
	if !id.Valid() {
		return nil, errs.New(errs.KindValidationFailed, "controller release digest is invalid")
	}
	releases, err := openDirectory(ctx, store.root, "releases")
	if err != nil {
		return nil, err
	}
	defer releases.Close()
	if err := privateDirectory(ctx, releases, store.uid); err != nil {
		return nil, err
	}
	root, err := openDirectory(ctx, releases, string(id)[7:])
	if err != nil {
		return nil, err
	}
	if err := privateDirectory(ctx, root, store.uid); err != nil {
		_ = root.Close() // Cleanup only after unsafe staging was rejected.
		return nil, err
	}
	return root, nil
}

func (store *Store) openRegular(
	ctx context.Context,
	root *os.Root,
	name string,
	maximum int64,
) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, '/') {
		return nil, unsafeFile()
	}
	directory, err := root.Open(".")
	if err != nil {
		return nil, fileError(err)
	}
	defer directory.Close()
	// os.Root may resolve in-root symlinks itself. Open the single leaf relative
	// to its pinned directory descriptor so O_NOFOLLOW is a kernel boundary.
	fd, err := unix.Openat(
		int(directory.Fd()),
		name,
		unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC,
		0,
	)
	if err != nil {
		return nil, fileError(err)
	}
	file := os.NewFile(uintptr(fd), name)
	info, err := file.Stat()
	if err == nil {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != store.uid || stat.Nlink != 1 || !info.Mode().IsRegular() ||
			info.Mode().Perm()&0o222 != 0 || info.Size() <= 0 || info.Size() > maximum {
			err = unsafeFile()
		}
	}
	if err != nil {
		_ = file.Close() // Cleanup only after invalid input was rejected.
		return nil, fileError(err)
	}
	return file, nil
}

func (store *Store) read(
	ctx context.Context,
	root *os.Root,
	name string,
	maximum int64,
) ([]byte, error) {
	file, err := store.openRegular(ctx, root, name, maximum)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, fileError(err)
	}
	if int64(len(raw)) > maximum {
		return nil, unsafeFile()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return raw, nil
}

// Inspect rehashes the opened candidate without executing it.
func (store *Store) Inspect(ctx context.Context, id upgrade.Digest) (upgrade.Manifest, error) {
	root, err := store.releaseDirectory(ctx, id)
	if err != nil {
		return upgrade.Manifest{}, err
	}
	defer root.Close()
	raw, err := store.read(ctx, root, "manifest.json", upgrade.MaxManifestBytes)
	if err != nil {
		return upgrade.Manifest{}, err
	}
	manifest, err := upgrade.ParseManifest(raw, id)
	if err != nil {
		return upgrade.Manifest{}, err
	}
	file, err := store.openRegular(ctx, root, "controller", MaximumBinaryBytes)
	if err != nil {
		return upgrade.Manifest{}, err
	}
	defer file.Close()
	if err := verifyCopy(ctx, io.Discard, file, manifest.ControllerSHA256); err != nil {
		return upgrade.Manifest{}, err
	}
	return manifest, nil
}

func verifyCopy(
	ctx context.Context,
	output io.Writer,
	input io.Reader,
	expected upgrade.Digest,
) error {
	if !expected.Valid() {
		return errs.New(errs.KindValidationFailed, "controller executable digest is invalid")
	}
	hash := sha256.New()
	destination := io.MultiWriter(output, hash)
	buffer := make([]byte, 32<<10)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		count, readErr := input.Read(buffer)
		total += int64(count)
		if total > MaximumBinaryBytes {
			return unsafeFile()
		}
		if count > 0 {
			if _, err := destination.Write(buffer[:count]); err != nil {
				return fileError(err)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return fileError(readErr)
		}
	}
	if total == 0 || upgrade.Digest(fmt.Sprintf("sha256:%x", hash.Sum(nil))) != expected {
		return errs.New(
			errs.KindValidationFailed,
			"controller executable does not match the pinned digest",
		)
	}
	return nil
}

func (store *Store) copyExecutable(
	ctx context.Context,
	source *os.Root,
	from, to string,
	expected upgrade.Digest,
) error {
	file, err := store.openRegular(ctx, source, from, MaximumBinaryBytes)
	if err != nil {
		return err
	}
	defer file.Close()
	return store.atomicWrite(ctx, store.root, to, 0o500, func(output io.Writer) error {
		return verifyCopy(ctx, output, file, expected)
	})
}

func (store *Store) atomicWrite(
	ctx context.Context,
	root *os.Root,
	name string,
	mode fs.FileMode,
	write func(io.Writer) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := root.Lstat(name)
	if err == nil && !info.Mode().IsRegular() {
		return unsafeFile()
	}
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fileError(err)
	}
	temporary := ".pending-" + rand.Text()
	file, err := root.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fileError(err)
	}
	defer func() {
		_ = file.Close() // Closed explicitly on success; otherwise cleanup only.
		_ = root.Remove(
			temporary,
		) // Removes only this unpublished random leaf; may already have been renamed.
	}()
	if err := write(file); err != nil {
		return err
	}
	if err := file.Chmod(mode); err != nil {
		return fileError(err)
	}
	if err := file.Sync(); err != nil {
		return fileError(err)
	}
	if err := file.Close(); err != nil {
		return fileError(err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := root.Rename(temporary, name); err != nil {
		return fileError(err)
	}
	directory, err := root.Open(".")
	if err != nil {
		return fileError(err)
	}
	defer directory.Close()
	return fileError(directory.Sync())
}

func unsafeFile() error {
	return errs.New(
		errs.KindValidationFailed,
		"controller release path, ownership, mode or size is unsafe",
	)
}
func fileError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errs.KindOf(err); ok {
		return err
	}
	return errs.Wrap(errs.KindInternal, err)
}
