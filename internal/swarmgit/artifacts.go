package swarmgit

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/AlanD20/groundplane/internal/swarmcheck"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

const maxArtifactBytes = 256 << 10

func (inspector *Inspector) requireArtifact(
	ctx context.Context,
	wave swarmcheck.WaveID,
	value swarmcheck.RepoPath,
	expectedDigest swarmcheck.Digest,
) error {
	_, digest, err := inspector.readArtifact(ctx, wave, value)
	if err != nil {
		return err
	}
	if digest != expectedDigest {
		return invalid(fmt.Sprintf("artifact %s digest does not match the manifest", value))
	}
	return nil
}

func (inspector *Inspector) readArtifact(
	ctx context.Context,
	wave swarmcheck.WaveID,
	value swarmcheck.RepoPath,
) ([]byte, swarmcheck.Digest, error) {
	if err := swarmcheck.ValidateArtifactPath(wave, value); err != nil {
		return nil, "", err
	}
	if err := ctx.Err(); err != nil {
		return nil, "", errs.Wrap(errs.KindInternal, err)
	}
	directoryFD, err := openDirectoryBeneath(
		ctx,
		inspector.Root,
		filepath.ToSlash(filepath.Dir(string(value))),
		false,
	)
	if err != nil {
		return nil, "", err
	}
	name := filepath.Base(string(value))
	fileFD, err := unix.Openat2(directoryFD, name, &unix.OpenHow{
		Flags:   uint64(unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS,
	})
	closeDirectoryErr := unix.Close(directoryFD)
	if err != nil {
		return nil, "", invalid(fmt.Sprintf("open artifact %s without symlinks: %v", value, err))
	}
	if closeDirectoryErr != nil {
		// The directory close failure precedes ownership transfer to os.File.
		_ = unix.Close(fileFD)
		return nil, "", errs.Wrap(errs.KindInternal, closeDirectoryErr)
	}
	file := os.NewFile(uintptr(fileFD), string(value))
	if file == nil {
		// NewFile documents nil only for an invalid descriptor.
		_ = unix.Close(fileFD)
		return nil, "", invalid(fmt.Sprintf("artifact %s descriptor is invalid", value))
	}
	info, err := file.Stat()
	if err != nil {
		// The stat error is authoritative; Close is best-effort cleanup.
		_ = file.Close()
		return nil, "", invalid(fmt.Sprintf("stat artifact %s: %v", value, err))
	}
	if !info.Mode().IsRegular() || info.Size() > maxArtifactBytes {
		// The artifact type error is authoritative; Close is best-effort cleanup.
		_ = file.Close()
		return nil, "", invalid(fmt.Sprintf("artifact %s is not a bounded regular file", value))
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxArtifactBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, "", invalid(fmt.Sprintf("read artifact %s: %v", value, readErr))
	}
	if closeErr != nil {
		return nil, "", invalid(fmt.Sprintf("close artifact %s: %v", value, closeErr))
	}
	if len(data) > maxArtifactBytes {
		return nil, "", invalid(fmt.Sprintf("artifact %s exceeds bounded size", value))
	}
	if err := ctx.Err(); err != nil {
		return nil, "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(data)
	return data, swarmcheck.Digest(hex.EncodeToString(digest[:])), nil
}

func (inspector *Inspector) writeArtifactAtomically(
	ctx context.Context,
	wave swarmcheck.WaveID,
	value swarmcheck.RepoPath,
	data []byte,
) error {
	if err := swarmcheck.ValidateArtifactPath(wave, value); err != nil {
		return err
	}
	if len(data) > maxArtifactBytes {
		return invalid(fmt.Sprintf("artifact %s exceeds bounded size", value))
	}
	directoryFD, err := openDirectoryBeneath(
		ctx,
		inspector.Root,
		filepath.ToSlash(filepath.Dir(string(value))),
		true,
	)
	if err != nil {
		return err
	}
	writeErr := writeArtifactAtDirectory(ctx, directoryFD, filepath.Base(string(value)), data)
	closeErr := unix.Close(directoryFD)
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return errs.Wrap(errs.KindInternal, closeErr)
	}
	return nil
}

func writeArtifactAtDirectory(
	ctx context.Context,
	directoryFD int,
	name string,
	data []byte,
) error {
	if name == "" || name == "." || name == ".." || strings.Contains(name, "/") {
		return invalid("artifact filename is invalid")
	}
	if err := ctx.Err(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	temporaryName, temporaryFD, err := createTemporaryAt(ctx, directoryFD)
	if err != nil {
		return err
	}
	removeTemporary := true
	defer func() {
		if removeTemporary {
			// Best-effort cleanup cannot replace the artifact write error.
			_ = unix.Unlinkat(directoryFD, temporaryName, 0)
		}
	}()
	temporary := os.NewFile(uintptr(temporaryFD), temporaryName)
	if temporary == nil {
		// NewFile documents nil only for an invalid descriptor.
		_ = unix.Close(temporaryFD)
		return invalid("artifact temporary descriptor is invalid")
	}
	if _, err := temporary.Write(data); err != nil {
		// The write error is authoritative; Close is best-effort cleanup.
		_ = temporary.Close()
		return invalid(fmt.Sprintf("write gate artifact: %v", err))
	}
	if err := ctx.Err(); err != nil {
		// Cancellation is authoritative; Close is best-effort cleanup.
		_ = temporary.Close()
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := temporary.Sync(); err != nil {
		// The sync error is authoritative; Close is best-effort cleanup.
		_ = temporary.Close()
		return invalid(fmt.Sprintf("sync gate artifact: %v", err))
	}
	if err := temporary.Close(); err != nil {
		return invalid(fmt.Sprintf("close gate artifact: %v", err))
	}
	if err := unix.Renameat(directoryFD, temporaryName, directoryFD, name); err != nil {
		return invalid(fmt.Sprintf("replace gate artifact: %v", err))
	}
	removeTemporary = false
	if err := unix.Fsync(directoryFD); err != nil {
		return invalid(fmt.Sprintf("sync gate artifact directory: %v", err))
	}
	return nil
}

func createTemporaryAt(ctx context.Context, directoryFD int) (string, int, error) {
	for range 16 {
		if err := ctx.Err(); err != nil {
			return "", -1, errs.Wrap(errs.KindInternal, err)
		}
		random := make([]byte, 16)
		if _, err := rand.Read(random); err != nil {
			return "", -1, errs.Wrap(errs.KindInternal, err)
		}
		name := ".gate.tmp-" + hex.EncodeToString(random)
		fd, err := unix.Openat(
			directoryFD,
			name,
			unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW,
			0o600,
		)
		if err == nil {
			return name, fd, nil
		}
		if !errors.Is(err, unix.EEXIST) {
			return "", -1, invalid(fmt.Sprintf("create gate artifact temporary file: %v", err))
		}
	}
	return "", -1, invalid("create gate artifact temporary file exhausted retries")
}

func openDirectoryBeneath(
	ctx context.Context,
	root string,
	relative string,
	create bool,
) (int, error) {
	if err := ctx.Err(); err != nil {
		return -1, errs.Wrap(errs.KindInternal, err)
	}
	rootFD, err := unix.Openat2(unix.AT_FDCWD, root, &unix.OpenHow{
		Flags:   uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC),
		Resolve: unix.RESOLVE_NO_SYMLINKS,
	})
	if err != nil {
		return -1, invalid(fmt.Sprintf("open repository root without symlinks: %v", err))
	}
	currentFD := rootFD
	for _, segment := range strings.Split(relative, "/") {
		if err := ctx.Err(); err != nil {
			// Cancellation is authoritative; Close is best-effort cleanup.
			_ = unix.Close(currentFD)
			return -1, errs.Wrap(errs.KindInternal, err)
		}
		if segment == "" || segment == "." || segment == ".." {
			// The path validation error is authoritative; Close is best-effort cleanup.
			_ = unix.Close(currentFD)
			return -1, invalid("artifact directory is not canonical")
		}
		if create {
			if err := unix.Mkdirat(
				currentFD,
				segment,
				0o700,
			); err != nil &&
				!errors.Is(err, unix.EEXIST) {
				// The mkdir error is authoritative; Close is best-effort cleanup.
				_ = unix.Close(currentFD)
				return -1, invalid(fmt.Sprintf("create artifact directory %s: %v", segment, err))
			}
		}
		nextFD, err := unix.Openat2(currentFD, segment, &unix.OpenHow{
			Flags:   uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC),
			Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS,
		})
		closeErr := unix.Close(currentFD)
		if err != nil {
			return -1, invalid(
				fmt.Sprintf("open artifact directory %s without symlinks: %v", segment, err),
			)
		}
		if closeErr != nil {
			// The close error precedes transfer to the next pinned directory.
			_ = unix.Close(nextFD)
			return -1, errs.Wrap(errs.KindInternal, closeErr)
		}
		currentFD = nextFD
	}
	return currentFD, nil
}
