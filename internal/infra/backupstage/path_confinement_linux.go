package backupstage

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

func openConfiguredRoot(ctx context.Context, root string, ops linuxOperations) (int, error) {
	current, err := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, systemError("open filesystem root", err)
	}
	for _, component := range strings.Split(strings.TrimPrefix(root, "/"), "/") {
		if err := contextError(ctx); err != nil {
			return -1, closeWithPrimary(ctx, err, current)
		}
		next, openErr := openDirectoryAt(ctx, current, component, rootResolvePolicy, ops)
		if openErr != nil {
			if errors.Is(openErr, unix.ENOSYS) {
				return -1, closeWithPrimary(ctx, requiredLinuxError("openat2"), current)
			}
			return -1, closeWithPrimary(ctx, systemError("resolve configured stage root", openErr), current)
		}
		if closeErr := unix.Close(current); closeErr != nil {
			return -1, closeWithPrimary(ctx,
				systemError("close traversed stage root descriptor", closeErr), next)
		}
		current = next
	}
	if err := validateDirectoryFD(ctx, current); err != nil {
		return -1, closeWithPrimary(ctx, err, current)
	}
	return current, nil
}

func openOrCreateDirectory(ctx context.Context, parentFD int, name string, ops linuxOperations) (int, error) {
	if err := contextError(ctx); err != nil {
		return -1, err
	}
	fd, err := openDirectoryAt(ctx, parentFD, name, descendantResolvePolicy, ops)
	created := false
	if errors.Is(err, unix.ENOENT) {
		mkdirErr := unix.Mkdirat(parentFD, name, directoryMode)
		if mkdirErr != nil && !errors.Is(mkdirErr, unix.EEXIST) {
			return -1, storageSystemError("create staging directory", mkdirErr)
		}
		created = mkdirErr == nil
		fd, err = openDirectoryAt(ctx, parentFD, name, descendantResolvePolicy, ops)
	}
	if err != nil {
		return -1, systemError("open staging directory", err)
	}
	if created {
		if err := unix.Fchown(fd, 0, 0); err != nil {
			return -1, closeWithPrimary(ctx, systemError("set staging directory ownership", err), fd)
		}
		if err := unix.Fchmod(fd, directoryMode); err != nil {
			return -1, closeWithPrimary(ctx, systemError("set staging directory mode", err), fd)
		}
		if err := unix.Fsync(fd); err != nil {
			return -1, closeWithPrimary(ctx, storageSystemError("sync staging directory", err), fd)
		}
		if err := unix.Fsync(parentFD); err != nil {
			return -1, closeWithPrimary(ctx, storageSystemError("sync staging directory parent", err), fd)
		}
	}
	if err := validateDirectoryFD(ctx, fd); err != nil {
		return -1, closeWithPrimary(ctx,
			joinPrivate(internalError("managed staging directory namespace is ambiguous"), err), fd)
	}
	return fd, nil
}

func openDirectoryAt(
	ctx context.Context, parentFD int, name string, resolve uint64, ops linuxOperations,
) (int, error) {
	return ops.openat2(parentFD, name, &unix.OpenHow{
		Flags: uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: resolve,
	})
}

func openPathAt(ctx context.Context, parentFD int, name string, ops linuxOperations) (int, error) {
	return ops.openat2(parentFD, name, &unix.OpenHow{
		Flags: uint64(unix.O_PATH | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: descendantResolvePolicy,
	})
}

func validateRootPath(root string) (string, error) {
	if root == "" || !filepath.IsAbs(root) || strings.ContainsRune(root, 0) {
		return "", validationError("stage root must be an absolute path")
	}
	clean := filepath.Clean(root)
	if clean == "/" {
		return "", validationError("stage root cannot be filesystem root")
	}
	if root != clean || !utf8.ValidString(root) {
		return "", validationError("stage root must be clean UTF-8 without a trailing separator")
	}
	return clean, nil
}

func validateIDs(ids IDs) error {
	if err := validateTaskComponent(ids.Task); err != nil {
		return err
	}
	if err := validateComponent(ids.Step, "step id"); err != nil {
		return err
	}
	return validateComponent(ids.Point, "point id")
}

func validateTaskComponent(value string) error {
	if err := validateComponent(value, "task id"); err != nil {
		return err
	}
	if isPackageInternalName(value) || isManagedPartial(value) {
		return validationError("task id is reserved for backup staging")
	}
	return nil
}

// validateFinalArtifactName is the sole admission check for caller-provided
// final names. No accepted final may match recovery's managed partial grammar.
func validateFinalArtifactName(value string) error {
	if err := validateComponent(value, "artifact name"); err != nil {
		return err
	}
	if isPackageInternalName(value) || isManagedPartial(value) {
		return validationError("artifact name is reserved for backup staging")
	}
	return nil
}

func isPackageInternalName(value string) bool {
	switch value {
	case reservationDirName, growthMarkerName, ownerMarkerName:
		return true
	default:
		return false
	}
}

func validateComponent(value, label string) error {
	if value == "" || value == "." || value == ".." || strings.ContainsRune(value, 0) ||
		strings.ContainsAny(value, `/\`) || filepath.IsAbs(value) || len(value) > 255 {
		return validationError("%s is not a safe path component", label)
	}
	return nil
}

func validateDirectoryFD(ctx context.Context, fd int) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return systemError("inspect staging directory", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Uid != 0 || stat.Gid != 0 || stat.Mode&0o7777 != directoryMode {
		return validationError("staging directory must be root-owned with exact mode 0700")
	}
	return nil
}

func validateRegularFD(ctx context.Context, fd int, expectedSize int64, expectedLinks uint64) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return systemError("inspect staging file", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != expectedLinks || stat.Uid != 0 || stat.Gid != 0 ||
		stat.Mode&0o7777 != fileMode {
		return validationError("staging file has unsafe metadata")
	}
	if expectedSize >= 0 && stat.Size != expectedSize {
		return validationError("staging file size changed")
	}
	return nil
}

func validatePointContents(ctx context.Context, fd int, ops linuxOperations) error {
	names, err := directoryNames(ctx, fd, "point directory")
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := contextError(ctx); err != nil {
			return err
		}
		entryFD, err := openPathAt(ctx, fd, name, ops)
		if err != nil {
			return systemError("inspect pre-existing point entry", err)
		}
		metadataErr := validateRegularFD(ctx, entryFD, -1, 1)
		closeErr := unix.Close(entryFD)
		if metadataErr != nil {
			return joinPrivate(internalError("managed point namespace is ambiguous"), metadataErr,
				rawOperationError("close pre-existing point entry descriptor", closeErr))
		}
		if closeErr != nil {
			return systemError("close pre-existing point entry descriptor", closeErr)
		}
	}
	return nil
}

func probeRenameat2(ctx context.Context, ops linuxOperations) error {
	err := ops.renameat2(-1, ".", -1, ".", unix.RENAME_NOREPLACE)
	if errors.Is(err, unix.ENOSYS) {
		return requiredLinuxError("renameat2")
	}
	if !errors.Is(err, unix.EBADF) {
		return systemError("probe renameat2 with deliberately invalid descriptors", err)
	}
	return nil
}
