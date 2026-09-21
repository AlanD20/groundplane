package entrymaterializer

import (
	"context"
	"golang.org/x/sys/unix"
)

func setCreatedDirectory(ctx context.Context, fd int, uid, gid uint32) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := unix.Fchown(fd, int(uid), int(gid)); err != nil {
		return wrapSystemError("set created parent ownership", err)
	}
	if err := unix.Fchmod(fd, directoryMode); err != nil {
		return wrapSystemError("set created parent mode", err)
	}
	return nil
}

func setCreatedTemporary(ctx context.Context, fd int, uid, gid uint32) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := unix.Fchown(fd, int(uid), int(gid)); err != nil {
		return wrapSystemError("set created temporary ownership", err)
	}
	if err := unix.Fchmod(fd, temporaryMode); err != nil {
		return wrapSystemError("set created temporary mode", err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return wrapSystemError("inspect created temporary", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Uid != uid ||
		stat.Gid != gid ||
		stat.Mode&0o7777 != temporaryMode ||
		stat.Size != 0 {
		return internalError("created temporary metadata mismatch")
	}
	return nil
}

func verifyDirectory(ctx context.Context, fd int, uid, gid uint32) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return wrapSystemError("inspect directory", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Uid != uid || stat.Gid != gid ||
		stat.Mode&0o7777 != directoryMode {
		return internalError("directory metadata mismatch")
	}
	return nil
}

func openDirectoryAt(ctx context.Context, ops linuxOps, parentFD int, name string) (int, error) {
	if err := contextError(ctx); err != nil {
		return -1, err
	}
	return ops.openat2(parentFD, name, &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW,
		Resolve: beneathPolicy,
	})
}

func openPathAt(ctx context.Context, ops linuxOps, parentFD int, name string) (int, error) {
	if err := contextError(ctx); err != nil {
		return -1, err
	}
	return ops.openat2(parentFD, name, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_CLOEXEC | unix.O_NOFOLLOW,
		Resolve: beneathPolicy,
	})
}
