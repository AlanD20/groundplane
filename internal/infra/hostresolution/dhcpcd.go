package hostresolution

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

const (
	StateDirectory  = agentprotocol.StatePath + "/host-resolution"
	DHCPGuardPath   = "/usr/lib/dhcpcd/dhcpcd-hooks/19-groundplane-dns"
	DHCPReleasePath = "/usr/lib/dhcpcd/dhcpcd-hooks/21-groundplane-dns"
	DHCPHookHeader  = "# Groundplane-owned dhcpcd DNS integration.\n"
)

//go:embed dhcpcd-guard.sh
var dhcpGuardTemplate string

//go:embed dhcpcd-release.sh
var dhcpReleaseTemplate string

// EnsureDHCPHook installs a passive guard where dhcpcd is already installed.
// It neither changes the current resolver nor restarts or reconfigures a link.
// The guard lets DHCP manage DNS except while the live file belongs to GP.
func EnsureDHCPHook(ctx context.Context) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "host DNS integration context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	directory := filepath.Dir(DHCPGuardPath)
	parent, err := unix.Open(directory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer unix.Close(parent)
	var info unix.Stat_t
	if err := unix.Fstat(parent, &info); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if info.Uid != 0 || info.Mode&0o022 != 0 {
		return errs.New(errs.KindStateConflict, "dhcpcd hook directory is not root-controlled")
	}
	if _, err := os.Stat("/usr/bin/flock"); err != nil {
		return errs.Wrap(errs.KindStateConflict, err)
	}
	if err := os.MkdirAll(StateDirectory, 0o700); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	state, err := unix.Open(StateDirectory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	stateErr := unix.Fstat(state, &info)
	if err := errors.Join(stateErr, unix.Close(state)); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if info.Uid != 0 || info.Mode&0o022 != 0 {
		return errs.New(errs.KindStateConflict, "host resolver state directory is not root-controlled")
	}
	// Publish the release first: no installed guard may omit its matching close.
	if err := publishDHCPHook(parent, DHCPReleasePath, []byte(dhcpReleaseTemplate)); err != nil {
		return err
	}
	return publishDHCPHook(parent, DHCPGuardPath,
		[]byte(strings.ReplaceAll(dhcpGuardTemplate, "@STATE@", StateDirectory)))
}

func publishDHCPHook(parent int, path string, content []byte) error {
	var info unix.Stat_t
	fd, err := unix.Openat(
		parent,
		filepath.Base(path),
		unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC,
		0,
	)
	if err == nil {
		file := os.NewFile(uintptr(fd), path)
		statErr := unix.Fstat(fd, &info)
		current, readErr := io.ReadAll(io.LimitReader(file, 8193))
		closeErr := file.Close()
		if err := errors.Join(statErr, readErr, closeErr); err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		if info.Mode&unix.S_IFMT != unix.S_IFREG || info.Uid != 0 || info.Nlink != 1 || info.Mode&0o022 != 0 ||
			len(current) > 8192 || !bytes.HasPrefix(current, []byte(DHCPHookHeader)) {
			return errs.New(errs.KindStateConflict, "reserved GP DNS hook has foreign ownership or content")
		}
		if bytes.Equal(current, content) {
			return nil
		}
	} else if !errors.Is(err, unix.ENOENT) {
		return errs.Wrap(errs.KindInternal, err)
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".groundplane-dns-*")
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err := file.Write(content); err != nil {
		return errs.WrapJoined(errs.KindInternal, err, file.Close())
	}
	if err := errors.Join(file.Chmod(0o644), file.Sync(), file.Close()); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := os.Rename(name, path); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := unix.Fsync(parent); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}
