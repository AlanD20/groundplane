// Package agentlistener creates the fixed local Agent gRPC Unix socket.
package agentlistener

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"

	"github.com/AlanD20/groundplane/internal/infra/runtimepath"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const SocketPath = "/run/groundplane/controller/agent.sock"

func Listen(ctx context.Context) (net.Listener, error) {
	return listenAt(ctx, "/", 0)
}

func listenAt(ctx context.Context, hostRoot string, expectedUID uint32) (net.Listener, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(hostRoot)
	if err != nil {
		return nil, errs.Wrap(errs.CodeInternal, fmt.Errorf("agent listener: open host root: %w", err))
	}
	defer root.Close()

	directoryName := runtimepath.RootedName(filepath.Dir(SocketPath))
	if _, err := runtimepath.PrepareDirectory(ctx, root, directoryName, expectedUID, 1, true); err != nil {
		return nil, err
	}
	socketName := runtimepath.RootedName(SocketPath)
	if err := removeStaleSocket(root, socketName, expectedUID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	hostSocket := filepath.Join(hostRoot, filepath.FromSlash(socketName))
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: hostSocket, Net: "unix"})
	if err != nil {
		return nil, errs.Wrap(errs.CodeInternal, fmt.Errorf("agent listener: listen: %w", err))
	}
	listener.SetUnlinkOnClose(true)
	if err := root.Chmod(socketName, 0o600); err != nil {
		// Best-effort cleanup follows the primary mode-setting failure.
		_ = listener.Close()
		return nil, errs.Wrap(errs.CodeInternal, fmt.Errorf("agent listener: set socket mode: %w", err))
	}
	info, err := root.Lstat(socketName)
	if err != nil {
		// Best-effort cleanup follows the primary inspection failure.
		_ = listener.Close()
		return nil, errs.Wrap(errs.CodeInternal, fmt.Errorf("agent listener: inspect socket: %w", err))
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 {
		// Best-effort cleanup follows the primary type-validation failure.
		_ = listener.Close()
		return nil, errs.New(errs.CodeInternal, "agent listener: created path is not a Unix socket")
	}
	if err := runtimepath.ValidateOwnership(info, expectedUID, "Agent socket"); err != nil {
		// Best-effort cleanup follows the primary ownership failure.
		_ = listener.Close()
		return nil, err
	}
	return listener, nil
}

func removeStaleSocket(root *os.Root, name string, expectedUID uint32) error {
	info, err := root.Lstat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent listener: inspect stale socket: %w", err))
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 {
		return errs.New(errs.CodeInternal, "agent listener: stale path is not a Unix socket")
	}
	if err := runtimepath.ValidateOwnership(info, expectedUID, "stale Agent socket"); err != nil {
		return err
	}
	if err := root.Remove(name); err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent listener: remove stale socket: %w", err))
	}
	return nil
}
