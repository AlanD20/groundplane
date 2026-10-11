package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/creack/pty"
)

const (
	hostShellPath = "/bin/bash"
	hostShellHome = "/root"
)

// HostShellSize is the validated terminal geometry for the fixed host shell.
type HostShellSize struct {
	Columns uint16
	Rows    uint16
}

// HostPTY is the one typed, fixed-policy PTY process exposed by the subprocess
// seam. Callers can exchange terminal bytes and resize or terminate the shell;
// they cannot choose an executable, argv, working directory, or environment.
type HostPTY struct {
	file        *os.File
	cmd         *exec.Cmd
	signalGroup func(int) error
	cgroupPath  string

	waitOnce sync.Once
	waitDone chan struct{}
	waitErr  error
	stopOnce sync.Once
	killOnce sync.Once
	killErr  error
}

// StartHostShell launches the Controller host's root login shell in a PTY.
// The Controller must already run as root. A fixed replacement environment
// prevents Controller secrets from leaking into the interactive shell.
func (r *OSRunner) StartHostShell(ctx context.Context, id string, size HostShellSize) (*HostPTY, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, errs.New(errs.KindInternal, "runner: host terminal context is unavailable")
	}
	if os.Geteuid() != 0 {
		return nil, errs.New(errs.KindValidationFailed, "host terminal requires a root Controller")
	}
	if size.Columns < 20 || size.Columns > 500 || size.Rows < 5 || size.Rows > 300 {
		return nil, errs.New(errs.KindValidationFailed, "host terminal size is out of range")
	}
	if err := ids.Validate(ids.KindTerminal, id); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "host terminal session id is invalid")
	}
	cgroupPath, cgroup, err := prepareHostTerminalCgroup(id)
	if err != nil {
		return nil, err
	}
	defer cgroup.Close()
	cmd := exec.Command(hostShellPath, "--login")
	cmd.Dir = hostShellHome
	cmd.Env = []string{
		"HOME=/root",
		"USER=root",
		"LOGNAME=root",
		"SHELL=/bin/bash",
		"TERM=xterm-256color",
		"COLORTERM=truecolor",
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"LANG=C.UTF-8",
		"HISTFILE=/dev/null",
		"HISTSIZE=0",
		"HISTFILESIZE=0",
	}
	if r.Logger != nil {
		r.Logger.Debug("runner: start fixed host PTY", "dir", hostShellHome)
	}
	file, tty, err := pty.Open()
	if err != nil {
		_ = os.Remove(cgroupPath)
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("runner: open host PTY: %w", err))
	}
	cmd.Stdin = tty
	cmd.Stdout = tty
	cmd.Stderr = tty
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true, Setctty: true, Ctty: 0,
		UseCgroupFD: true, CgroupFD: int(cgroup.Fd()),
	}
	if err := pty.Setsize(file, &pty.Winsize{Cols: size.Columns, Rows: size.Rows}); err != nil {
		_ = file.Close()
		_ = tty.Close()
		_ = os.Remove(cgroupPath)
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("runner: size host PTY: %w", err))
	}
	if err := cmd.Start(); err != nil {
		_ = file.Close()
		_ = tty.Close()
		_ = os.Remove(cgroupPath)
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("runner: start host PTY: %w", err))
	}
	_ = tty.Close()
	process := &HostPTY{
		file: file, cmd: cmd, signalGroup: r.processGroupSignaler(), cgroupPath: cgroupPath,
		waitDone: make(chan struct{}),
	}
	go process.wait()
	go func() {
		select {
		case <-ctx.Done():
			_ = process.Terminate()
		case <-process.waitDone:
		}
	}()
	return process, nil
}

func (p *HostPTY) Read(buffer []byte) (int, error)  { return p.file.Read(buffer) }
func (p *HostPTY) Write(buffer []byte) (int, error) { return p.file.Write(buffer) }

func (p *HostPTY) Resize(size HostShellSize) error {
	if size.Columns < 20 || size.Columns > 500 || size.Rows < 5 || size.Rows > 300 {
		return errs.New(errs.KindValidationFailed, "host terminal size is out of range")
	}
	if err := pty.Setsize(p.file, &pty.Winsize{Cols: size.Columns, Rows: size.Rows}); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("runner: resize host PTY: %w", err))
	}
	return nil
}

func (p *HostPTY) Wait() error {
	<-p.waitDone
	return p.waitErr
}

func (p *HostPTY) Terminate() error {
	var terminateErr error
	p.stopOnce.Do(func() {
		terminateErr = errors.Join(p.killOwnedProcesses(), p.file.Close())
	})
	return terminateErr
}

func (p *HostPTY) wait() {
	p.waitOnce.Do(func() {
		p.waitErr = p.cmd.Wait()
		if errors.Is(p.waitErr, io.EOF) {
			p.waitErr = nil
		}
		p.waitErr = errors.Join(p.waitErr, p.killOwnedProcesses())
		_ = p.file.Close()
		p.waitForCgroupRemoval()
		close(p.waitDone)
	})
}

func prepareHostTerminalCgroup(id string) (string, *os.File, error) {
	if _, err := os.Stat("/sys/fs/cgroup/cgroup.controllers"); err != nil {
		return "", nil, errs.New(errs.KindRequestUnavailable, "host terminal requires cgroup v2")
	}
	hostTerminalCgroupRoot, err := currentHostTerminalCgroupRoot()
	if err != nil {
		return "", nil, err
	}
	if err := os.MkdirAll(hostTerminalCgroupRoot, 0o700); err != nil {
		return "", nil, errs.Wrap(
			errs.KindRequestUnavailable,
			fmt.Errorf("runner: create host terminal cgroup root: %w", err),
		)
	}
	root, err := os.Lstat(hostTerminalCgroupRoot)
	if err != nil || !root.IsDir() || root.Mode()&os.ModeSymlink != 0 {
		return "", nil, errs.New(errs.KindRequestUnavailable, "host terminal cgroup root is invalid")
	}
	path := filepath.Join(hostTerminalCgroupRoot, id)
	if err := os.Mkdir(path, 0o700); err != nil {
		return "", nil, errs.Wrap(
			errs.KindRequestUnavailable,
			fmt.Errorf("runner: create host terminal cgroup: %w", err),
		)
	}
	if _, err := os.Stat(filepath.Join(path, "cgroup.kill")); err != nil {
		_ = os.Remove(path)
		return "", nil, errs.New(errs.KindRequestUnavailable, "host terminal cgroup kill is unavailable")
	}
	directory, err := os.Open(path)
	if err != nil {
		_ = os.Remove(path)
		return "", nil, errs.Wrap(errs.KindRequestUnavailable, fmt.Errorf("runner: open host terminal cgroup: %w", err))
	}
	return path, directory, nil
}

func currentHostTerminalCgroupRoot() (string, error) {
	raw, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "", errs.Wrap(errs.KindRequestUnavailable, fmt.Errorf("runner: read Controller cgroup: %w", err))
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "0::/") {
			continue
		}
		relative := strings.TrimPrefix(line, "0::")
		clean := filepath.Clean(relative)
		if !filepath.IsAbs(clean) || clean == "/.." || strings.HasPrefix(clean, "/../") {
			break
		}
		root := filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(clean, "/"), "groundplane-host-terminals")
		if root == "/sys/fs/cgroup/groundplane-host-terminals" || strings.HasPrefix(root, "/sys/fs/cgroup/") {
			return root, nil
		}
		break
	}
	return "", errs.New(errs.KindRequestUnavailable, "Controller cgroup v2 ownership is unavailable")
}

func (p *HostPTY) killOwnedProcesses() error {
	p.killOnce.Do(func() {
		p.killErr = os.WriteFile(filepath.Join(p.cgroupPath, "cgroup.kill"), []byte("1"), 0)
		if p.killErr != nil {
			p.killErr = errors.Join(p.killErr, p.signalGroup(p.cmd.Process.Pid))
		}
	})
	return p.killErr
}

func (p *HostPTY) waitForCgroupRemoval() {
	deadline := time.Now().Add(2 * time.Second)
	for {
		if err := os.Remove(p.cgroupPath); err == nil || errors.Is(err, os.ErrNotExist) {
			return
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
