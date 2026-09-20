package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/client"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func (operations *localOperations) StartDaemon(ctx context.Context, plan corerunner.Plan) error {
	controlRoot := filepath.Join(plan.Paths.SlotRoot, "control")
	for _, path := range []string{
		filepath.Join(controlRoot, "rootlesskit"),
		filepath.Join(controlRoot, "docker-exec"),
	} {
		if err := ensureOwnedDirectory(path, plan.Identity.UID, plan.Identity.GID, 0o700); err != nil {
			return err
		}
	}
	unit := daemonUnit(plan)
	args := []string{
		"--quiet", "--unit=" + unit,
		"--property=User=" + plan.Identity.User,
		"--property=Group=" + plan.Identity.Group,
		"--property=Environment=HOME=" + plan.Paths.RunnerHome,
		"--property=Environment=XDG_RUNTIME_DIR=" + filepath.Dir(plan.Paths.RawSocket),
		"--property=Restart=always", "--property=RestartSec=2",
		"--", "/usr/bin/rootlesskit",
		"--state-dir=" + filepath.Join(controlRoot, "rootlesskit"),
		"--net=slirp4netns", "--mtu=65520", "--disable-host-loopback",
		"--copy-up=/etc", "--copy-up=/run", "--propagation=rslave",
		"/usr/bin/dockerd", "--rootless", "--host=unix://" + plan.Paths.RawSocket,
		"--data-root=" + plan.Paths.DataRoot,
		"--exec-root=" + filepath.Join(controlRoot, "docker-exec"),
		"--pidfile=" + filepath.Join(controlRoot, "dockerd.pid"),
		"--storage-driver=fuse-overlayfs",
	}
	if err := operations.ensureUnit(ctx, unit, args); err != nil {
		return err
	}
	engine, err := waitForEngine(ctx, plan.Paths.RawSocket)
	if err != nil {
		return err
	}
	defer engine.Close()
	if err := ensureNetwork(ctx, engine, plan, false); err != nil {
		return err
	}
	return importImage(ctx, engine, plan.Container.ImageRef)
}

func (operations *localOperations) ObserveDaemon(
	ctx context.Context,
	plan corerunner.Plan,
) (corerunner.StepEvidence, error) {
	if err := operations.observeUnit(ctx, daemonUnit(plan)); err != nil {
		return corerunner.StepEvidence{}, err
	}
	engine, err := newEngine(plan.Paths.RawSocket)
	if err != nil {
		return corerunner.StepEvidence{}, err
	}
	defer engine.Close()
	if _, err := engine.ServerVersion(ctx, client.ServerVersionOptions{}); err != nil {
		return corerunner.StepEvidence{}, dockerError(ctx, "inspect Runner daemon", err)
	}
	if err := observeNetwork(ctx, engine, plan, false); err != nil {
		return corerunner.StepEvidence{}, err
	}
	return applied(corerunner.StepStartDaemon), nil
}

func (operations *localOperations) StopDaemon(ctx context.Context, plan corerunner.Plan) (string, error) {
	if err := operations.stopUnit(ctx, daemonUnit(plan)); err != nil {
		return "", err
	}
	return receipt(plan, corerunner.StepStopDaemon), nil
}

func (operations *localOperations) ObserveDaemonAbsent(
	ctx context.Context,
	plan corerunner.Plan,
) (corerunner.StepEvidence, error) {
	if err := operations.observeUnitAbsent(ctx, daemonUnit(plan)); err != nil {
		return corerunner.StepEvidence{}, err
	}
	return absent(plan, corerunner.StepStopDaemon), nil
}

func (operations *localOperations) daemonNonce(ctx context.Context, plan corerunner.Plan) (string, error) {
	result, err := operations.run(
		ctx, "systemctl", "show", "--property=MainPID", "--property=ExecMainStartTimestampMonotonic", daemonUnit(plan),
	)
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(string(result.Stdout))
	if value == "" || strings.Contains(value, "MainPID=0") {
		return "", errs.New(errs.KindStateConflict, "Runner daemon identity is unavailable")
	}
	digest := sha256.Sum256([]byte(plan.RunnerID + "\x00" + strconv.FormatUint(plan.RuntimeEpoch, 10) + "\x00" + value))
	return hex.EncodeToString(digest[:]), nil
}

func newEngine(socket string) (*client.Client, error) {
	engine, err := client.New(client.WithHost("unix://" + socket))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("create Runner Docker client: %w", err))
	}
	return engine, nil
}

func waitForEngine(ctx context.Context, socket string) (*client.Client, error) {
	deadline := time.Now().Add(startupTimeout)
	for {
		engine, err := newEngine(socket)
		if err == nil {
			_, versionErr := engine.ServerVersion(ctx, client.ServerVersionOptions{})
			if versionErr == nil {
				return engine, nil
			}
			_ = engine.Close()
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if time.Now().After(deadline) {
			return nil, errs.New(errs.KindInternal, "Runner Docker daemon did not become ready")
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func waitForSocket(ctx context.Context, path string) error {
	deadline := time.Now().Add(startupTimeout)
	for {
		if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSocket != 0 {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return errs.New(errs.KindInternal, "Runner socket did not become ready")
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func socketIdentity(path string) (uint64, uint64, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return 0, 0, errs.Wrap(errs.KindInternal, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || info.Mode()&os.ModeSocket == 0 || stat.Dev == 0 || stat.Ino == 0 {
		return 0, 0, errs.New(errs.KindStateConflict, "Runner socket identity changed")
	}
	return uint64(stat.Dev), stat.Ino, nil
}
