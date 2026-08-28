package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"

	commandrunner "github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	hostDockerSocket = "/var/run/docker.sock"
	commandTimeout   = 30 * time.Second
	startupTimeout   = 90 * time.Second
	maximumOutput    = 64 * 1024
)

type localOperations struct{ commands commandrunner.Runner }

// NewLocal fixes the production Linux host boundary behind the validated
// HostControl facade. Docker API traffic uses the Runner's private socket;
// host administration commands retain the repository-wide subprocess seam.
func NewLocal(commands commandrunner.Runner) (*HostControl, error) {
	if commands == nil {
		return nil, errs.New(errs.KindInternal, "runner host command runner is required")
	}
	return New(&localOperations{commands: commands})
}

func (operations *localOperations) EnsureIdentity(ctx context.Context, plan corerunner.Plan) error {
	if err := operations.ensureGroup(ctx, plan); err != nil {
		return err
	}
	if err := operations.ensureUser(ctx, plan); err != nil {
		return err
	}
	if err := ensureSubordinateRange("/etc/subuid", plan.Identity.User, plan.Identity.SubUIDStart, plan.Identity.SubUIDCount); err != nil {
		return err
	}
	if err := ensureSubordinateRange("/etc/subgid", plan.Identity.User, plan.Identity.SubGIDStart, plan.Identity.SubGIDCount); err != nil {
		return err
	}
	if _, err := operations.run(ctx, "loginctl", "enable-linger", plan.Identity.User); err != nil {
		return err
	}
	if err := ensureOwnedDirectory(plan.Paths.SlotRoot, 0, 0, 0o700); err != nil {
		return err
	}
	for _, path := range []string{
		plan.Paths.RunnerHome,
		plan.Paths.WorkRoot,
		plan.Paths.DataRoot,
		filepath.Join(plan.Paths.SlotRoot, "control"),
		filepath.Dir(plan.Paths.ProxySocket),
	} {
		if err := ensureOwnedDirectory(path, plan.Identity.UID, plan.Identity.GID, 0o700); err != nil {
			return err
		}
	}
	return nil
}

func (operations *localOperations) ObserveIdentity(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	if err := operations.identityMatches(ctx, plan); err != nil {
		return corerunner.StepEvidence{}, err
	}
	return applied(corerunner.StepEnsureIdentity), nil
}

func (operations *localOperations) EnsureNetwork(ctx context.Context, plan corerunner.Plan) error {
	engine, err := newEngine(hostDockerSocket)
	if err != nil {
		return err
	}
	defer engine.Close()
	return ensureNetwork(ctx, engine, plan, true)
}

func (operations *localOperations) ObserveNetwork(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	engine, err := newEngine(hostDockerSocket)
	if err != nil {
		return corerunner.StepEvidence{}, err
	}
	defer engine.Close()
	if err := observeNetwork(ctx, engine, plan, true); err != nil {
		return corerunner.StepEvidence{}, err
	}
	return applied(corerunner.StepEnsureNetwork), nil
}

func (operations *localOperations) EnsureEgress(ctx context.Context, plan corerunner.Plan) error {
	table := nftTable(plan)
	_, _ = operations.run(ctx, "nft", "delete", "table", "inet", table)
	denied := make([]string, 0, len(plan.Egress.DeniedCIDRs))
	for _, prefix := range plan.Egress.DeniedCIDRs {
		denied = append(denied, prefix.String())
	}
	source := fmt.Sprintf(
		"table inet %s { chain output { type filter hook output priority 0; policy accept; ip daddr %s accept; meta skuid %d ip daddr { %s } reject; } }\n",
		table,
		plan.Egress.ControllerEndpoint.Addr(),
		plan.Egress.SourceUID,
		strings.Join(denied, ", "),
	)
	_, err := operations.runInput(ctx, []byte(source), "nft", "-f", "-")
	return err
}

func (operations *localOperations) ObserveEgress(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	result, err := operations.run(ctx, "nft", "list", "table", "inet", nftTable(plan))
	if err != nil {
		return corerunner.StepEvidence{}, err
	}
	want := strconv.FormatUint(uint64(plan.Egress.SourceUID), 10)
	if !strings.Contains(string(result.Stdout), want) {
		return corerunner.StepEvidence{}, errs.New(errs.KindStateConflict, "runner egress policy identity changed")
	}
	return applied(corerunner.StepEnsureEgress), nil
}

func (operations *localOperations) StartProxy(ctx context.Context, plan corerunner.Plan) error {
	unit := proxyUnit(plan)
	args := []string{
		"--quiet", "--unit=" + unit,
		"--property=User=" + plan.Identity.User,
		"--property=Group=" + plan.Identity.Group,
		"--property=Restart=always", "--property=RestartSec=1",
		"--", "/usr/bin/socat",
		"UNIX-LISTEN:" + plan.Paths.ProxySocket + ",fork,mode=0600,unlink-early",
		"UNIX-CONNECT:" + plan.Paths.RawSocket,
	}
	if err := operations.ensureUnit(ctx, unit, args); err != nil {
		return err
	}
	return waitForSocket(ctx, plan.Paths.ProxySocket)
}

func (operations *localOperations) ObserveProxy(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	if err := operations.observeUnit(ctx, proxyUnit(plan)); err != nil {
		return corerunner.StepEvidence{}, err
	}
	if _, _, err := socketIdentity(plan.Paths.ProxySocket); err != nil {
		return corerunner.StepEvidence{}, err
	}
	return applied(corerunner.StepStartProxy), nil
}

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

func (operations *localOperations) ObserveDaemon(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
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

func (operations *localOperations) StartRunner(
	ctx context.Context,
	plan corerunner.Plan,
	token []byte,
) (runnerallocation.RunnerRuntimeEvidence, error) {
	engine, err := newEngine(plan.Paths.ProxySocket)
	if err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, err
	}
	defer engine.Close()
	if evidence, found, inspectErr := operations.inspectRunner(ctx, engine, plan); inspectErr != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, inspectErr
	} else if found {
		return evidence, nil
	}

	options := runnerContainerOptions(plan)
	created, err := engine.ContainerCreate(ctx, options)
	if err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, dockerError(ctx, "create Runner container", err)
	}
	if created.ID == "" {
		return runnerallocation.RunnerRuntimeEvidence{}, errs.New(errs.KindInternal, "Runner Docker returned an empty container id")
	}
	keep := false
	defer func() {
		if !keep {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), commandTimeout)
			defer cancel()
			_, _ = engine.ContainerRemove(cleanupCtx, created.ID, client.ContainerRemoveOptions{Force: true})
		}
	}()

	attached, err := engine.ContainerAttach(ctx, created.ID, client.ContainerAttachOptions{Stream: true, Stdin: true})
	if err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, dockerError(ctx, "attach Runner registration input", err)
	}
	defer attached.Close()
	if _, err := engine.ContainerStart(ctx, created.ID, client.ContainerStartOptions{}); err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, dockerError(ctx, "start Runner container", err)
	}
	document := registrationDocument(plan, token)
	defer clear(document)
	if _, err := attached.Conn.Write(document); err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, dockerError(ctx, "write Runner registration input", err)
	}
	if err := attached.CloseWrite(); err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, dockerError(ctx, "close Runner registration input", err)
	}
	if err := waitForRunnerConfiguration(ctx, engine, plan); err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, err
	}
	evidence, found, err := operations.inspectRunner(ctx, engine, plan)
	if err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, err
	}
	if !found {
		return runnerallocation.RunnerRuntimeEvidence{}, errs.New(errs.KindInternal, "Runner container disappeared after registration")
	}
	keep = true
	return evidence, nil
}

func (operations *localOperations) ObserveRunner(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	engine, err := newEngine(plan.Paths.ProxySocket)
	if err != nil {
		return corerunner.StepEvidence{}, err
	}
	defer engine.Close()
	evidence, found, err := operations.inspectRunner(ctx, engine, plan)
	if err != nil {
		return corerunner.StepEvidence{}, err
	}
	if !found {
		return corerunner.StepEvidence{}, errs.New(errs.KindStateConflict, "Runner container is absent")
	}
	return corerunner.StepEvidence{
		Step: corerunner.StepStartRunner, State: corerunner.EffectApplied, Ownership: &evidence,
	}, nil
}

func (operations *localOperations) StopRunner(ctx context.Context, plan corerunner.Plan) (string, error) {
	engine, err := newEngine(plan.Paths.ProxySocket)
	if err != nil {
		return "", err
	}
	defer engine.Close()
	_, err = engine.ContainerRemove(ctx, plan.Container.Name, client.ContainerRemoveOptions{Force: true})
	if err != nil && !containerderrdefs.IsNotFound(err) {
		return "", dockerError(ctx, "remove Runner container", err)
	}
	return receipt(plan, corerunner.StepStopRunner), nil
}

func (operations *localOperations) ObserveRunnerAbsent(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	engine, err := newEngine(plan.Paths.ProxySocket)
	if err != nil {
		return corerunner.StepEvidence{}, err
	}
	defer engine.Close()
	_, err = engine.ContainerInspect(ctx, plan.Container.Name, client.ContainerInspectOptions{})
	if err == nil {
		return corerunner.StepEvidence{}, errs.New(errs.KindStateConflict, "Runner container still exists")
	}
	if !containerderrdefs.IsNotFound(err) {
		return corerunner.StepEvidence{}, dockerError(ctx, "observe Runner container absence", err)
	}
	return absent(plan, corerunner.StepStopRunner), nil
}

func (operations *localOperations) StopDaemon(ctx context.Context, plan corerunner.Plan) (string, error) {
	if err := operations.stopUnit(ctx, daemonUnit(plan)); err != nil {
		return "", err
	}
	return receipt(plan, corerunner.StepStopDaemon), nil
}

func (operations *localOperations) ObserveDaemonAbsent(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	if err := operations.observeUnitAbsent(ctx, daemonUnit(plan)); err != nil {
		return corerunner.StepEvidence{}, err
	}
	return absent(plan, corerunner.StepStopDaemon), nil
}

func (operations *localOperations) StopProxy(ctx context.Context, plan corerunner.Plan) (string, error) {
	if err := operations.stopUnit(ctx, proxyUnit(plan)); err != nil {
		return "", err
	}
	if err := os.Remove(plan.Paths.ProxySocket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	return receipt(plan, corerunner.StepStopProxy), nil
}

func (operations *localOperations) ObserveProxyAbsent(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	if err := operations.observeUnitAbsent(ctx, proxyUnit(plan)); err != nil {
		return corerunner.StepEvidence{}, err
	}
	if _, err := os.Lstat(plan.Paths.ProxySocket); err == nil || !errors.Is(err, os.ErrNotExist) {
		return corerunner.StepEvidence{}, errs.New(errs.KindStateConflict, "Runner proxy socket still exists")
	}
	return absent(plan, corerunner.StepStopProxy), nil
}

func (operations *localOperations) RemoveNetwork(ctx context.Context, plan corerunner.Plan) (string, error) {
	engine, err := newEngine(hostDockerSocket)
	if err != nil {
		return "", err
	}
	defer engine.Close()
	_, err = engine.NetworkRemove(ctx, plan.Network.Name, client.NetworkRemoveOptions{})
	if err != nil && !containerderrdefs.IsNotFound(err) {
		return "", dockerError(ctx, "remove Runner network", err)
	}
	return receipt(plan, corerunner.StepRemoveNetwork), nil
}

func (operations *localOperations) ObserveNetworkAbsent(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	engine, err := newEngine(hostDockerSocket)
	if err != nil {
		return corerunner.StepEvidence{}, err
	}
	defer engine.Close()
	_, err = engine.NetworkInspect(ctx, plan.Network.Name, client.NetworkInspectOptions{})
	if err == nil || !containerderrdefs.IsNotFound(err) {
		return corerunner.StepEvidence{}, errs.New(errs.KindStateConflict, "Runner network still exists")
	}
	return absent(plan, corerunner.StepRemoveNetwork), nil
}

func (operations *localOperations) RemoveIdentity(ctx context.Context, plan corerunner.Plan) (string, error) {
	_, _ = operations.run(ctx, "loginctl", "disable-linger", plan.Identity.User)
	if _, err := operations.run(ctx, "userdel", plan.Identity.User); err != nil {
		result, probeErr := operations.run(ctx, "getent", "passwd", plan.Identity.User)
		if probeErr == nil || result.ExitCode == 0 {
			return "", err
		}
	}
	if _, err := operations.run(ctx, "groupdel", plan.Identity.Group); err != nil {
		result, probeErr := operations.run(ctx, "getent", "group", plan.Identity.Group)
		if probeErr == nil || result.ExitCode == 0 {
			return "", err
		}
	}
	if err := os.RemoveAll(plan.Paths.SlotRoot); err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	return receipt(plan, corerunner.StepRemoveIdentity), nil
}

func (operations *localOperations) ObserveIdentityAbsent(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	if result, err := operations.run(ctx, "getent", "passwd", plan.Identity.User); err == nil || result.ExitCode == 0 {
		return corerunner.StepEvidence{}, errs.New(errs.KindStateConflict, "Runner host user still exists")
	}
	if _, err := os.Lstat(plan.Paths.SlotRoot); err == nil || !errors.Is(err, os.ErrNotExist) {
		return corerunner.StepEvidence{}, errs.New(errs.KindStateConflict, "Runner slot root still exists")
	}
	return absent(plan, corerunner.StepRemoveIdentity), nil
}

func (operations *localOperations) RemoveEgress(ctx context.Context, plan corerunner.Plan) (string, error) {
	result, err := operations.run(ctx, "nft", "delete", "table", "inet", nftTable(plan))
	if err != nil && result.ExitCode != 1 {
		return "", err
	}
	return receipt(plan, corerunner.StepRemoveEgress), nil
}

func (operations *localOperations) ObserveEgressAbsent(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	result, err := operations.run(ctx, "nft", "list", "table", "inet", nftTable(plan))
	if err == nil || result.ExitCode == 0 {
		return corerunner.StepEvidence{}, errs.New(errs.KindStateConflict, "Runner egress policy still exists")
	}
	return absent(plan, corerunner.StepRemoveEgress), nil
}

func (operations *localOperations) ensureGroup(ctx context.Context, plan corerunner.Plan) error {
	result, err := operations.run(ctx, "getent", "group", plan.Identity.Group)
	if err != nil {
		if result.ExitCode != 2 {
			return err
		}
		_, err = operations.run(ctx, "groupadd", "--gid", strconv.FormatUint(uint64(plan.Identity.GID), 10), plan.Identity.Group)
		return err
	}
	fields := strings.Split(strings.TrimSpace(string(result.Stdout)), ":")
	if len(fields) < 3 || fields[0] != plan.Identity.Group || fields[2] != strconv.FormatUint(uint64(plan.Identity.GID), 10) {
		return errs.New(errs.KindStateConflict, "Runner host group identity changed")
	}
	return nil
}

func (operations *localOperations) ensureUser(ctx context.Context, plan corerunner.Plan) error {
	result, err := operations.run(ctx, "getent", "passwd", plan.Identity.User)
	if err != nil {
		if result.ExitCode != 2 {
			return err
		}
		_, err = operations.run(
			ctx, "useradd", "--uid", strconv.FormatUint(uint64(plan.Identity.UID), 10),
			"--gid", plan.Identity.Group, "--home-dir", plan.Paths.RunnerHome,
			"--no-create-home", "--shell", "/usr/sbin/nologin", plan.Identity.User,
		)
		return err
	}
	return validatePasswd(result.Stdout, plan)
}

func (operations *localOperations) identityMatches(ctx context.Context, plan corerunner.Plan) error {
	group, err := operations.run(ctx, "getent", "group", plan.Identity.Group)
	if err != nil {
		return err
	}
	groupFields := strings.Split(strings.TrimSpace(string(group.Stdout)), ":")
	if len(groupFields) < 3 || groupFields[2] != strconv.FormatUint(uint64(plan.Identity.GID), 10) {
		return errs.New(errs.KindStateConflict, "Runner host group identity changed")
	}
	user, err := operations.run(ctx, "getent", "passwd", plan.Identity.User)
	if err != nil {
		return err
	}
	if err := validatePasswd(user.Stdout, plan); err != nil {
		return err
	}
	if !hasSubordinateRange("/etc/subuid", plan.Identity.User, plan.Identity.SubUIDStart, plan.Identity.SubUIDCount) ||
		!hasSubordinateRange("/etc/subgid", plan.Identity.User, plan.Identity.SubGIDStart, plan.Identity.SubGIDCount) {
		return errs.New(errs.KindStateConflict, "Runner subordinate identity changed")
	}
	return observeOwnedDirectory(plan.Paths.RunnerHome, plan.Identity.UID, plan.Identity.GID, 0o700)
}

func (operations *localOperations) inspectRunner(
	ctx context.Context,
	engine *client.Client,
	plan corerunner.Plan,
) (runnerallocation.RunnerRuntimeEvidence, bool, error) {
	result, err := engine.ContainerInspect(ctx, plan.Container.Name, client.ContainerInspectOptions{})
	if containerderrdefs.IsNotFound(err) {
		return runnerallocation.RunnerRuntimeEvidence{}, false, nil
	}
	if err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, false, dockerError(ctx, "inspect Runner container", err)
	}
	inspected := result.Container
	if inspected.ID == "" || inspected.Config == nil || inspected.Config.Image != plan.Container.ImageRef ||
		inspected.State == nil || !inspected.State.Running {
		return runnerallocation.RunnerRuntimeEvidence{}, false, errs.New(errs.KindStateConflict, "Runner container identity changed")
	}
	device, inode, err := socketIdentity(plan.Paths.RawSocket)
	if err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, false, err
	}
	nonce, err := operations.daemonNonce(ctx, plan)
	if err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, false, err
	}
	return runnerallocation.RunnerRuntimeEvidence{
		ContainerID: inspected.ID, DaemonNonce: nonce, SocketDevice: device, SocketInode: inode,
	}, true, nil
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

func (operations *localOperations) ensureUnit(ctx context.Context, unit string, runArgs []string) error {
	load, _ := operations.run(ctx, "systemctl", "show", "--property=LoadState", "--value", unit)
	if strings.TrimSpace(string(load.Stdout)) == "loaded" {
		if _, err := operations.run(ctx, "systemctl", "start", unit); err != nil {
			return err
		}
		return operations.observeUnit(ctx, unit)
	}
	if _, err := operations.run(ctx, "systemd-run", runArgs...); err != nil {
		return err
	}
	return operations.observeUnit(ctx, unit)
}

func (operations *localOperations) observeUnit(ctx context.Context, unit string) error {
	result, err := operations.run(ctx, "systemctl", "is-active", unit)
	if err != nil || strings.TrimSpace(string(result.Stdout)) != "active" {
		return errs.New(errs.KindStateConflict, "Runner systemd unit is not active")
	}
	return nil
}

func (operations *localOperations) stopUnit(ctx context.Context, unit string) error {
	result, err := operations.run(ctx, "systemctl", "stop", unit)
	if err != nil && result.ExitCode != 5 {
		return err
	}
	_, _ = operations.run(ctx, "systemctl", "reset-failed", unit)
	return nil
}

func (operations *localOperations) observeUnitAbsent(ctx context.Context, unit string) error {
	result, err := operations.run(ctx, "systemctl", "is-active", unit)
	if err == nil || strings.TrimSpace(string(result.Stdout)) == "active" {
		return errs.New(errs.KindStateConflict, "Runner systemd unit is still active")
	}
	return nil
}

func (operations *localOperations) run(ctx context.Context, name string, args ...string) (commandrunner.Result, error) {
	return operations.runInput(ctx, nil, name, args...)
}

func (operations *localOperations) runInput(
	ctx context.Context,
	input []byte,
	name string,
	args ...string,
) (commandrunner.Result, error) {
	result, err := operations.commands.Run(ctx, commandrunner.RunCmdOpts{
		Name: name, Args: args, Timeout: commandTimeout, Stdin: input, CaptureLimitBytes: maximumOutput,
	})
	if err != nil {
		return result, errs.Wrap(errs.KindInternal, fmt.Errorf("runner host %s failed: %w", name, err))
	}
	if result.ExitCode != 0 {
		return result, errs.Newf(errs.KindInternal, "runner host %s exited with status %d", name, result.ExitCode)
	}
	return result, nil
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

func ensureNetwork(ctx context.Context, engine *client.Client, plan corerunner.Plan, rootful bool) error {
	err := observeNetwork(ctx, engine, plan, rootful)
	if err == nil {
		return nil
	}
	if !containerderrdefs.IsNotFound(rootCause(err)) {
		return err
	}
	enableIPv4 := true
	options := client.NetworkCreateOptions{
		Driver: "bridge", EnableIPv4: &enableIPv4,
		IPAM: &network.IPAM{Driver: "default", Config: []network.IPAMConfig{{
			Subnet: plan.Network.Subnet, Gateway: plan.Network.Gateway,
		}}},
		Labels: map[string]string{
			"groundplane.runner.id":     plan.RunnerID,
			"groundplane.runtime.epoch": strconv.FormatUint(plan.RuntimeEpoch, 10),
		},
	}
	if rootful {
		options.Options = map[string]string{"com.docker.network.bridge.name": plan.Network.BridgeName}
	}
	if _, err := engine.NetworkCreate(ctx, plan.Network.Name, options); err != nil {
		return dockerError(ctx, "create Runner network", err)
	}
	return observeNetwork(ctx, engine, plan, rootful)
}

func observeNetwork(ctx context.Context, engine *client.Client, plan corerunner.Plan, rootful bool) error {
	result, err := engine.NetworkInspect(ctx, plan.Network.Name, client.NetworkInspectOptions{})
	if err != nil {
		return dockerError(ctx, "inspect Runner network", err)
	}
	inspected := result.Network
	if inspected.Name != plan.Network.Name || inspected.Driver != "bridge" || inspected.IPAM.Config == nil ||
		len(inspected.IPAM.Config) != 1 || inspected.IPAM.Config[0].Subnet != plan.Network.Subnet ||
		inspected.IPAM.Config[0].Gateway != plan.Network.Gateway {
		return errs.New(errs.KindStateConflict, "Runner network identity changed")
	}
	if rootful && inspected.Options["com.docker.network.bridge.name"] != plan.Network.BridgeName {
		return errs.New(errs.KindStateConflict, "Runner bridge identity changed")
	}
	return nil
}

func importImage(ctx context.Context, destination *client.Client, imageRef string) error {
	source, err := newEngine(hostDockerSocket)
	if err != nil {
		return err
	}
	defer source.Close()
	stream, err := source.ImageSave(ctx, []string{imageRef})
	if err != nil {
		return dockerError(ctx, "export Runner image", err)
	}
	defer stream.Close()
	loaded, err := destination.ImageLoad(ctx, stream, client.ImageLoadWithQuiet(true))
	if err != nil {
		return dockerError(ctx, "import Runner image", err)
	}
	_, copyErr := io.Copy(io.Discard, loaded)
	closeErr := loaded.Close()
	if copyErr != nil || closeErr != nil {
		return errs.Wrap(errs.KindInternal, errors.Join(copyErr, closeErr))
	}
	return nil
}

func runnerContainerOptions(plan corerunner.Plan) client.ContainerCreateOptions {
	return client.ContainerCreateOptions{
		Name: plan.Container.Name,
		Config: &container.Config{
			Image: plan.Container.ImageRef, User: "0:0", WorkingDir: "/runner",
			AttachStdin: true, OpenStdin: true, StdinOnce: true,
			Env: []string{
				"HOME=/runner", "DOCKER_HOST=unix:///var/run/docker.sock", "RUNNER_ALLOW_RUNASROOT=1",
			},
			Labels: map[string]string{
				"groundplane.runner.id":     plan.RunnerID,
				"groundplane.runtime.epoch": strconv.FormatUint(plan.RuntimeEpoch, 10),
			},
		},
		HostConfig: &container.HostConfig{
			NetworkMode:    container.NetworkMode(plan.Network.Name),
			RestartPolicy:  container.RestartPolicy{Name: container.RestartPolicyUnlessStopped},
			ReadonlyRootfs: plan.Container.ReadOnlyRootFS,
			CapDrop:        append([]string(nil), plan.Container.CapDrop...),
			SecurityOpt:    append([]string(nil), plan.Container.SecurityOptions...),
			Tmpfs: map[string]string{
				"/tmp": "rw,noexec,nosuid,nodev,size=256m", "/run": "rw,noexec,nosuid,nodev,size=64m",
			},
			Mounts: []mount.Mount{
				{Type: mount.TypeBind, Source: plan.Paths.RunnerHome, Target: "/runner"},
				{Type: mount.TypeBind, Source: plan.Paths.ProxySocket, Target: plan.Container.DockerSocketTarget},
			},
		},
		NetworkingConfig: &network.NetworkingConfig{EndpointsConfig: map[string]*network.EndpointSettings{
			plan.Network.Name: {IPAMConfig: &network.EndpointIPAMConfig{IPv4Address: plan.Network.RunnerAddress}},
		}},
	}
}

func registrationDocument(plan corerunner.Plan, token []byte) []byte {
	labels := strings.Join(plan.Container.Labels, ",")
	document := make([]byte, 0, len(plan.Container.GitHubURL)+len(plan.Container.RunnerName)+len(labels)+len(token)+4)
	document = append(document, plan.Container.GitHubURL...)
	document = append(document, '\n')
	document = append(document, plan.Container.RunnerName...)
	document = append(document, '\n')
	document = append(document, labels...)
	document = append(document, '\n')
	document = append(document, token...)
	document = append(document, '\n')
	return document
}

func waitForRunnerConfiguration(ctx context.Context, engine *client.Client, plan corerunner.Plan) error {
	deadline := time.Now().Add(startupTimeout)
	for {
		if info, err := os.Stat(filepath.Join(plan.Paths.RunnerHome, ".runner")); err == nil && info.Mode().IsRegular() {
			return nil
		}
		inspected, err := engine.ContainerInspect(ctx, plan.Container.Name, client.ContainerInspectOptions{})
		if err != nil {
			return dockerError(ctx, "inspect Runner registration", err)
		}
		if inspected.Container.State == nil || !inspected.Container.State.Running {
			return errs.New(errs.KindInternal, "Runner registration exited before readiness")
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return errs.New(errs.KindInternal, "Runner registration did not become ready")
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

func ensureOwnedDirectory(path string, uid uint32, gid uint32, mode os.FileMode) error {
	if err := os.MkdirAll(path, mode); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := os.Chown(path, int(uid), int(gid)); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return observeOwnedDirectory(path, uid, gid, mode)
}

func observeOwnedDirectory(path string, uid uint32, gid uint32, mode os.FileMode) error {
	info, err := os.Lstat(path)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || info.Mode().Perm() != mode || stat.Uid != uid || stat.Gid != gid {
		return errs.New(errs.KindStateConflict, "Runner owned directory identity changed")
	}
	return nil
}

func ensureSubordinateRange(path string, user string, start uint32, count uint32) error {
	if hasSubordinateRange(path, user, start, count) {
		return nil
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	line := fmt.Sprintf("%s:%d:%d\n", user, start, count)
	_, writeErr := file.WriteString(line)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		return errs.Wrap(errs.KindInternal, errors.Join(writeErr, syncErr, closeErr))
	}
	return nil
}

func hasSubordinateRange(path string, user string, start uint32, count uint32) bool {
	content, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	want := fmt.Sprintf("%s:%d:%d", user, start, count)
	for _, line := range strings.Split(strings.TrimSpace(string(content)), "\n") {
		if line == want {
			return true
		}
	}
	return false
}

func validatePasswd(value []byte, plan corerunner.Plan) error {
	fields := strings.Split(strings.TrimSpace(string(value)), ":")
	if len(fields) < 7 || fields[0] != plan.Identity.User ||
		fields[2] != strconv.FormatUint(uint64(plan.Identity.UID), 10) ||
		fields[3] != strconv.FormatUint(uint64(plan.Identity.GID), 10) ||
		fields[5] != plan.Paths.RunnerHome || fields[6] != "/usr/sbin/nologin" {
		return errs.New(errs.KindStateConflict, "Runner host user identity changed")
	}
	return nil
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

func applied(step corerunner.Step) corerunner.StepEvidence {
	return corerunner.StepEvidence{Step: step, State: corerunner.EffectApplied}
}

func absent(plan corerunner.Plan, step corerunner.Step) corerunner.StepEvidence {
	return corerunner.StepEvidence{Step: step, State: corerunner.EffectAbsent, ReceiptSHA256: receipt(plan, step)}
}

func receipt(plan corerunner.Plan, step corerunner.Step) string {
	digest := sha256.Sum256([]byte("groundplane.runner-removal.v1\x00" + plan.Digest() + "\x00" + string(step)))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func runnerToken(plan corerunner.Plan) string {
	return strings.ToLower(strings.TrimPrefix(plan.RunnerID, "run_"))
}

func daemonUnit(plan corerunner.Plan) string {
	return "groundplane-runner-" + runnerToken(plan) + "-daemon.service"
}

func proxyUnit(plan corerunner.Plan) string {
	return "groundplane-runner-" + runnerToken(plan) + "-proxy.service"
}

func nftTable(plan corerunner.Plan) string { return "gpr_" + runnerToken(plan) }

func dockerError(ctx context.Context, operation string, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return errs.Wrap(errs.KindInternal, fmt.Errorf("%s: %w", operation, err))
}

func rootCause(err error) error {
	for errors.Unwrap(err) != nil {
		err = errors.Unwrap(err)
	}
	return err
}
