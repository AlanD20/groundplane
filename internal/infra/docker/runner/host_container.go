package runner

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

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
		return runnerallocation.RunnerRuntimeEvidence{}, errs.New(
			errs.KindInternal,
			"Runner Docker returned an empty container id",
		)
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
		return runnerallocation.RunnerRuntimeEvidence{}, errs.New(
			errs.KindInternal,
			"Runner container disappeared after registration",
		)
	}
	keep = true
	return evidence, nil
}

func (operations *localOperations) ObserveRunner(
	ctx context.Context,
	plan corerunner.Plan,
) (corerunner.StepEvidence, error) {
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

func (operations *localOperations) ObserveRunnerAbsent(
	ctx context.Context,
	plan corerunner.Plan,
) (corerunner.StepEvidence, error) {
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
		return runnerallocation.RunnerRuntimeEvidence{}, false, errs.New(
			errs.KindStateConflict,
			"Runner container identity changed",
		)
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
		if info, err := os.Stat(filepath.Join(plan.Paths.RunnerHome, ".runner")); err == nil &&
			info.Mode().IsRegular() {
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
