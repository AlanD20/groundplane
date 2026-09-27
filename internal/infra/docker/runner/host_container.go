package runner

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"net/netip"
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
	engine, err := newEngine(hostDockerSocket)
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
	engine, err := newEngine(hostDockerSocket)
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
	engine, err := newEngine(hostDockerSocket)
	if err != nil {
		return "", err
	}
	defer engine.Close()
	inspected, err := engine.ContainerInspect(ctx, plan.Container.Name, client.ContainerInspectOptions{})
	if containerderrdefs.IsNotFound(err) {
		return receipt(plan, corerunner.StepStopRunner), nil
	}
	if err != nil {
		return "", dockerError(ctx, "inspect Runner before removal", err)
	}
	if err := validateRunnerContainer(inspected.Container, plan); err != nil {
		return "", err
	}
	_, err = engine.ContainerRemove(ctx, inspected.Container.ID, client.ContainerRemoveOptions{Force: true})
	if err != nil && !containerderrdefs.IsNotFound(err) {
		return "", dockerError(ctx, "remove Runner container", err)
	}
	return receipt(plan, corerunner.StepStopRunner), nil
}

func (operations *localOperations) ObserveRunnerAbsent(
	ctx context.Context,
	plan corerunner.Plan,
) (corerunner.StepEvidence, error) {
	engine, err := newEngine(hostDockerSocket)
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
	if err := validateRunnerContainer(inspected, plan); err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, false, err
	}
	if inspected.State == nil || !inspected.State.Running {
		return runnerallocation.RunnerRuntimeEvidence{}, false, errs.New(
			errs.KindStateConflict,
			"Runner container identity changed",
		)
	}
	if err := registrationPresent(plan); err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, false, err
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

func validateRunnerContainer(inspected container.InspectResponse, plan corerunner.Plan) error {
	if inspected.ID == "" || inspected.Config == nil || inspected.HostConfig == nil ||
		inspected.Config.Image != plan.Container.ImageRef || inspected.Config.User != plan.Container.User ||
		inspected.Config.WorkingDir != plan.Paths.RunnerHome ||
		inspected.Config.Labels["groundplane.runner.id"] != plan.RunnerID ||
		inspected.Config.Labels["groundplane.runtime.epoch"] != strconv.FormatUint(plan.RuntimeEpoch, 10) ||
		inspected.Config.Labels["groundplane.runner.plan"] != plan.Digest() ||
		inspected.HostConfig.Privileged || !inspected.HostConfig.ReadonlyRootfs ||
		string(inspected.HostConfig.NetworkMode) != plan.Network.Name {
		return errs.New(errs.KindStateConflict, "Runner container ownership changed")
	}
	return nil
}

func runnerContainerOptions(plan corerunner.Plan) client.ContainerCreateOptions {
	return client.ContainerCreateOptions{
		Name: plan.Container.Name,
		Config: &container.Config{
			Image: plan.Container.ImageRef, User: plan.Container.User, WorkingDir: plan.Paths.RunnerHome,
			AttachStdin: true, OpenStdin: true, StdinOnce: true,
			Env: []string{
				"HOME=" + plan.Paths.RunnerHome, "DOCKER_HOST=unix://" + plan.Container.DockerSocketTarget,
				"DOCKER_CONFIG=" + runnerDockerConfig(plan),
				"GROUNDPLANE_HOST=http://" + plan.Egress.ControllerEndpoint.String(),
			},
			Labels: map[string]string{
				"groundplane.runner.id":     plan.RunnerID,
				"groundplane.runner.plan":   plan.Digest(),
				"groundplane.runtime.epoch": strconv.FormatUint(plan.RuntimeEpoch, 10),
			},
		},
		HostConfig: &container.HostConfig{
			DNS:            []netip.Addr{plan.Egress.ControllerEndpoint.Addr()},
			NetworkMode:    container.NetworkMode(plan.Network.Name),
			RestartPolicy:  container.RestartPolicy{Name: container.RestartPolicyDisabled},
			ReadonlyRootfs: plan.Container.ReadOnlyRootFS,
			CapDrop:        append([]string(nil), plan.Container.CapDrop...),
			SecurityOpt:    append([]string(nil), plan.Container.SecurityOptions...),
			Tmpfs: map[string]string{
				"/tmp": "rw,noexec,nosuid,nodev,size=256m", "/run": "rw,noexec,nosuid,nodev,size=64m",
			},
			Mounts: []mount.Mount{
				{Type: mount.TypeBind, Source: plan.Paths.RunnerHome, Target: plan.Paths.RunnerHome},
				{
					Type:     mount.TypeBind,
					Source:   filepath.Dir(plan.Paths.RawSocket),
					Target:   filepath.Dir(plan.Container.DockerSocketTarget),
					ReadOnly: true,
				},
			},
		},
		NetworkingConfig: &network.NetworkingConfig{EndpointsConfig: map[string]*network.EndpointSettings{
			plan.Network.Name: {IPAMConfig: &network.EndpointIPAMConfig{IPv4Address: plan.Network.RunnerAddress}},
		}},
	}
}

func (operations *localOperations) ResumeRunner(
	ctx context.Context,
	plan corerunner.Plan,
	containerID string,
) (runnerallocation.RunnerRuntimeEvidence, error) {
	if err := registrationPresent(plan); err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, err
	}
	engine, err := newEngine(hostDockerSocket)
	if err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, err
	}
	defer engine.Close()
	result, err := engine.ContainerInspect(ctx, plan.Container.Name, client.ContainerInspectOptions{})
	if err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, dockerError(ctx, "inspect registered Runner", err)
	}
	if err := validateRunnerContainer(result.Container, plan); err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, err
	}
	if result.Container.ID != containerID {
		return runnerallocation.RunnerRuntimeEvidence{}, errs.New(
			errs.KindStateConflict,
			"Runner container differs from its registered identity",
		)
	}
	if result.Container.State == nil || !result.Container.State.Running {
		if _, err := engine.ContainerStart(ctx, result.Container.ID, client.ContainerStartOptions{}); err != nil {
			return runnerallocation.RunnerRuntimeEvidence{}, dockerError(ctx, "resume registered Runner", err)
		}
	}
	evidence, found, err := operations.inspectRunner(ctx, engine, plan)
	if err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, err
	}
	if !found {
		return runnerallocation.RunnerRuntimeEvidence{}, errs.New(
			errs.KindStateConflict,
			"registered Runner disappeared",
		)
	}
	return evidence, nil
}

func registrationPresent(plan corerunner.Plan) error {
	for _, name := range []string{".runner", ".credentials", ".credentials_rsaparams"} {
		info, err := os.Lstat(filepath.Join(plan.Paths.RunnerHome, name))
		if err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
			return errs.New(
				errs.KindStateConflict,
				"Runner registration is incomplete; a fresh registration is required",
			)
		}
	}
	return nil
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
		if err := registrationPresent(plan); err == nil {
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
