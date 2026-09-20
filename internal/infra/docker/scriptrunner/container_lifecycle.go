package scriptrunner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"io"
)

func (runner *Runner) RecoverContainer(
	ctx context.Context,
	request scriptexecution.Request,
	body scriptexecution.BodyEvidence,
) (scriptexecution.ContainerRecovery, error) {
	if ctx == nil || runner == nil || runner.client == nil || runner.bodies == nil {
		return scriptexecution.ContainerRecovery{}, errs.New(
			errs.KindInternal,
			"Script runner: runtime is not configured",
		)
	}
	if err := validateRequest(request); err != nil {
		return scriptexecution.ContainerRecovery{}, err
	}
	prepared, err := preparedBodyForEvidence(runner.bodies, request, body)
	if err != nil {
		return scriptexecution.ContainerRecovery{}, err
	}
	inspected, err := runner.client.ContainerInspect(ctx, request.Projection.Name, client.ContainerInspectOptions{})
	if containerderrdefs.IsNotFound(err) {
		return scriptexecution.ContainerRecovery{Found: false}, nil
	}
	if err != nil {
		return scriptexecution.ContainerRecovery{}, operationError(ctx, "recover container by name", err)
	}
	if !validDockerContainerID(inspected.Container.ID) {
		return scriptexecution.ContainerRecovery{}, errs.New(
			errs.KindStateConflict,
			"Script runner: recovered container id is invalid",
		)
	}
	if err := validateOwnedContainer(inspected, inspected.Container.ID, request, prepared); err != nil {
		return scriptexecution.ContainerRecovery{}, err
	}
	if inspected.Container.State.Status != container.StateCreated || inspected.Container.State.Running {
		return scriptexecution.ContainerRecovery{}, errs.New(
			errs.KindStateConflict,
			"Script runner: recovered container is not stopped in created state",
		)
	}
	return scriptexecution.ContainerRecovery{
		Found:    true,
		Evidence: containerEvidence(request, inspected.Container.ID),
	}, nil
}

func (runner *Runner) CreateContainer(
	ctx context.Context,
	request scriptexecution.Request,
	body scriptexecution.BodyEvidence,
) (scriptexecution.ContainerEvidence, error) {
	if ctx == nil || runner == nil || runner.client == nil || runner.bodies == nil {
		return scriptexecution.ContainerEvidence{}, errs.New(
			errs.KindInternal,
			"Script runner: runtime is not configured",
		)
	}
	if err := validateRequest(request); err != nil {
		return scriptexecution.ContainerEvidence{}, err
	}
	prepared, err := preparedBodyForEvidence(runner.bodies, request, body)
	if err != nil {
		return scriptexecution.ContainerEvidence{}, err
	}

	options, err := createOptions(request, prepared.hostPath)
	if err != nil {
		return scriptexecution.ContainerEvidence{}, err
	}
	created, err := runner.client.ContainerCreate(ctx, options)
	if created.ID == "" && err != nil {
		return scriptexecution.ContainerEvidence{}, operationError(ctx, "create container", err)
	}
	if created.ID == "" {
		return scriptexecution.ContainerEvidence{}, errs.New(
			errs.KindInternal,
			"Script runner: Docker returned an empty container id",
		)
	}
	evidence := containerEvidence(request, created.ID)
	if !validDockerContainerID(created.ID) {
		return evidence, errs.New(errs.KindStateConflict, "Script runner: Docker returned an invalid container id")
	}
	inspected, inspectErr := runner.inspectOwnedContainer(ctx, created.ID, request, prepared)
	if inspectErr != nil {
		if kind, ok := errs.KindOf(inspectErr); ok && kind == errs.KindStateConflict {
			return evidence, inspectErr
		}
		return evidence, errs.Wrap(
			errs.KindStateConflict,
			fmt.Errorf("script runner: inspect ambiguous created container: %w", errors.Join(err, inspectErr)),
		)
	}
	if inspected.Container.State.Status != container.StateCreated || inspected.Container.State.Running {
		return evidence, errs.New(
			errs.KindStateConflict,
			"Script runner: created container is not stopped in created state",
		)
	}
	return evidence, nil
}

func (runner *Runner) RunContainer(
	ctx context.Context,
	request scriptexecution.Request,
	body scriptexecution.BodyEvidence,
	evidence scriptexecution.ContainerEvidence,
) (scriptexecution.RunResult, error) {
	var result scriptexecution.RunResult
	if ctx == nil || runner == nil || runner.client == nil || runner.bodies == nil {
		return result, errs.New(errs.KindInternal, "Script runner: runtime is not configured")
	}
	if err := validateRequest(request); err != nil {
		return result, err
	}
	prepared, err := preparedBodyForEvidence(runner.bodies, request, body)
	if err != nil {
		return result, err
	}
	expected := containerEvidence(request, evidence.ID)
	if !bytes.Equal(evidence.OwnershipLabelsSHA256, expected.OwnershipLabelsSHA256) {
		return result, errs.New(errs.KindStateConflict, "Script runner: container ownership digest differs")
	}
	inspected, err := runner.inspectOwnedContainer(ctx, evidence.ID, request, prepared)
	if err != nil {
		return result, operationError(ctx, "inspect captured container", err)
	}
	if inspected.Container.State.Status == container.StateExited && !inspected.Container.State.Running {
		return scriptExitResult(int64(inspected.Container.State.ExitCode))
	}
	if inspected.Container.State.Status != container.StateCreated &&
		inspected.Container.State.Status != container.StateRunning {
		return result, errs.New(
			errs.KindStateConflict,
			"Script runner: captured container has an invalid runtime state",
		)
	}
	if inspected.Container.State.Status == container.StateCreated {
		var connected map[string]*network.EndpointSettings
		if inspected.Container.NetworkSettings != nil {
			connected = inspected.Container.NetworkSettings.Networks
		}
		if err := connectSecondaryNetworks(ctx, runner.client, evidence.ID, request.Projection.Networks, connected); err != nil {
			return result, err
		}
	}

	attached, err := runner.client.ContainerAttach(ctx, evidence.ID, client.ContainerAttachOptions{
		Stream: true, Stdout: true, Stderr: true,
	})
	if err != nil {
		return result, operationError(ctx, "attach output", err)
	}
	defer attached.Close()
	drained := make(chan error, 1)
	go func() {
		_, drainErr := stdcopy.StdCopy(io.Discard, io.Discard, attached.Reader)
		drained <- drainErr
	}()

	if inspected.Container.State.Status == container.StateCreated {
		if _, err := runner.client.ContainerStart(ctx, evidence.ID, client.ContainerStartOptions{}); err != nil {
			return result, operationError(ctx, "start container", err)
		}
	}
	response, err := waitForContainer(ctx, runner.client, evidence.ID)
	if err != nil {
		return result, err
	}
	if response.Error != nil {
		return result, errs.New(errs.KindInternal, "Script runner: Docker reported a wait failure")
	}
	attached.Close()
	select {
	case drainErr := <-drained:
		if drainErr != nil && !errors.Is(drainErr, io.EOF) {
			return result, errs.Wrap(errs.KindInternal, fmt.Errorf("script runner: drain output: %w", drainErr))
		}
	case <-ctx.Done():
		return result, ctx.Err()
	}
	return scriptExitResult(response.StatusCode)
}

func waitForContainer(ctx context.Context, engine engineClient, containerID string) (container.WaitResponse, error) {
	wait := engine.ContainerWait(
		ctx,
		containerID,
		client.ContainerWaitOptions{Condition: container.WaitConditionNotRunning},
	)
	select {
	case response, ok := <-wait.Result:
		if !ok {
			return container.WaitResponse{}, errs.New(errs.KindInternal, "Script runner: Docker closed wait result")
		}
		return response, nil
	case err, ok := <-wait.Error:
		if !ok || err == nil {
			return container.WaitResponse{}, errs.New(errs.KindInternal, "Script runner: Docker closed wait error")
		}
		return container.WaitResponse{}, operationError(ctx, "wait for container", err)
	case <-ctx.Done():
		return container.WaitResponse{}, ctx.Err()
	}
}
