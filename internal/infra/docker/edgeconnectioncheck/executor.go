// Package edgeconnectioncheck runs the closed readiness/origin helper in the
// exact existing connector's network namespace. It never execs into the origin,
// reads the Tunnel token, changes provider configuration or starts a connector.
package edgeconnectioncheck

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/pkg/errs"
	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

type Engine interface {
	ContainerList(context.Context, client.ContainerListOptions) (client.ContainerListResult, error)
	ContainerInspect(context.Context, string, client.ContainerInspectOptions) (client.ContainerInspectResult, error)
	ContainerCreate(context.Context, client.ContainerCreateOptions) (client.ContainerCreateResult, error)
	ContainerAttach(context.Context, string, client.ContainerAttachOptions) (client.ContainerAttachResult, error)
	ContainerStart(context.Context, string, client.ContainerStartOptions) (client.ContainerStartResult, error)
	ContainerWait(context.Context, string, client.ContainerWaitOptions) client.ContainerWaitResult
	ContainerRemove(context.Context, string, client.ContainerRemoveOptions) (client.ContainerRemoveResult, error)
}

type Executor struct {
	engine Engine
	image  string
}
type Execution struct {
	Result               *contract.PrivateResult
	HelperName           string
	ConnectorContainerID string
	CleanupRequired      bool
}

func New(engine Engine, image string) (*Executor, error) {
	if engine == nil || !imageref.IsDigestPinned(image) {
		return nil, errs.New(
			errs.KindValidationFailed,
			"edge diagnostic requires an engine and pinned matching Agent image",
		)
	}
	return &Executor{engine: engine, image: image}, nil
}

func (executor *Executor) Execute(
	ctx context.Context,
	taskID string,
	connectorID string,
	target Target,
) (execution Execution, resultErr error) {
	if ctx == nil || executor == nil || executor.engine == nil || ids.Validate(ids.KindTask, taskID) != nil ||
		!validContainerID(connectorID) || target.request.Validate() != nil ||
		len(target.labels) == 0 {
		return execution, errs.New(errs.KindValidationFailed, "edge diagnostic ownership is invalid")
	}
	execution.HelperName, execution.ConnectorContainerID = contract.HelperName(taskID), connectorID
	ctx, cancel := context.WithTimeout(ctx, contract.PrivateTimeout+5*time.Second)
	defer cancel()
	// Start already owns possible helper effects. Even an early runtime failure
	// must prove absence using the original captured identity, not a replacement.
	defer func() {
		cleanupErr := executor.Cleanup(context.Background(), taskID, connectorID, target.request)
		execution.CleanupRequired = cleanupErr != nil
		if cleanupErr != nil {
			resultErr = errors.Join(resultErr, cleanupErr)
		}
	}()
	identity, err := executor.inspectRuntime(ctx, target)
	if err != nil {
		return execution, err
	}
	if identity.containerID != connectorID {
		return execution, errs.New(errs.KindStateConflict, "Tunnel container changed after capture; start a new check")
	}
	input, err := contract.MarshalRequest(target.request)
	if err != nil {
		return execution, err
	}
	existing, inspectErr := executor.engine.ContainerInspect(
		ctx,
		execution.HelperName,
		client.ContainerInspectOptions{},
	)
	if inspectErr == nil {
		execution.CleanupRequired = true
		if !executor.owns(existing, taskID, identity.containerID, target.request) {
			return execution, errs.New(
				errs.KindStateConflict,
				"edge diagnostic helper name is owned by another operation",
			)
		}
		return execution, errs.New(
			errs.KindStateConflict,
			"previous edge diagnostic helper needs cleanup; its probe will not be replayed",
		)
	}
	if !containerderrdefs.IsNotFound(inspectErr) {
		return execution, errs.New(errs.KindStorageUnavailable, "edge diagnostic helper ownership is unavailable")
	}
	created, err := executor.engine.ContainerCreate(
		ctx,
		executor.createOptions(taskID, identity.containerID, target.request),
	)
	if err != nil || !validContainerID(created.ID) {
		return execution, errs.New(errs.KindStorageUnavailable, "edge diagnostic helper could not be created")
	}
	attached, err := executor.engine.ContainerAttach(
		ctx,
		created.ID,
		client.ContainerAttachOptions{Stream: true, Stdin: true, Stdout: true, Stderr: true},
	)
	if err != nil || attached.Conn == nil || attached.Reader == nil {
		return execution, errs.New(errs.KindStorageUnavailable, "edge diagnostic helper stream is unavailable")
	}
	defer attached.Close()
	cancellationDone := make(chan struct{})
	stopIO := context.AfterFunc(ctx, func() { attached.Close(); close(cancellationDone) })
	defer func() {
		if !stopIO() {
			<-cancellationDone
		}
	}()
	outputDone := make(chan outputResult, 1)
	go readOutput(attached.Reader, outputDone)
	joined := false
	defer func() {
		attached.Close()
		if !joined {
			<-outputDone
		}
	}()
	wait := executor.engine.ContainerWait(
		ctx,
		created.ID,
		client.ContainerWaitOptions{Condition: container.WaitConditionNextExit},
	)
	if wait.Result == nil || wait.Error == nil {
		return execution, errs.New(errs.KindInternal, "edge diagnostic helper wait is unavailable")
	}
	if _, err := executor.engine.ContainerStart(ctx, created.ID, client.ContainerStartOptions{}); err != nil {
		return execution, errs.New(errs.KindStorageUnavailable, "edge diagnostic helper could not start")
	}
	if _, err := io.Copy(attached.Conn, bytes.NewReader(input)); err != nil {
		return execution, errs.New(errs.KindStorageUnavailable, "edge diagnostic input could not be delivered")
	}
	if err := attached.CloseWrite(); err != nil {
		return execution, errs.New(errs.KindStorageUnavailable, "edge diagnostic input could not be completed")
	}
	select {
	case response, open := <-wait.Result:
		if !open || response.Error != nil || response.StatusCode != 0 {
			return execution, errs.New(
				errs.KindRequestFailed,
				"edge diagnostic helper failed before producing a result",
			)
		}
	case <-wait.Error:
		return execution, errs.New(errs.KindStorageUnavailable, "edge diagnostic helper completion is unavailable")
	case <-ctx.Done():
		return execution, ctx.Err()
	}
	select {
	case output := <-outputDone:
		joined = true
		if output.err != nil {
			return execution, errs.New(
				errs.KindRequestFailed,
				"edge diagnostic helper output is invalid or exceeds its bound",
			)
		}
		result, err := contract.ReadPrivateResult(bytes.NewReader(output.content), target.request)
		if err != nil {
			return execution, err
		}
		current, err := executor.inspectRuntime(ctx, target)
		if err != nil || current != identity {
			return execution, errs.New(
				errs.KindStateConflict,
				"Tunnel runtime changed during the check; start a new check",
			)
		}
		execution.Result = &result
		return execution, nil
	case <-ctx.Done():
		return execution, ctx.Err()
	}
}

func (executor *Executor) Cleanup(ctx context.Context, taskID, connectorID string, request contract.Request) error {
	if ctx == nil || executor == nil || executor.engine == nil || ids.Validate(ids.KindTask, taskID) != nil ||
		!validContainerID(connectorID) ||
		request.Validate() != nil {
		return errs.New(errs.KindValidationFailed, "edge diagnostic cleanup authority is invalid")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	observed, err := executor.engine.ContainerInspect(
		ctx,
		contract.HelperName(taskID),
		client.ContainerInspectOptions{},
	)
	if containerderrdefs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return errs.New(errs.KindStorageUnavailable, "edge diagnostic cleanup could not read helper ownership")
	}
	if !executor.owns(observed, taskID, connectorID, request) {
		return errs.New(errs.KindStateConflict, "edge diagnostic cleanup ownership differs from the captured operation")
	}
	if _, err := executor.engine.ContainerRemove(ctx, observed.Container.ID, client.ContainerRemoveOptions{Force: true}); err != nil &&
		!containerderrdefs.IsNotFound(err) {
		return errs.New(errs.KindStorageUnavailable, "edge diagnostic helper cleanup is incomplete")
	}
	remaining, err := executor.engine.ContainerInspect(
		ctx,
		contract.HelperName(taskID),
		client.ContainerInspectOptions{},
	)
	if containerderrdefs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return errs.New(errs.KindStorageUnavailable, "edge diagnostic cleanup could not verify absence")
	}
	if !executor.owns(remaining, taskID, connectorID, request) {
		return errs.New(errs.KindStateConflict, "edge diagnostic helper name changed after removal")
	}
	return errs.New(errs.KindStorageUnavailable, "edge diagnostic helper is still present after removal")
}

func (executor *Executor) owns(
	observed client.ContainerInspectResult,
	taskID, connectorID string,
	request contract.Request,
) bool {
	value := observed.Container
	if !validContainerID(value.ID) || value.Config == nil || value.HostConfig == nil ||
		value.Config.Image != executor.image ||
		string(value.HostConfig.NetworkMode) != "container:"+connectorID {
		return false
	}
	labels := value.Config.Labels
	return labels["com.groundplane.managed"] == "true" && labels["com.groundplane.kind"] == "edge-connection-check" &&
		labels["com.groundplane.task-id"] == taskID && labels["com.groundplane.connector-container-id"] == connectorID &&
		labels["com.groundplane.environment-id"] == request.EnvironmentID && labels["com.groundplane.component-id"] == request.ComponentID &&
		labels["com.groundplane.service-id"] == request.ServiceID && labels["com.groundplane.route-id"] == request.RouteID &&
		labels["com.groundplane.request-id"] == request.RequestID && labels["com.groundplane.runtime-sha256"] == request.RuntimeSHA256
}

func (executor *Executor) createOptions(
	taskID, connectorID string,
	request contract.Request,
) client.ContainerCreateOptions {
	pidLimit := int64(32)
	return client.ContainerCreateOptions{Name: contract.HelperName(taskID), Config: &container.Config{
		Image: executor.image, User: "65534:65534", WorkingDir: "/", Cmd: []string{contract.HelperArgument},
		AttachStdin: true, AttachStdout: true, AttachStderr: true, OpenStdin: true, StdinOnce: true,
		Labels: map[string]string{"com.groundplane.managed": "true", "com.groundplane.kind": "edge-connection-check",
			"com.groundplane.task-id": taskID, "com.groundplane.connector-container-id": connectorID,
			"com.groundplane.environment-id": request.EnvironmentID, "com.groundplane.component-id": request.ComponentID,
			"com.groundplane.service-id": request.ServiceID, "com.groundplane.route-id": request.RouteID,
			"com.groundplane.request-id": request.RequestID, "com.groundplane.runtime-sha256": request.RuntimeSHA256},
	}, HostConfig: &container.HostConfig{NetworkMode: container.NetworkMode("container:" + connectorID),
		ReadonlyRootfs: true, CapDrop: []string{"ALL"}, SecurityOpt: []string{"no-new-privileges"},
		RestartPolicy: container.RestartPolicy{
			Name: container.RestartPolicyDisabled,
		}, LogConfig: container.LogConfig{Type: "none"},
		Resources: container.Resources{
			Memory:     64 << 20,
			MemorySwap: 64 << 20,
			PidsLimit:  &pidLimit,
			NanoCPUs:   250_000_000,
		}},
	}
}

type outputResult struct {
	content []byte
	err     error
}

func readOutput(reader io.Reader, done chan<- outputResult) {
	stdout, stderr := &boundedOutput{maximum: contract.MaximumOutputBytes}, &boundedOutput{maximum: 4096}
	_, err := stdcopy.StdCopy(stdout, stderr, reader)
	done <- outputResult{content: stdout.Bytes(), err: err}
}

type boundedOutput struct {
	bytes.Buffer
	maximum int
}

func (output *boundedOutput) Write(value []byte) (int, error) {
	if len(value) > output.maximum-output.Len() {
		return 0, errors.New("edge diagnostic output exceeds its bound")
	}
	return output.Buffer.Write(value)
}
