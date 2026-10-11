// Package backingaccesscheck runs the closed access procedure in the exact
// consumer container's network namespace. No database client is required in
// the application image, and the helper receives no Docker socket or host mount.
package backingaccesscheck

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const cleanupTimeout = 15 * time.Second

type Engine interface {
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
	Result          *backingaccess.Result
	HelperName      string
	CleanupRequired bool
}

// The composition root owns Engine's lifetime. The image must be the sealed
// matching Agent image, never a mutable registry tag selected at execution.
func New(engine Engine, image string) (*Executor, error) {
	if engine == nil || !imageref.IsDigestPinned(image) {
		return nil, errs.New(
			errs.KindValidationFailed,
			"Backing access helper requires an engine and pinned Agent image",
		)
	}
	return &Executor{engine: engine, image: image}, nil
}

func (executor *Executor) Execute(
	ctx context.Context, taskID, consumerID string,
	target *agentpb.ServiceObservationTarget, request backingaccess.Request,
) (execution Execution, resultErr error) {
	if ctx == nil || executor == nil || executor.engine == nil || ids.Validate(ids.KindTask, taskID) != nil ||
		!validContainerID(consumerID) || serviceobservation.ValidateTarget(target) != nil ||
		target.EnvironmentId != request.EnvironmentID || target.ServiceId != request.ServiceID || target.DatabaseProbe != nil {
		return execution, errs.New(errs.KindValidationFailed, "Backing access consumer selection is invalid")
	}
	encoded, err := backingaccess.MarshalRequest(request)
	if err != nil {
		return execution, err
	}
	defer clear(encoded)
	ctx, cancel := context.WithTimeout(ctx, backingaccess.Timeout+5*time.Second)
	defer cancel()
	inspected, err := executor.engine.ContainerInspect(ctx, consumerID, client.ContainerInspectOptions{})
	if err != nil || inspected.Container.ID != consumerID || inspected.Container.Config == nil ||
		inspected.Container.State == nil || !inspected.Container.State.Running ||
		inspected.Container.State.Restarting || inspected.Container.State.Paused {
		return execution, errs.New(
			errs.KindStateConflict,
			"consumer container must be running before testing its Backing access",
		)
	}
	if _, selected := serviceobservation.WorkloadReplica(inspected.Container.Config.Labels, target); !selected {
		return execution, errs.New(
			errs.KindStateConflict,
			"consumer container no longer matches the captured Service runtime",
		)
	}
	execution.HelperName = backingaccess.HelperName(taskID)
	// A deterministic operation name accounts for Create whose reply was lost.
	// Existing helpers are never resumed as a second forward probe.
	existing, inspectErr := executor.engine.ContainerInspect(
		ctx,
		execution.HelperName,
		client.ContainerInspectOptions{},
	)
	if inspectErr == nil {
		execution.CleanupRequired = true
		if !executor.owns(existing, taskID, request) {
			return execution, errs.New(
				errs.KindStateConflict,
				"Backing access helper name is owned by another operation",
			)
		}
		return execution, errs.New(
			errs.KindStateConflict,
			"previous Backing access helper needs cleanup before a new check",
		)
	}
	if !containerderrdefs.IsNotFound(inspectErr) {
		return execution, errs.New(errs.KindStorageUnavailable, "Backing access helper ownership could not be read")
	}
	defer func() {
		cleanupErr := executor.Cleanup(context.Background(), taskID, consumerID, request)
		execution.CleanupRequired = cleanupErr != nil
		if cleanupErr != nil {
			resultErr = errors.Join(resultErr, cleanupErr)
		}
	}()
	created, err := executor.engine.ContainerCreate(ctx, executor.createOptions(taskID, consumerID, request))
	if err != nil || !validContainerID(created.ID) {
		return execution, errs.New(errs.KindStorageUnavailable, "Backing access helper could not be created")
	}
	attached, err := executor.engine.ContainerAttach(ctx, created.ID, client.ContainerAttachOptions{
		Stream: true, Stdin: true, Stdout: true, Stderr: true,
	})
	if err != nil {
		return execution, errs.New(
			errs.KindStorageUnavailable,
			"Backing access helper private input stream is unavailable",
		)
	}
	defer attached.Close()
	cancellationDone := make(chan struct{})
	stopIO := context.AfterFunc(ctx, func() {
		attached.Close()
		close(cancellationDone)
	})
	defer func() {
		if !stopIO() {
			<-cancellationDone
		}
	}()
	outputDone := make(chan outputResult, 1)
	go readOutput(attached.Reader, outputDone)
	outputJoined := false
	defer func() {
		attached.Close()
		if !outputJoined {
			<-outputDone
		}
	}()
	wait := executor.engine.ContainerWait(
		ctx,
		created.ID,
		client.ContainerWaitOptions{Condition: container.WaitConditionNextExit},
	)
	if wait.Result == nil || wait.Error == nil {
		return execution, errs.New(errs.KindInternal, "Backing access helper wait is unavailable")
	}
	if _, err := executor.engine.ContainerStart(ctx, created.ID, client.ContainerStartOptions{}); err != nil {
		return execution, errs.New(
			errs.KindStorageUnavailable,
			"Backing access helper could not start in the consumer network",
		)
	}
	if _, err := io.Copy(attached.Conn, bytes.NewReader(encoded)); err != nil {
		return execution, errs.New(
			errs.KindStorageUnavailable,
			"Backing access helper private input could not be delivered",
		)
	}
	if err := attached.CloseWrite(); err != nil {
		return execution, errs.New(
			errs.KindStorageUnavailable,
			"Backing access helper private input could not be completed",
		)
	}
	select {
	case response, open := <-wait.Result:
		if !open || response.Error != nil || response.StatusCode != 0 {
			return execution, errs.New(errs.KindInternal, "Backing access helper failed before producing a result")
		}
	case <-wait.Error:
		return execution, errs.New(errs.KindStorageUnavailable, "Backing access helper completion is unavailable")
	case <-ctx.Done():
		return execution, ctx.Err()
	}
	select {
	case output := <-outputDone:
		outputJoined = true
		if output.err != nil {
			return execution, errs.New(
				errs.KindInternal,
				"Backing access helper output is invalid or exceeds its bound",
			)
		}
		result, err := backingaccess.ReadResult(bytes.NewReader(output.content))
		if err != nil {
			return execution, err
		}
		if err := result.ValidateFor(request); err != nil {
			return execution, err
		}
		execution.Result = &result
		return execution, nil
	case <-ctx.Done():
		return execution, ctx.Err()
	}
}

func (executor *Executor) Cleanup(ctx context.Context, taskID, consumerID string, request backingaccess.Request) error {
	if ctx == nil || executor == nil || executor.engine == nil || ids.Validate(ids.KindTask, taskID) != nil ||
		ids.Validate(
			ids.KindEnvironment,
			request.EnvironmentID,
		) != nil || ids.Validate(ids.KindService, request.ServiceID) != nil ||
		ids.Validate(ids.KindAttach, request.AttachID) != nil || !validContainerID(consumerID) {
		return errs.New(errs.KindValidationFailed, "Backing access helper cleanup authority is invalid")
	}
	ctx, cancel := context.WithTimeout(ctx, cleanupTimeout)
	defer cancel()
	inspected, err := executor.engine.ContainerInspect(
		ctx,
		backingaccess.HelperName(taskID),
		client.ContainerInspectOptions{},
	)
	if containerderrdefs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return errs.New(errs.KindStorageUnavailable, "Backing access helper cleanup could not verify ownership")
	}
	if !executor.owns(inspected, taskID, request) ||
		string(inspected.Container.HostConfig.NetworkMode) != "container:"+consumerID {
		return errs.New(
			errs.KindStateConflict,
			"Backing access helper cleanup ownership differs from the captured operation",
		)
	}
	_, err = executor.engine.ContainerRemove(ctx, inspected.Container.ID, client.ContainerRemoveOptions{Force: true})
	if err != nil && !containerderrdefs.IsNotFound(err) {
		return errs.New(errs.KindStorageUnavailable, "Backing access helper cleanup is incomplete")
	}
	return nil
}

func (executor *Executor) owns(
	inspected client.ContainerInspectResult,
	taskID string,
	request backingaccess.Request,
) bool {
	if !validContainerID(inspected.Container.ID) || inspected.Container.Config == nil ||
		inspected.Container.HostConfig == nil {
		return false
	}
	labels := inspected.Container.Config.Labels
	return inspected.Container.Config.Image == executor.image &&
		labels["com.groundplane.managed"] == "true" && labels["com.groundplane.kind"] == "backing-access-check" &&
		labels["com.groundplane.task-id"] == taskID && labels["com.groundplane.environment-id"] == request.EnvironmentID &&
		labels["com.groundplane.service-id"] == request.ServiceID &&
		labels["com.groundplane.attach-id"] == request.AttachID &&
		validContainerID(labels["com.groundplane.consumer-container-id"]) &&
		string(
			inspected.Container.HostConfig.NetworkMode,
		) == "container:"+labels["com.groundplane.consumer-container-id"]
}

func (executor *Executor) createOptions(
	taskID, consumerID string,
	request backingaccess.Request,
) client.ContainerCreateOptions {
	pidLimit := int64(32)
	return client.ContainerCreateOptions{
		Name: backingaccess.HelperName(taskID),
		Config: &container.Config{
			Image: executor.image, User: "65534:65534", WorkingDir: "/",
			Cmd:         []string{backingaccess.HelperArgument},
			AttachStdin: true, AttachStdout: true, AttachStderr: true, OpenStdin: true, StdinOnce: true,
			Labels: map[string]string{
				"com.groundplane.managed": "true", "com.groundplane.kind": "backing-access-check",
				"com.groundplane.task-id": taskID, "com.groundplane.environment-id": request.EnvironmentID,
				"com.groundplane.service-id": request.ServiceID, "com.groundplane.attach-id": request.AttachID,
				"com.groundplane.consumer-container-id": consumerID,
			},
		},
		HostConfig: &container.HostConfig{
			NetworkMode:    container.NetworkMode("container:" + consumerID),
			ReadonlyRootfs: true, CapDrop: []string{"ALL"}, SecurityOpt: []string{"no-new-privileges"},
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
			LogConfig:     container.LogConfig{Type: "none"},
			Resources: container.Resources{
				Memory:     64 << 20,
				MemorySwap: 64 << 20,
				PidsLimit:  &pidLimit,
				NanoCPUs:   250_000_000,
			},
		},
	}
}

func validContainerID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

type outputResult struct {
	content []byte
	err     error
}

func readOutput(reader io.Reader, result chan<- outputResult) {
	stdout := &boundedOutput{maximum: backingaccess.MaximumResponseBytes}
	stderr := &boundedOutput{maximum: 4096}
	_, err := stdcopy.StdCopy(stdout, stderr, reader)
	// Do not expose or persist stderr, even from the fixed helper.
	result <- outputResult{content: stdout.Bytes(), err: err}
}

type boundedOutput struct {
	bytes.Buffer
	maximum int
}

func (output *boundedOutput) Write(value []byte) (int, error) {
	if len(value) > output.maximum-output.Len() {
		return 0, errors.New("backing access helper output exceeds its bound")
	}
	return output.Buffer.Write(value)
}
