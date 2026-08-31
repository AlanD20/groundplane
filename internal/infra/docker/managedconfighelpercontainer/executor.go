// Package managedconfighelpercontainer runs one atomic managed-config write in
// a short-lived container with only the managed config root mounted writable.
package managedconfighelpercontainer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	dockerSocketPath     = "/var/run/docker.sock"
	helperArgument       = "managed-config-helper"
	cleanupTimeout       = 30 * time.Second
	validationReadyDelay = time.Second
	maximumHelperStdout  = 1024
	maximumHelperStderr  = 32 * 1024
)

type Engine interface {
	ImageInspect(context.Context, string, ...client.ImageInspectOption) (client.ImageInspectResult, error)
	ImagePull(context.Context, string, client.ImagePullOptions) (client.ImagePullResponse, error)
	ContainerCreate(context.Context, client.ContainerCreateOptions) (client.ContainerCreateResult, error)
	ContainerAttach(context.Context, string, client.ContainerAttachOptions) (client.ContainerAttachResult, error)
	ContainerWait(context.Context, string, client.ContainerWaitOptions) client.ContainerWaitResult
	ContainerStart(context.Context, string, client.ContainerStartOptions) (client.ContainerStartResult, error)
	ContainerStop(context.Context, string, client.ContainerStopOptions) (client.ContainerStopResult, error)
	ContainerRemove(context.Context, string, client.ContainerRemoveOptions) (client.ContainerRemoveResult, error)
	Close() error
}

type Executor struct {
	engine Engine
	image  string
}

// Validate runs the candidate through the registered pinned serving image
// before the atomic managed-config helper can publish it.
func (executor *Executor) Validate(
	ctx context.Context,
	image string,
	arguments []string,
	content []byte,
) (resultErr error) {
	if executor == nil || executor.engine == nil || ctx == nil || !imageref.IsDigestPinned(image) ||
		len(arguments) == 0 || len(content) == 0 {
		return errs.New(errs.KindValidationFailed, "managed-config validator configuration is invalid")
	}
	if err := executor.ensureImage(ctx, image); err != nil {
		return err
	}
	created, err := executor.engine.ContainerCreate(ctx, validationCreateOptions(image, arguments))
	if err != nil {
		return operationError(ctx, "create validator", err)
	}
	if created.ID == "" {
		return errs.New(errs.KindInternal, "managed-config validator create returned an empty container id")
	}
	containerID := created.ID
	defer func() {
		if cleanupErr := executor.remove(containerID); cleanupErr != nil {
			resultErr = errs.Wrap(errs.KindInternal, errors.Join(resultErr, cleanupErr))
		}
	}()
	attached, err := executor.engine.ContainerAttach(ctx, containerID, client.ContainerAttachOptions{
		Stream: true, Stdin: true, Stdout: true, Stderr: true,
	})
	if err != nil {
		return operationError(ctx, "attach validator", err)
	}
	outputDone := make(chan outputResult, 1)
	go readOutput(attached.Reader, outputDone)
	joinOutput := func() outputResult {
		attached.Close()
		return <-outputDone
	}
	wait := executor.engine.ContainerWait(ctx, containerID, client.ContainerWaitOptions{
		Condition: container.WaitConditionNextExit,
	})
	if wait.Result == nil || wait.Error == nil {
		joinOutput()
		return errs.New(errs.KindInternal, "managed-config validator wait channels are not configured")
	}
	if _, err := executor.engine.ContainerStart(ctx, containerID, client.ContainerStartOptions{}); err != nil {
		joinOutput()
		return operationError(ctx, "start validator", err)
	}
	if err := writeAll(attached.Conn, content); err != nil {
		joinOutput()
		return operationError(ctx, "write validator candidate", err)
	}
	if err := attached.CloseWrite(); err != nil {
		joinOutput()
		return operationError(ctx, "close validator candidate", err)
	}
	timer := time.NewTimer(validationReadyDelay)
	defer timer.Stop()
	select {
	case response, open := <-wait.Result:
		output := joinOutput()
		if !open || response.Error != nil || output.err != nil {
			return errs.New(errs.KindRequestFailed, "managed-config candidate validation failed")
		}
		return errs.New(errs.KindRequestFailed, "managed-config candidate validator stopped before readiness")
	case waitErr, open := <-wait.Error:
		joinOutput()
		if !open || waitErr == nil {
			return errs.New(errs.KindInternal, "managed-config validator wait failed without an error")
		}
		return operationError(ctx, "wait for validator", waitErr)
	case <-timer.C:
		if err := executor.stop(containerID); err != nil {
			joinOutput()
			return err
		}
		if output := joinOutput(); output.err != nil {
			return errs.Wrap(errs.KindInternal, output.err)
		}
		return nil
	case <-ctx.Done():
		stopErr := executor.stop(containerID)
		joinOutput()
		if stopErr != nil {
			return stopErr
		}
		return ctx.Err()
	}
}

func (executor *Executor) ensureImage(ctx context.Context, image string) error {
	if _, err := executor.engine.ImageInspect(ctx, image); err == nil {
		return nil
	} else if !containerderrdefs.IsNotFound(err) {
		return operationError(ctx, "inspect validator image", err)
	}
	pull, err := executor.engine.ImagePull(ctx, image, client.ImagePullOptions{})
	if err != nil {
		return operationError(ctx, "pull validator image", err)
	}
	waitErr := pull.Wait(ctx)
	closeErr := pull.Close()
	if waitErr != nil {
		return operationError(ctx, "wait for validator image pull", errors.Join(waitErr, closeErr))
	}
	if closeErr != nil {
		return operationError(ctx, "close validator image pull", closeErr)
	}
	return nil
}

func validationCreateOptions(image string, arguments []string) client.ContainerCreateOptions {
	return client.ContainerCreateOptions{
		Config: &container.Config{
			Image: image, Cmd: append([]string(nil), arguments...), User: "65534:65534",
			AttachStdin: true, AttachStdout: true, AttachStderr: true,
			OpenStdin: true, StdinOnce: true, WorkingDir: "/", NetworkDisabled: true,
		},
		HostConfig: &container.HostConfig{
			NetworkMode:    container.NetworkMode("none"),
			RestartPolicy:  container.RestartPolicy{Name: container.RestartPolicyDisabled},
			ReadonlyRootfs: true, CapDrop: []string{"ALL"}, CapAdd: []string{"NET_BIND_SERVICE"},
			SecurityOpt: []string{"no-new-privileges"},
		},
	}
}

func New(image string) (*Executor, error) {
	engine, err := client.New(client.WithHost("unix://" + dockerSocketPath))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	executor, err := NewWithEngine(engine, image)
	if err != nil {
		if closeErr := engine.Close(); closeErr != nil {
			return nil, errs.Wrap(errs.KindInternal, errors.Join(err, closeErr))
		}
		return nil, err
	}
	return executor, nil
}

func NewWithEngine(engine Engine, image string) (*Executor, error) {
	if engine == nil || !imageref.IsDigestPinned(image) {
		return nil, errs.New(errs.KindValidationFailed, "managed-config helper container configuration is invalid")
	}
	return &Executor{engine: engine, image: image}, nil
}

func (executor *Executor) Close() error {
	if executor == nil || executor.engine == nil {
		return nil
	}
	if err := executor.engine.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func (executor *Executor) Execute(
	ctx context.Context,
	request *agentpb.ManagedConfigHelperRequest,
) (response *agentpb.ManagedConfigHelperResponse, resultErr error) {
	if executor == nil || executor.engine == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "managed-config helper container is not configured")
	}
	framed, err := managedconfighelper.MarshalRequest(request)
	if err != nil {
		return nil, err
	}
	defer clear(framed)
	created, err := executor.engine.ContainerCreate(ctx, createOptions(executor.image))
	if err != nil {
		return nil, operationError(ctx, "create", err)
	}
	if created.ID == "" {
		return nil, errs.New(errs.KindInternal, "managed-config helper create returned an empty container id")
	}
	containerID := created.ID
	defer func() {
		if cleanupErr := executor.remove(containerID); cleanupErr != nil {
			resultErr = errs.Wrap(errs.KindInternal, errors.Join(resultErr, cleanupErr))
		}
	}()
	attached, err := executor.engine.ContainerAttach(ctx, containerID, client.ContainerAttachOptions{
		Stream: true, Stdin: true, Stdout: true, Stderr: true,
	})
	if err != nil {
		return nil, operationError(ctx, "attach", err)
	}
	outputDone := make(chan outputResult, 1)
	go readOutput(attached.Reader, outputDone)
	joinOutput := func() outputResult {
		attached.Close()
		return <-outputDone
	}
	wait := executor.engine.ContainerWait(ctx, containerID, client.ContainerWaitOptions{
		Condition: container.WaitConditionNextExit,
	})
	if wait.Result == nil || wait.Error == nil {
		joinOutput()
		return nil, errs.New(errs.KindInternal, "managed-config helper wait channels are not configured")
	}
	if _, err := executor.engine.ContainerStart(ctx, containerID, client.ContainerStartOptions{}); err != nil {
		joinOutput()
		return nil, operationError(ctx, "start", err)
	}
	if err := writeAll(attached.Conn, framed); err != nil {
		joinOutput()
		return nil, operationError(ctx, "write request", err)
	}
	if err := attached.CloseWrite(); err != nil {
		joinOutput()
		return nil, operationError(ctx, "close request", err)
	}
	var waited *container.WaitResponse
	waitResults := wait.Result
	waitErrors := wait.Error
	var output *outputResult
	for waited == nil || output == nil {
		select {
		case response, open := <-waitResults:
			if !open {
				waitResults = nil
				if waitErrors == nil && waited == nil {
					joinOutput()
					return nil, errs.New(errs.KindInternal, "managed-config helper wait closed without a status")
				}
				continue
			}
			owned := response
			waited = &owned
			waitResults = nil
			waitErrors = nil
		case waitErr, open := <-waitErrors:
			if !open {
				waitErrors = nil
				continue
			}
			joinOutput()
			if waitErr == nil {
				return nil, errs.New(errs.KindInternal, "managed-config helper wait returned an empty error")
			}
			return nil, operationError(ctx, "wait", waitErr)
		case read := <-outputDone:
			owned := read
			output = &owned
			if read.err != nil {
				attached.Close()
				return nil, errs.Wrap(errs.KindInternal, read.err)
			}
		case <-ctx.Done():
			stopErr := executor.stop(containerID)
			joinOutput()
			if stopErr != nil {
				return nil, stopErr
			}
			return nil, ctx.Err()
		}
	}
	attached.Close()
	if waited.Error != nil || waited.StatusCode != 0 {
		message := fmt.Sprintf("managed-config helper failed with exit code %d", waited.StatusCode)
		if stderr := strings.TrimSpace(string(output.stderr)); stderr != "" {
			message += ": " + stderr
		}
		return nil, errs.New(errs.KindInternal, message)
	}
	response, err = managedconfighelper.ReadResponse(ctx, bytes.NewReader(output.stdout))
	if err != nil {
		return nil, err
	}
	if response.GetTransactionId() != request.GetTransactionId() || response.GetOperation() != request.GetOperation() {
		return nil, errs.New(errs.KindStateConflict, "managed-config helper response identity changed")
	}
	return response, nil
}

func createOptions(image string) client.ContainerCreateOptions {
	return client.ContainerCreateOptions{
		Config: &container.Config{
			Image: image, User: "0", Cmd: []string{helperArgument},
			AttachStdin: true, AttachStdout: true, AttachStderr: true,
			OpenStdin: true, StdinOnce: true, WorkingDir: "/", NetworkDisabled: true,
		},
		HostConfig: &container.HostConfig{
			NetworkMode:    container.NetworkMode("none"),
			RestartPolicy:  container.RestartPolicy{Name: container.RestartPolicyDisabled},
			ReadonlyRootfs: true, CapDrop: []string{"ALL"},
			SecurityOpt: []string{"no-new-privileges"},
			Mounts: []mount.Mount{{
				Type: mount.TypeBind, Source: managedconfighelper.ManagedRoot,
				Target: managedconfighelper.ManagedRoot,
			}},
		},
	}
}

func (executor *Executor) stop(containerID string) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	timeout := 0
	_, err := executor.engine.ContainerStop(cleanupCtx, containerID, client.ContainerStopOptions{Timeout: &timeout})
	if err != nil && !containerderrdefs.IsNotFound(err) {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("managed-config helper container: stop: %w", err))
	}
	return nil
}

func (executor *Executor) remove(containerID string) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	_, err := executor.engine.ContainerRemove(cleanupCtx, containerID, client.ContainerRemoveOptions{Force: true})
	if err != nil && !containerderrdefs.IsNotFound(err) {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("managed-config helper container: remove: %w", err))
	}
	return nil
}

func operationError(ctx context.Context, operation string, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	return errs.Wrap(errs.KindInternal, fmt.Errorf("managed-config helper container: %s: %w", operation, err))
}

type outputResult struct {
	stdout []byte
	stderr []byte
	err    error
}

func readOutput(reader io.Reader, result chan<- outputResult) {
	stdout := &boundedBuffer{maximum: maximumHelperStdout}
	stderr := &boundedBuffer{maximum: maximumHelperStderr}
	if reader == nil {
		result <- outputResult{err: errors.New("managed-config helper attach reader is nil")}
		return
	}
	_, err := stdcopy.StdCopy(stdout, stderr, reader)
	result <- outputResult{
		stdout: append([]byte(nil), stdout.Bytes()...),
		stderr: append([]byte(nil), stderr.Bytes()...),
		err:    err,
	}
}

type boundedBuffer struct {
	bytes.Buffer
	maximum int
}

func (buffer *boundedBuffer) Write(value []byte) (int, error) {
	remaining := buffer.maximum - buffer.Len()
	if remaining <= 0 {
		return 0, errors.New("managed-config helper output exceeds its bound")
	}
	if len(value) > remaining {
		written, _ := buffer.Buffer.Write(value[:remaining])
		return written, errors.New("managed-config helper output exceeds its bound")
	}
	return buffer.Buffer.Write(value)
}

func writeAll(writer io.Writer, value []byte) error {
	for len(value) != 0 {
		written, err := writer.Write(value)
		if err != nil {
			return err
		}
		if written <= 0 {
			return errors.New("managed-config helper request writer made no progress")
		}
		value = value[written:]
	}
	return nil
}
