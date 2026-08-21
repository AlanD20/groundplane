// Package composehelpercontainer runs one validated Compose helper request in
// one short-lived, narrowly mounted container and owns its complete cleanup.
package composehelpercontainer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	dockerSocketPath    = "/var/run/docker.sock"
	helperArgument      = "compose-helper"
	cleanupTimeout      = 30 * time.Second
	maximumHelperStdout = 64 * 1024
	maximumHelperStderr = 32 * 1024
)

// Engine is the exact Moby surface required for one helper lifecycle.
type Engine interface {
	ContainerCreate(context.Context, client.ContainerCreateOptions) (client.ContainerCreateResult, error)
	ContainerAttach(context.Context, string, client.ContainerAttachOptions) (client.ContainerAttachResult, error)
	ContainerWait(context.Context, string, client.ContainerWaitOptions) client.ContainerWaitResult
	ContainerStart(context.Context, string, client.ContainerStartOptions) (client.ContainerStartResult, error)
	ContainerStop(context.Context, string, client.ContainerStopOptions) (client.ContainerStopResult, error)
	ContainerRemove(context.Context, string, client.ContainerRemoveOptions) (client.ContainerRemoveResult, error)
	Close() error
}

// Executor owns one digest-pinned helper image and creates no persistent
// container. The Engine remains owned by the Executor until Close.
type Executor struct {
	engine Engine
	image  string
}

// New constructs a production Executor against the fixed local Docker socket.
func New(image string) (*Executor, error) {
	engine, err := client.New(
		client.WithHost("unix://"+dockerSocketPath),
		client.WithAPIVersionNegotiation(),
	)
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

// NewWithEngine is the explicit dependency-injection constructor used by L1
// tests and application composition.
func NewWithEngine(engine Engine, image string) (*Executor, error) {
	if engine == nil {
		return nil, errs.New(errs.KindValidationFailed, "Compose helper container Engine is required")
	}
	if !imageref.IsDigestPinned(image) {
		return nil, errs.New(errs.KindValidationFailed, "Compose helper image must be digest-pinned")
	}
	return &Executor{engine: engine, image: image}, nil
}

// Close releases the Moby client owned by the Executor.
func (executor *Executor) Close() error {
	if executor == nil || executor.engine == nil {
		return nil
	}
	if err := executor.engine.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

// Execute runs exactly one helper request and removes its container on every
// path. Cleanup failure takes precedence because leaked Docker authority is an
// invariant failure.
func (executor *Executor) Execute(
	ctx context.Context,
	request *agentpb.ComposeHelperRequest,
) (response *agentpb.ComposeHelperResponse, resultErr error) {
	if executor == nil || executor.engine == nil {
		return nil, errs.New(errs.KindInternal, "Compose helper container Executor is not configured")
	}
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "Compose helper container context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	framed, err := composehelper.MarshalRequest(request)
	if err != nil {
		return nil, err
	}
	artifact, err := composehelper.ArtifactForRequest(request)
	if err != nil {
		return nil, err
	}
	created, err := executor.engine.ContainerCreate(ctx, createOptions(executor.image, artifact))
	if err != nil {
		return nil, operationError(ctx, "create", err)
	}
	if created.ID == "" {
		return nil, errs.New(errs.KindInternal, "Compose helper create returned an empty container id")
	}
	containerID := created.ID
	defer func() {
		cleanupErr := executor.remove(containerID)
		if cleanupErr != nil {
			response = nil
			resultErr = preferCleanup(resultErr, cleanupErr)
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
	var output *outputResult
	joinOutput := func() outputResult {
		attached.Close()
		if output != nil {
			return *output
		}
		return <-outputDone
	}

	wait := executor.engine.ContainerWait(ctx, containerID, client.ContainerWaitOptions{
		Condition: container.WaitConditionNextExit,
	})
	if wait.Result == nil || wait.Error == nil {
		joinOutput()
		return nil, errs.New(errs.KindInternal, "Compose helper wait channels are not configured")
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
	for waited == nil || output == nil {
		select {
		case waitResponse, open := <-waitResults:
			if !open {
				waitResults = nil
				if waitErrors == nil && waited == nil {
					joinOutput()
					return nil, errs.New(errs.KindInternal, "Compose helper wait closed without a status")
				}
				continue
			}
			owned := waitResponse
			waited = &owned
			waitResults = nil
			waitErrors = nil
		case waitErr, open := <-waitErrors:
			if !open {
				waitErrors = nil
				if waitResults == nil && waited == nil {
					joinOutput()
					return nil, errs.New(errs.KindInternal, "Compose helper wait closed without a status")
				}
				continue
			}
			joinOutput()
			if waitErr == nil {
				return nil, errs.New(errs.KindInternal, "Compose helper wait returned an empty error")
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
		return nil, errs.New(errs.KindInternal, "Compose helper process failed")
	}
	return composehelper.ReadResponse(context.Background(), bytes.NewReader(output.stdout))
}

func createOptions(image string, artifact *agentpb.ComposeArtifact) client.ContainerCreateOptions {
	mounts := []mount.Mount{{
		Type: mount.TypeBind, Source: dockerSocketPath, Target: dockerSocketPath,
	}}
	if artifact.AuthorizedVolumeDir != "" {
		mounts = append(mounts, mount.Mount{
			Type: mount.TypeBind, Source: artifact.AuthorizedVolumeDir,
			Target: artifact.AuthorizedVolumeDir, ReadOnly: true,
		})
	}
	return client.ContainerCreateOptions{
		Config: &container.Config{
			Image: image, User: "0", Cmd: []string{helperArgument},
			AttachStdin: true, AttachStdout: true, AttachStderr: true,
			OpenStdin: true, StdinOnce: true, WorkingDir: composehelper.WorkDirectory,
			NetworkDisabled: true,
		},
		HostConfig: &container.HostConfig{
			NetworkMode:    container.NetworkMode("none"),
			RestartPolicy:  container.RestartPolicy{Name: container.RestartPolicyDisabled},
			ReadonlyRootfs: true, CapDrop: []string{"ALL"},
			SecurityOpt: []string{"no-new-privileges"}, Mounts: mounts,
		},
	}
}

func (executor *Executor) stop(containerID string) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	timeout := 0
	_, err := executor.engine.ContainerStop(cleanupCtx, containerID, client.ContainerStopOptions{Timeout: &timeout})
	if err != nil && !containerderrdefs.IsNotFound(err) {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("Compose helper container: stop: %w", err))
	}
	return nil
}

func (executor *Executor) remove(containerID string) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	_, err := executor.engine.ContainerRemove(cleanupCtx, containerID, client.ContainerRemoveOptions{Force: true})
	if err != nil && !containerderrdefs.IsNotFound(err) {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("Compose helper container: remove: %w", err))
	}
	return nil
}

func operationError(ctx context.Context, operation string, err error) error {
	if err == nil {
		return nil
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return errs.Wrap(errs.KindInternal, fmt.Errorf("Compose helper container: %s: %w", operation, err))
}

func preferCleanup(original, cleanup error) error {
	if original == nil {
		return cleanup
	}
	return errs.Wrap(errs.KindInternal, errors.Join(cleanup, original))
}

type outputResult struct {
	stdout []byte
	err    error
}

func readOutput(reader io.Reader, result chan<- outputResult) {
	if reader == nil {
		result <- outputResult{err: errors.New("helper attach reader is nil")}
		return
	}
	stdout := &boundedBuffer{maximum: maximumHelperStdout}
	stderr := &boundedBuffer{maximum: maximumHelperStderr}
	_, err := stdcopy.StdCopy(stdout, stderr, reader)
	result <- outputResult{stdout: append([]byte(nil), stdout.Bytes()...), err: err}
}

type boundedBuffer struct {
	bytes.Buffer
	maximum int
}

func (buffer *boundedBuffer) Write(value []byte) (int, error) {
	remaining := buffer.maximum - buffer.Len()
	if remaining <= 0 {
		return 0, errors.New("helper output exceeds its bound")
	}
	if len(value) > remaining {
		written, _ := buffer.Buffer.Write(value[:remaining])
		return written, errors.New("helper output exceeds its bound")
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
			return errors.New("helper request writer made no progress")
		}
		value = value[written:]
	}
	return nil
}
