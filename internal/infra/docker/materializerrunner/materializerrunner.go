// Package materializerrunner streams one materialization request through a
// short-lived, policy-constrained Docker helper.
package materializerrunner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	dockerHost               = "unix:///var/run/docker.sock"
	helperEntrypoint         = "/usr/local/bin/groundplane-agent"
	helperCommand            = "materialize"
	helperMountPath          = "/run/groundplane/materialize"
	copyBufferSize           = 32 * 1024
	productionCleanupTimeout = 30 * time.Second
)

// Request is the complete input for one helper invocation. Stream ownership
// transfers to Run, which closes it on every return path. VolumeDir has already
// been authorized by the durable task plan; this adapter still rejects a
// malformed value before contacting Docker.
type Request struct {
	VolumeDir  string
	Stream     io.ReadCloser
	VerifyOnly bool
}

type engineClient interface {
	ContainerCreate(context.Context, client.ContainerCreateOptions) (client.ContainerCreateResult, error)
	ContainerAttach(context.Context, string, client.ContainerAttachOptions) (client.ContainerAttachResult, error)
	ContainerStart(context.Context, string, client.ContainerStartOptions) (client.ContainerStartResult, error)
	ContainerWait(context.Context, string, client.ContainerWaitOptions) client.ContainerWaitResult
	ContainerRemove(context.Context, string, client.ContainerRemoveOptions) (client.ContainerRemoveResult, error)
	Close() error
}

// Runner is the Docker implementation of the task-scoped materializer runner.
// Image identity, executable selection, and cleanup timing are constructor-owned
// policy rather than per-operation inputs.
type Runner struct {
	client         engineClient
	image          string
	volumeRoot     string
	cleanupTimeout time.Duration
}

// New connects to the local Docker Engine socket and fixes one approved Agent
// image for every helper created by the returned runner.
func New(ctx context.Context, image string, volumeRoot string) (*Runner, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "materializer runner: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !imageref.IsDigestPinned(image) {
		return nil, errs.New(errs.KindInternal, "materializer runner: helper image is not digest-pinned")
	}
	if err := environmentpath.ValidateRoot(volumeRoot); err != nil {
		return nil, errs.New(errs.KindInternal, "materializer runner: volume root policy is invalid")
	}
	engine, err := client.New(client.WithHost(dockerHost))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("materializer runner: create Docker client: %w", err))
	}
	return &Runner{
		client: engine, image: image, volumeRoot: volumeRoot,
		cleanupTimeout: productionCleanupTimeout,
	}, nil
}

// Close releases the Docker client transport.
func (r *Runner) Close() error {
	if r == nil || r.client == nil {
		return errs.New(errs.KindInternal, "materializer runner: Docker client is required")
	}
	if err := r.client.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("materializer runner: close Docker client: %w", err))
	}
	return nil
}

// Run creates one constrained helper, transfers the framed request through
// stdin, waits for a zero exit, and explicitly removes the helper.
func (r *Runner) Run(ctx context.Context, request Request) (resultErr error) {
	stream := &ownedStream{reader: request.Stream}
	containerID := ""
	var attachment *ownedAttachment
	var priorCleanupErrors []error
	defer func() {
		cleanupTimeout := productionCleanupTimeout
		if r != nil && r.cleanupTimeout > 0 {
			cleanupTimeout = r.cleanupTimeout
		}
		cleanupErr := runCleanup(r, cleanupTimeout, attachment, stream, containerID, priorCleanupErrors)
		if cleanupErr != nil {
			resultErr = errs.Wrap(
				errs.KindInternal,
				fmt.Errorf("materializer runner: cleanup failed: %w", cleanupErr),
			)
			return
		}
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				resultErr = err
			}
		}
	}()

	if ctx == nil {
		return errs.New(errs.KindInternal, "materializer runner: context is required")
	}
	if r == nil || r.client == nil {
		return errs.New(errs.KindInternal, "materializer runner: Docker client is required")
	}
	if r.cleanupTimeout <= 0 {
		return errs.New(errs.KindInternal, "materializer runner: cleanup timeout is required")
	}
	if !imageref.IsDigestPinned(r.image) {
		return errs.New(errs.KindInternal, "materializer runner: helper image is not digest-pinned")
	}
	if err := validateRequest(r.volumeRoot, request); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	stopStreamCancellation := context.AfterFunc(ctx, func() {
		// Final cleanup reports the owned close result; this call only unblocks
		// an in-flight read when the task is cancelled.
		_ = stream.Close()
	})
	defer stopStreamCancellation()

	created, err := r.client.ContainerCreate(ctx, createOptions(r.image, request))
	if created.ID != "" {
		containerID = created.ID
	}
	if err != nil {
		return operationError(ctx, "create helper", err)
	}
	if containerID == "" {
		return errs.New(errs.KindInternal, "materializer runner: Docker returned an empty helper id")
	}

	attached, err := r.client.ContainerAttach(ctx, containerID, client.ContainerAttachOptions{
		Stream: true,
		Stdin:  true,
	})
	if attached.Conn != nil {
		attachment = &ownedAttachment{connection: attached.Conn}
	}
	if err != nil {
		return operationError(ctx, "attach helper stdin", err)
	}
	if attachment == nil {
		return errs.New(errs.KindInternal, "materializer runner: Docker returned an empty helper attachment")
	}
	stopAttachCancellation := context.AfterFunc(ctx, func() {
		// Final cleanup reports the owned close result; this call only unblocks
		// an in-flight write when the task is cancelled.
		_ = attachment.Close()
	})
	defer stopAttachCancellation()

	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := r.client.ContainerStart(ctx, containerID, client.ContainerStartOptions{}); err != nil {
		return operationError(ctx, "start helper", err)
	}

	buffer := make([]byte, copyBufferSize)
	defer clear(buffer)
	if _, err := io.CopyBuffer(
		writerOnly{Writer: attachment},
		readerOnly{Reader: stream},
		buffer,
	); err != nil {
		return operationError(ctx, "stream helper request", err)
	}
	if err := stream.Close(); err != nil {
		return cleanupError("close request stream", err)
	}
	if err := attachment.CloseWrite(); err != nil {
		return operationError(ctx, "close helper stdin", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	response, cancellationCleanupErrors, err := r.wait(ctx, containerID)
	priorCleanupErrors = append(priorCleanupErrors, cancellationCleanupErrors...)
	if err != nil {
		return err
	}
	if response.Error != nil {
		return errs.New(errs.KindInternal, "materializer runner: Docker reported a helper wait failure")
	}
	if response.StatusCode != 0 {
		if request.VerifyOnly && response.StatusCode == 2 {
			return errs.New(errs.KindStateConflict, "materializer runner: pinned configuration is not restored")
		}
		return errs.Newf(errs.KindInternal, "materializer runner: helper exited with status %d", response.StatusCode)
	}
	return nil
}

func createOptions(image string, request Request) client.ContainerCreateOptions {
	command, capabilities := helperCommand, []string{"CHOWN", "FOWNER"}
	if request.VerifyOnly {
		command, capabilities = "verify-materialization", []string{"DAC_READ_SEARCH"}
	}
	return client.ContainerCreateOptions{
		Config: &container.Config{
			Image:           image,
			User:            "0",
			AttachStdin:     true,
			OpenStdin:       true,
			StdinOnce:       true,
			Entrypoint:      []string{helperEntrypoint},
			Cmd:             []string{command},
			NetworkDisabled: true,
		},
		HostConfig: &container.HostConfig{
			LogConfig:      container.LogConfig{Type: "none"},
			NetworkMode:    container.NetworkMode("none"),
			RestartPolicy:  container.RestartPolicy{Name: container.RestartPolicyDisabled},
			CapAdd:         capabilities,
			CapDrop:        []string{"ALL"},
			ReadonlyRootfs: true,
			SecurityOpt:    []string{"no-new-privileges=true"},
			Mounts: []mount.Mount{{
				Type:        mount.TypeBind,
				Source:      request.VolumeDir,
				Target:      helperMountPath,
				ReadOnly:    request.VerifyOnly,
				BindOptions: &mount.BindOptions{Propagation: mount.PropagationRPrivate},
			}},
		},
	}
}

func validateRequest(volumeRoot string, request Request) error {
	if _, err := environmentpath.Parse(volumeRoot, request.VolumeDir); err != nil {
		return errs.New(errs.KindInternal, "materializer runner: environment volume path is invalid")
	}
	if request.Stream == nil {
		return errs.New(errs.KindInternal, "materializer runner: request stream is required")
	}
	return nil
}

func (r *Runner) wait(
	ctx context.Context,
	containerID string,
) (container.WaitResponse, []error, error) {
	waitCtx, cancelWait := context.WithCancel(context.Background())
	defer cancelWait()
	forwarded := forwardWait(r.client.ContainerWait(waitCtx, containerID, client.ContainerWaitOptions{
		Condition: container.WaitConditionNotRunning,
	}))

	select {
	case outcome := <-forwarded:
		if err := ctx.Err(); err != nil {
			return container.WaitResponse{}, r.cancelWait(containerID, cancelWait, &outcome, forwarded), err
		}
		response, err := evaluateWaitOutcome(ctx, outcome)
		return response, nil, err
	case <-ctx.Done():
		return container.WaitResponse{}, r.cancelWait(containerID, cancelWait, nil, forwarded), ctx.Err()
	}
}

func (r *Runner) cancelWait(
	containerID string,
	cancelWait context.CancelFunc,
	prefetched *waitOutcome,
	forwarded <-chan waitOutcome,
) []error {
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), r.cleanupTimeout)
	defer cancelCleanup()
	removalErr := r.remove(cleanupCtx, containerID)
	cancelWait()

	var outcome waitOutcome
	var drainErr error
	if prefetched != nil {
		outcome = *prefetched
	} else {
		select {
		case outcome = <-forwarded:
		default:
			select {
			case outcome = <-forwarded:
			case <-cleanupCtx.Done():
				drainErr = fmt.Errorf("drain helper wait: %w", cleanupCtx.Err())
			}
		}
	}

	cleanupErrors := make([]error, 0, 3)
	if removalErr != nil {
		cleanupErrors = append(cleanupErrors, fmt.Errorf("remove cancelled helper: %w", removalErr))
	}
	if drainErr != nil {
		cleanupErrors = append(cleanupErrors, drainErr)
		return cleanupErrors
	}
	if outcome.err != nil && !errors.Is(outcome.err, context.Canceled) {
		cleanupErrors = append(cleanupErrors, fmt.Errorf("drain helper wait: %w", outcome.err))
	}
	if outcome.response.Error != nil {
		cleanupErrors = append(cleanupErrors, errors.New("drain helper wait: docker reported a failure"))
	}
	return cleanupErrors
}

func forwardWait(wait client.ContainerWaitResult) <-chan waitOutcome {
	forwarded := make(chan waitOutcome, 1)
	go func() {
		select {
		case response, ok := <-wait.Result:
			if !ok {
				forwarded <- waitOutcome{err: errors.New("docker closed the helper wait result")}
				return
			}
			forwarded <- waitOutcome{response: response}
		case err, ok := <-wait.Error:
			if !ok || err == nil {
				forwarded <- waitOutcome{err: errors.New("docker closed the helper wait error")}
				return
			}
			forwarded <- waitOutcome{err: err}
		}
	}()
	return forwarded
}

func evaluateWaitOutcome(ctx context.Context, outcome waitOutcome) (container.WaitResponse, error) {
	if outcome.err != nil {
		return container.WaitResponse{}, operationError(ctx, "wait for helper", outcome.err)
	}
	return outcome.response, nil
}

func (r *Runner) remove(ctx context.Context, containerID string) error {
	if _, err := r.client.ContainerRemove(ctx, containerID, client.ContainerRemoveOptions{Force: true}); err != nil {
		if containerderrdefs.IsNotFound(err) {
			return nil
		}
		return err
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
	return errs.Wrap(errs.KindInternal, fmt.Errorf("materializer runner: %s: %w", operation, err))
}

func cleanupError(operation string, err error) error {
	return errs.Wrap(errs.KindInternal, fmt.Errorf("materializer runner: %s: %w", operation, err))
}

type cleanupAction struct {
	name string
	run  func(context.Context) error
}

type cleanupResult struct {
	index int
	err   error
}

func runCleanup(
	runner *Runner,
	timeout time.Duration,
	attachment *ownedAttachment,
	stream *ownedStream,
	containerID string,
	prior []error,
) error {
	actions := make([]cleanupAction, 0, 3)
	if attachment != nil {
		actions = append(actions, cleanupAction{name: "close helper attachment", run: func(context.Context) error {
			return attachment.Close()
		}})
	}
	actions = append(actions, cleanupAction{name: "close request stream", run: func(context.Context) error {
		return stream.Close()
	}})
	if containerID != "" && runner != nil && runner.client != nil {
		actions = append(actions, cleanupAction{name: "remove helper", run: func(ctx context.Context) error {
			return runner.remove(ctx, containerID)
		}})
	}

	cleanupCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	results := make(chan cleanupResult, len(actions))
	for index, action := range actions {
		go func(index int, action cleanupAction) {
			results <- cleanupResult{index: index, err: action.run(cleanupCtx)}
		}(index, action)
	}

	actionErrors := make([]error, len(actions))
	completed := make([]bool, len(actions))
	remaining := len(actions)
	for remaining > 0 {
		select {
		case result := <-results:
			if !completed[result.index] {
				completed[result.index] = true
				actionErrors[result.index] = result.err
				remaining--
			}
		case <-cleanupCtx.Done():
			for index, action := range actions {
				if !completed[index] {
					actionErrors[index] = fmt.Errorf("%s: %w", action.name, cleanupCtx.Err())
				}
			}
			remaining = 0
		}
	}

	allErrors := append([]error(nil), prior...)
	for index, err := range actionErrors {
		if err != nil {
			allErrors = append(allErrors, fmt.Errorf("%s: %w", actions[index].name, err))
		}
	}
	return errors.Join(allErrors...)
}

type ownedStream struct {
	reader io.ReadCloser
	once   sync.Once
	err    error
}

type ownedAttachment struct {
	connection net.Conn
	once       sync.Once
	err        error
}

func (a *ownedAttachment) Write(content []byte) (int, error) {
	return a.connection.Write(content)
}

func (a *ownedAttachment) CloseWrite() error {
	if connection, ok := a.connection.(client.CloseWriter); ok {
		return connection.CloseWrite()
	}
	return nil
}

func (a *ownedAttachment) Close() error {
	a.once.Do(func() {
		a.err = a.connection.Close()
	})
	return a.err
}

func (s *ownedStream) Read(buffer []byte) (int, error) {
	return s.reader.Read(buffer)
}

func (s *ownedStream) Close() error {
	if s.reader == nil {
		return nil
	}
	s.once.Do(func() {
		s.err = s.reader.Close()
	})
	return s.err
}

type readerOnly struct {
	io.Reader
}

type writerOnly struct {
	io.Writer
}

type waitOutcome struct {
	response container.WaitResponse
	err      error
}
