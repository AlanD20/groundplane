// Package postgres16execution is the Docker-side port for the closed managed
// PostgreSQL 16 helper. It never accepts a client command, SQL, or environment
// from a workload or an operator.
package postgres16execution

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const dockerSocketPath = "/var/run/docker.sock"

// Engine is the concrete Docker process boundary, not an alternative command
// runner. ExecAttach starts the previously created Exec in the SDK in use.
type Engine interface {
	ImageInspect(context.Context, string, ...client.ImageInspectOption) (client.ImageInspectResult, error)
	ContainerList(context.Context, client.ContainerListOptions) (client.ContainerListResult, error)
	ContainerInspect(context.Context, string, client.ContainerInspectOptions) (client.ContainerInspectResult, error)
	VolumeInspect(context.Context, string, client.VolumeInspectOptions) (client.VolumeInspectResult, error)
	ExecCreate(context.Context, string, client.ExecCreateOptions) (client.ExecCreateResult, error)
	ExecAttach(context.Context, string, client.ExecAttachOptions) (client.ExecAttachResult, error)
	ExecStart(context.Context, string, client.ExecStartOptions) (client.ExecStartResult, error)
	ExecInspect(context.Context, string, client.ExecInspectOptions) (client.ExecInspectResult, error)
	Close() error
}

// StartRecorder is the Agent's durable assignment/step checkpoint boundary.
// It MUST reject a previous live attempt even with a fresh nonce. A lost Ack
// remains spent. Both methods persist exact Exec/image/source evidence before
// returning nil, because ExecAttach itself starts the helper.
type StartRecorder interface {
	RecordDumpStart(context.Context, Start) error
	RecordRestoreApplyStart(context.Context, Start) error
}

type Start struct {
	Request        postgres16protocol.Request
	Container      Container
	ExecID         string
	ImageReference string
	ImageID        string
	ManifestDigest string
}

// Mount is an exact observed mount claim from the caller's sealed container
// plan. The complete list is compared, so an extra mount cannot shadow the
// helper, private gate, clients, state, or local PostgreSQL socket.
type Mount struct {
	Type        mount.Type
	Name        string
	Source      string
	Destination string
	Driver      string
	Mode        string
	RW          bool
	Propagation mount.Propagation
}

// Container is the immutable per-execution identity supplied by the Agent's
// sealed Service/assignment plan, never derived from the current Docker inspect.
type Container struct {
	ID              string
	ImageID         string
	Name            string
	NetworkMode     string
	Runtime         string
	AppArmorProfile string
	UsernsMode      string
	CgroupnsMode    string
	Labels          map[string]string
	Mounts          []Mount
}

// Result carries Docker's exact Exec ID/inspection together with bounded
// stream evidence. Proof is present only for the closed proof-output operations.
type Result struct {
	ExecID   string
	ExitCode postgres16protocol.ExitCode
	Inspect  client.ExecInspectResult
	Stdin    postgres16protocol.ConfinementStreamEvidence
	Stdout   postgres16protocol.ConfinementStreamEvidence
	Stderr   postgres16protocol.ConfinementStreamEvidence
	Proof    []byte
}

type Executor struct {
	engine         Engine
	authority      postgres16protocol.ConfinementReleaseAuthority
	manifest       postgres16protocol.ManagedReleaseManifest
	imageReference string
	indexReference string
	imageID        string
	manifestDigest string
	startRecorder  StartRecorder
	mu             sync.Mutex
	spent          map[postgres16protocol.Nonce]struct{}
}

func New(
	index postgres16protocol.ManagedReleaseIndex,
	architecture string,
	recorder StartRecorder,
) (*Executor, error) {
	engine, err := client.New(client.WithHost("unix://" + dockerSocketPath))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	executor, err := NewWithEngine(engine, index, architecture, recorder)
	if err != nil {
		_ = engine.Close()
		return nil, err
	}
	return executor, nil
}

func NewWithEngine(
	engine Engine,
	index postgres16protocol.ManagedReleaseIndex,
	architecture string,
	recorder StartRecorder,
) (*Executor, error) {
	release, err := index.Select(architecture)
	if err != nil {
		return nil, err
	}
	authority, err := release.Manifest.Authority()
	if engine == nil || err != nil || release.Validate() != nil {
		return nil, errs.New(errs.KindValidationFailed, "managed PostgreSQL executor release is invalid")
	}
	return &Executor{
		engine: engine, authority: authority, manifest: release.Manifest,
		imageReference: release.RepositoryDigest, imageID: release.ImageID,
		indexReference: index.Image, manifestDigest: release.ManifestDigest(), startRecorder: recorder,
		spent: make(map[postgres16protocol.Nonce]struct{}),
	}, nil
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

// Execute is one attempt. Source is required only for restore-list/apply;
// artifact is required only for dump. Both must unblock when closed. After
// ExecAttach was attempted, every transport, deadline or inspection ambiguity
// returns RecoveryRequired and never performs an automatic retry.
func (executor *Executor) Execute(
	ctx context.Context, expected Container, request postgres16protocol.Request,
	source io.ReadCloser, artifact io.WriteCloser,
) (Result, error) {
	if executor == nil || executor.engine == nil || ctx == nil || request.Validate() != nil {
		return Result{}, errs.New(errs.KindValidationFailed, "managed PostgreSQL execution request is invalid")
	}
	policy := postgres16protocol.StreamPolicy{
		Input: postgres16protocol.InputNone, Output: postgres16protocol.OutputDiscardCount,
		OutputLimit: postgres16protocol.DiagnosticLimitBytes,
		StderrLimit: postgres16protocol.DiagnosticLimitBytes,
	}
	var err error
	if request.Operation != postgres16protocol.OperationStop {
		policy, err = postgres16protocol.PolicyFor(request.Operation)
	}
	if err != nil || (policy.Input == postgres16protocol.InputRestoreSource) != (source != nil) ||
		(policy.Output == postgres16protocol.OutputArtifact) != (artifact != nil) ||
		request.DeadlineUnixNano > uint64(^uint64(0)>>1) {
		return Result{}, errs.New(errs.KindValidationFailed, "managed PostgreSQL execution streams are invalid")
	}
	deadline := time.Unix(0, int64(request.DeadlineUnixNano))
	if !time.Now().Before(deadline) {
		return Result{}, errs.New(errs.KindValidationFailed, "managed PostgreSQL execution deadline elapsed")
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err := executor.attest(ctx, expected); err != nil {
		return Result{}, err
	}
	command, err := request.DockerExecCommand()
	if err != nil {
		return Result{}, err
	}
	created, err := executor.engine.ExecCreate(ctx, expected.ID, client.ExecCreateOptions{
		User: "0:0", Privileged: false, TTY: false,
		AttachStdin: source != nil, AttachStdout: true, AttachStderr: true,
		Env: postgres16protocol.Environment(), WorkingDir: "/", Cmd: command,
	})
	if err != nil || !validDockerID(created.ID) {
		return Result{}, errs.New(errs.KindInternal, "managed PostgreSQL Exec creation is unavailable")
	}
	result := Result{ExecID: created.ID}
	if err := executor.attest(ctx, expected); err != nil {
		return result, err
	}
	if request.Operation == postgres16protocol.OperationDump ||
		request.Operation == postgres16protocol.OperationRestoreApply {
		if err := executor.acknowledgeStart(ctx, request, expected, created.ID); err != nil {
			result.ExitCode = postgres16protocol.ExitRecoveryRequired
			return result, err
		}
	}
	result, err = executor.attachAndInspect(ctx, expected, request, policy, source, artifact, result)
	if err == nil && request.Operation.ValidRun() && request.Operation != postgres16protocol.OperationDump &&
		request.Operation != postgres16protocol.OperationRestoreApply {
		err = executor.RetireExecution(ctx, expected, request)
	}
	return result, err
}

func (executor *Executor) acknowledgeStart(
	ctx context.Context, request postgres16protocol.Request, expected Container, execID string,
) error {
	if executor.startRecorder == nil {
		return errs.New(errs.KindStateConflict, "managed PostgreSQL start recorder is unavailable")
	}
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if _, exists := executor.spent[request.Nonce]; exists {
		return errs.New(errs.KindStateConflict, "managed PostgreSQL attempt is spent")
	}
	// Mark before the durable call: an unknown recorder outcome cannot be used
	// to make a second start in this process. The recorder owns restart safety.
	executor.spent[request.Nonce] = struct{}{}
	start := Start{
		Request: request, Container: expected, ExecID: execID,
		ImageReference: executor.imageReference, ImageID: expected.ImageID,
		ManifestDigest: executor.manifestDigest,
	}
	if request.Operation == postgres16protocol.OperationDump {
		return executor.startRecorder.RecordDumpStart(ctx, start)
	}
	return executor.startRecorder.RecordRestoreApplyStart(ctx, start)
}
