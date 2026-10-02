// Package agent is the execution plane: an authenticated gRPC stream client
// driving a bounded worker pool for Controller-assigned tasks.
package agent

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/agent/backingadapter"
	composeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	directoryruntime "github.com/AlanD20/groundplane/internal/agent/environmentdirectory"
	logstream "github.com/AlanD20/groundplane/internal/agent/logstream"
	filematerialization "github.com/AlanD20/groundplane/internal/agent/materialization"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/internal/infra/agentstagingjournal"
	"github.com/AlanD20/groundplane/internal/infra/agentterminaljournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	agentChannelAgentMaximumReceiveMessageBytes = 5 * 1024 * 1024
	agentChannelAgentMaximumSendMessageBytes    = 16 * 1024 * 1024
	agentChannelReconnectInitialCeiling         = 100 * time.Millisecond
	agentChannelReconnectMaximumCeiling         = 5 * time.Second
)

type agentStream interface {
	Send(*agentpb.AgentMessage) error
	Recv() (*agentpb.ControllerMessage, error)
	CloseSend() error
}

type streamConnector func(context.Context, string) (agentStream, io.Closer, error)

type reconnectWaiter func(context.Context, uint) error

// Client owns one authenticated, single-use Controller stream. Identity and
// credential material are deliberately private and are never logged.
type Client struct {
	socketPath         string
	agentID            string
	token              [agentprotocol.RawTokenBytes]byte
	processGeneration  [16]byte
	volumeRoot         string
	statePath          string
	openStaging        func(context.Context, *agentstagingjournal.Journal) (*backupStagingState, error)
	openStagingJournal func(string) (*agentstagingjournal.Journal, error)
	logger             *slog.Logger
	connect            streamConnector
	reconnect          reconnectWaiter

	mu                     sync.Mutex
	started                bool
	pool                   *WorkerPool
	workersDone            <-chan struct{}
	compose                *composeruntime.Runtime
	environmentDirectories *directoryruntime.Runtime
	materializer           *filematerialization.Runtime
	adapterCompiler        backingadapter.Compiler
	componentActions       ComponentActionRuntime
	hostResolution         HostResolutionRuntime
	scriptRuntime          ScriptRuntime
	images                 WorkloadImageResolver
	observer               ServiceObserver
	logs                   *logstream.Subscriptions
	terminalJournal        *agentterminaljournal.Journal
	stagingJournal         *agentstagingjournal.Journal
	staging                *backupStagingState
}

func NewClient(
	socketPath string,
	agentID string,
	token []byte,
	volumeRoot string,
	logger *slog.Logger,
) (*Client, error) {
	if socketPath != agentprotocol.SocketPath {
		return nil, errs.New(errs.KindValidationFailed, "agent: invalid Controller socket path")
	}
	if err := ids.Validate(ids.KindAgent, agentID); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "agent: invalid Agent id")
	}
	if len(token) != agentprotocol.RawTokenBytes {
		return nil, errs.Newf(
			errs.KindValidationFailed,
			"agent: channel token must contain exactly %d decoded bytes",
			agentprotocol.RawTokenBytes,
		)
	}
	if logger == nil {
		return nil, errs.New(errs.KindValidationFailed, "agent: logger is required")
	}
	if err := environmentpath.ValidateRoot(volumeRoot); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "agent: invalid volume root policy")
	}

	client := &Client{
		socketPath:         socketPath,
		agentID:            agentID,
		volumeRoot:         volumeRoot,
		statePath:          agentprotocol.StatePath,
		openStaging:        openBackupStaging,
		openStagingJournal: agentstagingjournal.Open,
		logger:             logger,
		connect:            connectGRPC,
		reconnect:          waitForAgentChannelReconnect,
		logs:               logstream.New(nil),
	}
	if _, err := cryptorand.Read(client.processGeneration[:]); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	copy(client.token[:], token)
	return client, nil
}

func NewClientWithRuntimes(
	socketPath string,
	agentID string,
	token []byte,
	volumeRoot string,
	logger *slog.Logger,
	compose *composeruntime.Runtime,
	environmentDirectories *directoryruntime.Runtime,
	materializer *filematerialization.Runtime,
) (*Client, error) {
	if compose == nil || environmentDirectories == nil || materializer == nil {
		return nil, errs.New(errs.KindValidationFailed, "agent: execution runtimes are required")
	}
	client, err := NewClient(socketPath, agentID, token, volumeRoot, logger)
	if err != nil {
		return nil, err
	}
	client.compose = compose
	client.environmentDirectories = environmentDirectories
	client.materializer = materializer
	return client, nil
}

func NewClientWithLogReader(
	socketPath string,
	agentID string,
	token []byte,
	volumeRoot string,
	logger *slog.Logger,
	compose *composeruntime.Runtime,
	environmentDirectories *directoryruntime.Runtime,
	materializer *filematerialization.Runtime,
	logReader agentprotocol.LogReader,
) (*Client, error) {
	if logReader == nil {
		return nil, errs.New(errs.KindValidationFailed, "agent: log reader is required")
	}
	client, err := NewClientWithRuntimes(
		socketPath,
		agentID,
		token,
		volumeRoot,
		logger,
		compose,
		environmentDirectories,
		materializer,
	)
	if err != nil {
		return nil, err
	}
	client.logs = logstream.New(logReader)
	return client, nil
}

// Run authenticates, requires the initial runtime configuration, starts the
// worker pool, and maintains Ready heartbeats until shutdown or cancellation.
func (c *Client) Run(ctx context.Context) error {
	token, err := c.takeToken()
	if err != nil {
		return err
	}
	defer clear(token[:])
	if err := ctx.Err(); err != nil {
		return nil
	}
	if c.reconnect == nil {
		return errs.New(errs.KindInternal, "agent: reconnect waiter is not configured")
	}
	journal, err := agentterminaljournal.Open(c.statePath + "/terminal")
	if err != nil {
		return err
	}
	c.terminalJournal = journal
	defer journal.Close()
	stagingJournal, err := c.openStagingJournal(c.statePath + "/staging")
	if err != nil {
		return err
	}
	c.stagingJournal = stagingJournal
	defer stagingJournal.Close()

	var reconnectAttempt uint
	for {
		reconnect, runErr := c.runSession(ctx, &token)
		if runErr != nil || !reconnect {
			return runErr
		}
		c.logger.WarnContext(
			ctx,
			"agent channel interrupted; reconnecting",
			"attempt",
			reconnectAttempt+1,
		)
		if waitErr := c.reconnect(ctx, reconnectAttempt); waitErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			return errs.New(errs.KindInternal, "agent: reconnect wait failed")
		}
		if reconnectAttempt < 63 {
			reconnectAttempt++
		}
	}
}

func (c *Client) sendBackupCheckpoint(
	stream agentStream,
	request *agentpb.BackupCheckpointRequest,
) error {
	return stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackupCheckpointRequest{
		BackupCheckpointRequest: proto.Clone(request).(*agentpb.BackupCheckpointRequest),
	}})
}

func (c *Client) sendScriptCheckpoint(
	stream agentStream,
	request *agentpb.ScriptCheckpointRequest,
) error {
	return stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ScriptCheckpointRequest{
		ScriptCheckpointRequest: proto.Clone(request).(*agentpb.ScriptCheckpointRequest),
	}})
}

func (c *Client) sendBackingHookCheckpoint(
	stream agentStream,
	request *agentpb.BackingHookCheckpointRequest,
) error {
	return stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackingHookCheckpointRequest{
		BackingHookCheckpointRequest: proto.Clone(request).(*agentpb.BackingHookCheckpointRequest),
	}})
}

func (c *Client) startWorkerPool(ctx context.Context, size int) (context.CancelFunc, <-chan struct{}) {
	poolCtx, cancel := context.WithCancel(ctx)
	var pool *WorkerPool
	if c.compose == nil && c.environmentDirectories == nil && c.materializer == nil {
		pool = NewWorkerPool(size, c.volumeRoot, runner.New(c.logger), c.logger)
	} else {
		pool = NewWorkerPoolWithRuntimes(
			size,
			c.volumeRoot,
			runner.New(c.logger),
			c.logger,
			c.compose,
			c.environmentDirectories,
			c.materializer,
		)
	}
	if c.componentActions != nil {
		pool.SetComponentActionRuntime(c.componentActions)
	}
	if c.adapterCompiler != nil {
		pool.SetAdapterCompiler(c.adapterCompiler)
	}
	if c.hostResolution != nil {
		pool.SetHostResolutionRuntime(c.hostResolution)
	}
	if c.scriptRuntime != nil {
		pool.SetScriptRuntime(c.scriptRuntime)
	}
	c.pool = pool
	pool.persistBackupTerminal = c.persistBackupTerminal
	pool.backupStaging = c.staging
	done := make(chan struct{})
	c.workersDone = done
	go func() {
		defer close(done)
		pool.Run(poolCtx)
	}()
	return cancel, done
}

func validRuntimeConfig(config *agentpb.AgentConfig) bool {
	if config == nil || config.PullIntervalSeconds <= 0 || config.MaxConcurrentTasks <= 0 {
		return false
	}
	seen := make(map[string]struct{}, len(config.Labels))
	for index, label := range config.Labels {
		if label == nil || executionplan.RejectUnknown(label) != nil ||
			!utf8.ValidString(label.Key) || strings.IndexByte(label.Key, 0) >= 0 ||
			!utf8.ValidString(label.Value) || strings.IndexByte(label.Value, 0) >= 0 {
			return false
		}
		if _, duplicate := seen[label.Key]; duplicate {
			return false
		}
		if index > 0 && config.Labels[index-1].Key >= label.Key {
			return false
		}
		seen[label.Key] = struct{}{}
	}
	return true
}

func (c *Client) sendTaskEvent(stream agentStream, progress TaskProgress) error {
	state := agentpb.TaskState_TASK_STATE_UNSPECIFIED
	switch progress.State {
	case TaskProgressRunning:
		state = agentpb.TaskState_TASK_STATE_RUNNING
	case TaskProgressCompleted:
		state = agentpb.TaskState_TASK_STATE_COMPLETED
	case TaskProgressFailed:
		state = agentpb.TaskState_TASK_STATE_FAILED
	case TaskProgressTimedOut:
		state = agentpb.TaskState_TASK_STATE_TIMED_OUT
	case TaskProgressAborted:
		state = agentpb.TaskState_TASK_STATE_ABORTED
	default:
		return errs.New(errs.KindInternal, "agent: worker returned an invalid progress state")
	}
	return stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_TaskEvent{
		TaskEvent: &agentpb.TaskEvent{
			AssignmentId: progress.AssignmentID,
			TaskId:       progress.TaskID, PlanHash: append([]byte(nil), progress.PlanHash[:]...),
			StepId: progress.StepID, ExecutionEpoch: progress.ExecutionEpoch, Ordinal: progress.Ordinal,
			State: state, Chunk: append([]byte(nil), progress.Chunk...),
		},
	}})
}

func (c *Client) takeToken() ([agentprotocol.RawTokenBytes]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return [agentprotocol.RawTokenBytes]byte{}, errs.New(errs.KindInternal, "agent: client has already started")
	}
	c.started = true
	token := c.token
	clear(c.token[:])
	return token, nil
}

func (c *Client) sendReady(stream agentStream) error {
	clean, err := c.terminalDeliveryClean()
	if err != nil {
		return err
	}
	clean = clean && c.staging != nil && c.staging.ready && !c.staging.reinspect.Load()
	capacity := c.pool.Capacity()
	if !clean {
		capacity = 0
	}
	variant := ""
	if runtime.GOARCH == "arm64" {
		variant = "v8"
	}
	return stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Ready{
		Ready: &agentpb.Ready{
			Capacity: int32(capacity), Version: version.Value,
			TerminalDeliveryClean: &clean,
			OperatingSystem:       runtime.GOOS, Architecture: runtime.GOARCH,
			ArchitectureVariant: variant,
		},
	}})
}

func (c *Client) handleControllerMessage(ctx context.Context, message *agentpb.ControllerMessage) (bool, error) {
	if acknowledgement := message.GetTaskEventAck(); acknowledgement != nil {
		if c.pool == nil {
			return false, errs.New(
				errs.KindInternal,
				"agent: Task event acknowledgement arrived before worker configuration",
			)
		}
		return false, c.pool.AcceptTaskEventAck(ctx, acknowledgement)
	}
	if subscription := message.GetLogSubscribe(); subscription != nil {
		c.logs.Subscribe(ctx, proto.Clone(subscription).(*agentpb.LogSubscribe))
		return false, nil
	}
	if cancellation := message.GetLogCancel(); cancellation != nil {
		c.logs.Cancel(cancellation.GetRequestId())
		return false, nil
	}
	if credit := message.GetLogCredit(); credit != nil {
		return false, c.logs.Grant(credit)
	}
	if assignment := message.GetTaskAssignment(); assignment != nil {
		if c.staging == nil || !c.staging.ready {
			return false, invalidAgentStaging()
		}
		if err := c.staging.verifyPreparedAssignment(assignment); err != nil {
			return false, err
		}
		if assignment.ForwardDeadline == nil || assignment.ForwardDeadline.CheckValid() != nil ||
			assignment.RecoveryDeadline == nil || assignment.RecoveryDeadline.CheckValid() != nil ||
			assignment.ExecutionDeadline == nil || assignment.ExecutionDeadline.CheckValid() != nil {
			return false, errs.New(errs.KindInternal, "agent: Controller sent invalid fixed deadlines")
		}
		if assignment.GetExecutionEpoch() == 0 ||
			(assignment.GetExecutionMode() != agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD &&
				assignment.GetExecutionMode() != agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY) {
			return false, errs.New(errs.KindInternal, "agent: Controller sent invalid execution authority")
		}
		defer taskassignment.ClearScriptArtifacts(assignment.ScriptArtifacts)
		executionDeadline := assignment.ExecutionDeadline.AsTime()
		recoveryProofRequired := assignment.GetExecutionMode() == agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY &&
			executionDeadline.After(assignment.RecoveryDeadline.AsTime())
		if assignment.GetExecutionMode() == agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD &&
			!executionDeadline.Equal(assignment.ForwardDeadline.AsTime()) ||
			assignment.GetExecutionMode() == agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY &&
				!recoveryProofRequired && !executionDeadline.Equal(assignment.RecoveryDeadline.AsTime()) {
			return false, errs.New(errs.KindInternal, "agent: Controller sent mismatched execution deadline")
		}
		return false, c.pool.Submit(ctx, taskassignment.Assignment{
			BackupAuthority: assignment.BackupAuthority, BackupResume: assignment.BackupResume,
			BackupAuthoritySHA256: assignment.BackupAuthoritySha256, AssignmentGeneration: assignment.AssignmentGeneration,
			AssignmentID: assignment.AssignmentId,
			TaskID:       assignment.TaskId, OperationID: assignment.OperationId,
			RetryOf: assignment.RetryOf, Plan: assignment.Plan, ScriptArtifacts: assignment.ScriptArtifacts,
			ScriptCheckpoints:  assignment.ScriptCheckpoints,
			AutomaticReconcile: assignment.GetAutomaticReconcile(), ExecutionEpoch: assignment.GetExecutionEpoch(),
			ExecutionMode: assignment.GetExecutionMode(), ForwardDeadline: assignment.ForwardDeadline.AsTime(),
			RecoveryDeadline:            assignment.RecoveryDeadline.AsTime(),
			Deadline:                    executionDeadline,
			RecoveryProofRequired:       recoveryProofRequired,
			RestorationAuthority:        assignment.GetRestorationAuthority(),
			ReleaseRecoveryDirective:    assignment.GetReleaseRecoveryDirective(),
			ReleaseRecoveryRecordSHA256: append([]byte(nil), assignment.GetReleaseRecoveryRecordSha256()...),
		})
	}
	if abort := message.GetTaskAbort(); abort != nil {
		return false, c.handleTaskAbort(ctx, abort)
	}
	if transfer := message.GetMaterializationTransfer(); transfer != nil {
		return false, c.pool.AcceptMaterializationTransfer(ctx, transfer)
	}
	if transfer := message.GetManagedConfigTransfer(); transfer != nil {
		return false, c.pool.AcceptManagedConfigTransfer(ctx, transfer)
	}
	if transfer := message.GetBackupConfigTransfer(); transfer != nil {
		defer backupconfigtransfer.ClearFrame(transfer)
		return false, c.pool.backupConfigs.AcceptFrame(ctx, transfer)
	}
	if credit := message.GetBackupConfigCredit(); credit != nil {
		return false, c.pool.backupConfigs.AcceptCredit(ctx, credit)
	}
	if credit := message.GetBackupVolumeManifestAckCredit(); credit != nil {
		return false, c.pool.AcceptBackupVolumeManifestCredit(credit)
	}
	if frame := message.GetBackupVolumeManifestTransfer(); frame != nil {
		return false, c.pool.AcceptBackupVolumeManifestFrame(frame)
	}
	if transfer := message.GetBackupSecretSlotTransfer(); transfer != nil {
		chunk := transfer.GetChunk()
		if chunk != nil {
			defer func() {
				clear(chunk.Content)
				chunk.Content = nil
			}()
		}
		return false, c.pool.AcceptBackupSecretSlotTransfer(ctx, transfer)
	}
	if message.GetShutdown() != nil {
		return true, nil
	}
	return false, errs.New(errs.KindInternal, "agent: Controller sent an empty message")
}

type receiveResult struct {
	message *agentpb.ControllerMessage
	err     error
}

func receiveNext(ctx context.Context, stream agentStream, result chan<- receiveResult) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		message, err := stream.Recv()
		select {
		case result <- receiveResult{message: message, err: err}:
		case <-ctx.Done():
		}
	}()
	return done
}

func connectGRPC(ctx context.Context, socketPath string) (agentStream, io.Closer, error) {
	connection, err := grpc.NewClient(
		"passthrough:///groundplane-agent",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(agentChannelAgentMaximumReceiveMessageBytes),
			grpc.MaxCallSendMsgSize(agentChannelAgentMaximumSendMessageBytes),
		),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", socketPath)
		}),
	)
	if err != nil {
		return nil, nil, err
	}
	stream, err := agentpb.NewAgentChannelClient(connection).Connect(ctx)
	if err != nil {
		// Best effort: preserve the stream creation failure.
		_ = connection.Close()
		return nil, nil, err
	}
	return stream, connection, nil
}

func agentChannelTransportResult(
	ctx context.Context,
	cause error,
	message string,
	authenticatedReceive bool,
) (bool, error) {
	if ctx.Err() != nil {
		return false, nil
	}
	grpcStatus, hasGRPCStatus := status.FromError(cause)
	code := grpcStatus.Code()
	if errors.Is(cause, io.EOF) ||
		authenticatedReceive && hasGRPCStatus && (code == codes.Internal || code == codes.Unknown) {
		return true, nil
	}
	switch code {
	case codes.Canceled, codes.DeadlineExceeded, codes.Unavailable:
		return true, nil
	default:
		return false, errs.New(errs.KindInternal, message)
	}
}
func waitForAgentChannelReconnect(ctx context.Context, attempt uint) error {
	delay := agentChannelReconnectDelay(attempt, rand.Uint64())
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func agentChannelReconnectDelay(attempt uint, jitter uint64) time.Duration {
	ceiling := agentChannelReconnectInitialCeiling
	for current := uint(0); current < attempt && ceiling < agentChannelReconnectMaximumCeiling; current++ {
		if ceiling > agentChannelReconnectMaximumCeiling/2 {
			ceiling = agentChannelReconnectMaximumCeiling
			break
		}
		ceiling *= 2
	}
	minimum := ceiling / 2
	window := ceiling - minimum
	return minimum + time.Duration(jitter%uint64(window+1))
}
