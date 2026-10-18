// Package agent executes captured native work over an authenticated host channel.
package agent

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"github.com/AlanD20/groundplane/internal/agent/backupruntime"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/agent/attachruntime"
	"github.com/AlanD20/groundplane/internal/agent/backingadapter"
	"github.com/AlanD20/groundplane/internal/agent/backingruntime"
	"github.com/AlanD20/groundplane/internal/agent/componentruntime"
	composeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	"github.com/AlanD20/groundplane/internal/agent/entryruntime"
	directoryruntime "github.com/AlanD20/groundplane/internal/agent/environmentdirectory"
	"github.com/AlanD20/groundplane/internal/agent/environmentruntime"
	"github.com/AlanD20/groundplane/internal/agent/hierarchyruntime"
	"github.com/AlanD20/groundplane/internal/agent/lifecycleruntime"
	logstream "github.com/AlanD20/groundplane/internal/agent/logstream"
	filematerialization "github.com/AlanD20/groundplane/internal/agent/materialization"
	"github.com/AlanD20/groundplane/internal/agent/networkruntime"
	"github.com/AlanD20/groundplane/internal/agent/releaseruntime"
	"github.com/AlanD20/groundplane/internal/agent/removalruntime"
	"github.com/AlanD20/groundplane/internal/agent/resolverruntime"
	"github.com/AlanD20/groundplane/internal/agent/scriptruntime"
	"github.com/AlanD20/groundplane/internal/agent/volumeruntime"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentstagingjournal"
	"github.com/AlanD20/groundplane/internal/infra/agentterminaljournal"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
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
	workJournal         *agentworkjournal.Journal
	environmentExecutor *environmentruntime.Executor
	lifecycleExecutor   *lifecycleruntime.Executor
	backupExecutor      *backupruntime.Executor
	removalRecovery     *removalruntime.Runtime
	socketPath          string
	agentID             string
	token               [agentprotocol.RawTokenBytes]byte
	processGeneration   [16]byte
	volumeRoot          string
	statePath           string
	openStaging         func(context.Context, *agentstagingjournal.Journal) (*backupStagingState, error)
	openStagingJournal  func(string) (*agentstagingjournal.Journal, error)
	logger              *slog.Logger
	connect             streamConnector
	reconnect           reconnectWaiter

	mu                      sync.Mutex
	started                 bool
	pool                    *WorkerPool
	releaseExecutor         *releaseruntime.Executor
	hierarchyExecutor       *hierarchyruntime.Executor
	networkExecutor         *networkruntime.Executor
	componentExecutor       *componentruntime.Executor
	backingExecutor         *backingruntime.Executor
	resolverExecutor        *resolverruntime.Executor
	entryExecutor           *entryruntime.Executor
	volumeExecutor          *volumeruntime.Executor
	attachRemovalExecutor   *hierarchyruntime.AttachExecutor
	attachProvisionExecutor *attachruntime.Executor
	scriptExecutor          *scriptruntime.Executor
	releaseRecoveryExecutor *releaseruntime.RecoveryExecutor
	processOwnership        *agentownership.Lease
	executionWorkers        *executionWorkers
	workersDone             <-chan struct{}
	compose                 *composeruntime.Runtime
	environmentDirectories  *directoryruntime.Runtime
	materializer            *filematerialization.Runtime
	adapterCompiler         backingadapter.Compiler
	componentActions        ComponentActionRuntime
	hostResolution          HostResolutionRuntime
	scriptRuntime           ScriptRuntime
	images                  WorkloadImageResolver
	observer                ServiceObserver
	backingAccess           BackingAccessChecker
	dnsChecks               DNSConnectionChecker
	edgeChecks              EdgeConnectionChecker
	logs                    *logstream.Subscriptions
	terminalJournal         *agentterminaljournal.Journal
	stagingJournal          *agentstagingjournal.Journal
	staging                 *backupStagingState
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

// Run authenticates, reconciles saved native results and runs typed handlers
// with one shared capacity limit until shutdown or cancellation.
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

func (c *Client) handleControllerMessage(ctx context.Context, message *agentpb.ControllerMessage) (bool, error) {
	if message == nil {
		return false, errs.New(errs.KindValidationFailed, "Controller sent an empty message")
	}
	if subscription := message.GetLogSubscribe(); subscription != nil {
		c.logs.Subscribe(ctx, proto.CloneOf(subscription))
		return false, nil
	}
	if cancellation := message.GetLogCancel(); cancellation != nil {
		c.logs.Cancel(cancellation.GetRequestId())
		return false, nil
	}
	if credit := message.GetLogCredit(); credit != nil {
		return false, c.logs.Grant(credit)
	}
	if message.GetShutdown() != nil {
		return true, nil
	}
	return false, errs.New(errs.KindValidationFailed, "Controller sent an unsupported native channel message")
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
