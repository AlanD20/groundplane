package agentchannel

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const tokenSize = 32

// Token is the decoded, fixed-size Agent credential presented to an Authenticator.
type Token [tokenSize]byte

// Authorization is the non-secret result of successful Agent authentication.
type Authorization struct {
	Generation uint64
	Config     *agentpb.AgentConfig
}

// Authenticator authenticates one Agent generation without retaining or
// returning its credential.
type Authenticator interface {
	Authenticate(context.Context, string, Token) (Authorization, error)
}

// TaskStore is the durable execution seam used by one authenticated stream.
// Its etcd DTOs remain inside the Controller daemon and never cross the human
// API boundary.
type TaskStore interface {
	ListAgentAssignments(context.Context, string, uint64, int32) ([]etcd.TaskAssignment, error)
	ClaimNextTask(context.Context, string, uint64, time.Time) (etcd.TaskAssignment, bool, error)
	GetTask(context.Context, string) (etcd.Versioned[etcd.TaskRecord], error)
	AppendTaskEvent(context.Context, etcd.TaskEventInput, time.Time) (etcd.TaskEventAppend, error)
	AcknowledgeTask(
		context.Context,
		string,
		uint64,
		string,
		etcd.TaskStatus,
		time.Time,
	) (etcd.Versioned[etcd.TaskRecord], error)
}

// Server terminates the authenticated Controller side of AgentChannel.Connect.
type Server struct {
	agentpb.UnimplementedAgentChannelServer
	auth     Authenticator
	sessions *Registry
	tasks    TaskStore
	now      func() time.Time
}

// New returns an AgentChannel server backed by the supplied authenticator and
// session registry. A nil registry creates an isolated registry.
func New(auth Authenticator, sessions *Registry, tasks TaskStore) *Server {
	if sessions == nil {
		sessions = NewRegistry()
	}
	return &Server{auth: auth, sessions: sessions, tasks: tasks, now: time.Now}
}

// Connect authenticates the first message, publishes the authorized config,
// and then owns the live session until disconnect or fencing.
func (s *Server) Connect(stream agentpb.AgentChannel_ConnectServer) error {
	if s.auth == nil {
		return status.Error(codes.Internal, "agent channel is not configured")
	}

	first, err := stream.Recv()
	if err != nil {
		return unauthenticated()
	}
	authenticate, token, err := validateAuthenticate(first)
	if err != nil {
		return unauthenticated()
	}
	defer clear(token[:])
	defer clear(authenticate.Token)

	authorization, authErr := s.auth.Authenticate(stream.Context(), authenticate.AgentId, token)
	if authErr != nil {
		return unauthenticated()
	}
	if authorization.Config == nil {
		return status.Error(codes.Internal, "agent configuration is not available")
	}
	if authorization.Generation == 0 {
		return status.Error(codes.Internal, "agent authorization generation is not available")
	}
	if authorization.Config.PullIntervalSeconds <= 0 ||
		authorization.Config.MaxConcurrentTasks <= 0 {
		return status.Error(codes.Internal, "agent configuration is invalid")
	}

	session, err := s.sessions.Open(stream.Context(), authenticate.AgentId, authorization.Generation)
	if err != nil {
		return status.Error(codes.FailedPrecondition, "agent session is not current")
	}
	defer session.Close()
	delivered := make(map[string]struct{}, authorization.Config.MaxConcurrentTasks)

	config := proto.Clone(authorization.Config).(*agentpb.AgentConfig)
	if err := stream.Send(&agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_ConfigUpdate{
			ConfigUpdate: &agentpb.ConfigUpdate{AgentConfig: config},
		},
	}); err != nil {
		return err
	}

	type receiveResult struct {
		message *agentpb.AgentMessage
		err     error
	}
	received := make(chan receiveResult, 1)
	go func() {
		for {
			message, recvErr := stream.Recv()
			select {
			case received <- receiveResult{message: message, err: recvErr}:
			case <-session.Done():
				return
			}
			if recvErr != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-session.Done():
			return nil
		case result := <-received:
			if result.err != nil {
				if errors.Is(result.err, io.EOF) || errors.Is(result.err, context.Canceled) {
					return nil
				}
				return result.err
			}
			if result.message == nil {
				return status.Error(codes.InvalidArgument, "agent message is required")
			}
			if result.message.GetAuthenticate() != nil {
				return unauthenticated()
			}

			if ready := result.message.GetReady(); ready != nil {
				if ready.Capacity > authorization.Config.MaxConcurrentTasks {
					return status.Error(codes.InvalidArgument, "agent Ready capacity exceeds configuration")
				}
				if err := session.RecordReady(s.now(), ready.Capacity); err != nil {
					if ready.Capacity < 0 {
						return status.Error(codes.InvalidArgument, "agent Ready capacity must be non-negative")
					}
					return status.Error(codes.FailedPrecondition, "agent session is not current")
				}
				if s.tasks == nil {
					return status.Error(codes.Internal, "agent task store is not configured")
				}
				if err := s.dispatchReady(
					stream,
					session,
					authenticate.AgentId,
					authorization,
					ready.Capacity,
					delivered,
				); err != nil {
					return taskStoreStatus(err)
				}
				continue
			}
			if acknowledgement := result.message.GetTaskAck(); acknowledgement != nil {
				if s.tasks == nil {
					return status.Error(codes.Internal, "agent task store is not configured")
				}
				if err := s.acknowledge(
					stream.Context(),
					authenticate.AgentId,
					authorization.Generation,
					acknowledgement,
				); err != nil {
					return taskStoreStatus(err)
				}
				delete(delivered, acknowledgement.TaskId)
				continue
			}
			if event := result.message.GetTaskEvent(); event != nil {
				if s.tasks == nil {
					return status.Error(codes.Internal, "agent task store is not configured")
				}
				if err := s.recordTaskEvent(stream.Context(), event); err != nil {
					return taskStoreStatus(err)
				}
				continue
			}
			if result.message.GetObservedState() != nil {
				return status.Error(codes.Unimplemented, "authenticated agent message is not implemented")
			}
			return status.Error(codes.InvalidArgument, "authenticated agent message is empty")
		}
	}
}

func (s *Server) recordTaskEvent(ctx context.Context, event *agentpb.TaskEvent) error {
	if event == nil || len(event.PlanHash) != 32 || event.Attempt == 0 || event.Ordinal == 0 {
		return errs.New(errs.KindValidationFailed, "Agent Task event is invalid")
	}
	if len(event.Chunk) != 0 {
		return status.Error(codes.Unimplemented, "ephemeral Task output streaming is not implemented")
	}
	task, err := s.tasks.GetTask(ctx, event.TaskId)
	if err != nil {
		return err
	}
	planHash, err := hex.DecodeString(task.Record.PlanHash)
	if err != nil || !bytes.Equal(planHash, event.PlanHash) {
		return errs.New(errs.KindStateConflict, "Agent Task event plan hash does not match")
	}
	if task.Record.Status != etcd.TaskStatusRunning {
		return errs.New(errs.KindStateConflict, "Agent Task event does not belong to a running Task")
	}
	var state etcd.TaskEventState
	switch event.State {
	case agentpb.TaskState_TASK_STATE_PENDING:
		state = etcd.TaskEventStatePending
	case agentpb.TaskState_TASK_STATE_RUNNING:
		state = etcd.TaskEventStateRunning
	case agentpb.TaskState_TASK_STATE_COMPLETED:
		state = etcd.TaskEventStateCompleted
	case agentpb.TaskState_TASK_STATE_FAILED:
		state = etcd.TaskEventStateFailed
	case agentpb.TaskState_TASK_STATE_ABORTED:
		state = etcd.TaskEventStateAborted
	case agentpb.TaskState_TASK_STATE_TIMED_OUT:
		state = etcd.TaskEventStateTimedOut
	default:
		return errs.New(errs.KindValidationFailed, "Agent Task event state is invalid")
	}
	_, err = s.tasks.AppendTaskEvent(ctx, etcd.TaskEventInput{
		Identity: etcd.TaskEventIdentity{
			TaskID: event.TaskId, StepID: event.StepId,
			Attempt: event.Attempt, Ordinal: event.Ordinal,
		},
		State:   state,
		Payload: json.RawMessage(`{}`),
	}, s.now().UTC())
	return err
}

func (s *Server) dispatchReady(
	stream agentpb.AgentChannel_ConnectServer,
	session *Session,
	agentID string,
	authorization Authorization,
	capacity int32,
	delivered map[string]struct{},
) error {
	if capacity == 0 || !session.AssignmentsAllowed() {
		return nil
	}
	recovered, err := s.tasks.ListAgentAssignments(
		stream.Context(),
		agentID,
		authorization.Generation,
		authorization.Config.MaxConcurrentTasks,
	)
	if err != nil {
		return err
	}
	remaining := capacity
	for _, assignment := range recovered {
		if _, alreadyDelivered := delivered[assignment.Task.Record.ID]; alreadyDelivered {
			continue
		}
		if remaining == 0 {
			return nil
		}
		if err := sendTaskAssignment(stream, assignment.Task.Record); err != nil {
			return err
		}
		delivered[assignment.Task.Record.ID] = struct{}{}
		remaining--
	}
	for remaining > 0 {
		assignment, found, err := s.tasks.ClaimNextTask(
			stream.Context(),
			agentID,
			authorization.Generation,
			s.now().UTC(),
		)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if err := sendTaskAssignment(stream, assignment.Task.Record); err != nil {
			return err
		}
		delivered[assignment.Task.Record.ID] = struct{}{}
		remaining--
	}
	return nil
}

func sendTaskAssignment(
	stream agentpb.AgentChannel_ConnectServer,
	task etcd.TaskRecord,
) error {
	assignment, err := taskAssignmentMessage(task)
	if err != nil {
		return err
	}
	return stream.Send(&agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_TaskAssignment{TaskAssignment: assignment},
	})
}

func taskAssignmentMessage(task etcd.TaskRecord) (*agentpb.TaskAssignment, error) {
	planHash, err := hex.DecodeString(task.PlanHash)
	if err != nil || len(planHash) != 32 {
		return nil, errs.New(errs.KindInternal, "durable Task has an invalid plan hash")
	}
	if task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxInt32 {
		return nil, errs.New(errs.KindInternal, "durable Task has an invalid Agent timeout")
	}
	params := task.Params
	if params == nil {
		params = map[string]string{}
	}
	encodedParams, err := json.Marshal(params)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	steps := make([]*agentpb.Step, len(task.Steps))
	for index, step := range task.Steps {
		steps[index] = &agentpb.Step{
			StepId: step.ID,
			Op:     step.Op,
			Params: cloneTaskParams(step.Params),
		}
	}
	return &agentpb.TaskAssignment{
		TaskId: task.ID, OperationId: task.OperationID, RetryOf: task.RetryOf,
		PlanId: task.PlanID, PlanHash: planHash, RenderGeneration: task.RenderGeneration,
		Type: string(task.Type), Target: task.Target, Params: encodedParams,
		Steps: steps, TimeoutSeconds: int32(task.TimeoutSeconds),
	}, nil
}

func cloneTaskParams(params map[string]string) map[string]string {
	if params == nil {
		return nil
	}
	cloned := make(map[string]string, len(params))
	for key, value := range params {
		cloned[key] = value
	}
	return cloned
}

func (s *Server) acknowledge(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	acknowledgement *agentpb.TaskAck,
) error {
	if acknowledgement == nil || len(acknowledgement.PlanHash) != 32 {
		return errs.New(errs.KindValidationFailed, "Agent Task acknowledgement is invalid")
	}
	if acknowledgement.ExitCode != 0 || len(acknowledgement.Result) != 0 {
		return status.Error(codes.Unimplemented, "typed Task result is not implemented")
	}
	task, err := s.tasks.GetTask(ctx, acknowledgement.TaskId)
	if err != nil {
		return err
	}
	planHash, err := hex.DecodeString(task.Record.PlanHash)
	if err != nil || !bytes.Equal(planHash, acknowledgement.PlanHash) {
		return errs.New(errs.KindStateConflict, "Agent Task acknowledgement plan hash does not match")
	}
	var terminal etcd.TaskStatus
	switch acknowledgement.Terminal {
	case agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED:
		terminal = etcd.TaskStatusCompleted
	case agentpb.TaskTerminal_TASK_TERMINAL_FAILED:
		terminal = etcd.TaskStatusFailed
	case agentpb.TaskTerminal_TASK_TERMINAL_TIMED_OUT:
		terminal = etcd.TaskStatusTimedOut
	case agentpb.TaskTerminal_TASK_TERMINAL_ABORTED:
		terminal = etcd.TaskStatusAborted
	default:
		return errs.New(errs.KindValidationFailed, "Agent Task acknowledgement terminal state is invalid")
	}
	_, err = s.tasks.AcknowledgeTask(
		ctx,
		agentID,
		agentGeneration,
		acknowledgement.TaskId,
		terminal,
		s.now().UTC(),
	)
	return err
}

func taskStoreStatus(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.Unknown {
		return err
	}
	kind, ok := errs.KindOf(err)
	if !ok {
		return status.Error(codes.Internal, "agent task operation failed")
	}
	switch kind {
	case errs.KindValidationFailed:
		return status.Error(codes.InvalidArgument, "agent task message is invalid")
	case errs.KindTaskNotFound:
		return status.Error(codes.NotFound, "agent task was not found")
	case errs.KindStateConflict:
		return status.Error(codes.FailedPrecondition, "agent task state does not match")
	case errs.KindStorageUnavailable:
		return status.Error(codes.Unavailable, "agent task storage is unavailable")
	default:
		return status.Error(codes.Internal, "agent task operation failed")
	}
}

func validateAuthenticate(message *agentpb.AgentMessage) (*agentpb.Authenticate, Token, error) {
	if message == nil || message.GetAuthenticate() == nil {
		return nil, Token{}, errs.New(errs.KindValidationFailed, "Authenticate must be the first Agent message")
	}
	authenticate := message.GetAuthenticate()
	if err := ids.Validate(ids.KindAgent, authenticate.AgentId); err != nil {
		return nil, Token{}, errs.New(errs.KindValidationFailed, "agent id is invalid")
	}
	if len(authenticate.Token) != tokenSize {
		return nil, Token{}, errs.New(errs.KindValidationFailed, "agent token has an invalid length")
	}

	var token Token
	copy(token[:], authenticate.Token)
	return authenticate, token, nil
}

func unauthenticated() error {
	return status.Error(codes.Unauthenticated, "agent authentication failed")
}
