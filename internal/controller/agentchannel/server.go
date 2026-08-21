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

	"github.com/AlanD20/groundplane/internal/common/executionplan"
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
		etcd.TaskResultRecord,
		time.Time,
	) (etcd.Versioned[etcd.TaskRecord], error)
}

// PlanResolver deterministically rebuilds one Task's ephemeral execution plan
// from retained etcd inputs. Rendered artifacts are never persisted.
type PlanResolver interface {
	ResolveExecutionPlan(context.Context, etcd.TaskRecord) (*agentpb.ExecutionPlan, error)
}

// Server terminates the authenticated Controller side of AgentChannel.Connect.
type Server struct {
	agentpb.UnimplementedAgentChannelServer
	auth     Authenticator
	sessions *Registry
	tasks    TaskStore
	plans    PlanResolver
	now      func() time.Time
}

// New returns an AgentChannel server backed by the supplied authenticator and
// session registry. A nil registry creates an isolated registry.
func New(auth Authenticator, sessions *Registry, tasks TaskStore, plans PlanResolver) *Server {
	if sessions == nil {
		sessions = NewRegistry()
	}
	return &Server{auth: auth, sessions: sessions, tasks: tasks, plans: plans, now: time.Now}
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
		if err := s.sendTaskAssignment(stream, assignment.Task.Record); err != nil {
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
		if err := s.sendTaskAssignment(stream, assignment.Task.Record); err != nil {
			return err
		}
		delivered[assignment.Task.Record.ID] = struct{}{}
		remaining--
	}
	return nil
}

func (s *Server) sendTaskAssignment(
	stream agentpb.AgentChannel_ConnectServer,
	task etcd.TaskRecord,
) error {
	assignment, err := s.taskAssignmentMessage(stream.Context(), task)
	if err != nil {
		return err
	}
	return stream.Send(&agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_TaskAssignment{TaskAssignment: assignment},
	})
}

func (s *Server) taskAssignmentMessage(ctx context.Context, task etcd.TaskRecord) (*agentpb.TaskAssignment, error) {
	if s.plans == nil {
		return nil, errs.New(errs.KindInternal, "execution plan resolver is not configured")
	}
	planHash, err := hex.DecodeString(task.PlanHash)
	if err != nil || len(planHash) != 32 {
		return nil, errs.New(errs.KindInternal, "durable Task has an invalid plan hash")
	}
	if task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxInt32 {
		return nil, errs.New(errs.KindInternal, "durable Task has an invalid Agent timeout")
	}
	resolved, err := s.plans.ResolveExecutionPlan(ctx, task)
	if err != nil {
		return nil, err
	}
	plan, err := executionplan.Validate(resolved)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if plan.PlanId != task.PlanID || !bytes.Equal(plan.PlanHash, planHash) ||
		plan.RenderGeneration != uint64(task.RenderGeneration) || plan.TargetId != task.Target ||
		!operationMatchesTask(plan.Operation, task.Type) || !stepSummariesMatch(plan.Steps, task.Steps) {
		return nil, errs.New(errs.KindInternal, "resolved execution plan does not match its durable Task")
	}
	return &agentpb.TaskAssignment{
		TaskId: task.ID, OperationId: task.OperationID, RetryOf: task.RetryOf,
		Plan: plan, TimeoutSeconds: int32(task.TimeoutSeconds),
	}, nil
}

func stepSummariesMatch(steps []*agentpb.ExecutionStep, summaries []etcd.TaskStepRecord) bool {
	if len(steps) != len(summaries) {
		return false
	}
	for index, step := range steps {
		if step == nil || step.StepId != summaries[index].ID {
			return false
		}
	}
	return true
}

func operationMatchesTask(operation agentpb.PlanOperation, taskType etcd.TaskType) bool {
	switch taskType {
	case etcd.TaskDeploy:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_DEPLOY
	case etcd.TaskRollback:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK
	case etcd.TaskStart:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_START
	case etcd.TaskStop:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_STOP
	case etcd.TaskDestroy:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_DESTROY
	case etcd.TaskRemove:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_REMOVE
	default:
		return false
	}
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
	if err := validateComposeTaskResult(acknowledgement); err != nil {
		return err
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
		durableComposeTaskResult(acknowledgement),
		s.now().UTC(),
	)
	return err
}

func durableComposeTaskResult(acknowledgement *agentpb.TaskAck) etcd.TaskResultRecord {
	result := acknowledgement.GetComposeResult()
	diagnostic := etcd.TaskResultDiagnosticNone
	switch result.GetDiagnostic() {
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_CONFIG_REJECTED:
		diagnostic = etcd.TaskResultDiagnosticConfigRejected
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED:
		diagnostic = etcd.TaskResultDiagnosticComposeFailed
	}
	durable := etcd.TaskResultRecord{
		Kind: etcd.TaskResultCompose, ExitCode: acknowledgement.GetExitCode(),
		FailedStepID: result.GetFailedStepId(), Diagnostic: diagnostic,
		ReconciliationRequired: result.GetReconciliationRequired(),
		Projects:               make([]etcd.TaskObservedProjectSummary, len(result.GetProjects())),
	}
	for index, project := range result.GetProjects() {
		durable.Projects[index] = etcd.TaskObservedProjectSummary{
			ProjectName: project.GetProjectName(), ObservedAt: project.GetObservedAt().AsTime().UTC(),
			ContainerCount: uint32(len(project.GetContainers())), NetworkCount: uint32(len(project.GetNetworks())),
			VolumeCount: uint32(len(project.GetVolumes())), CollisionCount: uint32(len(project.GetCollisions())),
		}
	}
	return durable
}

func validateComposeTaskResult(acknowledgement *agentpb.TaskAck) error {
	result := acknowledgement.GetComposeResult()
	if result == nil || len(result.GetProjects()) > 64 {
		return errs.New(errs.KindValidationFailed, "Agent Compose Task result is invalid")
	}
	switch result.GetDiagnostic() {
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_CONFIG_REJECTED,
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED:
	default:
		return errs.New(errs.KindValidationFailed, "Agent Compose Task diagnostic is invalid")
	}
	if acknowledgement.GetTerminal() == agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED &&
		(acknowledgement.GetExitCode() != 0 || result.GetFailedStepId() != "" ||
			result.GetDiagnostic() != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE ||
			result.GetReconciliationRequired()) {
		return errs.New(errs.KindValidationFailed, "completed Agent Compose Task result is inconsistent")
	}
	for _, project := range result.GetProjects() {
		if project == nil || project.GetProjectName() == "" || project.GetObservedAt() == nil ||
			project.GetObservedAt().CheckValid() != nil || len(project.GetContainers()) > 4096 ||
			len(project.GetNetworks()) > 4096 || len(project.GetVolumes()) > 4096 ||
			len(project.GetCollisions()) > 4096 {
			return errs.New(errs.KindValidationFailed, "Agent Compose Task observation is invalid")
		}
		for _, collision := range project.GetCollisions() {
			if collision == nil || collision.GetKind() == agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_UNSPECIFIED ||
				collision.GetName() == "" {
				return errs.New(errs.KindValidationFailed, "Agent Compose Task collision is invalid")
			}
		}
	}
	return nil
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
