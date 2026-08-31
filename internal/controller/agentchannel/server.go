package agentchannel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/netip"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/managedconfig"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	Configuration(context.Context, string, uint64) (*agentpb.AgentConfig, error)
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
		string,
		etcd.TaskStatus,
		etcd.TaskResultRecord,
		time.Time,
	) (etcd.Versioned[etcd.TaskRecord], error)
}
type environmentCreationTaskStore interface {
	AcknowledgeEnvironmentCreation(
		context.Context,
		string,
		uint64,
		string,
		string,
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

type ScriptArtifactResolver interface {
	ResolveScriptAssignmentArtifacts(
		context.Context,
		etcd.TaskRecord,
		*agentpb.ExecutionPlan,
	) (*agentpb.ScriptAssignmentArtifacts, error)
	ResolveScriptExecutionCheckpoints(
		context.Context,
		etcd.TaskRecord,
		*agentpb.ExecutionPlan,
	) ([]*agentpb.ScriptExecutionCheckpoint, error)
}

// MaterializationResolver returns one task-owned transient plaintext source.
// The channel takes ownership and closes it on every path.
type MaterializationResolver interface {
	ResolveMaterialization(
		context.Context,
		etcd.TaskRecord,
		*agentpb.ExecutionPlan,
		*agentpb.ExecutionStep,
	) (io.ReadCloser, error)
}

type ManagedConfigSource struct {
	MediaType string
	Length    uint64
	Content   io.ReadCloser
}

// ManagedConfigResolver reconstructs one immutable Component artifact from
// durable, revision-pinned inputs. The channel owns and closes Content.
type ManagedConfigResolver interface {
	ResolveManagedConfig(
		context.Context,
		etcd.TaskRecord,
		*agentpb.ExecutionPlan,
		*agentpb.ExecutionStep,
	) (ManagedConfigSource, error)
}

// BackupSecretSlotResolver returns task-owned transient plaintext slots. The
// channel takes ownership of every returned buffer and clears it on every path.
type BackupSecretSlotResolver interface {
	ResolveBackupSecretSlots(
		context.Context,
		backupsecret.Request,
	) (map[agentpb.BackupSecretSlotPurpose][]byte, error)
}

// BackupCheckpointer durably accepts one Agent operation boundary before the
// stream acknowledges that the next side effect is authorized.
type BackupCheckpointer interface {
	CheckpointBackup(
		context.Context,
		string,
		uint64,
		*agentpb.BackupCheckpointRequest,
	) (*agentpb.BackupCheckpointAck, error)
}

type ScriptCheckpointer interface {
	CheckpointScript(
		context.Context,
		string,
		uint64,
		*agentpb.ScriptCheckpointRequest,
	) (*agentpb.ScriptCheckpointAck, error)
}

// Server terminates the authenticated Controller side of AgentChannel.Connect.
type Server struct {
	agentpb.UnimplementedAgentChannelServer
	auth              Authenticator
	sessions          *Registry
	tasks             TaskStore
	plans             PlanResolver
	materials         MaterializationResolver
	managed           ManagedConfigResolver
	secrets           BackupSecretSlotResolver
	checkpoints       BackupCheckpointer
	scriptCheckpoints ScriptCheckpointer
	scriptArtifacts   ScriptArtifactResolver
	now               func() time.Time
}

func (s *Server) EnableManagedConfigTransfers(resolver ManagedConfigResolver) error {
	if s == nil || resolver == nil {
		return errs.New(errs.KindInternal, "managed-config resolver is required")
	}
	s.managed = resolver
	return nil
}

func (s *Server) EnableScriptArtifacts(resolver ScriptArtifactResolver) error {
	if s == nil || resolver == nil {
		return errs.New(errs.KindInternal, "Script artifact resolver is required")
	}
	s.scriptArtifacts = resolver
	return nil
}

// New returns an AgentChannel server backed by the supplied authenticator and
// session registry. A nil registry creates an isolated registry.
func New(auth Authenticator, sessions *Registry, tasks TaskStore, plans PlanResolver) *Server {
	return NewWithMaterializations(auth, sessions, tasks, plans, nil)
}

// NewWithMaterializations returns a server with the transient value resolver
// needed by metadata-only materialization steps.
func NewWithMaterializations(
	auth Authenticator,
	sessions *Registry,
	tasks TaskStore,
	plans PlanResolver,
	materials MaterializationResolver,
) *Server {
	return NewWithPrivateTransfers(auth, sessions, tasks, plans, materials, nil)
}

// NewWithPrivateTransfers returns a server with both transient plaintext
// resolvers used by the sole authenticated stream send loop.
func NewWithPrivateTransfers(
	auth Authenticator,
	sessions *Registry,
	tasks TaskStore,
	plans PlanResolver,
	materials MaterializationResolver,
	secrets BackupSecretSlotResolver,
) *Server {
	return NewWithRuntimeServices(auth, sessions, tasks, plans, materials, secrets, nil)
}

// NewWithRuntimeServices returns a server with all private transfer and
// durable operation-boundary services used by the authenticated stream loop.
func NewWithRuntimeServices(
	auth Authenticator,
	sessions *Registry,
	tasks TaskStore,
	plans PlanResolver,
	materials MaterializationResolver,
	secrets BackupSecretSlotResolver,
	checkpoints BackupCheckpointer,
) *Server {
	if sessions == nil {
		sessions = NewRegistry()
	}
	return &Server{
		auth:        auth,
		sessions:    sessions,
		tasks:       tasks,
		plans:       plans,
		materials:   materials,
		secrets:     secrets,
		checkpoints: checkpoints,
		now:         time.Now,
	}
}

// NewWithManagedRuntimeServices returns the authenticated Agent channel with
// the generic managed-config transfer capability enabled when managed is set.
func NewWithManagedRuntimeServices(
	auth Authenticator,
	sessions *Registry,
	tasks TaskStore,
	plans PlanResolver,
	materials MaterializationResolver,
	secrets BackupSecretSlotResolver,
	checkpoints BackupCheckpointer,
	managed ManagedConfigResolver,
) *Server {
	server := NewWithRuntimeServices(auth, sessions, tasks, plans, materials, secrets, checkpoints)
	server.managed = managed
	return server
}

func NewWithScriptRuntimeServices(
	auth Authenticator,
	sessions *Registry,
	tasks TaskStore,
	plans PlanResolver,
	materials MaterializationResolver,
	secrets BackupSecretSlotResolver,
	checkpoints BackupCheckpointer,
	managed ManagedConfigResolver,
	scripts ScriptArtifactResolver,
	scriptCheckpoints ScriptCheckpointer,
) *Server {
	server := NewWithManagedRuntimeServices(
		auth, sessions, tasks, plans, materials, secrets, checkpoints, managed,
	)
	server.scriptArtifacts = scripts
	server.scriptCheckpoints = scriptCheckpoints
	return server
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
	authorization.Config = proto.Clone(authorization.Config).(*agentpb.AgentConfig)

	session, err := s.sessions.Open(
		stream.Context(),
		authenticate.AgentId,
		authorization.Generation,
	)
	if err != nil {
		return status.Error(codes.FailedPrecondition, "agent session is not current")
	}
	defer session.Close()
	delivered := make(map[string]string, authorization.Config.MaxConcurrentTasks)
	quarantined := make(map[string]string, authorization.Config.MaxConcurrentTasks)

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
		case abort := <-session.taskAborts():
			sendErr := stream.Send(&agentpb.ControllerMessage{
				Payload: &agentpb.ControllerMessage_TaskAbort{TaskAbort: &agentpb.TaskAbort{
					TaskId: abort.taskID, AssignmentId: abort.assignmentID,
					Reason: abort.reason,
				}},
			})
			if sendErr != nil {
				abort.result <- errs.New(errs.KindStorageUnavailable, "Agent Task abort delivery failed")
				return sendErr
			}
			abort.result <- nil
		case command := <-session.logMessages():
			if err := stream.Send(command.message); err != nil {
				command.result <- errs.New(errs.KindStorageUnavailable, "Agent log command delivery failed")
				return err
			}
			command.result <- nil
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
			if ready := result.message.GetLogReady(); ready != nil {
				if err := session.RecordLogReady(ready); err != nil {
					return status.Error(codes.InvalidArgument, "agent LogReady is invalid")
				}
				continue
			}
			if event := result.message.GetLogEvent(); event != nil {
				overflow, err := session.RecordLogEvent(event)
				if err != nil {
					return status.Error(codes.InvalidArgument, "agent LogEvent is invalid")
				}
				if overflow {
					if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_LogCancel{
						LogCancel: &agentpb.LogCancel{RequestId: event.GetRequestId()},
					}}); err != nil {
						return err
					}
					if err := session.RecordLogCancelDelivered(event.GetRequestId()); err != nil {
						return status.Error(codes.Internal, "agent LogCancel delivery state is invalid")
					}
				}
				continue
			}
			if end := result.message.GetLogEnd(); end != nil {
				if err := session.RecordLogEnd(end); err != nil {
					return status.Error(codes.InvalidArgument, "agent LogEnd is invalid")
				}
				continue
			}

			if ready := result.message.GetReady(); ready != nil {
				if err := validateReady(ready.Capacity, ready.Version); err != nil {
					return status.Error(codes.InvalidArgument, "agent Ready is invalid")
				}
				if ready.Capacity > authorization.Config.MaxConcurrentTasks {
					return status.Error(
						codes.InvalidArgument,
						"agent Ready capacity exceeds configuration",
					)
				}
				if err := session.RecordReady(s.now(), ready.Capacity, ready.Version); err != nil {
					return status.Error(codes.FailedPrecondition, "agent session is not current")
				}
				currentConfig, err := s.auth.Configuration(
					stream.Context(),
					authenticate.AgentId,
					authorization.Generation,
				)
				if err != nil {
					return taskStoreStatus(err)
				}
				if currentConfig == nil || currentConfig.PullIntervalSeconds <= 0 ||
					currentConfig.MaxConcurrentTasks <= 0 {
					return status.Error(codes.Internal, "agent configuration is invalid")
				}
				if !proto.Equal(currentConfig, authorization.Config) {
					// A config replacement drains under the old worker limit. The
					// Agent applies it only when every reservation has completed,
					// then advertises fresh capacity before dispatch resumes.
					if ready.Capacity != authorization.Config.MaxConcurrentTasks {
						continue
					}
					nextConfig := proto.Clone(currentConfig).(*agentpb.AgentConfig)
					if err := stream.Send(&agentpb.ControllerMessage{
						Payload: &agentpb.ControllerMessage_ConfigUpdate{
							ConfigUpdate: &agentpb.ConfigUpdate{
								AgentConfig: proto.Clone(nextConfig).(*agentpb.AgentConfig),
							},
						},
					}); err != nil {
						return err
					}
					authorization.Config = nextConfig
					continue
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
					quarantined,
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
					slog.Error(
						"controller: acknowledge Agent Task",
						slog.String("task_id", acknowledgement.TaskId),
						slog.String("assignment_id", acknowledgement.AssignmentId),
						slog.Any("error", err),
					)
					return taskStoreStatus(err)
				}
				if err := session.RecordTaskTerminal(
					acknowledgement.TaskId, acknowledgement.AssignmentId,
				); err != nil {
					return status.Error(codes.FailedPrecondition, "agent session is not current")
				}
				delete(delivered, acknowledgement.TaskId)
				delete(quarantined, acknowledgement.TaskId)
				continue
			}
			if event := result.message.GetTaskEvent(); event != nil {
				if s.tasks == nil {
					return status.Error(codes.Internal, "agent task store is not configured")
				}
				if err := s.recordTaskEvent(
					stream.Context(), authenticate.AgentId, authorization.Generation, event,
				); err != nil {
					slog.Error(
						"controller: record Agent Task event",
						slog.String("task_id", event.TaskId),
						slog.String("assignment_id", event.AssignmentId),
						slog.String("step_id", event.StepId),
						slog.Any("error", err),
					)
					return taskStoreStatus(err)
				}
				continue
			}
			if request := result.message.GetBackupCheckpointRequest(); request != nil {
				if s.checkpoints == nil {
					return status.Error(codes.Internal, "Backup checkpoint service is not configured")
				}
				acknowledgement, err := s.checkpoints.CheckpointBackup(
					stream.Context(), authenticate.AgentId, authorization.Generation, request,
				)
				if err != nil {
					return taskStoreStatus(err)
				}
				if err := stream.Send(&agentpb.ControllerMessage{
					Payload: &agentpb.ControllerMessage_BackupCheckpointAck{
						BackupCheckpointAck: acknowledgement,
					},
				}); err != nil {
					return err
				}
				continue
			}
			if request := result.message.GetScriptCheckpointRequest(); request != nil {
				if s.scriptCheckpoints == nil {
					return status.Error(codes.Internal, "Script checkpoint service is not configured")
				}
				acknowledgement, err := s.scriptCheckpoints.CheckpointScript(
					stream.Context(), authenticate.AgentId, authorization.Generation, request,
				)
				if err != nil {
					return taskStoreStatus(err)
				}
				if err := stream.Send(&agentpb.ControllerMessage{
					Payload: &agentpb.ControllerMessage_ScriptCheckpointAck{ScriptCheckpointAck: acknowledgement},
				}); err != nil {
					return err
				}
				continue
			}
			if result.message.GetObservedState() != nil {
				return status.Error(
					codes.Unimplemented,
					"authenticated agent message is not implemented",
				)
			}
			return status.Error(codes.InvalidArgument, "authenticated agent message is empty")
		}
	}
}

func (s *Server) recordTaskEvent(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	event *agentpb.TaskEvent,
) error {
	if event == nil || ids.Validate(ids.KindAssignment, event.AssignmentId) != nil ||
		len(event.PlanHash) != 32 || event.Attempt == 0 || event.Ordinal == 0 {
		return errs.New(errs.KindValidationFailed, "Agent Task event is invalid")
	}
	if len(event.Chunk) != 0 {
		return status.Error(
			codes.Unimplemented,
			"ephemeral Task output streaming is not implemented",
		)
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
		return errs.New(
			errs.KindStateConflict,
			"Agent Task event does not belong to a running Task",
		)
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
			AssignmentID: event.AssignmentId, AgentID: agentID, AgentGeneration: agentGeneration,
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
	delivered map[string]string,
	quarantined map[string]string,
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
	if !session.AssignmentsAllowed() {
		return nil
	}
	remaining := capacity
	for _, assignment := range recovered {
		if !session.AssignmentsAllowed() {
			return nil
		}
		if err := validateAgentDispatchClaim(
			assignment,
			agentID,
			authorization.Generation,
		); err != nil {
			return err
		}
		if delivered[assignment.Task.Record.ID] == assignment.Assignment.Record.AssignmentID {
			continue
		}
		if remaining == 0 {
			return nil
		}
		if quarantined[assignment.Task.Record.ID] == assignment.Assignment.Record.AssignmentID {
			remaining--
			continue
		}
		sent, err := s.dispatchTaskAssignment(stream, assignment)
		if err != nil {
			return err
		}
		if sent {
			delivered[assignment.Task.Record.ID] = assignment.Assignment.Record.AssignmentID
		} else {
			quarantined[assignment.Task.Record.ID] = assignment.Assignment.Record.AssignmentID
		}
		remaining--
	}
	for remaining > 0 {
		if !session.AssignmentsAllowed() {
			return nil
		}
		assignment, found, err := s.tasks.ClaimNextTask(
			stream.Context(),
			agentID,
			authorization.Generation,
			s.now().UTC(),
		)
		if err != nil {
			return err
		}
		if !found || !session.AssignmentsAllowed() {
			return nil
		}
		if err := validateAgentDispatchClaim(
			assignment,
			agentID,
			authorization.Generation,
		); err != nil {
			return err
		}
		if !session.AssignmentsAllowed() {
			return nil
		}
		sent, err := s.dispatchTaskAssignment(stream, assignment)
		if err != nil {
			return err
		}
		if sent {
			delivered[assignment.Task.Record.ID] = assignment.Assignment.Record.AssignmentID
		} else {
			quarantined[assignment.Task.Record.ID] = assignment.Assignment.Record.AssignmentID
		}
		remaining--
	}
	return nil
}

func validateAgentDispatchClaim(
	assignment etcd.TaskAssignment,
	agentID string,
	generation uint64,
) error {
	record := assignment.Assignment.Record
	if record.Executor != etcd.TaskExecutorAgent || record.AgentID != agentID ||
		record.AgentGeneration != generation || record.TaskID != assignment.Task.Record.ID ||
		ids.Validate(ids.KindAssignment, record.AssignmentID) != nil {
		return errs.New(
			errs.KindInternal,
			"durable Agent Task claim does not match its dispatch session",
		)
	}
	return nil
}

func (s *Server) sendTaskAssignment(
	stream agentpb.AgentChannel_ConnectServer,
	claim etcd.TaskAssignment,
) error {
	assignment, err := s.taskAssignmentMessage(stream.Context(), claim)
	if err != nil {
		return err
	}
	return s.sendResolvedTaskAssignment(stream, claim, assignment)
}

func (s *Server) dispatchTaskAssignment(
	stream agentpb.AgentChannel_ConnectServer,
	claim etcd.TaskAssignment,
) (bool, error) {
	assignment, err := s.taskAssignmentMessage(stream.Context(), claim)
	if err != nil {
		slog.Error(
			"controller: quarantine Agent Task assignment",
			slog.String("task_id", claim.Task.Record.ID),
			slog.String("assignment_id", claim.Assignment.Record.AssignmentID),
			slog.Any("error", err),
		)
		return false, nil
	}
	if err := s.sendResolvedTaskAssignment(stream, claim, assignment); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Server) sendResolvedTaskAssignment(
	stream agentpb.AgentChannel_ConnectServer,
	claim etcd.TaskAssignment,
	assignment *agentpb.TaskAssignment,
) error {
	defer clearScriptAssignmentArtifacts(assignment.GetScriptArtifacts())
	if err := stream.Send(&agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_TaskAssignment{TaskAssignment: assignment},
	}); err != nil {
		return err
	}
	for _, step := range assignment.GetPlan().GetSteps() {
		if step.GetBackupSourceCapture() != nil || step.GetBackupArtifactPrune() != nil {
			if err := s.sendBackupSecretSlots(
				stream.Context(),
				stream,
				backupsecret.Request{
					TaskID:          claim.Task.Record.ID,
					AssignmentID:    claim.Assignment.Record.AssignmentID,
					AgentID:         claim.Assignment.Record.AgentID,
					AgentGeneration: claim.Assignment.Record.AgentGeneration,
					Deadline:        claim.Assignment.Record.Deadline,
					StepID:          step.GetStepId(),
					Plan:            assignment.GetPlan(),
					Step:            step,
				},
			); err != nil {
				return err
			}
		}
		if action := step.GetComponentApply(); action != nil && action.GetManagedConfigContent() &&
			assignment.GetPlan().GetOperation() == agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY {
			if err := s.sendManagedConfig(
				stream, claim.Task.Record, assignment.GetAssignmentId(), assignment.GetPlan(), step,
			); err != nil {
				return err
			}
		}
		if step.GetMaterializeFile() == nil {
			continue
		}
		if err := s.sendMaterialization(
			stream, claim.Task.Record, assignment.GetAssignmentId(), assignment.GetPlan(), step,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) sendManagedConfig(
	stream agentpb.AgentChannel_ConnectServer,
	task etcd.TaskRecord,
	assignmentID string,
	plan *agentpb.ExecutionPlan,
	step *agentpb.ExecutionStep,
) (resultErr error) {
	if s.managed == nil {
		return errs.New(errs.KindInternal, "managed-config resolver is not configured")
	}
	source, err := s.managed.ResolveManagedConfig(stream.Context(), task, plan, step)
	if err != nil {
		return err
	}
	if source.Content == nil || !managedconfig.ValidMediaType(source.MediaType) || source.Length == 0 ||
		source.Length > managedconfig.MaximumArtifactBytes {
		if source.Content != nil {
			_ = source.Content.Close()
		}
		return errs.New(errs.KindInternal, "managed-config resolver returned an invalid source")
	}
	defer func() {
		if closeErr := source.Content.Close(); closeErr != nil {
			resultErr = errs.Wrap(errs.KindInternal, errors.Join(resultErr, closeErr))
		}
	}()
	action := step.GetComponentApply()
	headerMessage := managedConfigControllerMessage(task.ID, assignmentID, plan.GetPlanHash(), step.GetStepId())
	headerMessage.GetManagedConfigTransfer().Record = &agentpb.ManagedConfigTransfer_Header{
		Header: &agentpb.ManagedConfigTransferHeader{
			ArtifactId: action.GetArtifactId(), MediaType: source.MediaType, Length: source.Length,
			Sha256: append([]byte(nil), action.GetArtifactDigest()...),
		},
	}
	if err := stream.Send(headerMessage); err != nil {
		return err
	}
	buffer := make([]byte, managedconfig.MaximumChunkBytes)
	defer clear(buffer)
	hasher := sha256.New()
	remaining := source.Length
	var sequence uint32
	for remaining > 0 {
		readSize := min(uint64(len(buffer)), remaining)
		read, readErr := source.Content.Read(buffer[:int(readSize)])
		if read < 0 || read > int(readSize) || read == 0 && readErr == nil {
			return errs.New(errs.KindInternal, "managed-config source returned an invalid read")
		}
		if read > 0 {
			if _, err := hasher.Write(buffer[:read]); err != nil {
				return errs.New(errs.KindInternal, "managed-config source digest failed")
			}
			remaining -= uint64(read)
			sequence++
			content := append([]byte(nil), buffer[:read]...)
			message := managedConfigControllerMessage(task.ID, assignmentID, plan.GetPlanHash(), step.GetStepId())
			message.GetManagedConfigTransfer().Record = &agentpb.ManagedConfigTransfer_Chunk{
				Chunk: &agentpb.ManagedConfigTransferChunk{Sequence: sequence, Content: content},
			}
			sendErr := stream.Send(message)
			clear(content)
			message.GetManagedConfigTransfer().GetChunk().Content = nil
			if sendErr != nil {
				return sendErr
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) && remaining == 0 {
				break
			}
			return errs.New(errs.KindInternal, "managed-config source ended before its declared length")
		}
	}
	var extra [1]byte
	read, readErr := source.Content.Read(extra[:])
	clear(extra[:])
	if read != 0 || !errors.Is(readErr, io.EOF) {
		return errs.New(errs.KindInternal, "managed-config source exceeds its declared length")
	}
	digest := hasher.Sum(nil)
	defer clear(digest)
	if subtle.ConstantTimeCompare(digest, action.GetArtifactDigest()) != 1 {
		return errs.New(errs.KindInternal, "managed-config source digest does not match its plan")
	}
	endMessage := managedConfigControllerMessage(task.ID, assignmentID, plan.GetPlanHash(), step.GetStepId())
	endMessage.GetManagedConfigTransfer().Record = &agentpb.ManagedConfigTransfer_End{
		End: &agentpb.ManagedConfigTransferEnd{ChunkCount: sequence},
	}
	return stream.Send(endMessage)
}

func managedConfigControllerMessage(
	taskID string,
	assignmentID string,
	planHash []byte,
	stepID string,
) *agentpb.ControllerMessage {
	return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ManagedConfigTransfer{
		ManagedConfigTransfer: &agentpb.ManagedConfigTransfer{
			TaskId: taskID, AssignmentId: assignmentID,
			PlanHash: append([]byte(nil), planHash...), StepId: stepID,
		},
	}}
}

func (s *Server) sendMaterialization(
	stream agentpb.AgentChannel_ConnectServer,
	task etcd.TaskRecord,
	assignmentID string,
	plan *agentpb.ExecutionPlan,
	step *agentpb.ExecutionStep,
) (resultErr error) {
	if s.materials == nil {
		return errs.New(errs.KindInternal, "materialization resolver is not configured")
	}
	source, err := s.materials.ResolveMaterialization(stream.Context(), task, plan, step)
	if err != nil {
		return err
	}
	if source == nil {
		return errs.New(errs.KindInternal, "materialization resolver returned an empty source")
	}
	defer func() {
		if closeErr := source.Close(); closeErr != nil {
			resultErr = errs.Wrap(errs.KindInternal, errors.Join(resultErr, closeErr))
		}
	}()
	materialization := step.GetMaterializeFile()
	header := &agentpb.MaterializationTransferHeader{
		ArtifactId:        materialization.GetArtifactId(),
		MaterializationId: materialization.GetMaterializationId(),
		EnvironmentId:     materialization.GetEnvironmentId(),
		RenderGeneration:  plan.GetRenderGeneration(),
		Destination:       materialization.GetDestination(),
		ServiceId:         materialization.GetServiceId(),
		ServiceName:       materialization.GetServiceName(),
		OutputKind:        materialization.GetOutputKind(),
		Uid:               materialization.GetUid(),
		Gid:               materialization.GetGid(),
		Mode:              materialization.GetMode(),
		Length:            materialization.GetLength(),
		Sha256:            append([]byte(nil), materialization.GetSha256()...),
	}
	headerMessage := materializationControllerMessage(
		task.ID,
		assignmentID,
		plan.GetPlanHash(),
		step.GetStepId(),
	)
	headerMessage.GetMaterializationTransfer().Record = &agentpb.MaterializationTransfer_Header{
		Header: header,
	}
	if err := stream.Send(headerMessage); err != nil {
		return err
	}
	buffer := make([]byte, entrymaterialization.MaximumChunkBytes)
	defer clear(buffer)
	hasher := sha256.New()
	remaining := materialization.GetLength()
	var sequence uint32
	for remaining > 0 {
		readSize := min(uint64(len(buffer)), remaining)
		read, readErr := source.Read(buffer[:int(readSize)])
		if read < 0 || read > int(readSize) || (read == 0 && readErr == nil) {
			return errs.New(errs.KindInternal, "materialization source returned an invalid read")
		}
		if read > 0 {
			if _, err := hasher.Write(buffer[:read]); err != nil {
				return errs.New(errs.KindInternal, "materialization source digest failed")
			}
			remaining -= uint64(read)
			sequence++
			content := append([]byte(nil), buffer[:read]...)
			chunk := &agentpb.MaterializationTransferChunk{Sequence: sequence, Content: content}
			chunkMessage := materializationControllerMessage(
				task.ID, assignmentID, plan.GetPlanHash(), step.GetStepId(),
			)
			chunkMessage.GetMaterializationTransfer().Record = &agentpb.MaterializationTransfer_Chunk{
				Chunk: chunk,
			}
			sendErr := stream.Send(chunkMessage)
			clear(content)
			chunk.Content = nil
			if sendErr != nil {
				return sendErr
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) && remaining == 0 {
				break
			}
			return errs.New(
				errs.KindInternal,
				"materialization source ended before its declared length",
			)
		}
	}
	var extra [1]byte
	read, readErr := source.Read(extra[:])
	clear(extra[:])
	if read != 0 || !errors.Is(readErr, io.EOF) {
		return errs.New(errs.KindInternal, "materialization source exceeds its declared length")
	}
	digest := hasher.Sum(nil)
	defer clear(digest)
	if subtle.ConstantTimeCompare(digest, materialization.GetSha256()) != 1 {
		return errs.New(errs.KindInternal, "materialization source digest does not match its plan")
	}
	endMessage := materializationControllerMessage(
		task.ID,
		assignmentID,
		plan.GetPlanHash(),
		step.GetStepId(),
	)
	endMessage.GetMaterializationTransfer().Record = &agentpb.MaterializationTransfer_End{
		End: &agentpb.MaterializationTransferEnd{ChunkCount: sequence},
	}
	return stream.Send(endMessage)
}

func materializationControllerMessage(
	taskID string,
	assignmentID string,
	planHash []byte,
	stepID string,
) *agentpb.ControllerMessage {
	return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_MaterializationTransfer{
		MaterializationTransfer: &agentpb.MaterializationTransfer{
			TaskId: taskID, AssignmentId: assignmentID,
			PlanHash: append([]byte(nil), planHash...), StepId: stepID,
		},
	}}
}

func (s *Server) taskAssignmentMessage(
	ctx context.Context,
	claim etcd.TaskAssignment,
) (*agentpb.TaskAssignment, error) {
	task := claim.Task.Record
	record := claim.Assignment.Record
	if ids.Validate(ids.KindAssignment, record.AssignmentID) != nil || record.TaskID != task.ID ||
		record.Executor != etcd.TaskExecutorAgent {
		return nil, errs.New(errs.KindInternal, "durable Agent Task assignment is invalid")
	}
	if s.plans == nil {
		return nil, errs.New(errs.KindInternal, "execution plan resolver is not configured")
	}
	planHash, err := hex.DecodeString(task.PlanHash)
	if err != nil || len(planHash) != 32 {
		return nil, errs.New(errs.KindInternal, "durable Task has an invalid plan hash")
	}
	if task.TimeoutSeconds <= 0 {
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
	planIDMatches := plan.PlanId == task.PlanID
	planHashMatches := bytes.Equal(plan.PlanHash, planHash)
	renderGenerationMatches := plan.RenderGeneration == uint64(task.RenderGeneration)
	targetMatches := plan.TargetId == task.Target
	operationMatches := operationMatchesTask(plan.Operation, task)
	stepsMatch := stepSummariesMatch(plan.Steps, task.Steps)
	if !planIDMatches || !planHashMatches || !renderGenerationMatches || !targetMatches || !operationMatches ||
		!stepsMatch {
		return nil, errs.Newf(
			errs.KindInternal,
			"resolved execution plan does not match its durable Task: plan_id=%t plan_hash=%t render_generation=%t target=%t operation=%t steps=%t",
			planIDMatches,
			planHashMatches,
			renderGenerationMatches,
			targetMatches,
			operationMatches,
			stepsMatch,
		)
	}
	var scriptArtifacts *agentpb.ScriptAssignmentArtifacts
	var scriptCheckpoints []*agentpb.ScriptExecutionCheckpoint
	if len(plan.ScriptBodyArtifacts) != 0 {
		if s.scriptArtifacts == nil {
			return nil, errs.New(errs.KindInternal, "Script artifact resolver is not configured")
		}
		scriptArtifacts, err = s.scriptArtifacts.ResolveScriptAssignmentArtifacts(ctx, task, plan)
		if err != nil {
			return nil, err
		}
		scriptCheckpoints, err = s.scriptArtifacts.ResolveScriptExecutionCheckpoints(ctx, task, plan)
		if err != nil {
			clearScriptAssignmentArtifacts(scriptArtifacts)
			return nil, err
		}
	}
	return &agentpb.TaskAssignment{
		TaskId: task.ID, AssignmentId: record.AssignmentID,
		OperationId: task.OperationID, RetryOf: task.RetryOf,
		Plan: plan, ScriptArtifacts: scriptArtifacts, ScriptCheckpoints: scriptCheckpoints,
		AutomaticReconcile: etcd.IsAutomaticReconcileTask(task),
		Deadline:           timestamppb.New(record.Deadline.UTC()),
	}, nil
}

func clearScriptAssignmentArtifacts(artifacts *agentpb.ScriptAssignmentArtifacts) {
	if artifacts == nil {
		return
	}
	for _, body := range artifacts.Bodies {
		if body != nil {
			clear(body.Body)
			body.Body = nil
		}
	}
	for _, secret := range artifacts.Secrets {
		if secret != nil {
			clear(secret.Value)
			secret.Value = nil
		}
	}
	for _, entry := range artifacts.Entries {
		if entry != nil {
			clear(entry.Value)
			entry.Value = nil
		}
	}
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

func operationMatchesTask(operation agentpb.PlanOperation, task etcd.TaskRecord) bool {
	switch task.Type {
	case etcd.TaskScript:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_SCRIPT
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
	case etcd.TaskCreate:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE ||
			task.Params[etcd.TaskResourceKindParam] == etcd.TaskResourceVolume &&
				operation == agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	case etcd.TaskAttach:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_ATTACH
	case etcd.TaskDetach:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_DETACH
	case etcd.TaskUpdate:
		if operation == agentpb.PlanOperation_PLAN_OPERATION_RECONCILE {
			return true
		}
		if task.Params[etcd.TaskResourceKindParam] == etcd.TaskResourceComponent {
			return operation == agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY
		}
		_, backingCreation := task.Params[etcd.TaskBackingServiceHealthParam]
		return backingCreation && operation == agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE
	case etcd.TaskBackup:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_BACKUP
	case etcd.TaskBackupPrune:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE
	default:
		return false
	}
}

func durableEnvironmentDirectoryTaskResult(acknowledgement *agentpb.TaskAck) etcd.TaskResultRecord {
	return etcd.TaskResultRecord{
		Kind: etcd.TaskResultEnvironmentDirectory, ExitCode: acknowledgement.GetExitCode(),
		FailedStepID: acknowledgement.GetEnvironmentDirectoryResult().GetFailedStepId(),
		Diagnostic:   etcd.TaskResultDiagnosticNone,
	}
}

func validateEnvironmentDirectoryTaskResult(acknowledgement *agentpb.TaskAck) error {
	result := acknowledgement.GetEnvironmentDirectoryResult()
	if result == nil {
		return errs.New(
			errs.KindValidationFailed,
			"Agent Environment directory Task result is invalid",
		)
	}
	if acknowledgement.GetTerminal() == agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED &&
		(acknowledgement.GetExitCode() != 0 || result.GetFailedStepId() != "") {
		return errs.New(
			errs.KindValidationFailed,
			"completed Agent Environment directory Task result is inconsistent",
		)
	}
	return nil
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
		ProxyEvidence:          make([]etcd.TaskProxyEvidence, len(result.GetProxyEvidence())),
		RecreateEvidence:       make([]etcd.TaskRecreateEvidence, len(result.GetRecreateEvidence())),
	}
	for index, project := range result.GetProjects() {
		durable.Projects[index] = etcd.TaskObservedProjectSummary{
			ProjectName: project.GetProjectName(),
			ObservedAt:  project.GetObservedAt().AsTime().UTC(),
			ContainerCount: uint32(
				len(project.GetContainers()),
			),
			NetworkCount: uint32(len(project.GetNetworks())),
			VolumeCount: uint32(
				len(project.GetVolumes()),
			),
			CollisionCount: uint32(len(project.GetCollisions())),
		}
	}
	for index, evidence := range result.GetProxyEvidence() {
		durable.ProxyEvidence[index] = etcd.TaskProxyEvidence{
			ServiceID:       evidence.GetServiceId(),
			Target:          evidence.GetTarget(),
			ProxyGeneration: evidence.GetProxyGeneration(),
			ConfigSHA256:    hex.EncodeToString(evidence.GetConfigSha256()),
			ReleaseID:       evidence.GetReleaseId(),
			Compensated:     evidence.GetCompensated(),
		}
	}
	for index, evidence := range result.GetRecreateEvidence() {
		durable.RecreateEvidence[index] = etcd.TaskRecreateEvidence{
			ServiceID:   evidence.GetServiceId(),
			ReleaseID:   evidence.GetReleaseId(),
			ArtifactID:  evidence.GetArtifactId(),
			Compensated: evidence.GetCompensated(),
			Target:      evidence.GetTarget(),
		}
	}
	if evidence := result.GetDnsResolverCandidateObservation(); evidence != nil {
		durable.DNSResolverCandidateObservation = durableDNSResolverObservation(evidence)
	}
	if evidence := result.GetDnsResolverRollbackObservation(); evidence != nil {
		durable.DNSResolverRollbackObservation = durableDNSResolverObservation(evidence)
	}
	return durable
}

func durableDNSResolverObservation(
	evidence *agentpb.DNSResolverObservationEvidence,
) *etcd.TaskDNSResolverObservationEvidence {
	canonicalEvidence, _ := dnsproof.Marshal(evidence)
	static := evidence.GetStaticQuery()
	staticIPv4 := ""
	for _, answer := range static.GetAnswers() {
		if len(answer.GetIpv4()) == 4 {
			staticIPv4 = netip.AddrFrom4([4]byte(answer.GetIpv4())).String()
			break
		}
	}
	return &etcd.TaskDNSResolverObservationEvidence{
		ComponentID: evidence.GetComponentId(), ServiceID: evidence.GetServiceId(),
		ArtifactID: evidence.GetArtifactId(), ArtifactSHA256: hex.EncodeToString(evidence.GetArtifactSha256()),
		RenderGeneration: evidence.GetRenderGeneration(), ImageReference: evidence.GetImageReference(),
		VerifiedImageDigest: hex.EncodeToString(evidence.GetVerifiedImageDigest()),
		ListenEndpoint:      evidence.GetListenEndpoint(), ReloadSHA512: hex.EncodeToString(evidence.GetReloadSha512()),
		ObservedAt: evidence.GetObservedAt().AsTime().UTC(), StaticQueryPresent: static != nil,
		StaticQueryName: static.GetName(), StaticQueryIPv4: staticIPv4, StaticQuerySucceeded: static != nil,
		RecursiveQuerySucceeded: evidence.GetCatchAllQuery() != nil,
		ForwarderQueryCount:     uint32(len(evidence.GetForwarderQueries())),
		ForwarderSuccessCount:   uint32(len(evidence.GetForwarderQueries())),
		ProofSHA256:             hex.EncodeToString(evidence.GetProofSha256()), CanonicalEvidence: canonicalEvidence,
	}
}

func validateComposeTaskResult(acknowledgement *agentpb.TaskAck) error {
	result := acknowledgement.GetComposeResult()
	if result == nil || len(result.GetProjects()) > 64 || len(result.GetProxyEvidence()) > 32 ||
		len(result.GetRecreateEvidence()) > 32 {
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
		return errs.New(
			errs.KindValidationFailed,
			"completed Agent Compose Task result is inconsistent",
		)
	}
	for _, project := range result.GetProjects() {
		if project == nil || project.GetProjectName() == "" || project.GetObservedAt() == nil ||
			project.GetObservedAt().CheckValid() != nil || len(project.GetContainers()) > 4096 ||
			len(project.GetNetworks()) > 4096 || len(project.GetVolumes()) > 4096 ||
			len(project.GetCollisions()) > 4096 {
			return errs.New(errs.KindValidationFailed, "Agent Compose Task observation is invalid")
		}
		for _, collision := range project.GetCollisions() {
			if collision == nil ||
				collision.GetKind() == agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_UNSPECIFIED ||
				collision.GetName() == "" {
				return errs.New(
					errs.KindValidationFailed,
					"Agent Compose Task collision is invalid",
				)
			}
		}
	}
	previousServiceID := ""
	for _, evidence := range result.GetProxyEvidence() {
		if evidence == nil || ids.Validate(ids.KindService, evidence.GetServiceId()) != nil ||
			!validAgentReleaseTarget(evidence.GetTarget()) || evidence.GetProxyGeneration() == 0 ||
			len(evidence.GetConfigSha256()) != sha256.Size || evidence.GetReleaseId() == "" ||
			evidence.GetServiceId() <= previousServiceID {
			return errs.New(errs.KindValidationFailed, "Agent Compose Task proxy evidence is invalid or unsorted")
		}
		previousServiceID = evidence.GetServiceId()
	}
	previousServiceID = ""
	for _, evidence := range result.GetRecreateEvidence() {
		if evidence == nil || ids.Validate(ids.KindService, evidence.GetServiceId()) != nil ||
			(evidence.GetReleaseId() != "baseline" && ids.Validate(ids.KindDeployment, evidence.GetReleaseId()) != nil) ||
			ids.Validate(
				ids.KindConfig,
				evidence.GetArtifactId(),
			) != nil || !validAgentReleaseTarget(evidence.GetTarget()) || evidence.GetServiceId() <= previousServiceID {
			return errs.New(errs.KindValidationFailed, "Agent Compose Task recreate evidence is invalid or unsorted")
		}
		previousServiceID = evidence.GetServiceId()
	}
	for _, evidence := range []*agentpb.DNSResolverObservationEvidence{
		result.GetDnsResolverCandidateObservation(),
		result.GetDnsResolverRollbackObservation(),
	} {
		if evidence == nil {
			continue
		}
		staticValid := evidence.GetStaticQuery() == nil
		if static := evidence.GetStaticQuery(); static != nil {
			staticValid = static.GetName() != "" && static.GetType() == agentpb.DNSQueryType_DNS_QUERY_TYPE_A &&
				len(static.GetAnswers()) != 0
		}
		if dnsproof.Verify(evidence) != nil || ids.Validate(ids.KindComponent, evidence.GetComponentId()) != nil ||
			ids.Validate(ids.KindService, evidence.GetServiceId()) != nil ||
			ids.Validate(ids.KindConfig, evidence.GetArtifactId()) != nil ||
			len(evidence.GetArtifactSha256()) != sha256.Size || evidence.GetRenderGeneration() == 0 ||
			!imageref.IsDigestPinned(
				evidence.GetImageReference(),
			) || len(evidence.GetVerifiedImageDigest()) != sha256.Size ||
			evidence.GetListenEndpoint() != "127.0.0.1:53" || len(evidence.GetReloadSha512()) != sha512.Size ||
			evidence.GetObservedAt() == nil || evidence.GetObservedAt().CheckValid() != nil || !staticValid ||
			evidence.GetCatchAllQuery() == nil || len(evidence.GetForwarderQueries()) > 8 ||
			len(evidence.GetProofSha256()) != sha256.Size {
			return errs.New(errs.KindValidationFailed, "Agent DNS resolver observation evidence is invalid")
		}
	}
	return nil
}

func validAgentReleaseTarget(value string) bool {
	return value == "singleton" || value == "blue" || value == "green"
}

func taskStoreStatus(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.Unknown {
		return err
	}
	slog.Error("controller: Agent task operation failed", slog.Any("error", err))
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
		return nil, Token{}, errs.New(
			errs.KindValidationFailed,
			"Authenticate must be the first Agent message",
		)
	}
	authenticate := message.GetAuthenticate()
	if err := ids.Validate(ids.KindAgent, authenticate.AgentId); err != nil {
		return nil, Token{}, errs.New(errs.KindValidationFailed, "agent id is invalid")
	}
	if len(authenticate.Token) != tokenSize {
		return nil, Token{}, errs.New(
			errs.KindValidationFailed,
			"agent token has an invalid length",
		)
	}

	var token Token
	copy(token[:], authenticate.Token)
	return authenticate, token, nil
}

func unauthenticated() error {
	return status.Error(codes.Unauthenticated, "agent authentication failed")
}
