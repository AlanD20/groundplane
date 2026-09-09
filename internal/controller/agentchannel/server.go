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
	ReconnectAgentAssignment(context.Context, etcd.TaskAssignment) (etcd.TaskAssignment, error)
	ClaimNextTask(context.Context, string, uint64, time.Time) (etcd.TaskAssignment, bool, error)
	GetTask(context.Context, string) (etcd.Versioned[etcd.TaskRecord], error)
	ListTaskEvents(context.Context, string, int64) (etcd.TaskEventSnapshot, error)
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
	volumeCheckpoints VolumeRemovalCheckpointer
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
	volumeCheckpoints VolumeRemovalCheckpointer,
) *Server {
	server := NewWithManagedRuntimeServices(
		auth, sessions, tasks, plans, materials, secrets, checkpoints, managed,
	)
	server.scriptArtifacts = scripts
	server.scriptCheckpoints = scriptCheckpoints
	server.volumeCheckpoints = volumeCheckpoints
	return server
}

// Connect authenticates the first message, publishes the authorized config,
// and then owns the live session until disconnect or fencing.

func (s *Server) recordTaskEvent(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	event *agentpb.TaskEvent,
) error {
	if event == nil || ids.Validate(ids.KindAssignment, event.AssignmentId) != nil ||
		len(event.PlanHash) != 32 || event.ExecutionEpoch == 0 || event.Ordinal == 0 {
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
	if assignments, ok := s.tasks.(interface {
		GetTaskAssignment(context.Context, string) (etcd.TaskAssignment, error)
	}); ok {
		assignment, assignmentErr := assignments.GetTaskAssignment(ctx, event.GetTaskId())
		if assignmentErr != nil || assignment.Assignment.Record.AssignmentID != event.GetAssignmentId() ||
			assignment.Assignment.Record.ExecutionEpoch != event.GetExecutionEpoch() {
			return errs.New(errs.KindStateConflict, "Agent Task event execution epoch does not match")
		}
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
			Attempt: event.ExecutionEpoch, Ordinal: event.Ordinal,
		},
		State:   state,
		Payload: json.RawMessage(`{}`),
	}, s.now().UTC())
	return err
}

func (s *Server) sendTaskAssignment(
	stream agentpb.AgentChannel_ConnectServer,
	claim etcd.TaskAssignment,
) error {
	assignment, err := s.taskAssignmentMessage(stream.Context(), claim, false)
	if err != nil {
		return err
	}
	defer clearScriptAssignmentArtifacts(assignment.GetScriptArtifacts())
	return s.sendResolvedTaskAssignment(stream, claim, assignment)
}

func (s *Server) dispatchResolvedTaskAssignment(
	session *Session,
	stream agentpb.AgentChannel_ConnectServer,
	claim etcd.TaskAssignment,
	assignment *agentpb.TaskAssignment,
	recovered bool,
) (bool, error) {
	defer clearScriptAssignmentArtifacts(assignment.GetScriptArtifacts())
	expired := false
	sent, err := session.sendAssignment(func() error {
		if recovered && claim.Assignment.Record.ExecutionMode == etcd.TaskExecutionModeForward &&
			!s.now().UTC().Before(claim.Assignment.Record.Deadline.UTC()) {
			expired = true
			return nil
		}
		return s.sendResolvedTaskAssignment(stream, claim, assignment)
	})
	if expired {
		return false, nil
	}
	return sent, err
}

func (s *Server) sendResolvedTaskAssignment(
	stream agentpb.AgentChannel_ConnectServer,
	claim etcd.TaskAssignment,
	assignment *agentpb.TaskAssignment,
) error {
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
	recovered bool,
) (*agentpb.TaskAssignment, error) {
	_ = recovered
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
	if err := validateCandidateReleaseAssignment(ctx, s.tasks, claim, plan); err != nil {
		return nil, err
	}
	executionAuthority, err := candidateReleaseAssignmentAuthority(claim, plan)
	if err != nil {
		return nil, err
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
	executionDeadline := record.Deadline
	if record.ExecutionMode == etcd.TaskExecutionModeRecoveryOnly {
		executionDeadline = record.RecoveryDeadline
		if claim.RecoveryProofRequired {
			if record.RecoveryExecutionDeadline.IsZero() {
				return nil, errs.New(errs.KindInternal, "proof-required recovery has no execution budget")
			}
			executionDeadline = record.RecoveryExecutionDeadline
		} else if !record.RecoveryExecutionDeadline.IsZero() {
			return nil, errs.New(errs.KindInternal, "active recovery carries proof-required execution budget")
		}
	}
	return &agentpb.TaskAssignment{
		TaskId: task.ID, AssignmentId: record.AssignmentID,
		OperationId: task.OperationID, RetryOf: task.RetryOf,
		Plan: plan, ScriptArtifacts: scriptArtifacts, ScriptCheckpoints: scriptCheckpoints,
		AutomaticReconcile:          etcd.IsAutomaticReconcileTask(task),
		ForwardDeadline:             timestamppb.New(record.Deadline.UTC()),
		ExecutionEpoch:              record.ExecutionEpoch,
		ExecutionMode:               executionAuthority.mode,
		RecoveryDeadline:            timestamppb.New(record.RecoveryDeadline.UTC()),
		RestorationAuthority:        executionAuthority.restoration,
		ReleaseRecoveryRecordSha256: append([]byte(nil), executionAuthority.recoveryDigest...),
		ReleaseRecoveryDirective:    executionAuthority.recovery,
		ExecutionDeadline:           timestamppb.New(executionDeadline.UTC()),
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
	if evidence := result.GetCandidateAbsenceEvidence(); evidence != nil {
		durable.CandidateAbsenceEvidence = &etcd.TaskCandidateAbsenceEvidence{
			AssignmentID: evidence.GetAssignmentId(), PlanHash: hex.EncodeToString(evidence.GetPlanHash()),
			AuthoritySHA256:    hex.EncodeToString(evidence.GetAuthoritySha256()),
			ComposeProjectName: evidence.GetComposeProjectName(), CandidateArtifactID: evidence.GetCandidateArtifactId(),
			AbsenceProven: evidence.GetAbsenceProven(), Candidates: make([]etcd.TaskCandidateAbsenceCandidate, len(evidence.GetCandidates())),
		}
		for index, candidate := range evidence.GetCandidates() {
			durable.CandidateAbsenceEvidence.Candidates[index] = etcd.TaskCandidateAbsenceCandidate{
				ServiceID: candidate.GetServiceId(), ReleaseID: candidate.GetReleaseId(),
			}
		}
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

func validateComposeTaskResult(acknowledgement *agentpb.TaskAck) error {
	result := acknowledgement.GetComposeResult()
	if result == nil || len(result.GetProjects()) > 64 || len(result.GetProxyEvidence()) > 32 ||
		len(result.GetRecreateEvidence()) > 32 {
		return errs.New(errs.KindValidationFailed, "Agent Compose Task result is invalid")
	}
	if evidence := result.GetCandidateAbsenceEvidence(); evidence != nil {
		if evidence.GetAssignmentId() != acknowledgement.GetAssignmentId() ||
			!bytes.Equal(evidence.GetPlanHash(), acknowledgement.GetPlanHash()) ||
			len(evidence.GetAuthoritySha256()) != sha256.Size || evidence.GetComposeProjectName() == "" ||
			ids.Validate(ids.KindConfig, evidence.GetCandidateArtifactId()) != nil ||
			len(evidence.GetCandidates()) == 0 || len(evidence.GetCandidates()) > 32 {
			return errs.New(errs.KindValidationFailed, "Agent candidate absence evidence is invalid")
		}
		previous := ""
		for _, candidate := range evidence.GetCandidates() {
			identity := candidate.GetServiceId() + "\x00" + candidate.GetReleaseId()
			if ids.Validate(ids.KindService, candidate.GetServiceId()) != nil ||
				ids.Validate(ids.KindDeployment, candidate.GetReleaseId()) != nil || identity <= previous {
				return errs.New(errs.KindValidationFailed, "Agent candidate absence evidence is invalid or unsorted")
			}
			previous = identity
		}
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
			ids.Validate(ids.KindDeployment, evidence.GetReleaseId()) != nil ||
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
			len(evidence.GetImageConfigDigest()) != sha256.Size ||
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
