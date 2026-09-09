package agentchannel

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

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
		case command := <-session.state.imageCommands:
			if !session.imageCommandCurrent(command) {
				continue
			}
			if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ResolveWorkloadImages{
				ResolveWorkloadImages: command.request,
			}}); err != nil {
				return err
			}
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
		case <-session.taskDispatchWake():
			capacity, ready := session.dispatchCapacity()
			if !ready || capacity == 0 {
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
				capacity,
				delivered,
				quarantined,
			); err != nil {
				return taskStoreStatus(err)
			}
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
			if result.message.GetWorkloadImageResolutionResult() != nil {
				session.acceptImageResult(result.message)
				continue
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
					if !session.invalidateReady() {
						return status.Error(codes.FailedPrecondition, "agent session is not current")
					}
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
				if err := stream.Send(&agentpb.ControllerMessage{
					Payload: &agentpb.ControllerMessage_TaskEventAck{TaskEventAck: &agentpb.TaskEventAck{
						TaskId: event.GetTaskId(), AssignmentId: event.GetAssignmentId(),
						PlanHash: append([]byte(nil), event.GetPlanHash()...), StepId: event.GetStepId(),
						ExecutionEpoch: event.GetExecutionEpoch(), Ordinal: event.GetOrdinal(), State: event.GetState(),
					}},
				}); err != nil {
					return err
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
			if request := result.message.GetVolumeRemovalCheckpointRequest(); request != nil {
				ack, err := s.checkpointVolumeRemoval(
					stream.Context(),
					authenticate.AgentId,
					authorization.Generation,
					request,
				)
				if err != nil {
					return taskStoreStatus(err)
				}
				if err := stream.Send(ack); err != nil {
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
