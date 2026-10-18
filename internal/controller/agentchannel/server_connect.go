package agentchannel

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
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
	if err := session.RecordProcessAuthentication(authenticate); err != nil {
		return status.Error(codes.FailedPrecondition, "agent session authentication is not current")
	}
	registration, err := s.registerWorkProcess(session.ctx, session, authorization.Generation, authorization.Config.MaxConcurrentTasks)
	if err != nil {
		return taskStoreStatus(err)
	}
	delivered := make(map[string]string, authorization.Config.MaxConcurrentTasks)
	workDelivered := make(map[string]string, authorization.Config.MaxConcurrentTasks)
	quarantined := make(map[string]string, authorization.Config.MaxConcurrentTasks)

	config := proto.Clone(authorization.Config).(*agentpb.AgentConfig)
	if err := stream.Send(&agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_ConfigUpdate{
			ConfigUpdate: &agentpb.ConfigUpdate{AgentConfig: config},
		},
	}); err != nil {
		return err
	}

	if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_WorkExecutorRegistration{WorkExecutorRegistration: registration}}); err != nil {
		return err
	}
	payloads := newAssignmentPayloadDelivery(session.ctx, authorization.Config.MaxConcurrentTasks)
	defer payloads.close()
	received := make(chan agentReceiveResult, 1)
	go func() {
		for {
			message, recvErr := stream.Recv()
			select {
			case received <- agentReceiveResult{message: message, err: recvErr}:
			case <-session.Done():
				return
			}
			if recvErr != nil {
				return
			}
		}
	}()

	observations := &observationExchange{}
	backingAccess := &backingAccessExchange{}
	dnsChecks := &dnsCheckExchange{}
	edgeChecks := &edgeCheckExchange{}
	staging := &backupStagingSession{}
	controlBurst := 0
	for {
		event := nextConnectEvent(
			session,
			observations,
			backingAccess,
			dnsChecks,
			edgeChecks,
			received,
			payloads,
			controlBurst,
		)
		controlBurst = min(controlBurst+1, maximumControlBurst)
		switch event.kind {
		case connectEnded:
			return nil
		case connectPayloadFailed:
			return taskStoreStatus(event.err)
		case connectPayloadFrame:
			controlBurst = 0
			if err := payloads.deliver(stream, event.frame); err != nil {
				return taskStoreStatus(err)
			}
		case connectObservationExpired:
			if err := observations.cancel(session.ctx, stream); err != nil {
				return err
			}
		case connectObservationCommand:
			command := event.observation
			if err := observations.send(session.ctx, stream, session, command); err != nil {
				return err
			}
		case connectBackingAccessExpired:
			if err := backingAccess.cancel(session.ctx, stream); err != nil {
				return err
			}
		case connectBackingAccessCommand:
			if err := backingAccess.send(session.ctx, stream, session, event.backingAccess); err != nil {
				return err
			}
		case connectDNSCheckExpired:
			if err := dnsChecks.cancel(session.ctx, stream); err != nil {
				return err
			}
		case connectDNSCheckCommand:
			if err := dnsChecks.send(session.ctx, stream, session, event.dnsCheck); err != nil {
				return err
			}
		case connectEdgeCheckExpired:
			if err := edgeChecks.cancel(session.ctx, stream); err != nil {
				return err
			}
		case connectEdgeCheckCommand:
			if err := edgeChecks.send(session.ctx, stream, session, event.edgeCheck); err != nil {
				return err
			}
		case connectImageCommand:
			command := event.image
			if !session.imageCommandCurrent(command) {
				continue
			}
			if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ResolveWorkloadImages{
				ResolveWorkloadImages: command.request,
			}}); err != nil {
				return err
			}
		case connectTaskAbort:
			abort := event.abort
			message, resolveErr := s.taskAbortMessage(
				stream.Context(),
				authenticate.AgentId,
				authorization.Generation,
				abort,
			)
			if resolveErr != nil {
				abort.result <- resolveErr
				continue
			}
			sendErr := stream.Send(&agentpb.ControllerMessage{
				Payload: &agentpb.ControllerMessage_TaskAbort{TaskAbort: message},
			})
			if sendErr != nil {
				abort.result <- errs.New(errs.KindStorageUnavailable, "Agent Task abort delivery failed")
				return sendErr
			}
			abort.result <- nil
			payloads.stop(abort.assignmentID)
		case connectLogCommand:
			command := event.log
			if err := stream.Send(command.message); err != nil {
				command.result <- errs.New(errs.KindStorageUnavailable, "Agent log command delivery failed")
				return err
			}
			command.result <- nil
		case connectDispatchWake:
			capacity, ready := session.dispatchCapacity()
			if !ready || capacity == 0 {
				continue
			}
			if err := s.dispatchWork(stream, session, authorization.Generation, capacity, authorization.Config.MaxConcurrentTasks, workDelivered, payloads); err != nil {
				return taskStoreStatus(err)
			}
		case connectReceived:
			result := event.received
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
			if frame := result.message.GetBackupConfigTransfer(); frame != nil {
				if executionplan.RejectUnknown(result.message) != nil {
					clear(frame.GetValueChunk().GetContent())
					return status.Error(codes.InvalidArgument, "Config restore envelope is invalid")
				}
				err := payloads.acceptConfigRestoreFrame(frame)
				clear(frame.GetValueChunk().GetContent())
				if err != nil {
					return taskStoreStatus(err)
				}
				continue
			}
			if credit := result.message.GetBackupConfigCredit(); credit != nil {
				if executionplan.RejectUnknown(result.message) != nil {
					return status.Error(codes.InvalidArgument, "Config credit envelope is invalid")
				}
				if err := payloads.acceptConfigCredit(credit); err != nil {
					return taskStoreStatus(err)
				}
				continue
			}
			if frame := result.message.GetBackupVolumeManifestTransfer(); frame != nil {
				if executionplan.RejectUnknown(result.message) != nil {
					return status.Error(codes.InvalidArgument, "Volume manifest envelope is invalid")
				}
				if err := payloads.acceptVolumeManifestFrame(frame); err != nil {
					return taskStoreStatus(err)
				}
				continue
			}
			if credit := result.message.GetBackupVolumeManifestAckCredit(); credit != nil {
				if executionplan.RejectUnknown(result.message) != nil {
					return status.Error(codes.InvalidArgument, "Volume manifest credit envelope is invalid")
				}
				if err := payloads.acceptVolumeManifestCredit(credit); err != nil {
					return taskStoreStatus(err)
				}
				continue
			}
			if handled, err := s.handleBackupStagingDelivery(stream, session, authenticate.AgentId, authorization.Generation, result.message, staging); handled {
				if err != nil {
					return taskStoreStatus(err)
				}
				continue
			}
			if handled, err := s.handleTaskTerminalDelivery(stream, session, authenticate.AgentId, authorization.Generation, result.message, delivered, quarantined); handled {
				if err != nil {
					return taskStoreStatus(err)
				}
				continue
			}
			if result.message.GetWorkloadImageResolutionResult() != nil {
				session.acceptImageResult(result.message)
				continue
			}
			if result.message.GetEdgeConnectionResult() != nil {
				session.acceptEdgeCheckResult(result.message)
				if edgeChecks.active != nil &&
					result.message.GetEdgeConnectionResult().RequestId == edgeChecks.active.request.RequestId {
					edgeChecks.active = nil
				}
				continue
			}
			if result.message.GetDnsConnectionResult() != nil {
				session.acceptDNSCheckResult(result.message)
				if dnsChecks.active != nil &&
					result.message.GetDnsConnectionResult().RequestId == dnsChecks.active.request.RequestId {
					dnsChecks.active = nil
				}
				continue
			}
			if result.message.GetBackingAccessResult() != nil {
				session.acceptBackingAccessResult(result.message)
				if backingAccess.active != nil &&
					result.message.GetBackingAccessResult().RequestId == backingAccess.active.request.RequestId {
					backingAccess.active = nil
				}
				continue
			}
			if result.message.GetServiceObservationResult() != nil {
				session.acceptObservationResult(session.ctx, result.message)
				if observations.active != nil &&
					result.message.GetServiceObservationResult().RequestId == observations.active.request.RequestId {
					observations.active = nil
				}
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
				if executionplan.RejectUnknown(ready) != nil || ready.TerminalDeliveryClean == nil ||
					(!ready.GetTerminalDeliveryClean() && ready.Capacity != 0) {
					return status.Error(codes.InvalidArgument, "agent Ready terminal delivery state is invalid")
				}
				if err := validateReady(ready.Capacity, ready.Version); err != nil {
					return status.Error(codes.InvalidArgument, "agent Ready is invalid")
				}
				source, err := readyBuildProvenance(ready.GetSource())
				if err != nil {
					return status.Error(codes.InvalidArgument, "agent Ready build provenance is invalid")
				}
				if ready.Capacity > authorization.Config.MaxConcurrentTasks {
					return status.Error(
						codes.InvalidArgument,
						"agent Ready capacity exceeds configuration",
					)
				}
				if !staging.ready {
					if ready.Capacity != 0 || ready.GetTerminalDeliveryClean() {
						return status.Error(codes.FailedPrecondition, "Agent startup staging is not reconciled")
					}
					continue
				}
				if err := session.RecordReady(s.now(), ready.Capacity, ready.Version, source); err != nil {
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
					if ready.Capacity != authorization.Config.MaxConcurrentTasks ||
						!payloads.resize(currentConfig.MaxConcurrentTasks) {
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
				if err := s.dispatchWork(stream, session, authorization.Generation, ready.Capacity, authorization.Config.MaxConcurrentTasks, workDelivered, payloads); err != nil {
					return taskStoreStatus(err)
				}
				continue
			}
			if acknowledgement := result.message.GetTaskAck(); acknowledgement != nil {
				if s.tasks == nil {
					return status.Error(codes.Internal, "agent task store is not configured")
				}
				stage, err := s.acknowledge(
					stream.Context(),
					authenticate.AgentId,
					authorization.Generation,
					acknowledgement,
				)
				if err != nil {
					if kind, _ := errs.KindOf(err); kind == errs.KindStateConflict &&
						acknowledgement.GetBackupResult() != nil {
						handled, deliveryErr := s.sendRejectedBackupTaskTerminal(
							stream,
							session,
							authenticate.AgentId,
							authorization.Generation,
							acknowledgement,
						)
						if deliveryErr != nil {
							return taskStoreStatus(deliveryErr)
						}
						if handled {
							payloads.stop(acknowledgement.AssignmentId)
							continue
						}
					}
					if quarantineTaskReportConflict(stage, err, acknowledgement, delivered, quarantined) {
						slog.Warn(
							"controller: quarantine rejected Agent Task report",
							slog.String("task_id", acknowledgement.TaskId),
							slog.String("assignment_id", acknowledgement.AssignmentId),
							slog.Any("error", err),
						)
						continue
					}
					slog.Error(
						"controller: acknowledge Agent Task",
						slog.String("task_id", acknowledgement.TaskId),
						slog.String("assignment_id", acknowledgement.AssignmentId),
						slog.Any("error", err),
					)
					return taskStoreStatus(err)
				}
				if acknowledgement.GetBackupResult() != nil {
					payloads.stop(acknowledgement.AssignmentId)
					if err := s.sendTaskTerminalReceipt(stream, session, authenticate.AgentId, authorization.Generation, acknowledgement, false); err != nil {
						return taskStoreStatus(err)
					}
					continue
				}
				if err := session.RecordTaskTerminal(
					acknowledgement.TaskId, acknowledgement.AssignmentId,
				); err != nil {
					return status.Error(codes.FailedPrecondition, "agent session is not current")
				}
				payloads.stop(acknowledgement.AssignmentId)
				delete(delivered, acknowledgement.TaskId)
				delete(quarantined, acknowledgement.TaskId)
				continue
			}
			if event := result.message.GetTaskEvent(); event != nil {
				if s.tasks == nil {
					return status.Error(codes.Internal, "agent task store is not configured")
				}
				acknowledgement, err := s.recordTaskEvent(
					stream.Context(), authenticate.AgentId, authorization.Generation, event,
				)
				if err != nil {
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
					Payload: &agentpb.ControllerMessage_TaskEventAck{TaskEventAck: acknowledgement},
				}); err != nil {
					return err
				}
				continue
			}

			if request := result.message.GetWorkStartRequest(); request != nil {
				ack, err := s.startWork(session.ctx, session, authorization.Generation, request)
				if err != nil {
					return taskStoreStatus(err)
				}
				if err := stream.Send(ack); err != nil {
					return err
				}
				if err := payloads.startBackupPayloads(ack.GetWorkStartAck()); err != nil {
					return taskStoreStatus(err)
				}
				continue
			}
			if request := result.message.GetScriptWorkFailureRequest(); request != nil {
				identity, err := s.authenticatedWorkIdentity(session.ctx, session, authorization.Generation)
				if err != nil {
					return taskStoreStatus(err)
				}
				ack, err := s.scriptCompletion.FailScript(session.ctx, identity, request)
				if err != nil {
					return taskStoreStatus(err)
				}
				if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ScriptWorkFailureAck{ScriptWorkFailureAck: ack}}); err != nil {
					return err
				}
				continue
			}
			if request := result.message.GetScriptRecoveryCompletionRequest(); request != nil {
				identity, err := s.authenticatedWorkIdentity(session.ctx, session, authorization.Generation)
				if err != nil {
					return taskStoreStatus(err)
				}
				ack, err := s.scriptCompletion.CompleteScriptRecovery(session.ctx, identity, request)
				if err != nil {
					return taskStoreStatus(err)
				}
				if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ScriptRecoveryCompletionAck{ScriptRecoveryCompletionAck: ack}}); err != nil {
					return err
				}
				continue
			}
			if request := result.message.GetScriptWorkCompletionRequest(); request != nil {
				identity, err := s.authenticatedWorkIdentity(session.ctx, session, authorization.Generation)
				if err != nil {
					return taskStoreStatus(err)
				}
				ack, err := s.scriptCompletion.CompleteScript(session.ctx, identity, request)
				if err != nil {
					return taskStoreStatus(err)
				}
				if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ScriptWorkCompletionAck{ScriptWorkCompletionAck: ack}}); err != nil {
					return err
				}
				continue
			}
			if request := result.message.GetReleaseWorkCompletionRequest(); request != nil {
				if s.releaseCheckpoints == nil {
					return status.Error(codes.Internal, "Release completion service is not configured")
				}
				identity, err := s.authenticatedWorkIdentity(session.ctx, session, authorization.Generation)
				if err != nil {
					return taskStoreStatus(err)
				}
				ack, err := s.releaseCheckpoints.CompleteRelease(session.ctx, identity, request)
				if err != nil {
					return taskStoreStatus(err)
				}
				if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ReleaseWorkCompletionAck{ReleaseWorkCompletionAck: ack}}); err != nil {
					return err
				}
				continue
			}
			if request := result.message.GetReleaseWorkFailureRequest(); request != nil {
				if s.releaseCheckpoints == nil {
					return status.Error(codes.Internal, "Release failure service is not configured")
				}
				identity, err := s.authenticatedWorkIdentity(session.ctx, session, authorization.Generation)
				if err != nil {
					return taskStoreStatus(err)
				}
				ack, err := s.releaseCheckpoints.FailRelease(session.ctx, identity, request)
				if err != nil {
					return taskStoreStatus(err)
				}
				if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ReleaseWorkFailureAck{ReleaseWorkFailureAck: ack}}); err != nil {
					return err
				}
				continue
			}
			if request := result.message.GetReleaseRecoveryCheckpointRequest(); request != nil {
				if s.releaseCheckpoints == nil {
					return status.Error(codes.Internal, "Release recovery checkpoint service is not configured")
				}
				identity, err := s.authenticatedWorkIdentity(session.ctx, session, authorization.Generation)
				if err != nil {
					return taskStoreStatus(err)
				}
				ack, err := s.releaseCheckpoints.CheckpointRecovery(session.ctx, identity, request)
				if err != nil {
					return taskStoreStatus(err)
				}
				if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ReleaseRecoveryCheckpointAck{ReleaseRecoveryCheckpointAck: ack}}); err != nil {
					return err
				}
				continue
			}

			if request := result.message.GetBackupWorkSecretRequest(); request != nil {
				if err := s.deliverBackupWorkSecrets(stream, session, authorization.Generation, request); err != nil {
					return taskStoreStatus(err)
				}
				continue
			}
			if handled, reply, err := s.nativeOperationReport(session, authorization.Generation, result.message); handled {
				if err != nil {
					return err
				}
				payloads.finishBackupPayloads(result.message)
				sendErr := stream.Send(reply)
				clearNativeOperationReply(reply)
				if sendErr != nil {
					return sendErr
				}
				continue
			}
			if request := result.message.GetEnvironmentRemovalCheckpointRequest(); request != nil {
				if s.hierarchyReports == nil {
					return status.Error(codes.Internal, "Environment removal reporting is not configured")
				}
				identity, err := s.authenticatedWorkIdentity(session.ctx, session, authorization.Generation)
				if err != nil {
					return taskStoreStatus(err)
				}
				ack, err := s.hierarchyReports.CheckpointEnvironmentRemoval(session.ctx, identity, request)
				if err != nil {
					return taskStoreStatus(err)
				}
				if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_EnvironmentRemovalCheckpointAck{EnvironmentRemovalCheckpointAck: ack}}); err != nil {
					return err
				}
				continue
			}
			if request := result.message.GetEnvironmentRemovalFailureRequest(); request != nil {
				if s.hierarchyReports == nil {
					return status.Error(codes.Internal, "Environment removal reporting is not configured")
				}
				identity, err := s.authenticatedWorkIdentity(session.ctx, session, authorization.Generation)
				if err != nil {
					return taskStoreStatus(err)
				}
				ack, err := s.hierarchyReports.FailEnvironmentRemoval(session.ctx, identity, request)
				if err != nil {
					return taskStoreStatus(err)
				}
				if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_EnvironmentRemovalFailureAck{EnvironmentRemovalFailureAck: ack}}); err != nil {
					return err
				}
				continue
			}
			if request := result.message.GetEnvironmentRemovalCompletionRequest(); request != nil {
				if s.hierarchyReports == nil {
					return status.Error(codes.Internal, "Environment removal reporting is not configured")
				}
				identity, err := s.authenticatedWorkIdentity(session.ctx, session, authorization.Generation)
				if err != nil {
					return taskStoreStatus(err)
				}
				ack, err := s.hierarchyReports.CompleteEnvironmentRemoval(session.ctx, identity, request)
				if err != nil {
					return taskStoreStatus(err)
				}
				if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_EnvironmentRemovalCompletionAck{EnvironmentRemovalCompletionAck: ack}}); err != nil {
					return err
				}
				continue
			}
			if request := result.message.GetReleaseWorkCheckpointRequest(); request != nil {
				if s.releaseCheckpoints == nil {
					return status.Error(codes.Internal, "Release checkpoint service is not configured")
				}
				identity, err := s.authenticatedWorkIdentity(session.ctx, session, authorization.Generation)
				if err != nil {
					return taskStoreStatus(err)
				}
				ack, err := s.releaseCheckpoints.CheckpointRelease(session.ctx, identity, request)
				if err != nil {
					return taskStoreStatus(err)
				}
				if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ReleaseWorkCheckpointAck{ReleaseWorkCheckpointAck: ack}}); err != nil {
					return err
				}
				continue
			}
			if request := result.message.GetReleaseEffectEvidenceRequest(); request != nil {
				if s.releaseCheckpoints == nil {
					return status.Error(codes.Internal, "Release evidence service is not configured")
				}
				identity, err := s.authenticatedWorkIdentity(session.ctx, session, authorization.Generation)
				if err != nil {
					return taskStoreStatus(err)
				}
				ack, err := s.releaseCheckpoints.PublishReleaseEffect(session.ctx, identity, request)
				if err != nil {
					return taskStoreStatus(err)
				}
				if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ReleaseEffectEvidenceAck{ReleaseEffectEvidenceAck: ack}}); err != nil {
					return err
				}
				continue
			}
			if request := result.message.GetReleaseConfigurationRequest(); request != nil {
				if s.releaseCheckpoints == nil {
					return status.Error(codes.Internal, "Release configuration service is not configured")
				}
				identity, err := s.authenticatedWorkIdentity(session.ctx, session, authorization.Generation)
				if err != nil {
					return taskStoreStatus(err)
				}
				manifest, err := s.releaseCheckpoints.LoadReleaseConfiguration(session.ctx, identity, request)
				if err != nil {
					return taskStoreStatus(err)
				}
				if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ReleaseConfigurationManifest{ReleaseConfigurationManifest: manifest}}); err != nil {
					return err
				}
				continue
			}
			if request := result.message.GetReleaseConfigurationFileRequest(); request != nil {
				if s.releaseCheckpoints == nil {
					return status.Error(codes.Internal, "Release configuration service is not configured")
				}
				identity, err := s.authenticatedWorkIdentity(session.ctx, session, authorization.Generation)
				if err != nil {
					return taskStoreStatus(err)
				}
				delivery, err := s.releaseCheckpoints.LoadReleaseConfigurationFile(session.ctx, identity, request)
				if err != nil {
					releaseexecution.ClearConfigurationDelivery(delivery)
					return taskStoreStatus(err)
				}
				err = stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ReleaseConfigurationFileDelivery{ReleaseConfigurationFileDelivery: delivery}})
				releaseexecution.ClearConfigurationDelivery(delivery)
				if err != nil {
					return err
				}
				continue
			}
			if request := result.message.GetReleaseHookRequest(); request != nil {
				if s.releaseCheckpoints == nil {
					return status.Error(codes.Internal, "Release hook service is not configured")
				}
				identity, err := s.authenticatedWorkIdentity(session.ctx, session, authorization.Generation)
				if err != nil {
					return taskStoreStatus(err)
				}
				delivery, err := s.releaseCheckpoints.LoadReleaseHook(session.ctx, identity, request)
				if err != nil {
					releaseexecution.ClearHookDelivery(delivery)
					return taskStoreStatus(err)
				}
				err = stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ReleaseHookDelivery{ReleaseHookDelivery: delivery}})
				releaseexecution.ClearHookDelivery(delivery)
				if err != nil {
					return err
				}
				continue
			}
			if request := result.message.GetScriptWorkCheckpointRequest(); request != nil {
				identity, err := s.authenticatedWorkIdentity(session.ctx, session, authorization.Generation)
				if err != nil {
					return taskStoreStatus(err)
				}
				ack, err := s.checkpointScriptWork(session.ctx, identity, request)
				if err != nil {
					return taskStoreStatus(err)
				}
				if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ScriptWorkCheckpointAck{ScriptWorkCheckpointAck: ack}}); err != nil {
					return err
				}
				continue
			}
			if request := result.message.GetScriptCheckpointRequest(); request != nil {
				if s.scriptCheckpoints == nil {
					return status.Error(codes.Internal, "Script checkpoint service is not configured")
				}
				identity, err := s.authenticatedWorkIdentity(session.ctx, session, authorization.Generation)
				if err != nil {
					return taskStoreStatus(err)
				}
				acknowledgement, err := s.scriptCheckpoints.CheckpointScript(
					session.ctx, identity, request,
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
			if request := result.message.GetBackingHookCheckpointRequest(); request != nil {
				if s.backingHookCheckpoints == nil {
					return status.Error(codes.Internal, "Backing hook checkpoint service is not configured")
				}
				acknowledgement, err := s.backingHookCheckpoints.CheckpointBackingHook(
					stream.Context(), authenticate.AgentId, authorization.Generation, request,
				)
				if err != nil {
					return taskStoreStatus(err)
				}
				if err := stream.Send(&agentpb.ControllerMessage{
					Payload: &agentpb.ControllerMessage_BackingHookCheckpointAck{
						BackingHookCheckpointAck: acknowledgement,
					},
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
