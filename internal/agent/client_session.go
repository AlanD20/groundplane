package agent

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (c *Client) runSession(
	ctx context.Context,
	token *[agentprotocol.RawTokenBytes]byte,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, nil
	}

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, connection, err := c.connect(streamCtx, c.socketPath)
	if err != nil {
		return agentChannelTransportResult(ctx, err, "agent: connect to Controller", false)
	}
	defer func() {
		// Best effort: stream teardown cannot supersede the primary Run result.
		_ = stream.CloseSend()
		// Best effort: connection teardown cannot supersede the primary Run result.
		_ = connection.Close()
	}()

	authToken := append([]byte(nil), token[:]...)
	authenticate := &agentpb.Authenticate{
		AgentId: c.agentID, Token: authToken,
		ExecutionPlanSchema: executionplan.SchemaVersion,
		ProcessGeneration:   append([]byte(nil), c.processGeneration[:]...),
	}
	message := &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Authenticate{Authenticate: authenticate}}
	if err := stream.Send(message); err != nil {
		clear(authToken)
		authenticate.Token = nil
		return agentChannelTransportResult(ctx, err, "agent: send authentication", false)
	}
	clear(authToken)
	authenticate.Token = nil

	initial, err := stream.Recv()
	if err != nil {
		return agentChannelTransportResult(ctx, err, "agent: receive initial configuration", false)
	}
	config := initial.GetConfigUpdate().GetAgentConfig()
	if config == nil {
		return false, errs.New(errs.KindInternal, "agent: Controller did not send configuration first")
	}
	if !validRuntimeConfig(config) {
		return false, errs.New(errs.KindInternal, "agent: Controller sent invalid initial configuration")
	}

	pullInterval := time.Duration(config.PullIntervalSeconds) * time.Second
	config = proto.Clone(config).(*agentpb.AgentConfig)
	staging, err := c.openStaging(streamCtx, c.stagingJournal)
	if err != nil {
		return false, err
	}
	c.staging = staging
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if closeErr := staging.close(closeCtx); closeErr != nil {
			c.logger.Error("agent: close Backup staging", "error", closeErr)
		}
		c.staging = nil
	}()
	poolCancel, workersDone := c.startWorkerPool(streamCtx, int(config.MaxConcurrentTasks))
	defer func() {
		poolCancel()
		<-workersDone
	}()
	if err := c.replayTerminalDelivery(streamCtx, stream); err != nil {
		return agentChannelTransportResult(ctx, err, "agent: replay terminal delivery", false)
	}
	if err := c.sendReady(stream); err != nil {
		return agentChannelTransportResult(ctx, err, "agent: send readiness", false)
	}
	if err := stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackupStagingInventory{BackupStagingInventory: proto.CloneOf(staging.inventory)}}); err != nil {
		return agentChannelTransportResult(ctx, err, "agent: send startup staging inventory", false)
	}

	ticker := time.NewTicker(pullInterval)
	defer ticker.Stop()
	received := make(chan receiveResult, 1)
	images := newImageSession(streamCtx, c.images)
	defer images.close()
	observations := newObservationSession(c.observer)
	defer observations.Close()
	receiveNext(streamCtx, stream, received)
	controlBurst := 0
	for {
		// PostgreSQL execution markers outlive source-file cleanup. Drain
		// existing work and finish terminal delivery before
		// reopening the normal startup handshake; do not advertise Ready first.
		if staging.reinspect.Load() && c.pool.Capacity() == int(config.MaxConcurrentTasks) {
			clean, err := c.terminalDeliveryClean()
			if err != nil {
				return false, err
			}
			if clean {
				return true, nil
			}
		}
		event := nextSessionEvent(ctx, c.pool, observations, images, received, c.logs.Outputs(), ticker.C, controlBurst)
		controlBurst = min(controlBurst+1, maximumControlBurst)
		switch event.kind {
		case sessionEnded:
			return false, nil
		case sessionBulk:
			if err := c.pool.sendBackupBulkFrame(stream, event.bulk); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send Config Restore frame", false)
			}
			controlBurst = 0
		case sessionObservation:
			if err := observations.Close(); err != nil {
				return false, err
			}
			if err := sendServiceObservation(streamCtx, stream, event.observation); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send service observation", false)
			}
		case sessionImage:
			output := event.image
			images.busy = false
			if output.err != nil {
				return agentChannelTransportResult(ctx, output.err, "agent: resolve workload images", true)
			}
			message := &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_WorkloadImageResolutionResult{
				WorkloadImageResolutionResult: output.result,
			}}
			if err := stream.Send(message); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send workload image result", false)
			}
		case sessionReadiness:
			if err := c.sendReady(stream); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send readiness", false)
			}
		case sessionReceived:
			result := event.received
			if result.err != nil {
				return agentChannelTransportResult(ctx, result.err, "agent: receive Controller message", true)
			}
			if handled, err := c.handleBackupStagingDelivery(streamCtx, stream, result.message); handled {
				if err != nil {
					return agentChannelTransportResult(ctx, err, "agent: startup staging delivery", false)
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if handled, err := c.handleTerminalDelivery(streamCtx, stream, result.message); handled {
				if err != nil {
					return agentChannelTransportResult(ctx, err, "agent: terminal delivery", false)
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if handled, err := observations.handle(streamCtx, stream, result.message); handled {
				if err != nil {
					return agentChannelTransportResult(ctx, err, "agent: receive service observation", true)
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if request := result.message.GetResolveWorkloadImages(); request != nil {
				if proto.Size(result.message) > workloadimage.MaximumEnvelopeBytes ||
					len(result.message.ProtoReflect().GetUnknown()) != 0 {
					return false, errs.New(errs.KindValidationFailed, "agent: invalid image request envelope")
				}
				if err := images.start(request); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if update := result.message.GetConfigUpdate(); update != nil {
				next := update.GetAgentConfig()
				if !validRuntimeConfig(next) {
					return false, errs.New(errs.KindInternal, "agent: Controller sent invalid live configuration")
				}
				if c.pool.Capacity() != int(config.MaxConcurrentTasks) {
					return false, errs.New(
						errs.KindStateConflict,
						"agent: live configuration arrived before the worker pool drained",
					)
				}
				poolCancel()
				<-workersDone
				config = proto.Clone(next).(*agentpb.AgentConfig)
				poolCancel, workersDone = c.startWorkerPool(streamCtx, int(config.MaxConcurrentTasks))
				pullInterval = time.Duration(config.PullIntervalSeconds) * time.Second
				ticker.Reset(pullInterval)
				receiveNext(streamCtx, stream, received)
				if err := c.sendReady(stream); err != nil {
					return agentChannelTransportResult(
						ctx,
						err,
						"agent: send readiness after configuration update",
						false,
					)
				}
				continue
			}
			if acknowledgement := result.message.GetBackupCheckpointAck(); acknowledgement != nil {
				if err := c.pool.AcceptBackupCheckpointAck(acknowledgement); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if acknowledgement := result.message.GetScriptCheckpointAck(); acknowledgement != nil {
				if err := c.pool.AcceptScriptCheckpointAck(acknowledgement); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if acknowledgement := result.message.GetBackingHookCheckpointAck(); acknowledgement != nil {
				if err := c.pool.AcceptBackingHookCheckpointAck(acknowledgement); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if acknowledgement := result.message.GetVolumeRemovalCheckpointAck(); acknowledgement != nil {
				if err := c.pool.AcceptVolumeRemovalCheckpointAck(acknowledgement); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			shutdown, err := c.handleControllerMessage(streamCtx, result.message)
			if err != nil {
				return false, err
			}
			if shutdown {
				return false, nil
			}
			receiveNext(streamCtx, stream, received)
		case sessionWorker:
			output := event.worker
			if err := output.validate(); err != nil {
				return false, err
			}
			if output.Error != nil {
				return false, output.Error
			}
			if output.BackingHookCheckpoint != nil {
				if err := c.sendBackingHookCheckpoint(stream, output.BackingHookCheckpoint); err != nil {
					return agentChannelTransportResult(ctx, err, "agent: send backing hook checkpoint", false)
				}
				continue
			}
			if output.VolumeCheckpoint != nil {
				if err := stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_VolumeRemovalCheckpointRequest{
					VolumeRemovalCheckpointRequest: output.VolumeCheckpoint,
				}}); err != nil {
					return agentChannelTransportResult(ctx, err, "agent: send Volume removal checkpoint", false)
				}
				continue
			}
			if output.ScriptCheckpoint != nil {
				if err := c.sendScriptCheckpoint(stream, output.ScriptCheckpoint); err != nil {
					return agentChannelTransportResult(ctx, err, "agent: send Script checkpoint", false)
				}
				continue
			}
			if output.BackupCheckpoint != nil {
				if err := c.sendBackupCheckpoint(stream, output.BackupCheckpoint); err != nil {
					return agentChannelTransportResult(ctx, err, "agent: send Backup checkpoint", false)
				}
				continue
			}
			if output.BackupConfigCredit != nil {
				if err := stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackupConfigCredit{
					BackupConfigCredit: output.BackupConfigCredit,
				}}); err != nil {
					return agentChannelTransportResult(ctx, err, "agent: send Config credit", false)
				}
				continue
			}
			if output.BackupVolumeCredit != nil {
				if err := stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackupVolumeManifestAckCredit{
					BackupVolumeManifestAckCredit: output.BackupVolumeCredit,
				}}); err != nil {
					return agentChannelTransportResult(ctx, err, "agent: send Volume manifest credit", false)
				}
				continue
			}
			if output.Progress != nil {
				if err := c.sendTaskEvent(stream, *output.Progress); err != nil {
					return agentChannelTransportResult(ctx, err, "agent: send task event", false)
				}
				continue
			}
			if err := c.sendTaskAck(stream, *output.Result); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send task acknowledgement", false)
			}
			if err := c.sendReady(stream); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send readiness", false)
			}
		case sessionLog:
			if err := stream.Send(event.log); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send log message", false)
			}
		}
	}
}
