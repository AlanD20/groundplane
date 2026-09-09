package agent

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
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
	authenticate := &agentpb.Authenticate{AgentId: c.agentID, Token: authToken}
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
	poolCancel, workersDone := c.startWorkerPool(streamCtx, int(config.MaxConcurrentTasks))
	defer func() {
		poolCancel()
		<-workersDone
	}()
	if err := c.sendReady(stream); err != nil {
		return agentChannelTransportResult(ctx, err, "agent: send readiness", false)
	}

	ticker := time.NewTicker(pullInterval)
	defer ticker.Stop()
	received := make(chan receiveResult, 1)
	images := newImageSession(streamCtx, c.images)
	defer images.close()
	receiveNext(streamCtx, stream, received)
	for {
		select {
		case <-ctx.Done():
			return false, nil
		case output := <-images.outputs:
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
		case <-ticker.C:
			if err := c.sendReady(stream); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send readiness", false)
			}
		case result := <-received:
			if result.err != nil {
				return agentChannelTransportResult(ctx, result.err, "agent: receive Controller message", true)
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
		case output := <-c.pool.Outputs():
			if output.VolumeCheckpoint != nil {
				if output.ScriptCheckpoint != nil || output.BackupCheckpoint != nil || output.Progress != nil ||
					output.Result != nil {
					return false, errs.New(errs.KindInternal, "agent: worker returned an invalid output union")
				}
				if err := stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_VolumeRemovalCheckpointRequest{
					VolumeRemovalCheckpointRequest: output.VolumeCheckpoint,
				}}); err != nil {
					return agentChannelTransportResult(ctx, err, "agent: send Volume removal checkpoint", false)
				}
				continue
			}
			if output.ScriptCheckpoint != nil {
				if output.BackupCheckpoint != nil ||
					output.Progress != nil || output.Result != nil {
					return false, errs.New(errs.KindInternal, "agent: worker returned an invalid output union")
				}
				if err := c.sendScriptCheckpoint(stream, output.ScriptCheckpoint); err != nil {
					return agentChannelTransportResult(ctx, err, "agent: send Script checkpoint", false)
				}
				continue
			}
			if output.BackupCheckpoint != nil {
				if output.Progress != nil || output.Result != nil || output.ScriptCheckpoint != nil {
					return false, errs.New(errs.KindInternal, "agent: worker returned an invalid output union")
				}
				if err := c.sendBackupCheckpoint(stream, output.BackupCheckpoint); err != nil {
					return agentChannelTransportResult(ctx, err, "agent: send Backup checkpoint", false)
				}
				continue
			}
			if output.Progress != nil {
				if output.Result != nil {
					return false, errs.New(errs.KindInternal, "agent: worker returned an invalid output union")
				}
				if err := c.sendTaskEvent(stream, *output.Progress); err != nil {
					return agentChannelTransportResult(ctx, err, "agent: send task event", false)
				}
				continue
			}
			if output.Result == nil {
				return false, errs.New(errs.KindInternal, "agent: worker returned an empty output")
			}
			if err := c.sendTaskAck(stream, *output.Result); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send task acknowledgement", false)
			}
			if err := c.sendReady(stream); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send readiness", false)
			}
		case message := <-c.logs.Outputs():
			if err := stream.Send(message); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send log message", false)
			}
		}
	}
}
