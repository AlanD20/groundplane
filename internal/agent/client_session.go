package agent

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
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
	if c.backupExecutor == nil || c.removalRecovery == nil || c.lifecycleExecutor == nil || c.releaseExecutor == nil || c.releaseRecoveryExecutor == nil || c.scriptExecutor == nil || c.hierarchyExecutor == nil || c.attachRemovalExecutor == nil || c.attachProvisionExecutor == nil || c.volumeExecutor == nil || c.networkExecutor == nil || c.componentExecutor == nil || c.entryExecutor == nil {
		return false, errs.New(errs.KindInternal, "native Release executor is not configured")
	}
	if err := c.processOwnership.Check(); err != nil {
		return false, err
	}

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	native := newNativeExchange()
	defer native.close()
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
		ChannelSchema:       agentprotocol.SchemaVersion,
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
	registered, err := stream.Recv()
	if err != nil {
		return agentChannelTransportResult(ctx, err, "agent: receive executor registration", false)
	}
	if err := native.bindExecutor(registered.GetWorkExecutorRegistration(), c.agentID, c.processGeneration); err != nil {
		return false, err
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
	releases, err := newExecutionWorkers(streamCtx, int(config.MaxConcurrentTasks), native.identity, c.releaseExecutor, c.releaseRecoveryExecutor, c.scriptExecutor, c.hierarchyExecutor, c.attachRemovalExecutor, c.volumeExecutor, c.networkExecutor, c.componentExecutor, c.entryExecutor, c.attachProvisionExecutor, c.lifecycleExecutor, c.removalRecovery, c.backupExecutor, native, c.processOwnership)
	if err != nil {
		return false, err
	}
	c.executionWorkers = releases
	defer func() {
		if releases != nil {
			releases.close()
		}
		c.executionWorkers = nil
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
	backingAccess := newBackingAccessSession(c.backingAccess)
	defer backingAccess.Close()
	dnsChecks := newDNSCheckSession(c.dnsChecks)
	defer dnsChecks.close()
	edgeChecks := newEdgeCheckSession(c.edgeChecks)
	defer edgeChecks.close()
	receiveNext(streamCtx, stream, received)
	controlBurst := 0
	for {
		// PostgreSQL execution markers outlive source-file cleanup. Drain
		// existing work and finish terminal delivery before
		// reopening the normal startup handshake; do not advertise Ready first.
		if staging.reinspect.Load() && c.availableWorkCapacity() == int(config.MaxConcurrentTasks) {
			clean, err := c.terminalDeliveryClean()
			if err != nil {
				return false, err
			}
			if clean {
				return true, nil
			}
		}
		event := nextSessionEvent(
			ctx,
			c.pool,
			observations,
			images,
			backingAccess,
			dnsChecks,
			edgeChecks,
			received,
			c.logs.Outputs(),
			ticker.C,
			controlBurst,
			native.outgoing,
			releases.outputs,
		)
		controlBurst = min(controlBurst+1, maximumControlBurst)
		switch event.kind {
		case sessionNativeResult:
			if err := releases.consume(event.nativeResult); err != nil {
				return false, err
			}
			if event.nativeResult.err != nil {
				return false, event.nativeResult.err
			}
			if err := c.sendReady(stream); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send readiness after Release completion", false)
			}
		case sessionNative:
			err := stream.Send(event.native)
			backupconfigtransfer.ClearFrame(event.native.GetBackupConfigTransfer())
			if err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send native work request", false)
			}
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
		case sessionBackingAccess:
			if err := backingAccess.Close(); err != nil {
				return false, err
			}
			if err := sendBackingAccess(streamCtx, stream, event.backingAccess); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send Backing access result", false)
			}
		case sessionDNSCheck:
			dnsChecks.close()
			if err := sendDNSCheck(streamCtx, stream, event.dnsCheck); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send DNS check result", false)
			}
		case sessionEdgeCheck:
			edgeChecks.close()
			if err := sendEdgeCheck(streamCtx, stream, event.edgeCheck); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send Tunnel check result", false)
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
			if err := c.processOwnership.Check(); err != nil {
				return false, err
			}
			if transfer := result.message.GetBackupWorkSecretTransfer(); transfer != nil {
				if err := native.acceptBackupSecrets(streamCtx, transfer); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if handled, err := native.acceptBackupConfig(streamCtx, result.message); handled {
				if err != nil {
					return false, err
				}
				continue
			}
			if handled, err := native.accept(result.message); handled {
				if err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if assignment := result.message.GetAttachRecoveryAssignment(); assignment != nil {
				err := releases.submitAttachRecovery(assignment, c.availableWorkCapacity())
				attachexecution.ClearRecoveryAssignment(assignment)
				if err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if assignment := result.message.GetAttachProvisionRecoveryAssignment(); assignment != nil {
				if err := releases.submitProvisionRecovery(assignment, c.availableWorkCapacity()); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if assignment := result.message.GetAttachProvisionAssignment(); assignment != nil {
				err := releases.submitAttachProvision(assignment, c.availableWorkCapacity())
				attachexecution.ClearProvision(assignment.Provision)
				if err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if assignment := result.message.GetAttachRemovalAssignment(); assignment != nil {
				err := releases.submitAttachRemoval(assignment, c.availableWorkCapacity())
				attachexecution.ClearRemoval(assignment.Removal)
				if err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if assignment := result.message.GetEntryConfigurationWorkAssignment(); assignment != nil {
				if err := releases.submitEntry(assignment, c.availableWorkCapacity()); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if assignment := result.message.GetComponentWorkAssignment(); assignment != nil {
				if err := releases.submitComponent(assignment, c.availableWorkCapacity()); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if assignment := result.message.GetNetworkWorkAssignment(); assignment != nil {
				if err := releases.submitNetwork(assignment, c.availableWorkCapacity()); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if assignment := result.message.GetServiceRemovalRecoveryAssignment(); assignment != nil {
				if err := releases.submitRemovalRecovery(assignment, c.availableWorkCapacity()); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if assignment := result.message.GetBackupWorkAssignment(); assignment != nil {
				if err := releases.submitBackup(assignment, c.availableWorkCapacity(), staging.runtime); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if assignment := result.message.GetBackupRecoveryAssignment(); assignment != nil {
				if err := releases.submitBackupRecovery(assignment, c.availableWorkCapacity(), staging.runtime); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if assignment := result.message.GetServiceLifecycleAssignment(); assignment != nil {
				if err := releases.submitLifecycle(assignment, c.availableWorkCapacity()); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if assignment := result.message.GetVolumeWorkAssignment(); assignment != nil {
				if err := releases.submitVolume(assignment, c.availableWorkCapacity()); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if assignment := result.message.GetEnvironmentRemovalAssignment(); assignment != nil {
				if err := releases.submitEnvironmentRemoval(assignment, c.availableWorkCapacity()); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if assignment := result.message.GetScriptWorkAssignment(); assignment != nil {
				if err := releases.submitScript(assignment, c.availableWorkCapacity()); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if assignment := result.message.GetScriptRecoveryAssignment(); assignment != nil {
				if err := releases.submitScriptRecovery(assignment, c.availableWorkCapacity()); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if cancellation := result.message.GetWorkCancel(); cancellation != nil {
				if err := releases.cancelAttempt(cancellation); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if assignment := result.message.GetReleaseWorkAssignment(); assignment != nil {
				if err := releases.submit(assignment, c.availableWorkCapacity()); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if assignment := result.message.GetReleaseRecoveryAssignment(); assignment != nil {
				if err := releases.submitRecovery(assignment, c.availableWorkCapacity()); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
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
			if handled, err := backingAccess.handle(streamCtx, stream, result.message); handled {
				if err != nil {
					return agentChannelTransportResult(ctx, err, "agent: receive Backing access check", true)
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if handled, err := dnsChecks.handle(streamCtx, stream, result.message); handled {
				if err != nil {
					return agentChannelTransportResult(ctx, err, "agent: receive DNS check", false)
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if handled, err := edgeChecks.handle(streamCtx, stream, result.message); handled {
				if err != nil {
					return agentChannelTransportResult(ctx, err, "agent: receive Tunnel check", false)
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
				if c.availableWorkCapacity() != int(config.MaxConcurrentTasks) {
					return false, errs.New(
						errs.KindStateConflict,
						"agent: live configuration arrived before the worker pool drained",
					)
				}
				poolCancel()
				<-workersDone
				releases.close()
				config = proto.Clone(next).(*agentpb.AgentConfig)
				poolCancel, workersDone = c.startWorkerPool(streamCtx, int(config.MaxConcurrentTasks))
				releases, err = newExecutionWorkers(streamCtx, int(config.MaxConcurrentTasks), native.identity, c.releaseExecutor, c.releaseRecoveryExecutor, c.scriptExecutor, c.hierarchyExecutor, c.attachRemovalExecutor, c.volumeExecutor, c.networkExecutor, c.componentExecutor, c.entryExecutor, c.attachProvisionExecutor, c.lifecycleExecutor, c.removalRecovery, c.backupExecutor, native, c.processOwnership)
				if err != nil {
					return false, err
				}
				c.executionWorkers = releases
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
			if output.ScriptCheckpoint != nil {
				if err := c.sendScriptCheckpoint(stream, output.ScriptCheckpoint); err != nil {
					return agentChannelTransportResult(ctx, err, "agent: send Script checkpoint", false)
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
