package agent

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/AlanD20/groundplane/internal/agent/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/agent/backupsecrettransfer"
	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type nativeExchangeKey struct{ attempt, kind string }
type nativeWaiter struct {
	identity    *agentpb.WorkAttemptIdentity
	executionID string
	response    chan *agentpb.ControllerMessage
	abandoned   bool
}

// nativeExchange is scoped to one authenticated stream. Only the session writer
// consumes outgoing; workers never call stream.Send concurrently.
type nativeExchange struct {
	backupConfigs *backupconfiguration.Inbox
	backupVolumes map[string]*nativeVolumeSlot
	backupSecrets *backupsecrettransfer.WorkInbox
	mu            sync.Mutex
	pending       map[nativeExchangeKey]*nativeWaiter
	outgoing      chan *agentpb.AgentMessage
	done          chan struct{}
	closed        bool
	identity      workexecution.Identity
}

func newNativeExchange() *nativeExchange {
	return &nativeExchange{backupConfigs: backupconfiguration.NewInbox(), backupVolumes: make(map[string]*nativeVolumeSlot), backupSecrets: backupsecrettransfer.NewWorkInbox(), pending: make(map[nativeExchangeKey]*nativeWaiter), outgoing: make(chan *agentpb.AgentMessage, 256), done: make(chan struct{})}
}

func (exchange *nativeExchange) bindExecutor(registration *agentpb.WorkExecutorRegistration, agentID string, process [16]byte) error {
	if registration == nil || executionplan.RejectUnknown(registration) != nil || registration.AgentId != agentID ||
		!bytes.Equal(registration.ProcessGeneration, process[:]) {
		return errs.New(errs.KindStateConflict, "Controller executor registration differs from this Agent process")
	}
	identity := workexecution.Identity{Executor: workexecution.Agent, HostID: registration.HostId, AgentID: registration.AgentId, Generation: registration.ExecutorGeneration}
	if err := identity.Validate(); err != nil {
		return err
	}
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	if exchange.closed || exchange.identity.Generation != 0 {
		return errs.New(errs.KindStateConflict, "native executor registration is closed or already bound")
	}
	exchange.identity = identity
	return nil
}

func (exchange *nativeExchange) request(ctx context.Context, message *agentpb.AgentMessage, identity *agentpb.WorkAttemptIdentity, kind, executionID string) (*agentpb.ControllerMessage, error) {
	if ctx == nil || identity == nil || identity.AttemptId == "" || executionplan.RejectUnknown(message) != nil || proto.Size(message) > 20*1024 {
		return nil, errs.New(errs.KindValidationFailed, "native work exchange request is invalid")
	}
	key := nativeExchangeKey{identity.AttemptId, kind}
	waiter := &nativeWaiter{identity: proto.CloneOf(identity), executionID: executionID, response: make(chan *agentpb.ControllerMessage, 1)}
	exchange.mu.Lock()
	if _, err := workexecution.DecodeAgentAttempt(identity, exchange.identity); err != nil {
		exchange.mu.Unlock()
		return nil, err
	}
	if exchange.closed || len(exchange.pending) >= 256 || exchange.pending[key] != nil {
		exchange.mu.Unlock()
		return nil, errs.New(errs.KindStateConflict, "native work exchange is closed, full or already awaiting this response")
	}
	exchange.pending[key] = waiter
	exchange.mu.Unlock()
	defer func() {
		exchange.mu.Lock()
		defer exchange.mu.Unlock()
		waiter.abandoned = true
		select {
		case response := <-waiter.response:
			backingprovision.ClearPrivateDelivery(response)
			releaseexecution.ClearHookDelivery(response.GetReleaseHookDelivery())
			releaseexecution.ClearConfigurationDelivery(response.GetReleaseConfigurationFileDelivery())
			removalexecution.ClearConfigurationDelivery(response.GetServiceRemovalConfigurationFileDelivery())
		default:
		}
	}()
	select {
	case exchange.outgoing <- proto.CloneOf(message):
	case <-ctx.Done():
		exchange.mu.Lock()
		if exchange.pending[key] == waiter {
			delete(exchange.pending, key)
		}
		exchange.mu.Unlock()
		return nil, ctx.Err()
	case <-exchange.done:
		return nil, errs.New(errs.KindRequestFailed, "native Agent stream closed")
	}
	select {
	case response := <-waiter.response:
		return response, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-exchange.done:
		return nil, errs.New(errs.KindRequestFailed, "native Agent stream closed")
	}
}

func (exchange *nativeExchange) accept(message *agentpb.ControllerMessage) (bool, error) {
	if handled, err := exchange.acceptBackupVolume(message); handled {
		return true, err
	}
	var identity *agentpb.WorkAttemptIdentity
	kind, executionID := "", ""
	switch {
	case message.GetBackingProvisionCheckpointAck() != nil:
		identity, kind = message.GetBackingProvisionCheckpointAck().Attempt, "backing_checkpoint"
	case message.GetBackingProvisionCompletionAck() != nil:
		identity, kind = message.GetBackingProvisionCompletionAck().Attempt, "backing_completion"
	case message.GetBackingProvisionFailureAck() != nil:
		identity, kind = message.GetBackingProvisionFailureAck().Attempt, "backing_failure"
	case message.GetBackingProvisionRecoveryAck() != nil:
		identity, kind = message.GetBackingProvisionRecoveryAck().Attempt, "backing_recovery"
	case message.GetBackingProvisionConfigurationManifest() != nil:
		manifest := message.GetBackingProvisionConfigurationManifest()
		identity, kind, executionID = manifest.Attempt, "backing_configuration", manifest.StageId
	case message.GetBackingProvisionFileDelivery() != nil:
		delivery := message.GetBackingProvisionFileDelivery()
		identity, kind, executionID = delivery.Attempt, "backing_file", delivery.StageId+"/"+delivery.GetFile().GetDestination()
	case message.GetBackingProvisionHookDelivery() != nil:
		delivery := message.GetBackingProvisionHookDelivery()
		identity, kind, executionID = delivery.Attempt, "backing_hook", delivery.StageId
	case message.GetEnvironmentProvisionResultAck() != nil:
		identity, kind = message.GetEnvironmentProvisionResultAck().Attempt, "environment_provision_result"
	case message.GetBackupConfigRecoveryPrepareAck() != nil:
		ack := message.GetBackupConfigRecoveryPrepareAck()
		identity, kind, executionID = ack.GetAssignment().GetAttempt(), "backup_config_recovery_prepare", fmt.Sprint(ack.SourceOrdinal)
	case message.GetBackupWorkCompletionAck() != nil:
		identity, kind = message.GetBackupWorkCompletionAck().Attempt, "backup_completion"
	case message.GetBackupWorkFailureAck() != nil:
		identity, kind = message.GetBackupWorkFailureAck().Attempt, "backup_failure"
	case message.GetBackupRecoveryAck() != nil:
		identity, kind = message.GetBackupRecoveryAck().Attempt, "backup_prune_recovery"
	case message.GetBackupWorkCheckpointAck() != nil:
		ack := message.GetBackupWorkCheckpointAck()
		identity, kind, executionID = ack.Attempt, "backup_checkpoint", fmt.Sprintf("%d/%d", ack.SourceOrdinal, ack.CheckpointSequence)
	case message.GetEntryConfigurationManifest() != nil:
		manifest := message.GetEntryConfigurationManifest()
		identity, kind, executionID = manifest.Attempt, "entry_configuration", manifest.StageId
	case message.GetEntryConfigurationFileDelivery() != nil:
		delivery := message.GetEntryConfigurationFileDelivery()
		identity, kind, executionID = delivery.Attempt, "entry_configuration_file", delivery.StageId+"/"+delivery.GetFile().GetDestination()
	case message.GetEntryConfigurationWorkCheckpointAck() != nil:
		identity, kind = message.GetEntryConfigurationWorkCheckpointAck().Attempt, "entry_checkpoint"
	case message.GetEntryConfigurationWorkCompletionAck() != nil:
		identity, kind = message.GetEntryConfigurationWorkCompletionAck().Attempt, "entry_completion"
	case message.GetEntryConfigurationWorkFailureAck() != nil:
		identity, kind = message.GetEntryConfigurationWorkFailureAck().Attempt, "entry_failure"
	case message.GetEntryConfigurationWorkRecoveryReportAck() != nil:
		identity, kind = message.GetEntryConfigurationWorkRecoveryReportAck().Attempt, "entry_recovery"
	case message.GetComponentConfigurationManifest() != nil:
		manifest := message.GetComponentConfigurationManifest()
		identity, kind, executionID = manifest.Attempt, "component_configuration", manifest.StageId
	case message.GetComponentConfigurationFileDelivery() != nil:
		delivery := message.GetComponentConfigurationFileDelivery()
		identity, kind, executionID = delivery.Attempt, "component_configuration_file", delivery.StageId+"/"+delivery.GetFile().GetDestination()
	case message.GetComponentWorkCheckpointAck() != nil:
		identity, kind = message.GetComponentWorkCheckpointAck().Attempt, "component_checkpoint"
	case message.GetComponentWorkCompletionAck() != nil:
		identity, kind = message.GetComponentWorkCompletionAck().Attempt, "component_completion"
	case message.GetComponentWorkFailureAck() != nil:
		identity, kind = message.GetComponentWorkFailureAck().Attempt, "component_failure"
	case message.GetComponentWorkRecoveryReportAck() != nil:
		identity, kind = message.GetComponentWorkRecoveryReportAck().Attempt, "component_recovery"
	case message.GetNetworkWorkCheckpointAck() != nil:
		identity, kind = message.GetNetworkWorkCheckpointAck().Attempt, "network_checkpoint"
	case message.GetNetworkWorkCompletionAck() != nil:
		identity, kind = message.GetNetworkWorkCompletionAck().Attempt, "network_completion"
	case message.GetNetworkWorkFailureAck() != nil:
		identity, kind = message.GetNetworkWorkFailureAck().Attempt, "network_failure"
	case message.GetNetworkWorkRecoveryReportAck() != nil:
		identity, kind = message.GetNetworkWorkRecoveryReportAck().Attempt, "network_recovery"
	case message.GetVolumeWorkCheckpointAck() != nil:
		identity, kind = message.GetVolumeWorkCheckpointAck().Attempt, "volume_checkpoint"
	case message.GetServiceLifecycleCheckpointAck() != nil:
		identity, kind = message.GetServiceLifecycleCheckpointAck().Attempt, "lifecycle_checkpoint"
	case message.GetServiceLifecycleRecoveryReportAck() != nil:
		identity, kind = message.GetServiceLifecycleRecoveryReportAck().Attempt, "lifecycle_recovery"
	case message.GetServiceRemovalRecoveryCheckpointAck() != nil:
		identity, kind = message.GetServiceRemovalRecoveryCheckpointAck().Attempt, "removal_recovery_checkpoint"
	case message.GetServiceRemovalRecoveryCompletionAck() != nil:
		identity, kind = message.GetServiceRemovalRecoveryCompletionAck().Attempt, "removal_recovery_completion"
	case message.GetServiceRemovalRecoveryFailureAck() != nil:
		identity, kind = message.GetServiceRemovalRecoveryFailureAck().Attempt, "removal_recovery_failure"
	case message.GetServiceRemovalConfigurationManifest() != nil:
		manifest := message.GetServiceRemovalConfigurationManifest()
		identity, kind, executionID = manifest.Attempt, "removal_configuration", manifest.StageId
	case message.GetServiceRemovalConfigurationFileDelivery() != nil:
		delivery := message.GetServiceRemovalConfigurationFileDelivery()
		identity, kind, executionID = delivery.Attempt, "removal_configuration_file", delivery.StageId+"/"+delivery.GetFile().GetDestination()
	case message.GetServiceLifecycleCompletionAck() != nil:
		identity, kind = message.GetServiceLifecycleCompletionAck().Attempt, "lifecycle_completion"
		if stopped := message.GetServiceLifecycleCompletionAck().Stopped; stopped != nil {
			identity = stopped.Attempt
		}
	case message.GetServiceLifecycleFailureAck() != nil:
		identity, kind = message.GetServiceLifecycleFailureAck().Attempt, "lifecycle_failure"
	case message.GetServiceLifecycleHookDelivery() != nil:
		delivery := message.GetServiceLifecycleHookDelivery()
		identity, kind, executionID = delivery.Attempt, "lifecycle_hook", delivery.StageId
	case message.GetVolumeWorkCompletionAck() != nil:
		identity, kind = message.GetVolumeWorkCompletionAck().Attempt, "volume_completion"
	case message.GetVolumeWorkFailureAck() != nil:
		identity, kind = message.GetVolumeWorkFailureAck().Attempt, "volume_failure"
	case message.GetVolumeWorkRecoveryReportAck() != nil:
		identity, kind = message.GetVolumeWorkRecoveryReportAck().Attempt, "volume_recovery"
	case message.GetScriptWorkFailureAck() != nil:
		identity, kind = message.GetScriptWorkFailureAck().Attempt, "script_failure"
	case message.GetScriptRecoveryCompletionAck() != nil:
		identity, kind = message.GetScriptRecoveryCompletionAck().Attempt, "script_recovery_completion"
	case message.GetScriptWorkCompletionAck() != nil:
		identity, kind = message.GetScriptWorkCompletionAck().Attempt, "script_completion"
	case message.GetReleaseWorkFailureAck() != nil:
		identity, kind = message.GetReleaseWorkFailureAck().Attempt, "release_failure"
	case message.GetReleaseWorkCompletionAck() != nil:
		identity, kind = message.GetReleaseWorkCompletionAck().Attempt, "release_completion"
	case message.GetReleaseConfigurationManifest() != nil:
		identity, kind = message.GetReleaseConfigurationManifest().Attempt, "configuration_manifest"
		executionID = message.GetReleaseConfigurationManifest().StageId
	case message.GetReleaseConfigurationFileDelivery() != nil:
		delivery := message.GetReleaseConfigurationFileDelivery()
		identity, kind = delivery.Attempt, "configuration_file"
		executionID = delivery.StageId + "/" + delivery.GetFile().GetDestination()
	case message.GetScriptWorkCheckpointAck() != nil:
		identity, kind = message.GetScriptWorkCheckpointAck().Attempt, "script_checkpoint"
	case message.GetReleaseEffectEvidenceAck() != nil:
		identity, kind = message.GetReleaseEffectEvidenceAck().Attempt, "effect_evidence"
	case message.GetWorkStartAck() != nil:
		identity, kind = message.GetWorkStartAck().Attempt, "start"
	case message.GetReleaseRecoveryCheckpointAck() != nil:
		identity, kind = message.GetReleaseRecoveryCheckpointAck().Attempt, "release_recovery_checkpoint"
	case message.GetReleaseWorkCheckpointAck() != nil:
		identity, kind = message.GetReleaseWorkCheckpointAck().Attempt, "release_checkpoint"
	case message.GetEnvironmentRemovalCheckpointAck() != nil:
		identity, kind = message.GetEnvironmentRemovalCheckpointAck().Attempt, "environment_removal_checkpoint"
	case message.GetAttachRemovalCheckpointAck() != nil:
		identity, kind = message.GetAttachRemovalCheckpointAck().Attempt, "attach_removal_checkpoint"
	case message.GetAttachProvisionCheckpointAck() != nil:
		identity, kind = message.GetAttachProvisionCheckpointAck().Attempt, "attach_provision_checkpoint"
	case message.GetAttachProvisionCompletionAck() != nil:
		identity, kind = message.GetAttachProvisionCompletionAck().Attempt, "attach_provision_completion"
	case message.GetAttachProvisionFailureAck() != nil:
		identity, kind = message.GetAttachProvisionFailureAck().Attempt, "attach_provision_failure"
	case message.GetAttachProvisionFactsAck() != nil:
		identity, kind = message.GetAttachProvisionFactsAck().Attempt, "attach_provision_facts"
	case message.GetAttachProvisionRecoveryReportAck() != nil:
		identity, kind = message.GetAttachProvisionRecoveryReportAck().Attempt, "attach_provision_recovery"
	case message.GetAttachRemovalCompletionAck() != nil:
		identity, kind = message.GetAttachRemovalCompletionAck().Attempt, "attach_removal_completion"
	case message.GetAttachRemovalFailureAck() != nil:
		identity, kind = message.GetAttachRemovalFailureAck().Attempt, "attach_removal_failure"
	case message.GetAttachRecoveryCompletionAck() != nil:
		identity, kind = message.GetAttachRecoveryCompletionAck().Attempt, "attach_recovery_completion"
	case message.GetEnvironmentRemovalCompletionAck() != nil:
		identity, kind = message.GetEnvironmentRemovalCompletionAck().Attempt, "environment_removal_completion"
	case message.GetEnvironmentRemovalFailureAck() != nil:
		identity, kind = message.GetEnvironmentRemovalFailureAck().Attempt, "environment_removal_failure"
	case message.GetReleaseHookDelivery() != nil:
		identity, kind = message.GetReleaseHookDelivery().Attempt, "hook"
		executionID = message.GetReleaseHookDelivery().GetRun().GetScriptExecutionId()
	default:
		return false, nil
	}
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	fail := func() (bool, error) {
		backingprovision.ClearPrivateDelivery(message)
		entryexecution.ClearConfigurationDelivery(message.GetEntryConfigurationFileDelivery())
		componentexecution.ClearConfigurationDelivery(message.GetComponentConfigurationFileDelivery())
		releaseexecution.ClearHookDelivery(message.GetReleaseHookDelivery())
		releaseexecution.ClearConfigurationDelivery(message.GetReleaseConfigurationFileDelivery())
		removalexecution.ClearConfigurationDelivery(message.GetServiceRemovalConfigurationFileDelivery())
		return true, errs.New(errs.KindStateConflict, "native work response does not match a pending request")
	}
	if identity == nil || executionplan.RejectUnknown(message) != nil {
		return fail()
	}
	key := nativeExchangeKey{identity.AttemptId, kind}
	waiter := exchange.pending[key]
	if exchange.closed || waiter == nil || !proto.Equal(identity, waiter.identity) || executionID != waiter.executionID {
		return fail()
	}
	delete(exchange.pending, key)
	if waiter.abandoned {
		backingprovision.ClearPrivateDelivery(message)
		entryexecution.ClearConfigurationDelivery(message.GetEntryConfigurationFileDelivery())
		componentexecution.ClearConfigurationDelivery(message.GetComponentConfigurationFileDelivery())
		releaseexecution.ClearHookDelivery(message.GetReleaseHookDelivery())
		releaseexecution.ClearConfigurationDelivery(message.GetReleaseConfigurationFileDelivery())
		removalexecution.ClearConfigurationDelivery(message.GetServiceRemovalConfigurationFileDelivery())
		return true, nil
	}
	waiter.response <- message
	return true, nil
}

func (exchange *nativeExchange) Configuration(ctx context.Context, request *agentpb.ReleaseConfigurationRequest) (*agentpb.ReleaseConfigurationManifest, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ReleaseConfigurationRequest{ReleaseConfigurationRequest: request}}, request.GetAttempt(), "configuration_manifest", request.GetStageId())
	if err != nil {
		return nil, err
	}
	return response.GetReleaseConfigurationManifest(), nil
}

func (exchange *nativeExchange) ConfigurationFile(ctx context.Context, request *agentpb.ReleaseConfigurationFileRequest) (*agentpb.ReleaseConfigurationFileDelivery, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ReleaseConfigurationFileRequest{ReleaseConfigurationFileRequest: request}}, request.GetAttempt(), "configuration_file", request.GetStageId()+"/"+request.GetDestination())
	if err != nil {
		return nil, err
	}
	return response.GetReleaseConfigurationFileDelivery(), nil
}

func (exchange *nativeExchange) close() {
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	if exchange.closed {
		return
	}
	exchange.closed = true
	exchange.backupConfigs.Release("")
	exchange.backupSecrets.ReleaseAll()
	for id, slot := range exchange.backupVolumes {
		close(slot.done)
		delete(exchange.backupVolumes, id)
	}
	close(exchange.done)
	for key, waiter := range exchange.pending {
		waiter.abandoned = true
		delete(exchange.pending, key)
	}
	// Workers are joined before session teardown. No producer remains that can
	// enqueue plaintext after this final drain.
	for {
		select {
		case message := <-exchange.outgoing:
			backupconfigtransfer.ClearFrame(message.GetBackupConfigTransfer())
		default:
			return
		}
	}
}

func (exchange *nativeExchange) Hook(ctx context.Context, request *agentpb.ReleaseHookRequest) (*agentpb.ReleaseHookDelivery, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ReleaseHookRequest{ReleaseHookRequest: request}}, request.GetAttempt(), "hook", request.GetExecutionId())
	if err != nil {
		return nil, err
	}
	return response.GetReleaseHookDelivery(), nil
}

func (exchange *nativeExchange) ReleaseCheckpoint(ctx context.Context, request *agentpb.ReleaseWorkCheckpointRequest) (*agentpb.ReleaseWorkCheckpointAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ReleaseWorkCheckpointRequest{ReleaseWorkCheckpointRequest: request}}, request.GetAttempt(), "release_checkpoint", "")
	if err != nil {
		return nil, err
	}
	return response.GetReleaseWorkCheckpointAck(), nil
}

func (exchange *nativeExchange) FailAttachRemoval(ctx context.Context, request *agentpb.AttachRemovalFailureRequest) (*agentpb.AttachRemovalFailureAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_AttachRemovalFailureRequest{AttachRemovalFailureRequest: request}}, request.GetAttempt(), "attach_removal_failure", "")
	if err != nil {
		return nil, err
	}
	return response.GetAttachRemovalFailureAck(), nil
}

func (exchange *nativeExchange) CompleteAttachRemoval(ctx context.Context, request *agentpb.AttachRemovalCompletionRequest) (*agentpb.AttachRemovalCompletionAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_AttachRemovalCompletionRequest{AttachRemovalCompletionRequest: request}}, request.GetAttempt(), "attach_removal_completion", "")
	if err != nil {
		return nil, err
	}
	return response.GetAttachRemovalCompletionAck(), nil
}

func (exchange *nativeExchange) AttachRemovalCheckpoint(ctx context.Context, request *agentpb.AttachRemovalCheckpointRequest) (*agentpb.AttachRemovalCheckpointAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_AttachRemovalCheckpointRequest{AttachRemovalCheckpointRequest: request}}, request.GetAttempt(), "attach_removal_checkpoint", "")
	if err != nil {
		return nil, err
	}
	return response.GetAttachRemovalCheckpointAck(), nil
}

func (exchange *nativeExchange) EnvironmentRemovalCheckpoint(ctx context.Context, request *agentpb.EnvironmentRemovalCheckpointRequest) (*agentpb.EnvironmentRemovalCheckpointAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_EnvironmentRemovalCheckpointRequest{EnvironmentRemovalCheckpointRequest: request}}, request.GetAttempt(), "environment_removal_checkpoint", "")
	if err != nil {
		return nil, err
	}
	return response.GetEnvironmentRemovalCheckpointAck(), nil
}

func (exchange *nativeExchange) CompleteEnvironmentRemoval(ctx context.Context, request *agentpb.EnvironmentRemovalCompletionRequest) (*agentpb.EnvironmentRemovalCompletionAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_EnvironmentRemovalCompletionRequest{EnvironmentRemovalCompletionRequest: request}}, request.GetAttempt(), "environment_removal_completion", "")
	if err != nil {
		return nil, err
	}
	return response.GetEnvironmentRemovalCompletionAck(), nil
}

func (exchange *nativeExchange) FailEnvironmentRemoval(ctx context.Context, request *agentpb.EnvironmentRemovalFailureRequest) (*agentpb.EnvironmentRemovalFailureAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_EnvironmentRemovalFailureRequest{EnvironmentRemovalFailureRequest: request}}, request.GetAttempt(), "environment_removal_failure", "")
	if err != nil {
		return nil, err
	}
	return response.GetEnvironmentRemovalFailureAck(), nil
}

func (exchange *nativeExchange) StartAttempt(ctx context.Context, attempt workexecution.Attempt) (*agentpb.WorkStartAck, error) {
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, err
	}
	return exchange.Start(ctx, &agentpb.WorkStartRequest{Attempt: identity})
}

func (exchange *nativeExchange) Start(ctx context.Context, request *agentpb.WorkStartRequest) (*agentpb.WorkStartAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_WorkStartRequest{WorkStartRequest: request}}, request.GetAttempt(), "start", "")
	if err != nil {
		return nil, err
	}
	return response.GetWorkStartAck(), nil
}

func (exchange *nativeExchange) Script(attempt workexecution.Attempt) (checkpointdelivery.ScriptExchange, error) {
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, checkpoint *agentpb.ScriptCheckpointRequest) (*agentpb.ScriptCheckpointAck, error) {
		if checkpoint == nil || checkpoint.TaskId != attempt.TaskID || checkpoint.WorkItemId != attempt.WorkItemID || checkpoint.AttemptId != attempt.ID {
			return nil, errs.New(errs.KindStateConflict, "Script checkpoint does not belong to the assigned native attempt")
		}
		validated, err := executionplan.ValidateScriptCheckpointRequest(checkpoint)
		if err != nil {
			return nil, err
		}
		request := &agentpb.ScriptWorkCheckpointRequest{Attempt: proto.CloneOf(identity), Checkpoint: validated}
		response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ScriptWorkCheckpointRequest{ScriptWorkCheckpointRequest: request}}, identity, "script_checkpoint", "")
		if err != nil {
			return nil, err
		}
		return executionplan.ValidateScriptCheckpointAck(response.GetScriptWorkCheckpointAck().GetCheckpoint(), validated)
	}, nil
}

func (exchange *nativeExchange) EffectEvidence(ctx context.Context, identity *agentpb.WorkAttemptIdentity, raw []byte, reference workexecution.Input) (workexecution.Input, error) {
	if reference.Validate() != nil || len(raw) == 0 || len(raw) > releaseexecution.MaximumCheckpointBytes {
		return workexecution.Input{}, errs.New(errs.KindValidationFailed, "Release effect evidence is invalid")
	}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil {
		return workexecution.Input{}, err
	}
	request := &agentpb.ReleaseEffectEvidenceRequest{Attempt: proto.CloneOf(identity), Record: append([]byte(nil), raw...), Evidence: &agentpb.WorkInputReference{InputId: reference.ID, Sha256: digest}}
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ReleaseEffectEvidenceRequest{ReleaseEffectEvidenceRequest: request}}, identity, "effect_evidence", "")
	if err != nil {
		return workexecution.Input{}, err
	}
	ack := response.GetReleaseEffectEvidenceAck()
	if ack == nil || !proto.Equal(ack.Evidence, request.Evidence) {
		return workexecution.Input{}, errs.New(errs.KindStateConflict, "Release effect evidence receipt differs from submitted proof")
	}
	return reference, nil
}
