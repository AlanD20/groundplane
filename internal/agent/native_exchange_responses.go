package agent

import (
	"fmt"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (exchange *nativeExchange) accept(message *agentpb.ControllerMessage) (bool, error) {
	if message.GetWorkReportRejection() != nil {
		return true, exchange.rejectReport(message)
	}
	if handled, err := exchange.acceptBackupVolume(message); handled {
		return true, err
	}
	var identity *agentpb.WorkAttemptIdentity
	kind, executionID := "", ""
	switch {
	case message.GetResolverWorkCheckpointAck() != nil:
		identity, kind = message.GetResolverWorkCheckpointAck().Attempt, "resolver_checkpoint"
	case message.GetResolverWorkResultAck() != nil:
		ack := message.GetResolverWorkResultAck()
		switch ack.Kind {
		case "completion", "failure", "recovery":
			identity, kind = ack.Attempt, "resolver_"+ack.Kind
		default:
			return true, errs.New(errs.KindStateConflict, "DNS resolver acknowledgement has an unknown result kind")
		}
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
		identity, kind, executionID = delivery.Attempt, "backing_file", delivery.StageId+"/"+delivery.GetFile().
			GetDestination()
	case message.GetBackingProvisionHookDelivery() != nil:
		delivery := message.GetBackingProvisionHookDelivery()
		identity, kind, executionID = delivery.Attempt, "backing_hook", delivery.StageId
	case message.GetEnvironmentProvisionResultAck() != nil:
		identity, kind = message.GetEnvironmentProvisionResultAck().Attempt, "environment_provision_result"
	case message.GetBackupConfigRecoveryPrepareAck() != nil:
		ack := message.GetBackupConfigRecoveryPrepareAck()
		identity, kind, executionID = ack.GetAssignment().
			GetAttempt(),
			"backup_config_recovery_prepare", fmt.Sprint(
				ack.SourceOrdinal,
			)
	case message.GetBackupWorkCompletionAck() != nil:
		identity, kind = message.GetBackupWorkCompletionAck().Attempt, "backup_completion"
	case message.GetBackupWorkFailureAck() != nil:
		identity, kind = message.GetBackupWorkFailureAck().Attempt, "backup_failure"
	case message.GetBackupRecoveryAck() != nil:
		identity, kind = message.GetBackupRecoveryAck().Attempt, "backup_prune_recovery"
	case message.GetBackupWorkCheckpointAck() != nil:
		ack := message.GetBackupWorkCheckpointAck()
		identity, kind, executionID = ack.Attempt, "backup_checkpoint", fmt.Sprintf(
			"%d/%d",
			ack.SourceOrdinal,
			ack.CheckpointSequence,
		)
	case message.GetEntryConfigurationManifest() != nil:
		manifest := message.GetEntryConfigurationManifest()
		identity, kind, executionID = manifest.Attempt, "entry_configuration", manifest.StageId
	case message.GetEntryConfigurationFileDelivery() != nil:
		delivery := message.GetEntryConfigurationFileDelivery()
		identity, kind, executionID = delivery.Attempt, "entry_configuration_file", delivery.StageId+"/"+delivery.GetFile().
			GetDestination()
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
		identity, kind, executionID = delivery.Attempt, "component_configuration_file", delivery.StageId+"/"+delivery.GetFile().
			GetDestination()
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
		identity, kind, executionID = delivery.Attempt, "removal_configuration_file", delivery.StageId+"/"+delivery.GetFile().
			GetDestination()
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
	if exchange.closed || waiter == nil || !proto.Equal(identity, waiter.identity) ||
		executionID != waiter.executionID {
		return fail()
	}
	if waiter.terminalReport != nil {
		if exchange.journal == nil {
			return fail()
		}
		if err := exchange.journal.AcknowledgeTerminalDelivery(exchange.ctx, waiter.terminalReport, message); err != nil {
			return true, err
		}
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
	waiter.response <- nativeResponse{message: message}
	return true, nil
}
