package executionplan

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupservicefact"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func BackupConsumerObservation(input BackupConsumerInput, observed *agentpb.ObservedProject) ([]byte, error) {
	if input.Artifact == nil || input.Service == nil || input.Fact == nil || observed == nil || RejectUnknown(observed) != nil || proto.Size(observed) > MaximumPlanBytes ||
		observed.ProjectName != input.Artifact.ProjectName || observed.ObservedAt == nil || observed.ObservedAt.CheckValid() != nil || len(observed.Collisions) != 0 {
		return nil, invalidBackupConsumerHelper()
	}
	seen := make(map[string]bool)
	for _, container := range observed.Containers {
		if container == nil || container.ContainerId == "" || seen[container.ContainerId] || container.ServiceId != input.Fact.ServiceId ||
			container.State < agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_CREATED || container.State > agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_DEAD ||
			container.ImageId != "sha256:"+hex.EncodeToString(input.Fact.LocalImageIdSha256) || uint32(len(container.Labels)) != input.Fact.RequiredLabelCount {
			return nil, invalidBackupConsumerHelper()
		}
		labels, err := backupservicefact.LabelsDigest(container.Labels)
		if err != nil || !bytes.Equal(labels, input.Fact.RequiredLabelsSha256) {
			return nil, invalidBackupConsumerHelper()
		}
		seen[container.ContainerId] = true
	}
	count := uint32(len(observed.Containers))
	if count != 0 && count != input.Service.ExpectedReplicas || input.Fact.PriorRuntimeIntent.Kind == agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_ABSENT && count != 0 {
		return nil, invalidBackupConsumerHelper()
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(observed)
	if err != nil {
		return nil, invalidBackupConsumerHelper()
	}
	digest := sha256.Sum256(raw)
	return digest[:], nil
}

func ValidateBackupConsumerHelperResponse(input BackupConsumerHelperInput, response *agentpb.BackupConsumerHelperResponse) error {
	if response == nil || RejectUnknown(response) != nil || proto.Size(response) > MaximumPlanBytes || response.SourceOrdinal != input.SourceOrdinal ||
		response.ServiceId != input.Consumer.Fact.GetServiceId() || response.Inspection != input.Inspection || response.ObservedAt == nil || response.ObservedAt.CheckValid() != nil {
		return invalidBackupConsumerHelper()
	}
	attempt, err := workexecution.DecodeAgentAttempt(response.Attempt, input.Consumer.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, input.Consumer.Attempt) {
		return invalidBackupConsumerHelper()
	}
	digest, err := BackupConsumerObservation(input.Consumer, response.Observed)
	if err != nil || !bytes.Equal(digest, response.ObservationSha256) || !proto.Equal(response.ObservedAt, response.Observed.ObservedAt) {
		return invalidBackupConsumerHelper()
	}
	if input.Inspection {
		return nil
	}
	if input.Operation == agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_RECOVER && len(response.Observed.Containers) == 0 {
		return invalidBackupConsumerHelper()
	}
	for _, container := range response.Observed.Containers {
		running := container.State == agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING
		if input.Operation == agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_STOP && running ||
			input.Operation == agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_RECOVER && !running {
			return invalidBackupConsumerHelper()
		}
	}
	return nil
}
