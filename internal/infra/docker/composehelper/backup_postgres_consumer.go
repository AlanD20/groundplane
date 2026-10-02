package composehelper

import (
	"bytes"
	"crypto/sha256"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/backupservicefact"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// A PostgreSQL consumer action is only a derived, single-Service Compose
// transition inside the sealed Restore plan. The caller cannot supply an
// artifact, Compose name, image, command, or arbitrary service set.
func validateBackupPostgresConsumerRequest(request *agentpb.ComposeHelperRequest,
	plan *agentpb.ExecutionPlan, selected *agentpb.ExecutionStep,
) (*agentpb.ComposeHelperRequest, *agentpb.ExecutionStep, *agentpb.ComposeArtifact, error) {
	action := request.GetBackupPostgresConsumer()
	backup := selected.GetBackupStep()
	if request.GetRestorationAuthority() != nil || request.GetBackupVolumeConsumer() != nil ||
		action == nil || plan.BackupScope == nil || backup.GetRestore().GetPostgres() == nil ||
		plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_RESTORE ||
		!slices.Contains(backup.ConsumerServiceIds, action.ServiceId) ||
		(action.Operation != agentpb.BackupPostgresConsumerOperation_BACKUP_POSTGRES_CONSUMER_OPERATION_STOP &&
			action.Operation != agentpb.BackupPostgresConsumerOperation_BACKUP_POSTGRES_CONSUMER_OPERATION_RECOVER) {
		return nil, nil, nil, invalidBackupPostgresConsumer()
	}
	var fact *agentpb.BackupServiceFact
	for _, candidate := range plan.BackupScope.GetServices() {
		if candidate.ServiceId == action.ServiceId {
			if fact != nil {
				return nil, nil, nil, invalidBackupPostgresConsumer()
			}
			fact = candidate
		}
	}
	if fact == nil || fact.PriorRuntimeIntent.GetKind() !=
		agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING {
		return nil, nil, nil, invalidBackupPostgresConsumer()
	}
	var artifact *agentpb.ComposeArtifact
	var service *agentpb.ComposeService
	for _, candidate := range plan.Artifacts {
		for _, member := range candidate.Services {
			if member.ServiceId == action.ServiceId && member.ComposeName == fact.CurrentName {
				if service != nil {
					return nil, nil, nil, invalidBackupPostgresConsumer()
				}
				artifact, service = candidate, member
			}
		}
	}
	if artifact == nil || artifact.OwnerId != plan.BackupScope.EnvironmentId ||
		uint32(len(service.ExpectedLabels)) != fact.RequiredLabelCount ||
		service.ImageReference == "" {
		return nil, nil, nil, invalidBackupPostgresConsumer()
	}
	labelsSHA, err := backupservicefact.LabelsDigest(service.ExpectedLabels)
	if err != nil || len(fact.LocalImageIdSha256) != sha256.Size ||
		!bytes.Equal(labelsSHA, fact.RequiredLabelsSha256) ||
		len(service.ImageConfigDigest) != 0 &&
			!bytes.Equal(service.ImageConfigDigest, fact.LocalImageIdSha256) {
		return nil, nil, nil, invalidBackupPostgresConsumer()
	}
	derived := &agentpb.ExecutionStep{StepId: selected.StepId, TimeoutSeconds: selected.TimeoutSeconds}
	if action.Operation == agentpb.BackupPostgresConsumerOperation_BACKUP_POSTGRES_CONSUMER_OPERATION_STOP {
		derived.Payload = &agentpb.ExecutionStep_ComposeStop{ComposeStop: &agentpb.ComposeStop{
			ArtifactId: artifact.ArtifactId, ServiceIds: []string{action.ServiceId}, GraceSeconds: 30}}
	} else {
		derived.Payload = &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId: artifact.ArtifactId, ServiceIds: []string{action.ServiceId}, NoDependencies: true}}
	}
	owned := proto.CloneOf(request)
	owned.Plan = plan
	return owned, derived, artifact, nil
}

func invalidBackupPostgresConsumer() error {
	return errs.New(errs.KindValidationFailed, "PostgreSQL consumer action differs from sealed Restore plan")
}
