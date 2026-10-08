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

// A database consumer action is only a derived, single-Service Compose
// transition inside the sealed Restore plan. The caller cannot supply an
// artifact, Compose name, image, command, or arbitrary service set.
func validateBackupDatabaseConsumerRequest(request *agentpb.ComposeHelperRequest,
	plan *agentpb.ExecutionPlan, selected *agentpb.ExecutionStep,
) (*agentpb.ComposeHelperRequest, *agentpb.ExecutionStep, *agentpb.ComposeArtifact, error) {
	backup := selected.GetBackupStep()
	postgres, mysql := request.GetBackupPostgresConsumer(), request.GetBackupMysqlConsumer()
	var serviceID string
	var stop bool
	if postgres != nil && mysql == nil && backup.GetRestore().GetPostgres() != nil {
		serviceID = postgres.ServiceId
		stop = postgres.Operation == agentpb.BackupPostgresConsumerOperation_BACKUP_POSTGRES_CONSUMER_OPERATION_STOP
		if !stop &&
			postgres.Operation != agentpb.BackupPostgresConsumerOperation_BACKUP_POSTGRES_CONSUMER_OPERATION_RECOVER {
			return nil, nil, nil, invalidBackupDatabaseConsumer()
		}
	} else if mysql != nil && postgres == nil && backup.GetRestore().GetMysql() != nil {
		serviceID = mysql.ServiceId
		stop = mysql.Operation == agentpb.BackupMySQLConsumerOperation_BACKUP_MYSQL_CONSUMER_OPERATION_STOP
		if !stop && mysql.Operation != agentpb.BackupMySQLConsumerOperation_BACKUP_MYSQL_CONSUMER_OPERATION_RECOVER {
			return nil, nil, nil, invalidBackupDatabaseConsumer()
		}
	} else {
		return nil, nil, nil, invalidBackupDatabaseConsumer()
	}
	if request.GetRestorationAuthority() != nil || request.GetBackupVolumeConsumer() != nil ||
		plan.BackupScope == nil || plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_RESTORE ||
		!slices.Contains(backup.ConsumerServiceIds, serviceID) {
		return nil, nil, nil, invalidBackupDatabaseConsumer()
	}
	var fact *agentpb.BackupServiceFact
	for _, candidate := range plan.BackupScope.GetServices() {
		if candidate.ServiceId == serviceID {
			if fact != nil {
				return nil, nil, nil, invalidBackupDatabaseConsumer()
			}
			fact = candidate
		}
	}
	if fact == nil || fact.PriorRuntimeIntent.GetKind() !=
		agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING {
		return nil, nil, nil, invalidBackupDatabaseConsumer()
	}
	var artifact *agentpb.ComposeArtifact
	var service *agentpb.ComposeService
	for _, candidate := range plan.Artifacts {
		for _, member := range candidate.Services {
			if member.ServiceId == serviceID && member.ComposeName == fact.CurrentName {
				if service != nil {
					return nil, nil, nil, invalidBackupDatabaseConsumer()
				}
				artifact, service = candidate, member
			}
		}
	}
	if artifact == nil || artifact.OwnerId != plan.BackupScope.EnvironmentId ||
		uint32(len(service.ExpectedLabels)) != fact.RequiredLabelCount ||
		service.ImageReference == "" {
		return nil, nil, nil, invalidBackupDatabaseConsumer()
	}
	labelsSHA, err := backupservicefact.LabelsDigest(service.ExpectedLabels)
	if err != nil || len(fact.LocalImageIdSha256) != sha256.Size ||
		!bytes.Equal(labelsSHA, fact.RequiredLabelsSha256) ||
		len(service.ImageConfigDigest) != 0 &&
			!bytes.Equal(service.ImageConfigDigest, fact.LocalImageIdSha256) {
		return nil, nil, nil, invalidBackupDatabaseConsumer()
	}
	derived := &agentpb.ExecutionStep{StepId: selected.StepId, TimeoutSeconds: selected.TimeoutSeconds}
	if stop {
		derived.Payload = &agentpb.ExecutionStep_ComposeStop{ComposeStop: &agentpb.ComposeStop{
			ArtifactId: artifact.ArtifactId, ServiceIds: []string{serviceID}, GraceSeconds: 30}}
	} else {
		derived.Payload = &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId: artifact.ArtifactId, ServiceIds: []string{serviceID}, NoDependencies: true}}
	}
	owned := proto.CloneOf(request)
	owned.Plan = plan
	return owned, derived, artifact, nil
}

func invalidBackupDatabaseConsumer() error {
	return errs.New(errs.KindValidationFailed, "database consumer action differs from sealed Restore plan")
}
