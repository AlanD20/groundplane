package composehelper

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backupservicefact"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// A Backup Volume consumer request keeps the original sealed Backup plan in
// the existing helper protocol. The Compose step is derived locally only
// after that plan, artifact, Volume projection, and Service fact agree.
func validateBackupVolumeConsumerRequest(request *agentpb.ComposeHelperRequest,
	plan *agentpb.ExecutionPlan, selected *agentpb.ExecutionStep,
) (*agentpb.ComposeHelperRequest, *agentpb.ExecutionStep, *agentpb.ComposeArtifact, error) {
	action := request.GetBackupVolumeConsumer()
	backup := selected.GetBackupStep()
	if request.GetRestorationAuthority() != nil || action == nil || backup == nil ||
		(plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_BACKUP &&
			plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_RESTORE) ||
		!slices.Contains(backup.ConsumerServiceIds, action.ServiceId) ||
		(action.Operation != agentpb.BackupVolumeConsumerOperation_BACKUP_VOLUME_CONSUMER_OPERATION_STOP &&
			action.Operation != agentpb.BackupVolumeConsumerOperation_BACKUP_VOLUME_CONSUMER_OPERATION_RECOVER) {
		return nil, nil, nil, invalidBackupVolumeConsumer()
	}
	projection := backup.GetCapture().GetVolume().GetProjection()
	if backup.GetRestore().GetVolume() != nil {
		projection = backup.GetRestore().GetVolume().GetProjection()
	}
	if projection == nil {
		return nil, nil, nil, invalidBackupVolumeConsumer()
	}
	var artifact *agentpb.ComposeArtifact
	for _, candidate := range plan.Artifacts {
		if candidate.ArtifactId == projection.ArtifactId {
			artifact = candidate
			break
		}
	}
	var fact *agentpb.BackupServiceFact
	for _, candidate := range plan.BackupScope.GetServices() {
		if candidate.ServiceId == action.ServiceId {
			fact = candidate
			break
		}
	}
	if artifact == nil || fact == nil || fact.PriorRuntimeIntent.GetKind() !=
		agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING {
		return nil, nil, nil, invalidBackupVolumeConsumer()
	}
	var matched *agentpb.ComposeService
	for _, service := range artifact.Services {
		if service.ServiceId == action.ServiceId && service.ComposeName == fact.CurrentName {
			if matched != nil {
				return nil, nil, nil, invalidBackupVolumeConsumer()
			}
			matched = service
		}
	}
	if matched == nil || uint32(len(matched.ExpectedLabels)) != fact.RequiredLabelCount {
		return nil, nil, nil, invalidBackupVolumeConsumer()
	}
	labelsSHA, err := backupservicefact.LabelsDigest(matched.ExpectedLabels)
	if err != nil || !bytes.Equal(labelsSHA, fact.RequiredLabelsSha256) ||
		!strings.HasPrefix(matched.ImageReference, "sha256:") {
		return nil, nil, nil, invalidBackupVolumeConsumer()
	}
	image, err := hex.DecodeString(strings.TrimPrefix(matched.ImageReference, "sha256:"))
	if err != nil || len(image) != sha256.Size || !bytes.Equal(image, fact.LocalImageIdSha256) {
		return nil, nil, nil, invalidBackupVolumeConsumer()
	}
	derived := &agentpb.ExecutionStep{StepId: selected.StepId, TimeoutSeconds: selected.TimeoutSeconds}
	if action.Operation == agentpb.BackupVolumeConsumerOperation_BACKUP_VOLUME_CONSUMER_OPERATION_STOP {
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

func invalidBackupVolumeConsumer() error {
	return errs.New(errs.KindValidationFailed, "Backup Volume consumer action differs from sealed plan")
}
