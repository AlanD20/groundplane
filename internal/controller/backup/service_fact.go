package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backupservicefact"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	backupplanning "github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// NewBackupServiceFactResolver pins desired, intent, and acknowledged applied
// Compose state to the caller's single MVCC view. In particular, the effective
// Blueprint artifact is not a substitute for an acknowledged running workload.
func NewBackupServiceFactResolver(store etcdstore.Store) backupplanning.BackupServiceFactResolver {
	return func(ctx context.Context, input backupplanning.BackupServiceFactInput) (*backupplanning.BackupServiceFactEvidence, error) {
		if store == nil || ctx == nil ||
			(ids.Validate(ids.KindService, input.ServiceID) != nil && ids.Validate(ids.KindBackingService, input.ServiceID) != nil) ||
			ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil || input.ReadRevision <= 0 {
			return nil, errs.New(errs.KindValidationFailed, "backup Service fact request is invalid")
		}
		projection, found, err := blueprints.ReadCurrentProjection(ctx, store, input.EnvironmentID, input.ReadRevision)
		if err != nil {
			return nil, err
		}
		if !found || projection.ReadRevision != input.ReadRevision {
			return nil, errs.New(errs.KindStateConflict, "backup Service desired projection is unavailable")
		}
		member := false
		for _, desired := range projection.Record.DesiredServices {
			if desired.Desired.ID == input.ServiceID && desired.EnvironmentID == input.EnvironmentID {
				member = true
				break
			}
		}
		if !member {
			return nil, errs.New(errs.KindStateConflict, "backup Service is absent from the selected Environment")
		}

		read, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
			blueprints.EnvironmentBlueprintHeadKey(input.EnvironmentID),
			services.ServiceRuntimeKey(input.ServiceID),
			serviceruntimerecord.Key(input.ServiceID),
		}, Revision: input.ReadRevision})
		if err != nil {
			return nil, err
		}
		if read == nil || read.ReadRevision != input.ReadRevision || len(read.Values) != 3 {
			return nil, errs.New(errs.KindInternal, "backup Service fact fixed-revision read is incomplete")
		}
		defer etcdstore.ClearValues(read.Values)
		desired, intent, applied := read.Values[0], read.Values[1], read.Values[2]
		if desired == nil || desired.ModRevision != projection.Revision || intent == nil || applied == nil {
			return nil, errs.New(errs.KindStateConflict, "backup Service applied or intent evidence is unavailable")
		}
		runtime, err := services.DecodeServiceRuntimeRecord(intent.Value)
		if err != nil || runtime.EnvironmentID != input.EnvironmentID || runtime.ServiceID != input.ServiceID {
			return nil, errs.New(errs.KindStateConflict, "backup Service runtime intent is invalid")
		}
		prior := &agentpb.BackupPriorRuntimeIntent{}
		switch runtime.Runtime.RuntimeIntent {
		case "running":
			prior.Kind = agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING
			prior.Intent = backupServiceRevisionDigest(intent)
		case "stopped":
			prior.Kind = agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_STOPPED
			prior.Intent = backupServiceRevisionDigest(intent)
		case "absent":
			prior.Kind = agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_ABSENT
		default:
			return nil, errs.New(errs.KindStateConflict, "backup Service runtime intent is invalid")
		}

		acknowledged, err := releases.DecodeReleaseRecord[serviceruntimerecord.Record](
			applied.Value, "service-acknowledged-runtime",
		)
		if err != nil || acknowledged.EnvironmentID != input.EnvironmentID ||
			acknowledged.Runtime.ServiceID != input.ServiceID || serviceruntimerecord.Validate(acknowledged) != nil {
			return nil, errs.New(errs.KindStateConflict, "backup Service applied Compose runtime is invalid")
		}
		artifact := &agentpb.ComposeArtifact{}
		if proto.Unmarshal(acknowledged.Runtime.CurrentArtifact, artifact) != nil {
			return nil, errs.New(errs.KindStateConflict, "backup Service applied Compose artifact is invalid")
		}
		var workload *agentpb.ComposeService
		for _, candidate := range artifact.GetServices() {
			if candidate.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON ||
				candidate.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
				if workload != nil || candidate.GetServiceId() != input.ServiceID {
					return nil, errs.New(errs.KindStateConflict, "backup Service applied Compose workload is ambiguous")
				}
				workload = candidate
			}
		}
		if workload == nil || workload.GetComposeName() == "" ||
			!workloadimage.LocalIDValid(workload.GetImageReference()) ||
			len(workload.GetExpectedLabels()) == 0 {
			return nil, errs.New(errs.KindStateConflict, "backup Service applied Compose workload is incomplete")
		}
		localImageID, err := hex.DecodeString(strings.TrimPrefix(workload.GetImageReference(), "sha256:"))
		if err != nil || len(localImageID) != sha256.Size {
			return nil, errs.New(errs.KindStateConflict, "backup Service applied local image ID is invalid")
		}
		labelDigest, err := backupservicefact.LabelsDigest(workload.GetExpectedLabels())
		if err != nil {
			return nil, err
		}
		return &backupplanning.BackupServiceFactEvidence{Artifact: artifact, Fact: &agentpb.BackupServiceFact{
			ServiceId: input.ServiceID, CurrentName: workload.GetComposeName(),
			Service: backupServiceRevisionDigest(desired), Compose: backupServiceRevisionDigest(applied),
			PriorRuntimeIntent: prior, RequiredLabelCount: uint32(len(workload.GetExpectedLabels())),
			RequiredLabelsSha256: labelDigest, LocalImageIdSha256: localImageID,
		}}, nil
	}
}

func backupServiceRevisionDigest(value *etcdstore.KeyValue) *agentpb.RevisionDigest {
	digest := sha256.Sum256(value.Value)
	return &agentpb.RevisionDigest{ModRevision: value.ModRevision, Sha256: digest[:]}
}
