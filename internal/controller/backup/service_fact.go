package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backupservicefact"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingpostgresrelease"
	backupplanning "github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicefactauthority"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
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
		var kind servicefactauthority.Kind
		adapter := ""
		for _, desired := range projection.Record.DesiredServices {
			if desired.Desired.ID == input.ServiceID && desired.EnvironmentID == input.EnvironmentID {
				kind, err = servicefactauthority.KindForService(desired)
				if err != nil {
					return nil, err
				}
				adapter = desired.Desired.Adapter
				break
			}
		}
		if kind == 0 {
			return nil, errs.New(errs.KindStateConflict, "backup Service is absent from the selected Environment")
		}

		keys := []string{
			blueprints.EnvironmentBlueprintHeadKey(input.EnvironmentID),
			services.ServiceRuntimeKey(input.ServiceID),
			servicefactauthority.Key(kind, input.EnvironmentID, input.ServiceID),
		}
		if kind == servicefactauthority.BackingRuntime && adapter == "postgres" {
			keys = append(keys, backingpostgresrelease.Key(input.EnvironmentID, input.ServiceID))
		}
		read, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: input.ReadRevision})
		if err != nil {
			return nil, err
		}
		if read == nil || read.ReadRevision != input.ReadRevision || len(read.Values) != len(keys) {
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

		selected, err := servicefactauthority.ReadApplied(applied, kind, input.EnvironmentID, input.ServiceID)
		if err != nil {
			return nil, err
		}
		if kind == servicefactauthority.BackingRuntime && adapter == "postgres" {
			if err := validateBackupBackingCatalog(read.Values[3], selected, input); err != nil {
				return nil, err
			}
		} else if kind == servicefactauthority.BackingRuntime &&
			(adapter != "mysql" || projection.Record.BackingRuntime == nil ||
				projection.Record.BackingRuntime.Adapter != "mysql" || selected.Workload.PostgresToolsImage != "") {
			return nil, errs.New(errs.KindStateConflict, "MySQL backup Service lacks acknowledged applied runtime evidence")
		}
		artifact, workload := selected.Artifact, selected.Workload
		localImageID, err := hex.DecodeString(strings.TrimPrefix(selected.LocalImageID, "sha256:"))
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
