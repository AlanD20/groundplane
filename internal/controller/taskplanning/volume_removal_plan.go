package taskplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type volumeRemovalPlanReader interface {
	Manifest(context.Context, string) (removal.EvidenceManifest, bool, error)
}

// Removal plan inputs come from the immutable accepted manifest, not additional
// mutable Task parameters. Retry keeps the origin artifact and plan identities.
func (resolver *TaskPlanResolver) volumeRemovalPlanParams(
	ctx context.Context,
	task etcd.TaskRecord,
) (map[string]string, error) {
	if resolver == nil || resolver.volumeRemovalPlans == nil || task.TimeoutSeconds != removal.TimeoutSeconds {
		return nil, errs.New(errs.KindInternal, "Volume removal plan evidence is not configured")
	}
	manifest, found, err := resolver.volumeRemovalPlans.Manifest(ctx, task.OperationID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errs.New(errs.KindStateConflict, "Volume removal manifest is absent")
	}
	value, err := removal.EncodeEvidenceManifest(manifest)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(value)
	clear(value)
	origin := task.Params[removal.OriginTaskParam]
	ordinal, err := strconv.ParseUint(task.Params[removal.AttemptParam], 10, 32)
	if err != nil || ordinal == 0 || ids.Validate(ids.KindTask, origin) != nil ||
		manifest.OperationID != task.OperationID || manifest.VolumeID != task.Target ||
		manifest.DesiredRevisionID != task.Params[etcd.EnvironmentDesiredRevisionParam] ||
		(ordinal == 1 && (task.ID != origin || task.RetryOf != "")) ||
		(ordinal > 1 && (task.ID == origin || ids.Validate(ids.KindTask, task.RetryOf) != nil)) {
		return nil, errs.New(errs.KindStateConflict, "Volume removal plan identity changed")
	}
	intent, err := decodeVolumeTaskDigest(task.Params[removal.IntentParam])
	if err != nil {
		return nil, err
	}
	expected := etcd.EnvironmentVolumeRemovalTaskParams(removal.Runtime{
		EnvironmentID: manifest.EnvironmentID, DesiredRevisionID: manifest.DesiredRevisionID, OriginTaskID: origin,
		Key: manifest.Key, ImpactSHA256: manifest.ImpactSHA256, EvidenceManifestSHA256: digest, IntentSHA256: [sha256.Size]byte(intent),
	}, uint32(ordinal))
	if len(task.Params) != len(expected) {
		return nil, errs.New(errs.KindStateConflict, "Volume removal plan parameters changed")
	}
	for key, value := range expected {
		if task.Params[key] != value {
			return nil, errs.New(errs.KindStateConflict, "Volume removal plan parameters changed")
		}
	}
	return map[string]string{
		etcd.TaskResourceKindParam:                      etcd.TaskResourceVolume,
		taskjournal.TaskMaterializationEnvironmentParam: manifest.EnvironmentID,
		etcd.EnvironmentDesiredRevisionParam:            manifest.DesiredRevisionID,
		etcd.TaskComposeArtifactParam:                   "cfg_" + strings.TrimPrefix(origin, "task_"),
		VolumeTaskActionParam:                           VolumeTaskActionRemove, VolumeTaskComposeKeyParam: manifest.Key,
		VolumeTaskIntentSHA256Param: hex.EncodeToString(
			intent,
		), VolumeTaskBaselineRevisionParam: manifest.SourceRevisionID,
	}, nil
}
