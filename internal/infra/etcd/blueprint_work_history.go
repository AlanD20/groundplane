package etcd

import (
	"context"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (ledger *ReleaseLedger) prepareCompletedBlueprintReleaseHistory(ctx context.Context, view workqueue.ResumeView, input releaseexecution.Input, completed BlueprintReleaseCompletion, at time.Time) (workqueue.Change, error) {
	release := input.Release
	keys := []string{
		releases.ReleaseIntentStagingKey(completed.PublicationID, release.ReleaseId),
		releases.ReleaseCheckpointStagingKey(completed.PublicationID, release.ReleaseId),
		releases.ReleaseProjectionKey(release.ServiceId),
		releases.ReleaseTerminalKey(release.ReleaseId),
		releases.ReleaseRetentionKey(release.ReleaseId),
		releases.ReleaseManifestStagingKey(completed.PublicationID),
	}
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.Work.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, releases.CorruptReleaseRecord()
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := make([]keyvalue.Condition, 0, len(keys))
	for index, value := range read.Values {
		if value != nil && (value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision) {
			return workqueue.Change{}, releases.CorruptReleaseRecord()
		}
		conditions = append(conditions, keyvalue.Condition{Key: keys[index], ModRevision: keyvalue.RevisionOf(value)})
	}
	if read.Values[0] == nil || read.Values[1] == nil || read.Values[5] == nil ||
		read.Values[3] != nil || read.Values[4] != nil || keyvalue.RevisionOf(read.Values[2]) != completed.Predecessor.ProjectionRevision {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Release history or serving state changed before completion")
	}
	manifest, err := releases.DecodeReleaseRecord[releases.ReleaseStagedManifest](read.Values[5].Value, "release-staged-manifest")
	if err != nil || manifest.PublicationID != completed.PublicationID || manifest.OperationID != release.OperationId || len(manifest.Members) != 1 {
		return workqueue.Change{}, releases.CorruptReleaseRecord()
	}
	member := manifest.Members[0]
	intent, err := releases.DecodeReleaseRecord[domain.Intent](read.Values[0].Value, "release-intent")
	if err != nil || domain.ValidateIntent(intent) != nil || intent.ID != release.ReleaseId || intent.ServiceID != release.ServiceId ||
		intent.EnvironmentID != release.EnvironmentId || intent.OperationID != release.OperationId ||
		intent.OriginatingTaskID != input.Attempt.TaskID || member.ReleaseID != intent.ID || member.ServiceID != intent.ServiceID {
		return workqueue.Change{}, releases.CorruptReleaseRecord()
	}
	intentDigest, err := domain.Digest(intent)
	if err != nil || intentDigest != member.IntentDigest {
		return workqueue.Change{}, releases.CorruptReleaseRecord()
	}
	checkpoint, err := releases.DecodeReleaseRecord[domain.Checkpoint](read.Values[1].Value, "release-checkpoint")
	if err != nil || domain.ValidateCheckpoint(checkpoint) != nil || checkpoint.ReleaseID != intent.ID ||
		checkpoint.State != domain.StatePending || len(checkpoint.Evidence) != 0 || at.Before(checkpoint.UpdatedAt) {
		return workqueue.Change{}, releases.CorruptReleaseRecord()
	}
	checkpointDigest, err := domain.Digest(checkpoint)
	if err != nil || checkpointDigest != member.CheckpointDigest {
		return workqueue.Change{}, releases.CorruptReleaseRecord()
	}
	projection, err := decodeReleaseProjection(read.Values[2], release.EnvironmentId, release.ServiceId)
	if err != nil {
		return workqueue.Change{}, err
	}
	if projection.Revision == math.MaxUint64 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Service release revision is exhausted")
	}
	projection.EnvironmentID, projection.ServiceID = release.EnvironmentId, release.ServiceId
	projection.ServingReleaseID, projection.CurrentSuccessfulReleaseID = release.ReleaseId, release.ReleaseId
	projection.ServingSlot, projection.ActiveOperationID = intent.Slot, ""
	projection.Revision++
	checkpoint.State, checkpoint.UpdatedAt = domain.StateCompleted, at
	// Detailed execution belongs to native work progress, not synthetic legacy
	// EffectEvidence. The retained summary names the actual attempt and proof.
	retention := releaseRollbackMaterial(intent, domain.StateCompleted, at)
	terminal := domain.TerminalSummary{ReleaseID: release.ReleaseId, Outcome: domain.StateCompleted,
		FinalServingReleaseID: release.ReleaseId, EffectDigests: []string{completed.Proof.Progress.Record.Serving.Evidence.SHA256},
		AttemptIDs: []string{input.Attempt.ID}, RollbackMaterialDigest: retention.Digest, CompletedAt: at}
	mutations, err := releaseTerminalRecordMutations(keys[:5], checkpoint, terminal, retention, projection, true)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: conditions, Mutations: mutations}, nil
}
