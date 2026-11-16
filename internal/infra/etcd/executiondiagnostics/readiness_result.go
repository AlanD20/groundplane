package executiondiagnostics

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// ReleaseReadinessResult reads the immutable proof already admitted under the
// authenticated Release cursor. Recovery never overwrites this forward evidence.
func (repository *Repository) ReleaseReadinessResult(
	ctx context.Context,
	attempt workexecution.Attempt,
	revision int64,
) (*workexecution.Stage, error) {
	if attempt.FinishedAt == nil || attempt.StartedAt == nil || attempt.Direction != workexecution.Forward ||
		attempt.Outcome != workexecution.Failed {
		return nil, nil
	}
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{
			Revision: revision,
			Keys:     []string{releasework.ProgressKey(attempt.ID), workqueue.CheckpointKey(attempt.ID)},
		},
	)
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 {
		return nil, invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, nil
	}
	if read.Values[1] == nil || len(read.Values[0].Value) > 16<<10 {
		return nil, invalid()
	}
	parent, err := recordcodec.Decode[releasework.Record](read.Values[0].Value, "release-work-progress")
	if err != nil || parent.TaskID != attempt.TaskID || parent.WorkItemID != attempt.WorkItemID ||
		parent.Input != attempt.Input ||
		parent.Progress.AttemptID != attempt.ID {
		return nil, invalid()
	}
	if !releaseexecution.IsReadinessBoundary(parent.Progress) {
		return nil, nil
	}
	active := attempt
	active.FinishedAt, active.Result, active.Outcome = nil, nil, ""
	report := releaseexecution.CheckpointReport{
		Sequence: parent.CheckpointSequence,
		StageID:  parent.StageID,
		Progress: parent.Progress,
	}
	expected, err := report.Checkpoint(active)
	if err != nil {
		return nil, err
	}
	checkpoint, err := workqueue.DecodeCheckpoint(read.Values[1].Value)
	if err != nil || !releaseexecution.EqualCheckpoint(checkpoint, expected) ||
		checkpoint.RecordedAt.After(*attempt.FinishedAt) {
		return nil, invalid()
	}
	evidence, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{
			Revision: revision,
			Keys:     []string{releasework.EffectEvidenceKey(attempt.ID, report.Sequence)},
		},
	)
	if err != nil {
		return nil, err
	}
	if evidence == nil || evidence.ReadRevision != revision || len(evidence.Values) != 1 {
		return nil, invalid()
	}
	defer keyvalue.ClearValues(evidence.Values)
	if evidence.Values[0] == nil {
		return nil, nil
	}
	raw := evidence.Values[0].Value
	if len(raw) > releaseexecution.MaximumCheckpointBytes {
		return nil, invalid()
	}
	proof, err := jcs.Decode[releaseexecution.EffectEvidence](raw)
	if err != nil || proof.AttemptID != attempt.ID || proof.Input != attempt.Input ||
		proof.Sequence != report.Sequence ||
		proof.StageID != report.StageID {
		return nil, invalid()
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, invalid()
	}
	if proof.Result.ReadinessFailure == nil {
		return nil, nil
	}
	if proof.Result.Configuration != nil || len(proof.Result.Helper) != 0 || proof.Result.Readiness != nil ||
		proof.Result.DrainedAt != nil {
		return nil, invalid()
	}
	stage, err := proof.Result.ReadinessFailure.DiagnosticStage(attempt, parent.Progress, proof.RecordedAt)
	if err != nil {
		return nil, err
	}
	return &stage, nil
}
