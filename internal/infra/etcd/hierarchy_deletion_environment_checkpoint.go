package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareHierarchyEnvironmentCheckpoint binds the report to the same immutable
// removal source used for delivery. The reporting transport must authenticate
// the attempt identity and commit this change before returning an acknowledgement.
func (repository *HierarchyDeletionRepository) PrepareHierarchyEnvironmentCheckpoint(
	ctx context.Context,
	view workqueue.ResumeView,
	input hierarchyexecution.EnvironmentInput,
	report hierarchyexecution.EnvironmentReport,
) (workqueue.Change, error) {
	captured, err := repository.LoadHierarchyEnvironmentInput(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(captured.Input.ComposeArtifact)
	raw, err := hierarchydeletion.EncodeEnvironmentInput(captured.Input)
	if err != nil {
		return workqueue.Change{}, err
	}
	digest := sha256.Sum256(raw)
	clear(raw)
	sourceID := ids.DeriveAt(
		ids.KindPlan,
		view.Work.Task.Task.CreatedAt,
		view.Work.Item.Item.ID,
		"environment-removal-source",
	)
	if input.Removal == nil || input.Removal.CapturedSource == nil ||
		input.Removal.CapturedSource.InputId != sourceID ||
		!bytes.Equal(input.Removal.CapturedSource.Sha256, digest[:]) ||
		input.Removal.EnvironmentId != captured.Input.EnvironmentID ||
		input.Removal.ExpectedVolumeDir != captured.Input.VolumeDirectory {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	change, err := hierarchydeletion.PrepareEnvironmentCheckpoint(ctx, repository.store, view, input, report)
	if err != nil {
		return workqueue.Change{}, err
	}
	recovery, err := repository.hierarchyEnvironmentRecoveryConditions(ctx, view, input)
	if err != nil {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, recovery...)
	change.Conditions = append(change.Conditions, captured.Conditions...)
	return change, nil
}
