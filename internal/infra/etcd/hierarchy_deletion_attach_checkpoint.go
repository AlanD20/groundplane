package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *HierarchyDeletionRepository) PrepareHierarchyAttachCheckpoint(
	ctx context.Context,
	view workqueue.ResumeView,
	input attachexecution.RemovalInput,
	report attachexecution.RemovalReport,
) (workqueue.Change, error) {
	capture, err := repository.LoadHierarchyAttachCapture(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer capture.Clear()
	if err := validateHierarchyAttachSource(view.Work, capture, input); err != nil {
		return workqueue.Change{}, err
	}
	change, err := attachremoval.PrepareRemovalCheckpoint(ctx, repository.store, view, input, report)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, capture.Conditions...)
	return change, nil
}

func validateHierarchyAttachSource(
	view workqueue.View,
	capture HierarchyAttachCapture,
	input attachexecution.RemovalInput,
) error {
	if capture.Revision <= 0 || input.Removal == nil || input.Removal.CapturedSource == nil {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	raw, err := attachremoval.EncodeWorkCapture(capture.Capture)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(raw)
	clear(raw)
	source := input.Removal.CapturedSource
	if source.InputId != ids.DeriveAt(
		ids.KindPlan,
		view.Task.Task.CreatedAt,
		view.Item.Item.ID,
		"attach-removal-source",
	) ||
		!bytes.Equal(source.Sha256, digest[:]) ||
		input.Removal.AttachId != capture.Source.Input.Attach.ID ||
		input.Removal.BackingServiceId != capture.Source.Input.Attach.BackingServiceID {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	return nil
}
