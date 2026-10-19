package imagedelivery

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) loadOperation(
	ctx context.Context,
	delivery workqueue.Delivery,
) (keyvalue.Versioned[imageoperations.Record], error) {
	item, attempt := delivery.Item.Item, delivery.Attempt.Attempt
	if item.Validate() != nil || attempt.Validate() != nil || item.Operation != workexecution.Image ||
		attempt.Executor.Executor != workexecution.Controller || attempt.TaskID != item.TaskID ||
		attempt.WorkItemID != item.ID || item.ActiveAttemptID != attempt.ID || attempt.FinishedAt != nil ||
		item.Input == nil || *item.Input != attempt.Input {
		return keyvalue.Versioned[imageoperations.Record]{}, errs.New(
			errs.KindStateConflict,
			"image execution no longer owns this attempt",
		)
	}
	loaded, err := service.operations.Load(ctx, item.TaskID)
	if err != nil {
		return keyvalue.Versioned[imageoperations.Record]{}, err
	}
	input := loaded.Record.Input
	ref, err := input.Reference()
	if err != nil || input.WorkItemID != item.ID || ref != attempt.Input || input.Target() != item.TargetID ||
		input.Placement != item.Placement {
		return keyvalue.Versioned[imageoperations.Record]{}, errs.New(
			errs.KindStateConflict,
			"image execution differs from captured input",
		)
	}
	return loaded, nil
}

func (service *Service) AuthorizeStart(ctx context.Context, delivery workqueue.Delivery) (workqueue.Change, error) {
	loaded, err := service.loadOperation(ctx, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	if loaded.Record.Phase != imageoperations.Ready || delivery.Attempt.Attempt.Direction != workexecution.Forward {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"image operation requires reconciliation rather than a new start",
		)
	}
	return workqueue.Change{
		Conditions: []keyvalue.Condition{
			{Key: imageoperations.Key(loaded.Record.Input.TaskID), ModRevision: loaded.Revision},
		},
	}, nil
}

func (service *Service) Execute(ctx context.Context, delivery workqueue.Delivery) error {
	loaded, err := service.loadOperation(ctx, delivery)
	if err != nil {
		return err
	}
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt == nil || delivery.Item.Item.Phase != workexecution.Running ||
		attempt.Direction != workexecution.Forward {
		return errs.New(errs.KindStateConflict, "image operation has no fresh execution grant")
	}
	journal, err := imageoperations.NewJournal(service.operations, loaded.Record.Input, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	if loaded.Record.Input.Action == workexecution.ActionRemove {
		return service.removeOperation(
			ctx,
			loaded.Record.Input,
			journal,
			&imageReporter{repository: service.diagnostics, attempt: attempt},
		)
	}
	if err := journal.StartFetch(ctx, time.Now().UTC()); err != nil {
		return err
	}
	plan := *loaded.Record.Input.Fetch
	reporter := &imageReporter{repository: service.diagnostics, attempt: attempt}
	fetchStage := reporter.start(
		ctx,
		"fetch_content",
		"Fetch image content",
		"Download and verify the exact registry manifest and image configuration accepted for this request",
	)
	id, fetchErr := service.registry.Fetch(ctx, plan, func(progress imagefetch.Progress) error {
		if reporter.lost {
			return nil
		}
		bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := journal.Report(bounded, progress)
		cancel()
		if err != nil {
			reporter.incomplete(ctx, err)
		}
		// Progress is diagnostic, not permission to execute. The captured-input
		// journal still fences every authoritative state transition separately.
		return nil
	})
	settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if fetchErr != nil || id != plan.ConfigDigest {
		reporter.finish(
			ctx,
			fetchStage,
			workexecution.StageFailed,
			"The fetch did not return the accepted image identity; inspect the image fetch failure details",
		)
		if fetchErr == nil {
			fetchErr = errs.New(errs.KindStateConflict, "fetched image differs from accepted content")
		}
		return errors.Join(fetchErr, journal.RejectForward(settlement, workexecution.Failed, time.Now().UTC()))
	}
	if err := journal.Fetched(settlement, id, time.Now().UTC()); err != nil {
		reporter.finish(
			ctx,
			fetchStage,
			workexecution.StageUnknown,
			"Docker returned the image, but GP could not save the verified image result",
		)
		return err
	}
	reporter.finish(
		ctx,
		fetchStage,
		workexecution.StageSucceeded,
		"Docker returned the exact accepted image configuration: "+id,
	)
	tag, err := plan.TagReference()
	if err != nil {
		return err
	}
	if tag == "" {
		return journal.CompleteDigestFetch(settlement, id, time.Now().UTC())
	}
	if err := journal.BeginTagWrite(ctx, time.Now().UTC()); err != nil {
		return errors.Join(err, journal.RejectForward(settlement, workexecution.Failed, time.Now().UTC()))
	}
	tagStage := reporter.start(
		ctx,
		"assign_tag",
		"Assign local image tag",
		"Point "+tag+" to the verified image content",
	)
	outcome, tagErr := service.registry.AssignTag(ctx, plan)
	switch outcome {
	case imagefetch.TagAssignmentApplied:
		reporter.finish(
			ctx,
			tagStage,
			workexecution.StageSucceeded,
			"The requested local tag points to the accepted image",
		)
	case imagefetch.TagAssignmentUnchanged:
		reporter.finish(ctx, tagStage, workexecution.StageFailed, "The requested local tag was not changed")
	default:
		reporter.finish(
			ctx,
			tagStage,
			workexecution.StageUnknown,
			"Docker tag completion is unknown; the tag write remains protected",
		)
	}
	tagSettlement, finish := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer finish()
	return errors.Join(tagErr, journal.RecordTagOutcome(tagSettlement, outcome, time.Now().UTC()))
}

func (service *Service) removeOperation(
	ctx context.Context,
	input imageoperations.Input,
	journal *imageoperations.Journal,
	reporter *imageReporter,
) error {
	if err := journal.BeginRemoval(ctx, time.Now().UTC()); err != nil {
		return err
	}
	preflightErr := service.checkRemoval(ctx, input.RemoveID, input.OperationID)
	if preflightErr != nil {
		settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		return errors.Join(
			preflightErr,
			journal.RecordRemovalOutcome(settlement, imageoperations.RemovalRejected, time.Now().UTC()),
		)
	}
	stage := reporter.start(
		ctx,
		"remove_image",
		"Remove local image",
		"Remove the selected image only after all active-use and retained-recovery protections permit it",
	)
	removeErr := service.registry.Remove(ctx, input.RemoveID)
	settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	outcome := imageoperations.RemovalUnknown
	if removeErr == nil {
		outcome = imageoperations.RemovalAbsent
	}
	if errors.Is(removeErr, errs.New(errs.KindResourceInUse, "")) {
		outcome = imageoperations.RemovalRejected
	}
	switch outcome {
	case imageoperations.RemovalAbsent:
		reporter.finish(ctx, stage, workexecution.StageSucceeded, "Docker confirmed that the local image was removed")
	case imageoperations.RemovalRejected:
		reporter.finish(
			ctx,
			stage,
			workexecution.StageFailed,
			"Docker refused removal because the image is still in use",
		)
	default:
		reporter.finish(
			ctx,
			stage,
			workexecution.StageUnknown,
			"Docker removal completion is unknown; image protection remains held",
		)
	}
	return errors.Join(removeErr, journal.RecordRemovalOutcome(settlement, outcome, time.Now().UTC()))
}
