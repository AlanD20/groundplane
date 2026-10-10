package imagedelivery

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) executeFetch(ctx context.Context, task etcd.TaskRecord, plan imagefetch.Plan) error {
	progress := imagefetch.Progress{Phase: "checking"}
	report := func(value imagefetch.Progress) error {
		progress = value
		return service.recordFetchProgress(ctx, task, progress)
	}
	if err := report(progress); err != nil {
		return err
	}
	tag, err := plan.TagReference()
	if err != nil {
		return err
	}
	if tag != "" {
		var completed bool
		completed, err = service.tasks.ImageTagCommitCompleted(ctx, task)
		if err == nil && completed {
			progress.Phase = "verified"
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			return service.recordFetchProgress(cleanup, task, progress)
		}
	}
	var id string
	if err == nil {
		id, err = service.registry.Fetch(ctx, plan, report)
	}
	if err == nil && id != plan.ConfigDigest {
		err = errs.New(errs.KindStateConflict, "fetched image differs from the accepted content")
	}
	if err == nil {
		err = service.commitFetchedTag(ctx, task, plan)
	}
	if err == nil {
		progress.Phase = "verified"
		if tag == "" {
			return report(progress)
		}
		// The Docker effect is already exact and durably receipted. Persist its
		// final progress even when Abort/deadline raced the storage acknowledgement.
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return service.recordFetchProgress(cleanup, task, progress)
	}
	problem := errs.New(errs.KindInternal, "").ToProblem()
	var domainError *errs.Error
	if errors.As(err, &domainError) {
		problem = domainError.ToProblem()
	}
	progress.ErrorCode, progress.ErrorDetail = string(problem.Code), problem.Detail
	if errors.Is(err, context.Canceled) {
		progress.ErrorCode, progress.ErrorDetail = "context.canceled", "Image fetch was interrupted."
	} else if errors.Is(err, context.DeadlineExceeded) {
		progress.ErrorCode, progress.ErrorDetail = "context.deadline_exceeded", "Image fetch exceeded its time limit."
	}
	// Persist the public diagnostic even when the execution deadline/Abort fired.
	// This does not terminalize the Task or grant permission to resume it.
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return errors.Join(err, service.recordFetchProgress(cleanup, task, progress))
}

func (service *Service) commitFetchedTag(
	ctx context.Context,
	task etcd.TaskRecord,
	plan imagefetch.Plan,
) error {
	tag, err := plan.TagReference()
	if err != nil || tag == "" {
		return err
	}
	completed, err := service.tasks.BeginImageTagCommit(ctx, task)
	if err != nil || completed {
		return err
	}
	outcome, tagErr := service.registry.AssignTag(ctx, plan)
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	switch outcome {
	case imagefetch.TagAssignmentApplied:
		// Exact post-write inspection is stronger than a daemon/client cleanup
		// error. The durable receipt resolves a crash before Task acknowledgement.
		return service.tasks.CompleteImageTagCommit(cleanup, task, time.Now().UTC())
	case imagefetch.TagAssignmentUnchanged:
		if tagErr == nil {
			tagErr = errs.New(errs.KindInternal, "image tag remained unchanged without a failure")
		}
		return errors.Join(tagErr, service.tasks.AbandonImageTagCommit(cleanup, task))
	case imagefetch.TagAssignmentUnknown:
		if tagErr != nil {
			return tagErr // Keep the fence: only this selection may recover the outcome.
		}
		return errs.New(errs.KindInternal, "image tag assignment returned no outcome")
	default:
		return errs.New(errs.KindInternal, "image tag assignment outcome is invalid")
	}
}

func (service *Service) recordFetchProgress(
	ctx context.Context,
	task etcd.TaskRecord,
	progress imagefetch.Progress,
) error {
	key := taskjournal.TaskStorageKey(task.ID)
	for range 3 {
		read, err := service.store.Get(ctx, key)
		if err != nil {
			return err
		}
		if read == nil || read.Entry == nil {
			return errs.New(errs.KindTaskNotFound, "image fetch Task is missing")
		}
		current, err := etcd.DecodeTaskRecord(read.Entry.Value)
		if err != nil {
			return err
		}
		if current.ID != task.ID || current.PlanHash != task.PlanHash || current.Type != taskjournal.TaskFetch ||
			current.Executor != taskjournal.TaskExecutorController || current.Status != taskjournal.TaskStatusRunning {
			return errs.New(errs.KindStateConflict, "image fetch Task is no longer executing")
		}
		current.ImageFetchProgress = progress
		if now := time.Now().UTC(); now.After(current.UpdatedAt) {
			current.UpdatedAt = now
		}
		value, err := etcd.EncodeTaskRecord(current)
		if err != nil {
			return err
		}
		result, err := service.store.Transact(
			ctx,
			[]keyvalue.Condition{{Key: key, ModRevision: read.Entry.ModRevision}},
			[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}},
		)
		clear(value)
		if err != nil {
			return err
		}
		if result.Succeeded {
			return nil
		}
	}
	return errs.New(errs.KindStateConflict, "image fetch Task changed while recording progress")
}
