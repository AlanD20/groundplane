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
	id, err := service.registry.Fetch(ctx, plan, report)
	if err == nil && id != plan.ConfigDigest {
		err = errs.New(errs.KindStateConflict, "fetched image differs from the accepted content")
	}
	if err == nil {
		progress.Phase = "verified"
		return report(progress)
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
