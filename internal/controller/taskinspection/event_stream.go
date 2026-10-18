package taskinspection

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EventStream struct {
	repository *executiondiagnostics.Repository
	taskID     string
	after      uint64
	initial    executiondiagnostics.TaskEvents
}

func (service *Service) OpenTaskEventStream(ctx context.Context, taskID string, after uint64) (*EventStream, error) {
	page, err := service.diagnostics.TaskEvents(ctx, taskID, after)
	if err != nil {
		return nil, err
	}
	if page.Expired {
		return nil, errs.New(errs.KindCursorExpired, "Task events before the retained interval have expired; reload Task details before resuming")
	}
	return &EventStream{repository: service.diagnostics, taskID: taskID, after: after, initial: page}, nil
}

func (stream *EventStream) Run(ctx context.Context, emit func(executiondiagnostics.TaskEvent) error) error {
	if emit == nil {
		return errs.New(errs.KindInternal, "Task event consumer is required")
	}
	page := stream.initial
	after := stream.after
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if page.Expired {
			return errs.New(errs.KindCursorExpired, "Task event consumer fell behind retained history")
		}
		for _, event := range page.Items {
			if err := emit(event); err != nil {
				return err
			}
			after = event.Sequence
		}
		if page.Finished {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		var err error
		page, err = stream.repository.TaskEvents(ctx, stream.taskID, after)
		if err != nil {
			return err
		}
	}
}
