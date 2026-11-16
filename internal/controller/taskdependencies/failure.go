package taskdependencies

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (preparations *Preparations) rejectCompilerFailure(ctx context.Context, task workqueue.TaskSnapshot,
	guard workqueue.Change, failure error, at time.Time,
) (bool, error) {
	if errors.Is(failure, context.Canceled) || errors.Is(failure, context.DeadlineExceeded) ||
		errors.Is(failure, errs.New(errs.KindStorageUnavailable, "")) {
		return false, failure
	}
	public := errs.Wrap(errs.KindInternal, failure).ToProblem()
	var domain *errs.Error
	if errors.As(failure, &domain) {
		public = domain.ToProblem()
	}
	detail := strings.TrimSpace(public.Detail)
	if len(detail) > 4096 {
		detail = detail[:4096]
		for !utf8.ValidString(detail) {
			detail = detail[:len(detail)-1]
		}
	}
	evidence, reference, err := workqueue.PrepareAcceptedPreparationFailure(task, string(public.Code), detail, at)
	if err != nil {
		return false, err
	}
	defer keyvalue.ClearMutationValues(evidence.Mutations)
	rejection, err := preparations.queue.PreparePlanningRejection(
		ctx,
		task,
		workexecution.Failed,
		reference,
		at,
		at.Add(workexecution.TaskHistoryRetention),
	)
	if err != nil {
		return false, err
	}
	defer keyvalue.ClearMutationValues(rejection.Mutations)
	result, err := preparations.queue.Commit(ctx, guard, evidence, rejection)
	return result.Succeeded, err
}
