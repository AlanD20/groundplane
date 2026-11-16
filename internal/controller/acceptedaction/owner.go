package acceptedaction

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Compiler[T any] func(context.Context, workqueue.TaskSnapshot, int64, time.Time, T) (taskdependencies.Prepared, error)

type typedOwner[T any] struct {
	payloads *Payloads
	kind     string
	compile  Compiler[T]
}

func Registration[T any](submitter *Submitter, kind string, action workexecution.Action, target ids.Kind,
	compile Compiler[T],
) taskdependencies.Registration {
	var payloads *Payloads
	if submitter != nil {
		payloads = submitter.payloads
	}
	return taskdependencies.Registration{
		Action: action,
		Target: target,
		Kind:   kind,
		Owner:  typedOwner[T]{payloads: payloads, kind: kind, compile: compile},
	}
}

func (owner typedOwner[T]) PrepareAccepted(ctx context.Context, task workqueue.TaskSnapshot, revision int64,
	at time.Time,
) (taskdependencies.Prepared, error) {
	if owner.compile == nil {
		return taskdependencies.Prepared{}, errs.New(errs.KindInternal, "accepted action compiler is required")
	}
	var prepared taskdependencies.Prepared
	guard, err := Open(ctx, owner.payloads, task, owner.kind, revision, func(input T) error {
		var err error
		prepared, err = owner.compile(ctx, task, revision, at, input)
		return err
	})
	if err != nil {
		return prepared, err
	}
	prepared.Authority.Conditions = append(prepared.Authority.Conditions, guard.Conditions...)
	return prepared, nil
}
