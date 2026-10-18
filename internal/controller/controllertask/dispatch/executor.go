package dispatch

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type Executor interface {
	Execute(context.Context, etcd.TaskRecord) error
}
