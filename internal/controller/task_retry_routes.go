package controller

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type TaskRetrier interface {
	RetryTask(context.Context, string, string) (etcd.IdempotencyResponse, error)
}
