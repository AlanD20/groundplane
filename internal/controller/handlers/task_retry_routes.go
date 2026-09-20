package handlers

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type TaskRetrier interface {
	RetryTask(context.Context, string, string) (etcd.IdempotencyResponse, error)
}

type TaskAborter interface {
	AbortTask(context.Context, string, string) (etcd.IdempotencyResponse, error)
}
