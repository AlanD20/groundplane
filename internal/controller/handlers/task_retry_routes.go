package handlers

import (
	"context"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
)

type TaskRetrier interface {
	RetryTask(context.Context, string, string) (idempotencyrecord.IdempotencyResponse, error)
}

type TaskAborter interface {
	AbortTask(context.Context, string, string) (idempotencyrecord.IdempotencyResponse, error)
}
