package handlers

import (
	"context"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

type TaskRetrier interface {
	RetryTask(context.Context, string, string, apiTypes.TaskRetryRequest) (idempotencyrecord.IdempotencyResponse, error)
}

type taskRetryInput struct {
	ID             string `path:"id" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true"`
	Body           apiTypes.TaskRetryRequest
}

type TaskAborter interface {
	AbortTask(context.Context, string, string) (idempotencyrecord.IdempotencyResponse, error)
}
