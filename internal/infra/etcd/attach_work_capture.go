package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type AttachWorkCapture struct {
	Task   workexecution.Task
	Item   workexecution.Item
	Change workqueue.Change
	input  attachprovision.InputPublication
}

func (capture AttachWorkCapture) Finish(ctx context.Context, _ bool) error {
	defer blueprintattachinputs.Clear(&capture.input.Generation)
	return capture.input.Finish(ctx)
}
