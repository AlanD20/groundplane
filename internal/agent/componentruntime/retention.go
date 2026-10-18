package componentruntime

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

func (executor *Executor) Retire(ctx context.Context, terminal workexecution.Attempt) error {
	return executor.journal.Retire(ctx, terminal)
}
