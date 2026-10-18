package scriptruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// Retire releases local replay storage after the session consumes an
// acknowledged result. Authoritative operation inputs remain Controller-owned.
func (executor *Executor) Retire(ctx context.Context, attempt workexecution.Attempt) error {
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return executor.journal.RetireAcknowledgedScript(bounded, attempt)
}
