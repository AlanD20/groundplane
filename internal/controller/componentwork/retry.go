package componentwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareWorkRetry grants only restoration of the captured predecessor. Once
// restoration settles, a failed Apply requires a new operator Apply rather than
// replaying a potentially faulty Component configuration.
func (owner *AgentWork) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	previous workqueue.AttemptSnapshot, revision int64, at time.Time,
) (workqueue.Change, error) {
	if item.Item.Operation != workexecution.Component || item.Item.Placement.Executor != workexecution.Agent ||
		item.Item.Phase != workexecution.Attention || item.Item.AttemptNumber == 0 {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable,
			"Component execution has no unresolved cleanup to retry; submit a new Apply to run the configuration again")
	}
	return owner.inputs.PrepareRecoveryRetry(
		ctx,
		workqueue.View{Task: task, Item: item, ReadRevision: revision},
		previous,
		at,
	)
}
