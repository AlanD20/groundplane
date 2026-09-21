package etcd

import (
	attachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func prepareAttachTaskPlanReference(task TaskRecord) (string, []byte, bool, error) {
	applies, err := taskOwnsAttachLifecycle(task)
	if err != nil || !applies {
		return "", nil, false, err
	}
	if ids.Validate(ids.KindPlan, task.PlanID) != nil {
		return "", nil, false, errs.New(errs.KindInternal, "Attach Task plan id is invalid")
	}
	value, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return "", nil, false, err
	}
	return attachrender.AttachTaskPlanReferenceKey(task.PlanID, task.ID), value, true, nil
}
