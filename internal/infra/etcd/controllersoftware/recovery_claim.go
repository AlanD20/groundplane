package controllersoftware

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Startup may restore an admission hold only for the exact work retaining the
// native release claim. Reading this proof never grants a forward effect.
func (repository *Repository) VerifyRecoveryClaim(ctx context.Context, item workexecution.Item, revision int64) error {
	if item.Operation != workexecution.ControllerSoftware || len(item.Claims) != 1 || item.Claims[0] != (workexecution.Claim{ScopeID: TargetID()}) || revision <= 0 {
		return errs.New(errs.KindStateConflict, "native handoff protection is invalid")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{workqueue.ClaimKey(item.Claims[0])}, Revision: revision})
	if err != nil {
		return err
	}
	if read == nil {
		return errs.New(errs.KindStorageUnavailable, "native handoff claim read is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "native handoff resource protection is missing")
	}
	owner, err := recordcodec.Decode[workqueue.ClaimOwner](read.Values[0].Value, "execution_claim")
	if err != nil {
		return err
	}
	if owner.TaskID != item.TaskID || owner.WorkItemID != item.ID {
		return errs.New(errs.KindStateConflict, "native handoff resource belongs to different work")
	}
	return nil
}
