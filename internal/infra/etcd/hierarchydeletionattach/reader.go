package hierarchydeletionattach

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ReadByPlan(
	ctx context.Context,
	store Reader,
	planID string,
) (Input, error) {
	if ids.Validate(ids.KindPlan, planID) != nil {
		return Input{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy Attach plan id is invalid",
		)
	}
	read, err := store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{PlanKey(planID)}},
	)
	if err != nil {
		return Input{}, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return Input{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	operationID := string(read.Values[0].Value)
	keyvalue.ClearValues(read.Values)
	if !hierarchydeletion.ValidHierarchyDeletionPrivateID(operationID, "del") {
		return Input{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	input, _, err := Read(
		ctx,
		store,
		ParentPlanKey(operationID, planID),
	)
	if err != nil {
		return Input{}, err
	}
	if input.PlanID != planID || input.ParentOperationID != operationID {
		Clear(&input)
		return Input{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return input, nil
}
