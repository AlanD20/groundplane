package workqueue

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// ReadServiceClaim resolves the current effect owner at one fixed revision.
// Ownership is cross-checked against the work item, not inferred from a Task's
// overall status. It grants neither execution nor permission to release a claim.
func (repository *Repository) ReadServiceClaim(ctx context.Context, resourceID string, revision int64) (ClaimSnapshot, workexecution.Placement, error) {
	if err := ids.Validate(ids.KindService, resourceID); err != nil {
		return ClaimSnapshot{}, workexecution.Placement{}, err
	}
	claim := workexecution.Claim{ScopeID: resourceID}
	read, err := repository.read(ctx, []string{ClaimKey(claim)}, revision)
	if err != nil {
		return ClaimSnapshot{}, workexecution.Placement{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	result := ClaimSnapshot{Claim: claim}
	value := read.Values[0]
	if value == nil {
		return result, workexecution.Placement{}, nil
	}
	owner, err := DecodeClaimOwner(value.Value)
	if err != nil {
		return ClaimSnapshot{}, workexecution.Placement{}, corrupt()
	}
	view, err := repository.ReadWork(ctx, owner.TaskID, owner.WorkItemID, read.ReadRevision)
	if err != nil {
		return ClaimSnapshot{}, workexecution.Placement{}, err
	}
	if view.Item.Item.Phase == workexecution.Settled || !slices.Contains(view.Item.Item.Claims, claim) {
		return ClaimSnapshot{}, workexecution.Placement{}, corrupt()
	}
	result.Owner, result.Revision = &owner, value.ModRevision
	return result, view.Item.Item.Placement, nil
}
