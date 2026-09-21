package desiredrevision

import (
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintIdentityAllocator reconstructs Blueprint-owned identities from the
// durable Task authority returned by the staging claim.
type BlueprintIdentityAllocator struct {
	taskID    string
	createdAt time.Time
	next      map[ids.Kind]uint64
}

// NewBlueprintIdentityAllocator accepts only a validated durable stage claim.
func NewBlueprintIdentityAllocator(
	claim blueprints.EnvironmentBlueprintStageClaim,
) (*BlueprintIdentityAllocator, error) {
	if ids.Validate(ids.KindTask, claim.TaskID) != nil ||
		!etcd.ValidDesiredRevisionTime(claim.CreatedAt) {
		return nil, errs.New(errs.KindInternal, "Blueprint identity authority is invalid")
	}
	return &BlueprintIdentityAllocator{
		taskID: claim.TaskID, createdAt: claim.CreatedAt, next: make(map[ids.Kind]uint64),
	}, nil
}

// New derives the next stable identity for one kind. Call order is part of the
// controller-owned deterministic reconciliation plan.
func (allocator *BlueprintIdentityAllocator) New(kind ids.Kind) string {
	sequence := allocator.next[kind]
	allocator.next[kind] = sequence + 1
	return allocator.Named(kind, "sequence/"+strconv.FormatUint(sequence, 10))
}

// Named derives a stable identity for a unique semantic purpose.
func (allocator *BlueprintIdentityAllocator) Named(kind ids.Kind, purpose string) string {
	return ids.DeriveAt(kind, allocator.createdAt, allocator.taskID, "blueprint/"+purpose)
}
