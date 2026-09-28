package keyvalue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ImageSelectionConditions(ctx context.Context, conditions []Condition) []Condition {
	revision, exists := imagefence.Revision(ctx)
	if !exists {
		return conditions
	}
	result := make([]Condition, len(conditions), len(conditions)+1)
	copy(result, conditions)
	return append(result, Condition{Key: imagefence.Key, ModRevision: revision})
}

// The extra compare is private to storage, not an extra caller failure read.
func ImageSelectionResult(ctx context.Context, result TransactionResult) (TransactionResult, error) {
	revision, exists := imagefence.Revision(ctx)
	if !exists || result.Succeeded {
		return result, nil
	}
	if len(result.FailureReads) == 0 {
		return TransactionResult{}, errs.New(errs.KindInternal, "image fence failure evidence is missing")
	}
	last := result.FailureReads[len(result.FailureReads)-1]
	if (last == nil && revision != 0) || (last != nil && last.ModRevision != revision) {
		return TransactionResult{}, errs.New(
			errs.KindResourceInUse,
			"host images changed before publication; retry the operation",
		)
	}
	result.FailureReads = result.FailureReads[:len(result.FailureReads)-1]
	return result, nil
}
