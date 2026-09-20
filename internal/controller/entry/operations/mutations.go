package operations

import "github.com/AlanD20/groundplane/pkg/errs"

// MutationService composes individual and bulk desired-state operations.
type MutationService struct {
	*entryDesiredMutationService
	*entryBulkUpsertService
}

func NewMutationService(
	desired *entryDesiredMutationService,
	bulk *entryBulkUpsertService,
) (*MutationService, error) {
	if desired == nil || bulk == nil {
		return nil, errs.New(errs.KindInternal, "Entry mutation service is not configured")
	}
	return &MutationService{entryDesiredMutationService: desired, entryBulkUpsertService: bulk}, nil
}
