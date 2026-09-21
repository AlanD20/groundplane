package hierarchymutations

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) diagnoseCreate(ctx context.Context, primary string, slug string) error {
	primaryResult, err := repository.store.Get(ctx, primary)
	if err != nil {
		return err
	}
	slugResult, err := repository.store.Get(ctx, slug)
	if err != nil {
		return err
	}
	if slugResult.Entry != nil {
		return errs.New(errs.KindSlugConflict, "slug is already in use")
	}
	if primaryResult.Entry != nil {
		return errs.New(errs.KindStateConflict, "stable id is already in use")
	}
	return errs.New(errs.KindStateConflict, "hierarchy changed during create")
}
