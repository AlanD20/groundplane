package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RecoverTaskSecretPinSources runs during construction, before requests or
// dispatch. It removes unpublished preparation and resumes authorized release.
func RecoverTaskSecretPinSources(ctx context.Context, store hierarchyStore) error {
	pins, err := tasksecretpins.NewEtcdRepository(store, "")
	if err != nil {
		return err
	}
	if err := pins.RecoverPreparations(ctx); err != nil {
		return err
	}
	for {
		progressed, err := pins.ResumeRelease(ctx)
		if err != nil || !progressed {
			return err
		}
	}
}

func (repository *TaskRepository) ResumeTaskSourceReleases(ctx context.Context) (bool, error) {
	if repository == nil || repository.store == nil {
		return false, errs.New(errs.KindInternal, "Task source release repository is missing")
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, "")
	if err != nil {
		return false, err
	}
	return pins.ResumeRelease(ctx)
}
