package releasegroups

import (
	errors "errors"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	testing "testing"
	time "time"
)

func TestReleaseGroupRemovalPublicationCannotDeletePrimary(t *testing.T) {

	t.Parallel()

	now := time.Date(2026, 8, 26, 15, 10, 0, 0, time.UTC)
	groupID := ids.NewAt(ids.KindReleaseGroup, now, 1)
	prepared := newReleaseGroupPreparedMutation(
		ids.NewAt(ids.KindEnvironment, now, 2), groupID, 17, testtaskjournal.TaskRemove, nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: ReleaseGroupRecordKey(groupID)}},
	)
	err := prepared.ValidateRemovalPrimaryMutations()
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("validateReleaseGroupPreparedMutation() error = %v, want internal boundary failure", err)
	}
}
