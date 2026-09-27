package runners

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
)

// ListRuntimeRunners supplies the host maintenance loop across ownership scopes.
// Operator lists retain their required Tenant, Project or Environment filter.
func (repository *Reader) ListRuntimeRunners(
	ctx context.Context,
	request etcdstore.PageRequest,
) (etcdstore.Page[RunnerRecord], error) {
	page, err := recordquery.ListPrimary(
		ctx, repository.store, "runner-runtime", "", "", runnerPrefix, ids.KindRunner, request,
		DecodeRunnerDesiredAggregate,
		func(record RunnerRecord) string { return record.Desired.ID },
		func(RunnerRecord) bool { return true },
	)
	if err != nil {
		return etcdstore.Page[RunnerRecord]{}, err
	}
	return repository.hydrateRunnerPage(ctx, page)
}
