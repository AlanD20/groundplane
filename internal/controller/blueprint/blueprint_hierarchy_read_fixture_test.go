package blueprint

import (
	"context"

	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// The hierarchy read seam supplies a coherent existing Environment; every other
// read and all claim/staging/publication methods are real persistence wrappers.
type blueprintPreflightRepository struct {
	*durableRepository
	tenant      testhierarchy.TenantRecord
	project     testhierarchy.ProjectRecord
	environment testhierarchy.EnvironmentRecord
}

func (r *blueprintPreflightRepository) GetTenant(
	context.Context,
	string,
) (testkeyvalue.Versioned[testhierarchy.TenantRecord], error) {
	return testkeyvalue.Versioned[testhierarchy.TenantRecord]{Record: r.tenant, Revision: 1, ReadRevision: 1}, nil
}

func (r *blueprintPreflightRepository) GetProject(
	context.Context,
	string,
) (testkeyvalue.Versioned[testhierarchy.ProjectRecord], error) {
	return testkeyvalue.Versioned[testhierarchy.ProjectRecord]{Record: r.project, Revision: 1, ReadRevision: 1}, nil
}

func (r *blueprintPreflightRepository) GetEnvironment(
	context.Context,
	string,
) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	return testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
		Record:       r.environment,
		Revision:     1,
		ReadRevision: 1,
	}, nil
}
