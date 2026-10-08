package etcd

import (
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// DNS consumes the exact applied projection being promoted in the same terminal
// transaction. Failed/restored Tasks continue to use the applied predecessor.
func hostResolutionTerminalProjection(
	task TaskRecord, status taskjournal.TaskStatus, change taskMaterializationProjectionChange,
) (*projectionrecord.EnvironmentComposeProjection, error) {
	if status != taskjournal.TaskStatusCompleted || !change.applies {
		return nil, nil
	}
	key := projectionrecord.EnvironmentComposeProjectionStorageKey(task.Owner.EnvironmentID)
	var selected *projectionrecord.EnvironmentComposeProjection
	for _, mutation := range change.mutations {
		if mutation.Key != key {
			continue
		}
		if mutation.Type != etcdstore.MutationPut || selected != nil {
			return nil, errs.New(errs.KindInternal, "host-resolution applied projection contribution is invalid")
		}
		projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(mutation.Value)
		if err != nil {
			return nil, err
		}
		if projection.EnvironmentID != task.Owner.EnvironmentID {
			return nil, projectionrecord.CorruptEnvironmentComposeProjection()
		}
		selected = &projection
	}
	return selected, nil
}
