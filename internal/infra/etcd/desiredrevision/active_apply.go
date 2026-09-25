package desiredrevision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// HasActiveEnvironmentBlueprintApply reads the Environment's Task journal at
// one revision. A later Apply publication compares the desired head, so a
// concurrent first publication cannot slip between this check and its own.
func (repository *Repository) HasActiveEnvironmentBlueprintApply(
	ctx context.Context, environmentID string,
) (bool, error) {
	page, err := recordquery.ListVisibleIndex(
		ctx, repository.store, "tasks", "environment", environmentID,
		taskjournal.TaskEnvironmentIndexPrefix+environmentID+"/",
		taskjournal.TaskStorageKey, ids.KindTask, keyvalue.PageRequest{Limit: 1},
		etcd.DecodeTaskRecord, func(task etcd.TaskRecord) string { return task.ID },
		func(task etcd.TaskRecord) bool { return task.Owner.EnvironmentID == environmentID },
		func(task etcd.TaskRecord) bool {
			return task.Type == taskjournal.TaskUpdate && task.Target == environmentID &&
				task.Params[blueprints.EnvironmentDesiredRevisionParam] != "" &&
				!taskjournal.IsTerminalTaskStatus(task.Status)
		},
	)
	return len(page.Items) != 0, err
}
