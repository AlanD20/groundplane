package app

import (
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func materializationLifecycleTask(at time.Time, environmentID string, generation int32) etcd.TaskRecord {
	task := validTaskRecord(at)
	task.Type = testtaskjournal.TaskUpdate
	task.Target = environmentID
	task.RenderGeneration = generation
	task.Params[testtaskjournal.TaskMaterializationEnvironmentParam] = environmentID
	return task
}
