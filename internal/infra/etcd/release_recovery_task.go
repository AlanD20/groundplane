package etcd

import "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"

func releaseRecoveryTask(task TaskRecord) bool {
	return task.Params[releaserender.TaskReleasePublicationParam] != "" || authoredBlueprintServiceRemovalTask(task)
}
