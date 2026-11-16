package etcd

import "github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"

// Deferred captures outlive local staging buffers until their Task promotion.
func ownedRunnerCapture(change workqueue.Change) workqueue.Change {
	result := workqueue.Change{Conditions: append(change.Conditions[:0:0], change.Conditions...),
		Mutations: append(change.Mutations[:0:0], change.Mutations...)}
	for index := range result.Mutations {
		result.Mutations[index].Value = append([]byte(nil), result.Mutations[index].Value...)
	}
	return result
}
