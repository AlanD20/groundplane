package etcd

import (
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"testing"
)

// Rationale: only a Controller-authored marker with the bounded system actor
// may authorize automatic reconciliation; an operator task is never automatic.
func TestIsAutomaticReconcileTaskRequiresSystemActor(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		actor  testtaskjournal.TaskActor
		params map[string]string
		want   bool
	}{
		{
			name:  "system marker",
			actor: testtaskjournal.TaskActorSystem,
			params: map[string]string{
				TaskAutomaticReconcileParam: "true",
			},
			want: true,
		},
		{
			name:  "operator marker",
			actor: testtaskjournal.TaskActorOperator,
			params: map[string]string{
				TaskAutomaticReconcileParam: "true",
			},
		},
		{
			name:  "system without marker",
			actor: testtaskjournal.TaskActorSystem,
		},
		{
			name:  "system false marker",
			actor: testtaskjournal.TaskActorSystem,
			params: map[string]string{
				TaskAutomaticReconcileParam: "false",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			task := TaskRecord{Actor: test.actor, Params: test.params}
			if got := IsAutomaticReconcileTask(task); got != test.want {
				t.Fatalf("IsAutomaticReconcileTask() = %t, want %t", got, test.want)
			}
		})
	}
}
