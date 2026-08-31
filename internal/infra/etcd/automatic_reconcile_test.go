package etcd

import "testing"

// Rationale: only a Controller-authored marker with the bounded system actor
// may authorize automatic reconciliation; an operator task is never automatic.
func TestIsAutomaticReconcileTaskRequiresSystemActor(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		actor  TaskActor
		params map[string]string
		want   bool
	}{
		{
			name:  "system marker",
			actor: TaskActorSystem,
			params: map[string]string{
				TaskAutomaticReconcileParam: "true",
			},
			want: true,
		},
		{
			name:  "operator marker",
			actor: TaskActorOperator,
			params: map[string]string{
				TaskAutomaticReconcileParam: "true",
			},
		},
		{
			name:  "system without marker",
			actor: TaskActorSystem,
		},
		{
			name:  "system false marker",
			actor: TaskActorSystem,
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
