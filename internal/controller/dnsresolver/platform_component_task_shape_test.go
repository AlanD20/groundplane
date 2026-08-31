package dnsresolver

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestComponentTaskEnsureServiceUsesSealedProcedureShape(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		count     int
		want      bool
		wantError bool
	}{
		{name: "in-place update", count: 2},
		{name: "service ensure", count: 4, want: true},
		{name: "invalid", count: 3, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			task := etcd.TaskRecord{Steps: make([]etcd.TaskStepRecord, test.count)}
			got, err := componentTaskEnsureService(task)
			if (err != nil) != test.wantError || got != test.want {
				t.Fatalf("componentTaskEnsureService() = %t, %v", got, err)
			}
		})
	}
}
