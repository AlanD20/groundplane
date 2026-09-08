package etcd

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

func TestTaskComponentAuthoritySurvivesStorageAndRetry(t *testing.T) {
	now := taskJournalTime()
	task := validTaskRecord(now)
	task.ComponentActionStepIDs = []string{task.Steps[0].ID}
	task.ManagedComponentTeardownSources = []ManagedComponentRuntimeSource{{
		ComponentKind: core.ComponentKindIngressCaddy, ComponentID: ids.NewAt(ids.KindComponent, now, 10),
		ServiceID: ids.NewAt(ids.KindService, now, 11), ComposeName: "caddy",
		RevisionID: ids.NewAt(ids.KindTask, now, 12), ArtifactID: ids.NewAt(ids.KindConfig, now, 13),
		ArtifactSHA256: strings.Repeat("a", 64),
	}}
	encoded, err := encodeTaskRecord(task)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := decodeTaskRecord(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(task.ComponentActionStepIDs, stored.ComponentActionStepIDs) {
		t.Fatal("storage lost Component action authority")
	}
	if !reflect.DeepEqual(task.ManagedComponentTeardownSources, stored.ManagedComponentTeardownSources) {
		t.Fatal("storage lost managed Component teardown authority")
	}
	running, err := transitionTaskStatus(stored, TaskStatusPending, TaskStatusRunning, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	failed, err := transitionTaskStatus(running, TaskStatusRunning, TaskStatusFailed, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	retry, err := cloneRetryTask(failed, ids.NewAt(ids.KindTask, now, 99), TaskActorOperator, now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(retry.ComponentActionStepIDs, task.ComponentActionStepIDs) ||
		!reflect.DeepEqual(retry.ManagedComponentTeardownSources, task.ManagedComponentTeardownSources) ||
		retry.PlanHash != task.PlanHash || retry.PlanID != task.PlanID {
		t.Fatal("retry changed sealed effect authority")
	}
	retry.ComponentActionStepIDs[0] = "changed"
	if failed.ComponentActionStepIDs[0] != task.Steps[0].ID {
		t.Fatal("retry aliases prior Component authority")
	}
	retry.ManagedComponentTeardownSources[0].ServiceID = "changed"
	if failed.ManagedComponentTeardownSources[0].ServiceID == "changed" {
		t.Fatal("retry aliases managed Component teardown authority")
	}
	for _, ids := range [][]string{{"missing"}, {task.Steps[0].ID, task.Steps[0].ID}} {
		changed := cloneTaskRecord(task)
		changed.ComponentActionStepIDs = ids
		if _, err := encodeTaskRecord(changed); err == nil {
			t.Fatalf("accepted invalid effect authority: %v", ids)
		}
	}
}
