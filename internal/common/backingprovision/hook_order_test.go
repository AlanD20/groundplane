package backingprovision

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// BACK-14: after-start cannot precede runtime readiness or repeat after an
// acknowledged result; a hook failure cannot be replaced by invented success.
func TestNativeAfterStartRequiresReadinessAndResumesAfterSavedResult(t *testing.T) {
	at := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	started := at
	reference := workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)}
	attempt := workexecution.Attempt{ID: ids.New(ids.KindAttempt), TaskID: ids.New(ids.KindTask),
		WorkItemID: ids.New(ids.KindWorkItem), Number: 1, Direction: workexecution.Forward,
		Input: reference, AssignedAt: at, StartedAt: &started, Deadline: at.Add(time.Minute),
		Executor: workexecution.Identity{Executor: workexecution.Agent,
			HostID: ids.New(ids.KindHost), AgentID: ids.New(ids.KindAgent), Generation: 1}}
	layout := Layout{Input: reference, ServiceID: ids.New(ids.KindService), Healthcheck: true, Hook: true}
	progress, err := NewProgress(attempt, layout, at)
	if err != nil {
		t.Fatal(err)
	}
	container := strings.Repeat("b", 64)
	observations := []Observation{
		{Stage: PrepareDirectories, DirectoryReady: true},
		{Stage: ReconcileRuntime, RuntimeReconciled: true, ContainerID: container},
		{Stage: WaitReady, RuntimeReady: true, Healthy: true, ContainerID: container},
		{
			Stage:       AfterStart,
			HookResult:  workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("c", 64)},
			ContainerID: container,
		},
		{Stage: VerifyRuntime, RuntimeVerified: true, RuntimeReady: true, Healthy: true, ContainerID: container},
	}
	for index, observation := range observations {
		if progress.Stage != observation.Stage {
			t.Fatalf("next stage=%s, expected=%s", progress.Stage, observation.Stage)
		}
		before := progress
		at = at.Add(time.Second)
		progress, err = progress.Begin(attempt, layout, at)
		if err != nil || ValidateTransition(attempt, layout, &before, progress) != nil {
			t.Fatalf("begin %s: %v", observation.Stage, err)
		}
		observation.ServiceID, observation.Input, observation.At = layout.ServiceID, reference, at
		if index < 3 {
			premature := progress
			premature.Stage = AfterStart
			if premature.Validate(attempt, layout) == nil {
				t.Fatal("hook acquired authority before runtime readiness")
			}
		}
		if observation.Stage == WaitReady {
			unhealthy := observation
			unhealthy.Healthy = false
			if _, err := progress.Confirm(attempt, layout, unhealthy, at); err == nil {
				t.Fatal("unhealthy runtime unlocked after-start")
			}
		}
		if observation.Stage == AfterStart {
			missingResult := observation
			missingResult.HookResult = workexecution.Input{}
			if _, err := progress.Confirm(attempt, layout, missingResult, at); err == nil {
				t.Fatal("hook completed without its recorded result")
			}
		}
		before = progress
		progress, err = progress.Confirm(attempt, layout, observation, at)
		if err != nil || ValidateTransition(attempt, layout, &before, progress) != nil {
			t.Fatalf("confirm %s: %v", observation.Stage, err)
		}
		raw, err := EncodeProgress(attempt, layout, progress)
		if err != nil {
			t.Fatal(err)
		}
		progress, err = DecodeProgress(attempt, layout, raw)
		if err != nil {
			t.Fatal(err)
		}
		if observation.Stage == AfterStart && progress.Stage != VerifyRuntime {
			t.Fatal("saved hook result replayed after-start")
		}
	}
	if progress.Stage != Complete {
		t.Fatal("runtime and hook observations did not complete the procedure")
	}
}
