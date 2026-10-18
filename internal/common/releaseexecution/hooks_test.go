package releaseexecution

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// TASK-69: a post hook can initialize candidate readiness. Switching traffic or
// waiting for readiness first can expose an uninitialized workload or deadlock.
func TestNativeHooksRunBeforeReadinessAndCannotReplayAfterCheckpoint(t *testing.T) {
	for _, strategy := range []agentpb.WorkReleaseStrategy{
		agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE,
		agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_BLUE_GREEN,
	} {
		for _, rollback := range []bool{false, true} {
			t.Run(strategy.String()+map[bool]string{false: "/deploy", true: "/rollback"}[rollback], func(t *testing.T) {
				input := nativeHookProgressInput(rollback, strategy)
				at := *input.Attempt.StartedAt
				progress, err := NewProgress(input, at)
				if err != nil {
					t.Fatal(err)
				}
				var observed []string
				for count := 0; progress.Phase != Complete; count++ {
					if count > 20 {
						t.Fatal("Release failed to reach a finite completion")
					}
					before := progress
					at = at.Add(time.Second)
					progress, err = progress.Begin(input, at)
					if err != nil || ValidateTransition(input, &before, progress) != nil {
						t.Fatalf("begin: %v", err)
					}
					if _, err := progress.Begin(input, at); err == nil {
						t.Fatal("interrupted action received a second execution grant")
					}
					if progress.Phase == PreHooks || progress.Phase == PostHooks {
						hook, err := progress.ActiveHook(input)
						if err != nil {
							t.Fatal(err)
						}
						observed = append(observed, hook.ExecutionId)
						// An inspection copy must not rewrite captured hook authority.
						hook.ExecutionId = "changed"
						unchanged, err := progress.ActiveHook(input)
						if err != nil || unchanged.ExecutionId == "changed" {
							t.Fatal("hook selection aliases its returned copy")
						}
					} else if progress.Phase == Rollout {
						observed = append(observed, string(progress.Action))
					}
					before = progress
					proof := workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("b", 64)}
					progress, err = progress.Confirm(input, proof, at)
					if err != nil || ValidateTransition(input, &before, progress) != nil {
						t.Fatalf("confirm: %v", err)
					}
					stage, err := before.StageID(input)
					if err != nil {
						t.Fatal(err)
					}
					report := CheckpointReport{Sequence: progress.Sequence, StageID: stage, Progress: progress}
					request, err := EncodeCheckpointRequest(input, report)
					if err != nil {
						t.Fatal(err)
					}
					restored, err := DecodeCheckpointRequest(input, request)
					if err != nil || !EqualProgress(restored.Progress, progress) {
						t.Fatalf("saved boundary cannot resume: %v", err)
					}
					progress = restored.Progress
					if progress.Phase == PostHooks {
						bypass := progress
						bypass.Phase, bypass.Hook, bypass.Action = Rollout, 0, WaitCandidate
						if ValidateTransition(input, &before, bypass) == nil {
							t.Fatal("candidate readiness bypassed unfinished post hooks")
						}
					}
				}
				want := []string{input.Release.Hooks[4].ExecutionId, input.Release.Hooks[1].ExecutionId,
					string(StartCandidate), input.Release.Hooks[3].ExecutionId, input.Release.Hooks[0].ExecutionId,
					string(WaitCandidate), string(SwitchTraffic), string(VerifyServing)}
				if !slices.Equal(observed, want) {
					t.Fatalf("executed %v; want %v", observed, want)
				}
			})
		}
	}
}

func TestNativeReleaseWithoutHooksRetainsReadinessAndPriorRemoval(t *testing.T) {
	input := nativeHookProgressInput(false, agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE)
	input.Release.Hooks = nil
	input.Artifacts.Prior = &executionplan.NativeServingRuntime{}
	at := *input.Attempt.StartedAt
	progress, err := NewProgress(input, at)
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []RolloutAction{"", RemovePrevious, StartCandidate, WaitCandidate, SwitchTraffic, VerifyServing} {
		if progress.Action != action {
			t.Fatalf("action = %s, want %s", progress.Action, action)
		}
		at = at.Add(time.Second)
		progress, err = progress.Begin(input, at)
		if err != nil {
			t.Fatal(err)
		}
		progress, err = progress.Confirm(input, input.Attempt.Input, at)
		if err != nil {
			t.Fatal(err)
		}
	}
	if progress.Phase != Complete {
		t.Fatal("a Release without hooks did not complete after serving verification")
	}
}

// The operation cursor consumes a supplied decoded capture here. Delivery and
// Script execution are separate boundaries; this fixture does not prove either.
func nativeHookProgressInput(rollback bool, strategy agentpb.WorkReleaseStrategy) Input {
	at := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	input := Input{Attempt: workexecution.Attempt{
		ID: ids.New(ids.KindAttempt), TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem),
		Number: 1, Direction: workexecution.Forward, AssignedAt: at, StartedAt: &at, Deadline: at.Add(time.Minute),
		Input: workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)},
		Executor: workexecution.Identity{Executor: workexecution.Agent, HostID: ids.New(ids.KindHost),
			AgentID: ids.New(ids.KindAgent), Generation: 1},
	}, Release: &agentpb.ReleaseWorkInput{Strategy: strategy}}
	pre, post := "pre-deploy", "post-deploy"
	if rollback {
		pre, post = "pre-rollback", "post-rollback"
	}
	hook := func(phase string, order uint32) *agentpb.ReleaseWorkHook {
		return &agentpb.ReleaseWorkHook{
			Phase:       phase,
			Order:       order,
			ExecutionId: ids.NewULID(),
			ScriptId:    ids.New(ids.KindScript),
			Input: &agentpb.WorkInputReference{
				InputId: ids.New(ids.KindPlan),
				Sha256:  []byte(strings.Repeat("a", 32)),
			},
		}
	}
	input.Release.Hooks = []*agentpb.ReleaseWorkHook{
		hook(post, 20), hook(pre, 20), hook("on-failure", 0), hook(post, 10), hook(pre, 10),
	}
	input.Artifacts.Proxy = &agentpb.ComposeService{}
	return input
}
