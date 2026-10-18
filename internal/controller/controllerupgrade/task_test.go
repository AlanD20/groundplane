package controllerupgrade

import (
	"encoding/json"
	"testing"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
)

// Rationale: a native recovery journal must own captured predecessor identity,
// not share a mutable Agent pointer with its caller.
func TestNativeUpdateCaptureFreezesRecoveryInput(t *testing.T) {
	input := nativeTestInput(t)
	now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	capture := nativeTestCapture(t, input, now)
	task, item, err := capture.Work()
	if err != nil {
		t.Fatal(err)
	}
	if err := capture.Match(item); err != nil {
		t.Fatal(err)
	}
	journal, err := capture.Journal(now, task.Deadline)
	if err != nil {
		t.Fatal(err)
	}
	if journal.TaskID != task.ID || journal.Manifest != input.Manifest || journal.Release != input.Release ||
		journal.PreviousController != input.PreviousController || *journal.Agent != *input.Agent {
		t.Fatal("captured recovery input changed")
	}
	input.Agent.Generation++
	if journal.Agent.Generation == input.Agent.Generation {
		t.Fatal("journal aliases caller Agent predecessor")
	}
}

// Rationale: changed execution/input ownership cannot authorize another binary,
// resource, host or budget under the accepted Controller software action.
func TestNativeUpdateCaptureRejectsChangedAuthority(t *testing.T) {
	for _, scenario := range []string{"hash", "target", "executor", "work item", "task", "claims", "manifest", "deadline"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
			capture := nativeTestCapture(t, nativeTestInput(t), now)
			task, item, err := capture.Work()
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "deadline" {
				if _, err := capture.Journal(now, task.Deadline.Add(time.Second)); err == nil {
					t.Fatal("extended activation deadline accepted")
				}
				return
			}
			switch scenario {
			case "hash":
				item.Input.SHA256 = string(upgrade.Hash([]byte("other")))[7:]
			case "target":
				item.TargetID = ids.New(ids.KindAgent)
			case "executor":
				item.Placement = workexecution.Placement{Executor: workexecution.Agent, HostID: ids.New(ids.KindHost)}
			case "work item":
				item.ID = ids.New(ids.KindWorkItem)
			case "task":
				item.TaskID = ids.New(ids.KindTask)
			case "claims":
				item.Claims = nil
			case "manifest":
				capture.Input.Manifest.ControllerVersion = "changed"
			}
			if err := capture.Match(item); err == nil {
				t.Fatal("changed native activation authority accepted")
			}
		})
	}
}

func nativeTestCapture(t *testing.T, input upgrade.Input, now time.Time) controllersoftware.Capture {
	t.Helper()
	return controllersoftware.Capture{TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem),
		PlanID: ids.New(ids.KindPlan), Input: input, CreatedAt: now}
}

func nativeTestInput(t *testing.T) upgrade.Input {
	t.Helper()
	journal := watchdogJournal()
	raw, err := json.Marshal(journal.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		t.Fatal(err)
	}
	return upgrade.Input{
		Release: upgrade.Hash(canonical), Manifest: journal.Manifest, PreviousController: journal.PreviousController,
		Agent: &upgrade.AgentPredecessor{ID: ids.New(ids.KindAgent), Generation: 1,
			Image: "registry.example/agent@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
	}
}
