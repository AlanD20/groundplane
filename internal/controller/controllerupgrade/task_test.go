package controllerupgrade

import (
	"encoding/json"
	"testing"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: native recovery must replay exact immutable inputs from its Task,
// not mutable installation configuration or a subsequently staged manifest.
func TestNativeUpdateTaskFreezesValidatedRecoveryInput(t *testing.T) {
	input := nativeTestInput(t)
	now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	task, err := NewTask(now, "native-update-key", input)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := DecodeTask(task, now, now.Add(600*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if journal.TaskID != task.ID || journal.Manifest != input.Manifest || journal.Release != input.Release ||
		journal.PreviousController != input.PreviousController || *journal.Agent != *input.Agent {
		t.Fatal("native update Task changed its frozen recovery input")
	}
	input.Agent.Generation++
	if journal.Agent.Generation == input.Agent.Generation {
		t.Fatal("Task decoding retained a mutable input alias")
	}
}

// Rationale: changes to the manifest, execution authority or deadline cannot
// smuggle a new executable or extend a committed native operation's budget.
func TestNativeUpdateTaskRejectsChangedAuthority(t *testing.T) {
	for _, scenario := range []string{"hash", "target", "executor", "extra", "timeout", "manifest", "deadline"} {
		t.Run(scenario, func(t *testing.T) {
			input := nativeTestInput(t)
			now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
			task, err := NewTask(now, "native-update-key", input)
			if err != nil {
				t.Fatal(err)
			}
			deadline := now.Add(600 * time.Second)
			switch scenario {
			case "hash":
				task.PlanHash = string(upgrade.Hash([]byte("other")))[7:]
			case "target":
				task.Target = ids.New(ids.KindAgent)
			case "executor":
				task.Executor = testtaskjournal.TaskExecutorAgent
			case "extra":
				task.Params["path"] = "/other/controller"
			case "timeout":
				task.TimeoutSeconds++
			case "manifest":
				input.Manifest.ControllerVersion = "changed"
				raw, err := json.Marshal(input)
				if err != nil {
					t.Fatal(err)
				}
				canonical, err := jcs.Canonicalize(raw)
				if err != nil {
					t.Fatal(err)
				}
				task.Params[InputParam] = string(canonical)
			case "deadline":
				deadline = deadline.Add(time.Second)
			}
			if _, err := DecodeTask(task, now, deadline); err == nil {
				t.Fatal("changed native recovery authority accepted")
			}
		})
	}
}

func nativeTestInput(t *testing.T) Input {
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
	return Input{
		Release: upgrade.Hash(canonical), Manifest: journal.Manifest, PreviousController: journal.PreviousController,
		Agent: &upgrade.AgentPredecessor{ID: ids.New(ids.KindAgent), Generation: 1,
			Image: "registry.example/agent@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
	}
}
