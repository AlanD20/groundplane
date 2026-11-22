package localagent

import (
	"context"
	"errors"
	"testing"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestPreparedUpdateQualifiesRetainedEntryBeforeForwardReplacement(t *testing.T) {
	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	harness := newTestManager(t, repository, trace)
	request := recoveryRequest()
	authority := preparedUpdateTestAuthority(request, UpdateFinish, upgrade.PhaseStarting)

	result, err := harness.manager.RecoverPreparedUpdate(
		context.Background(), request, UpdateFinish, nil, authority,
	)
	if err != nil || result != UpdateReadyDesired {
		t.Fatalf("prepared forward recovery = %q, %v", result, err)
	}
	if harness.tasks.upgradeReadyCalls != 1 || harness.tasks.rollbackReadyCalls != 0 ||
		trace.contains("require_idle") || !containsOrdered(
		trace.values(), "require_upgrade_ready", "generate", "begin_replacement",
	) {
		t.Fatalf("prepared forward qualification order = %v", trace.values())
	}
	if repository.record.Record.Generation != initialGeneration+1 ||
		repository.record.Record.Image != replacementTestImage || repository.record.Record.Phase != PhaseReady {
		t.Fatalf("prepared forward record = %#v", repository.record.Record)
	}
}

func TestPreparedUpdateDoesNotRestoreStartingCandidateBeforeControllerRollback(t *testing.T) {
	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	harness := newTestManager(t, repository, trace)
	harness.sessions.ready = make(chan struct{})
	request := recoveryRequest()
	authority := preparedUpdateTestAuthority(request, UpdateFinish, upgrade.PhaseStarting)
	result := make(chan error, 1)
	go func() {
		_, err := harness.manager.RecoverPreparedUpdate(
			context.Background(), request, UpdateFinish, nil, authority,
		)
		result <- err
	}()
	harness.clock.awaitTimer(t).fire(testNow.Add(ReadyTimeout))
	if err := <-result; err == nil {
		t.Fatal("starting candidate failure restored the Agent before Controller rollback")
	}
	if repository.record.Record.Generation != initialGeneration+1 ||
		repository.record.Record.Image != replacementTestImage || repository.record.Record.Phase != PhaseUpdating ||
		harness.tasks.rollbackReadyCalls != 0 || countTrace(trace.values(), "begin_replacement") != 1 {
		t.Fatalf(
			"starting candidate changed rollback authority: record=%#v trace=%v",
			repository.record.Record,
			trace.values(),
		)
	}
}

func TestPreparedUpdateRollsBackOfflineCandidateWithoutAgentProbe(t *testing.T) {
	for _, phase := range []Phase{PhaseUpdating, PhaseReady} {
		t.Run(string(phase), func(t *testing.T) {
			trace := &traceLog{}
			repository := seededRepository(trace, phase)
			repository.record.Record.Generation = initialGeneration + 1
			repository.record.Record.Image = replacementTestImage
			harness := newTestManager(t, repository, trace)
			request := recoveryRequest()
			authority := preparedUpdateTestAuthority(request, UpdateRestore, upgrade.PhaseRolledBack)

			result, err := harness.manager.RecoverPreparedUpdate(
				context.Background(), request, UpdateRestore, nil, authority,
			)
			if err != nil || result != UpdateReadyPrevious {
				t.Fatalf("offline candidate rollback = %q, %v", result, err)
			}
			if harness.tasks.rollbackReadyCalls != 1 || harness.tasks.upgradeReadyCalls != 0 ||
				trace.contains("require_idle") || harness.tasks.rollbackOriginal != initialGeneration ||
				harness.tasks.rollbackCurrent != initialGeneration+1 || harness.tasks.rollbackMaximum != 2 ||
				!containsOrdered(
					trace.values(), "require_replacement_rollback_ready", "generate", "begin_replacement",
				) {
				t.Fatalf("offline rollback qualification = %v, tasks=%#v", trace.values(), harness.tasks)
			}
			if repository.record.Record.Generation != initialGeneration+2 ||
				repository.record.Record.Image != testImage || repository.record.Record.Phase != PhaseReady {
				t.Fatalf("offline rollback record = %#v", repository.record.Record)
			}
		})
	}
}

func TestPreparedUpdateRejectsWrongAuthorityBeforeMutation(t *testing.T) {
	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	harness := newTestManager(t, repository, trace)
	before := cloneStored(repository.record)

	result, err := harness.manager.RecoverPreparedUpdate(
		context.Background(), recoveryRequest(), UpdateFinish, nil, PreparedUpdateAuthority{},
	)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || result != "" {
		t.Fatalf("wrong prepared authority = %q, %v", result, err)
	}
	if repository.record.Revision != before.Revision {
		t.Fatal("invalid authority changed the durable record revision")
	}
	if repository.record.Record.Generation != before.Record.Generation ||
		harness.runtime.generateCalls != 0 ||
		harness.tasks.upgradeReadyCalls != 0 ||
		harness.tasks.rollbackReadyCalls != 0 ||
		len(trace.values()) != 0 {
		t.Fatalf(
			"wrong authority mutated state: before=%#v after=%#v trace=%v",
			before,
			repository.record,
			trace.values(),
		)
	}
}

func TestPreparedUpdateSettledPhasesCannotStartOrResumeReplacement(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		goal       UpdateGoal
		phase      upgrade.Phase
		generation uint64
		image      string
		agentPhase Phase
		wantResult UpdateResult
		wantError  bool
	}{
		{name: "healthy settles ready candidate", goal: UpdateFinish, phase: upgrade.PhaseHealthy,
			generation: initialGeneration + 1, image: replacementTestImage, agentPhase: PhaseReady,
			wantResult: UpdateReadyDesired},
		{name: "healthy rejects predecessor", goal: UpdateFinish, phase: upgrade.PhaseHealthy,
			generation: initialGeneration, image: testImage, agentPhase: PhaseReady, wantError: true},
		{name: "recovered settles unchanged predecessor", goal: UpdateRestore, phase: upgrade.PhaseRecovered,
			generation: initialGeneration, image: testImage, agentPhase: PhaseReady,
			wantResult: UpdateReadyPrevious},
		{name: "recovered rejects unfinished rollback", goal: UpdateRestore, phase: upgrade.PhaseRecovered,
			generation: initialGeneration + 2, image: testImage, agentPhase: PhaseUpdating, wantError: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			trace := &traceLog{}
			repository := seededRepository(trace, scenario.agentPhase)
			repository.record.Record.Generation = scenario.generation
			repository.record.Record.Image = scenario.image
			harness := newTestManager(t, repository, trace)
			request := recoveryRequest()
			authority := preparedUpdateTestAuthority(request, scenario.goal, scenario.phase)

			result, err := harness.manager.RecoverPreparedUpdate(
				context.Background(), request, scenario.goal, nil, authority,
			)
			if scenario.wantError {
				if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || result != "" {
					t.Fatalf("settled phase effect rejection = %q, %v", result, err)
				}
			} else if err != nil || result != scenario.wantResult {
				t.Fatalf("settled phase result = %q, %v", result, err)
			}
			if repository.record.Record.Generation != scenario.generation ||
				repository.record.Record.Image != scenario.image || repository.record.Record.Phase != scenario.agentPhase ||
				harness.runtime.generateCalls != 0 || harness.tasks.upgradeReadyCalls != 0 ||
				harness.tasks.rollbackReadyCalls != 0 || trace.contains("begin_replacement") {
				t.Fatalf(
					"settled native phase changed Agent lifecycle: record=%#v trace=%v",
					repository.record.Record,
					trace.values(),
				)
			}
		})
	}
}

func preparedUpdateTestAuthority(
	request UpdateRequest,
	goal UpdateGoal,
	phase upgrade.Phase,
) PreparedUpdateAuthority {
	return PreparedUpdateAuthority{
		taskID:  testRequest(testAgentID).EnrollmentTaskID,
		request: request,
		goal:    goal,
		phase:   phase,
		process: upgrade.Hash([]byte("prepared update test process")),
	}
}

func countTrace(values []string, expected string) int {
	count := 0
	for _, value := range values {
		if value == expected {
			count++
		}
	}
	return count
}
