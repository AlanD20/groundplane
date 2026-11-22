package localagent

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the native journal phase and executing Controller jointly decide
// whether Agent recovery may advance, restore, or only settle prior effects.
func TestPreparedUpdateAuthorityBindsProcessPhaseAndCandidateRollback(t *testing.T) {
	request, journal := preparedAuthorityFixture(t)
	for _, scenario := range []struct {
		name           string
		phase          upgrade.Phase
		goal           UpdateGoal
		process        upgrade.Digest
		allowsRollback bool
	}{
		{name: "starting candidate", phase: upgrade.PhaseStarting, goal: UpdateFinish,
			process: journal.Manifest.ControllerSHA256},
		{name: "healthy settlement", phase: upgrade.PhaseHealthy, goal: UpdateFinish,
			process: journal.Manifest.ControllerSHA256},
		{name: "rolled back predecessor", phase: upgrade.PhaseRolledBack, goal: UpdateRestore,
			process: journal.PreviousController, allowsRollback: true},
		{name: "recovered settlement", phase: upgrade.PhaseRecovered, goal: UpdateRestore,
			process: journal.PreviousController},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			candidate := journal
			candidate.Phase = scenario.phase
			candidate.Failure = nil
			if scenario.goal == UpdateRestore {
				candidate.Failure = taskprogress.CaptureControllerFailure(errors.New("candidate failed"))
			}
			authority, err := AuthorizePreparedUpdate(candidate, scenario.process, request, scenario.goal)
			if err != nil {
				t.Fatal(err)
			}
			if err := authority.Validate(request, scenario.goal); err != nil {
				t.Fatalf("authorized identity did not validate: %v", err)
			}
			if authority.AllowsCandidateRollback() != scenario.allowsRollback {
				t.Fatalf("AllowsCandidateRollback() = %t", authority.AllowsCandidateRollback())
			}
		})
	}

	journal.Phase = upgrade.PhaseRolledBack
	journal.Failure = taskprogress.CaptureControllerFailure(errors.New("candidate failed"))
	for _, scenario := range []struct {
		name    string
		process upgrade.Digest
		request UpdateRequest
		goal    UpdateGoal
	}{
		{name: "candidate process restoring", process: journal.Manifest.ControllerSHA256,
			request: request, goal: UpdateRestore},
		{name: "wrong generation", process: journal.PreviousController,
			request: UpdateRequest{AgentID: request.AgentID, PreviousImage: request.PreviousImage,
				DesiredImage: request.DesiredImage, StartingGeneration: request.StartingGeneration + 1}, goal: UpdateRestore},
		{name: "wrong goal", process: journal.PreviousController, request: request, goal: UpdateFinish},
	} {
		t.Run("reject "+scenario.name, func(t *testing.T) {
			if _, err := AuthorizePreparedUpdate(journal, scenario.process, scenario.request, scenario.goal); !errors.Is(
				err,
				errs.New(errs.KindStateConflict, ""),
			) {
				t.Fatalf("unsafe authority = %v", err)
			}
		})
	}
}

func preparedAuthorityFixture(t *testing.T) (UpdateRequest, upgrade.Journal) {
	t.Helper()
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	request := UpdateRequest{AgentID: testAgentID, PreviousImage: testImage,
		DesiredImage: replacementTestImage, StartingGeneration: initialGeneration}
	journal := upgrade.Journal{
		Schema: 2, TaskID: ids.NewAt(ids.KindTask, now, 1), Phase: upgrade.PhaseStarting,
		Release: upgrade.Hash([]byte("release")), PreviousController: upgrade.Hash([]byte("previous")),
		Manifest: upgrade.Manifest{
			Schema: upgrade.ManifestSchema, ControllerSHA256: upgrade.Hash([]byte("candidate")),
			ControllerVersion: "0.1.0", ControllerSource: buildversion.BuildProvenance{
				Kind: "release", Commit: strings.Repeat("c", 40),
			},
			AgentImage: request.DesiredImage, StorageEpoch: upgrade.StorageEpoch,
			ChannelSchema: agentprotocol.SchemaVersion,
		},
		Agent: &upgrade.AgentPredecessor{ID: request.AgentID, Image: request.PreviousImage,
			Generation: request.StartingGeneration},
		StartedAt: now, Deadline: now.Add(upgrade.TaskTimeoutSeconds * time.Second),
		TrialBootID: "12345678-1234-1234-1234-123456789abc",
	}
	if err := journal.Validate(); err != nil {
		t.Fatal(err)
	}
	return request, journal
}
