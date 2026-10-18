package controllerupgrade

import (
	"context"
	"errors"
	"testing"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a crash after Prepare but before activation must not leave an
// expired Task permanently paused in preparation or activate an untouched host.
func TestCoordinatorExpiredPreparedJournalCancelsWithoutCutover(t *testing.T) {
	h := newCoordinatorHarness(t)
	h.store.prepareExpected(h.expected)
	h.now = h.expected.Deadline.Add(time.Hour)
	ctx := context.Background()
	coordinator := h.coordinator(t, h.input.PreviousController)
	if err := coordinator.Restore(ctx, h.expected); err != nil {
		t.Fatal(err)
	}
	status, err := coordinator.Execute(ctx, h.expected, false, h.progress(ctx))
	if err != nil || status.Outcome != workexecution.Failed || !status.Settled || status.Failure == nil ||
		h.store.current().Phase != upgrade.PhaseCancelled || h.unit.launches != 0 || len(h.agents.goals) != 0 {
		t.Fatalf("expired preparation = %s, %v, journal %s", status.Outcome, err, h.store.current().Phase)
	}
}

// Rationale: the installed path can name the candidate while the outgoing
// process still runs. Only the actual running executable can qualify a trial.
func TestCoordinatorRejectsCandidateWithPredecessorProcessIdentity(t *testing.T) {
	h := newCoordinatorHarness(t)
	h.store.prepareExpected(h.expected)
	h.store.startCandidate()
	ctx, cancel := context.WithCancel(context.Background())
	h.unit.launch = cancel
	coordinator := h.coordinator(t, h.input.PreviousController)
	if err := coordinator.Restore(ctx, h.expected); err != nil {
		t.Fatal(err)
	}
	status, err := coordinator.Execute(ctx, h.expected, true, h.progress(ctx))
	if !errors.Is(err, context.Canceled) || status.Outcome != "" ||
		h.store.current().Phase != upgrade.PhaseRollingBack ||
		len(h.agents.goals) != 0 {
		t.Fatalf("wrong process qualification = %s, %v", status.Outcome, err)
	}
}

// Rationale: a trial without healthy etcd cannot establish operation ownership
// or perform an Agent replacement, even when both listeners successfully bound.
func TestCoordinatorStorageReadinessFailureDoesNotMutateAgent(t *testing.T) {
	h := newCoordinatorHarness(t)
	h.store.prepareExpected(h.expected)
	h.store.startCandidate()
	ctx, cancel := context.WithCancel(context.Background())
	h.unit.launch = cancel
	coordinator := h.coordinator(t, h.input.Manifest.ControllerSHA256)
	coordinator.readiness.storage = &readinessStorage{err: errs.New(errs.KindStorageUnavailable, "unavailable")}
	if err := coordinator.Restore(ctx, h.expected); err != nil {
		t.Fatal(err)
	}
	status, err := coordinator.Execute(ctx, h.expected, true, h.progress(ctx))
	if !errors.Is(err, context.Canceled) || status.Outcome != "" ||
		h.store.current().Phase != upgrade.PhaseRollingBack ||
		len(h.agents.goals) != 0 {
		t.Fatalf("unready qualification = %s, %v", status.Outcome, err)
	}
}

// Rationale: a confirmed qualification survives lost Task acknowledgement and
// deadline expiry. A restarted candidate must never roll back that healthy win.
func TestCoordinatorHealthyReplayCompletesAfterDeadline(t *testing.T) {
	h := newCoordinatorHarness(t)
	h.store.prepareExpected(h.expected)
	h.store.startCandidate()
	h.store.journal.Phase = upgrade.PhaseHealthy
	h.now = h.expected.Deadline.Add(time.Hour)
	coordinator := h.coordinator(t, h.input.Manifest.ControllerSHA256)
	if err := coordinator.Restore(context.Background(), h.expected); err != nil {
		t.Fatal(err)
	}
	status, err := coordinator.Execute(
		context.Background(),
		h.expected,
		true,
		h.progress(context.Background()),
	)
	if err != nil || status.Outcome != workexecution.Succeeded || h.unit.launches != 0 ||
		h.store.current().Phase != upgrade.PhaseHealthy {
		t.Fatalf("healthy replay = %s, %v", status.Outcome, err)
	}
}

// Rationale: runtime/config drift before activation invalidates the frozen
// predecessor; it must fail without publishing a journal or stopping services.
func TestCoordinatorChangedPredecessorFailsWithoutActivation(t *testing.T) {
	for _, changed := range []string{"installed", "agent", "manifest"} {
		t.Run(changed, func(t *testing.T) {
			h := newCoordinatorHarness(t)
			switch changed {
			case "installed":
				h.store.installed = upgrade.Hash([]byte("other"))
			case "agent":
				h.agents.agent.Generation++
			case "manifest":
				h.store.manifest.ControllerVersion = "other"
			}
			coordinator := h.coordinator(t, h.input.PreviousController)
			if err := coordinator.Restore(context.Background(), h.expected); err != nil {
				t.Fatal(err)
			}
			status, err := coordinator.Execute(
				context.Background(),
				h.expected,
				true,
				h.progress(context.Background()),
			)
			if err != nil || status.Outcome != workexecution.Failed || h.store.found ||
				h.unit.launches != 0 ||
				len(h.agents.goals) != 0 {
				t.Fatalf("changed predecessor = %s, %v", status.Outcome, err)
			}
		})
	}
}
