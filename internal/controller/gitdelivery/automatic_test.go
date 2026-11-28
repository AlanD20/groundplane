package gitdelivery

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestAutomaticDeliveryReconcilesHeadTransitionsAndLatePushes(t *testing.T) {
	ctx := t.Context()
	service, binding, github := testService(t, &testReleases{})
	service.repository.store = &pushTestStore{service.repository.store.(*testStore)}
	initial, err := service.GetBinding(ctx, binding.ServiceID)
	if err != nil {
		t.Fatal(err)
	}
	binding.Automatic = true
	current, err := service.Bind(ctx, binding, initial.Revision)
	if err != nil {
		t.Fatal(err)
	}
	commitA, commitB := testCommit, strings.Repeat("c", 40)
	// Leave one old automatic receipt in the original per-SHA namespace to
	// exercise an in-place update of a Controller with deployment history.
	legacy := StartInput{IdempotencyKey: "branch." + strconv.FormatInt(current.Revision, 10) + "." + commitA,
		ExpectedCommit: commitA, Automatic: true}
	captured, err := service.repository.admission(ctx, binding.ServiceID, legacy)
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.begin(ctx, captured, legacy, nil)
	if err != nil {
		t.Fatal(err)
	}
	if repeated, err := service.ObserveHead(ctx, current, commitA); err != nil || repeated.ID != first.ID {
		t.Fatalf("upgrade did not preserve the existing head: got %s, %v", repeated.ID, err)
	}
	github.commit = commitB
	if _, err := service.ObserveHead(ctx, current, commitA); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("stale observation replayed the last deployment after GitHub moved: %v", err)
	}
	if err := service.pollBinding(ctx, binding.ServiceID); err != nil {
		t.Fatalf("branch poll did not admit the new head: %v", err)
	}
	second, err := service.ObserveHead(ctx, current, commitB)
	if err != nil || second.ID == first.ID || second.Commit != commitB {
		t.Fatalf("new head was not admitted: got %s (%s), %v", second.ID, second.Commit, err)
	}
	if _, err := service.ObserveHead(ctx, current, commitA); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("late push for the first head was not rejected: %v", err)
	}
	github.commit = commitA
	returned, err := service.ObserveHead(ctx, current, commitA)
	if err != nil || returned.ID == first.ID || returned.ID == second.ID || returned.Commit != commitA {
		t.Fatalf(
			"return to the first SHA did not create a new transition: got %s (%s), %v",
			returned.ID,
			returned.Commit,
			err,
		)
	}
	if repeated, err := service.ObserveHead(ctx, current, commitA); err != nil || repeated.ID != returned.ID {
		t.Fatalf("duplicate observation created a different deployment: got %s, %v", repeated.ID, err)
	}
	event := Push{
		ConnectionID: binding.ConnectionID, ConnectionScopeKind: binding.ConnectionScopeKind,
		ConnectionScopeID: binding.ConnectionScopeID, ConnectionGeneration: binding.ConnectionGeneration,
		InstallationID: binding.InstallationID, RepositoryID: binding.RepositoryID,
		Branch: binding.Branch, Commit: commitB,
	}
	if err := service.notifyPushTarget(ctx, binding.ServiceID, event); err != nil {
		t.Fatalf("out-of-order push was not discarded: %v", err)
	}
	page, err := service.List(ctx, binding.ServiceID, "", 10)
	if err != nil || len(page.Items) != 3 || github.dispatches != 0 {
		t.Fatalf("observations changed existing history or ran duplicate work: %d deployments, %d dispatches, %v",
			len(page.Items), github.dispatches, err)
	}
}
