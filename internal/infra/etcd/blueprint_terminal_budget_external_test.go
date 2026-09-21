package etcd_test

import (
	"context"
	"fmt"
	"testing"

	testtaskmaterialization "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: the real QA startup publishes ten native candidates successfully;
// its terminal ACK must atomically promote them without an ordinary-budget trap.
func TestBlueprintTenCandidateProducerCompletes(t *testing.T) {
	for _, count := range []int{10, 32} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			testBlueprintExecutedArtifact(t, false, false, func(fixture *etcd.ExecutedArtifactFixture,
				resolver *testtaskplanning.TaskPlanResolver, prior testreleaserender.ReleaseRenderInput, _ domain.Intent,
				applied *agentpb.ComposeArtifact) {
				proveFullMixedProducer(t, fixture, resolver, prior, applied, false, false, count, 0, false)
			})
		})
	}
}

// Rationale: reconnect must resume a real producer's closed hook completion,
// never execute an obsolete synthetic plan or reactivate source references.
func TestBlueprintActualClosingHookReconnect(t *testing.T) {
	testBlueprintExecutedArtifact(t, false, false, func(fixture *etcd.ExecutedArtifactFixture,
		resolver *testtaskplanning.TaskPlanResolver, prior testreleaserender.ReleaseRenderInput, _ domain.Intent,
		applied *agentpb.ComposeArtifact) {
		proveFullMixedProducer(t, fixture, resolver, prior, applied, false, false, 1, 1, true)
	})
}

// Rationale: the terminal envelope must fit the full candidate and hook
// cardinality together, including the final bounded source-release fragment.
func TestBlueprintMaximumCandidatesAndHooksComplete(t *testing.T) {
	testBlueprintExecutedArtifact(t, false, false, func(fixture *etcd.ExecutedArtifactFixture,
		resolver *testtaskplanning.TaskPlanResolver, prior testreleaserender.ReleaseRenderInput, _ domain.Intent,
		applied *agentpb.ComposeArtifact) {
		proveFullMixedProducer(t, fixture, resolver, prior, applied, false, false, 32, 16, false)
	})
}

// Rationale: source closure must retain the original recovery ACK, including
// its epoch and recovery digest, rather than persisting the normalized failure.
func TestBlueprintHookRecoveryTerminalReport(t *testing.T) {
	testBlueprintExecutedArtifact(t, false, false, func(fixture *etcd.ExecutedArtifactFixture,
		resolver *testtaskplanning.TaskPlanResolver, prior testreleaserender.ReleaseRenderInput, _ domain.Intent,
		applied *agentpb.ComposeArtifact) {
		proveFullMixedProducer(t, fixture, resolver, prior, applied, false, true, 1, 1, false)
	})
}

type unexpectedHookEntryResolver struct{ t *testing.T }

// Rationale: real terminal composition must remain atomic on compare loss and
// replay read-only after a committed transaction loses its response.
func TestBlueprintClosingHookTerminalFaults(t *testing.T) {
	for _, fault := range []string{"compare-loss", "uncertain-commit"} {
		t.Run(fault, func(t *testing.T) {
			testBlueprintExecutedArtifact(t, false, false, func(fixture *etcd.ExecutedArtifactFixture,
				resolver *testtaskplanning.TaskPlanResolver, prior testreleaserender.ReleaseRenderInput, _ domain.Intent,
				applied *agentpb.ComposeArtifact) {
				fixture.TerminalCommitFault = fault
				proveFullMixedProducer(t, fixture, resolver, prior, applied, false, false, 10, 1, true)
			})
		})
	}
}

func (resolver unexpectedHookEntryResolver) ResolveTaskMaterializationSource(
	context.Context, string, testtaskmaterialization.Source,
) ([]byte, error) {
	resolver.t.Fatal("entry-free hook fixture unexpectedly requested an Entry value")
	return nil, nil
}
