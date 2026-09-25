package taskplanning

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type blueprintNetworkUnitPlanReader struct {
	*blueprintPlanReader
	desired    testenvironmentprojection.EnvironmentDesiredInput
	identities testenvironmentprojection.EnvironmentOwnedIdentities
}

func (reader *blueprintNetworkUnitPlanReader) GetEnvironmentDesiredInputRevision(
	context.Context,
	string,
	string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentDesiredInput], bool, error) {
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentDesiredInput]{Record: reader.desired}, true, nil
}

func (reader *blueprintNetworkUnitPlanReader) GetEnvironmentOwnedIdentitiesRevision(
	context.Context,
	string,
	string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentOwnedIdentities], bool, error) {
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentOwnedIdentities]{
		Record: reader.identities,
	}, true, nil
}

// BP-10 / NET-01: a private Zone child must reconstruct the same single-Network
// plan from its parent revision after Controller restart, without selecting a
// later runtime projection or including workload effects.
func TestBlueprintNetworkUnitReconstructsPinnedAuthoredZone(t *testing.T) {
	base, parent := blueprintPlanTestState(t)
	environmentID := base.environment.ID
	networkID := base.projection.DesiredZones[0].Desired.ID
	normalized := []byte(`name: authored
networks:
  frontend:
    driver: bridge
    internal: true
    ipam:
      config:
        - subnet: 10.40.0.0/24
`)
	reader := &blueprintNetworkUnitPlanReader{
		blueprintPlanReader: base,
		desired: testenvironmentprojection.EnvironmentDesiredInput{
			EnvironmentID: environmentID, RevisionID: parent.ID, RenderGeneration: 1,
			Input: core.BlueprintDesiredInput{
				NormalizedCompose: normalized,
				NetworkPool:       base.environment.NetworkPool,
			},
		},
		identities: testenvironmentprojection.EnvironmentOwnedIdentities{
			EnvironmentID: environmentID, RevisionID: parent.ID, RenderGeneration: 1,
			Networks: []testenvironmentprojection.OwnedIdentity{{
				ID: networkID, Name: "frontend", BirthRevisionID: parent.ID,
			}},
		},
	}
	owner, err := testtaskjournal.EnvironmentTaskOwner(base.project, base.environment)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	task := etcd.TaskRecord{
		ID: ids.NewAt(ids.KindTask, at, 10), OperationID: ids.NewAt(ids.KindOperation, at, 11),
		Owner: owner, Actor: testtaskjournal.TaskActorSystem, Executor: testtaskjournal.TaskExecutorAgent,
		PlanID: ids.NewAt(ids.KindPlan, at, 12), RenderGeneration: 1,
		Type: testtaskjournal.TaskUpdate, Target: networkID,
		Params: map[string]string{
			testblueprints.EnvironmentDesiredRevisionParam: parent.ID,
			testtaskjournal.TaskBlueprintParentParam:       parent.ID,
			testtaskjournal.TaskBlueprintNetworkUnitParam:  networkID,
		},
		TimeoutSeconds: 120, Status: testtaskjournal.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: at, UpdatedAt: at,
	}
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	prepared, first, err := resolver.PrepareBlueprintNetworkUnit(context.Background(), task)
	if err != nil {
		t.Fatalf("PrepareBlueprintNetworkUnit() error = %v", err)
	}
	second, err := resolver.ResolveExecutionPlan(context.Background(), prepared)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	ensure := first.GetSteps()[0].GetManagedNetworkEnsure()
	if !bytes.Equal(first.GetPlanHash(), second.GetPlanHash()) || !proto.Equal(first, second) ||
		first.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY ||
		first.GetTargetId() != environmentID || len(first.GetArtifacts()) != 1 ||
		len(first.GetArtifacts()[0].GetNetworks()) != 1 || len(first.GetArtifacts()[0].GetServices()) != 0 ||
		ensure.GetNetworkId() != networkID || ensure.GetArtifactId() != first.GetArtifacts()[0].GetArtifactId() {
		t.Fatalf("Blueprint Network plans diverged: first=%#v second=%#v", first, second)
	}
}
