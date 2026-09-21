package taskplanning

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

func TestBlueprintNetworkPreparationOwnsOnlyRenderedNetworksAndReplays(t *testing.T) {
	_, task := blueprintPlanTestState(t)
	ownedID := ids.NewAt(ids.KindNetwork, task.CreatedAt, 400)
	externalID := ids.NewAt(ids.KindNetwork, task.CreatedAt, 401)
	project := &composetypes.Project{
		Services: composetypes.Services{"api": {Image: "example/api:1"}},
		Networks: composetypes.Networks{
			"owned": {}, "attached": {External: true},
		},
	}
	input := composeRenderTestInput(project)
	input.Identities = testcomposeidentity.Snapshot{
		Services: []testcomposeidentity.Resource{{ID: ids.NewAt(ids.KindService, task.CreatedAt, 402), Name: "api"}},
		Networks: []testcomposeidentity.Resource{{ID: ownedID, Name: "owned"}},
	}
	input.ExternalNetworks = []testcomposeidentity.Resource{{ID: externalID, Name: "attached"}}
	artifact, err := testcomposerender.RenderCompose(input)
	if err != nil {
		t.Fatal(err)
	}
	updated, steps, err := prepareBlueprintReleaseNetworks(task, artifact, "attach-provisioned")
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 1 || steps[0].GetManagedNetworkEnsure().GetNetworkId() != ownedID ||
		steps[0].PrerequisiteStepId != "attach-provisioned" {
		t.Fatalf("network preparation = %v", steps)
	}
	if !strings.Contains(string(artifact.CanonicalYaml), "external: true") {
		t.Fatal("fixture lost external network")
	}
	if _, mutated := task.Params[blueprintNetworkPrepareStepsParam]; mutated {
		t.Fatal("preparation mutated caller Task params")
	}
	_, replayed, err := prepareBlueprintReleaseNetworks(updated, artifact, "attach-provisioned")
	if err != nil || !proto.Equal(steps[0], replayed[0]) {
		t.Fatalf("replay differs: %v", err)
	}
	updated.CreatedAt = updated.CreatedAt.AddDate(0, 0, 1)
	_, retried, err := prepareBlueprintReleaseNetworks(updated, artifact, "attach-provisioned")
	if err != nil || !proto.Equal(steps[0], retried[0]) {
		t.Fatalf("retry changed plan-owned network identity: %v", err)
	}
	updated.Params[blueprintNetworkPrepareStepsParam] = ids.NewAt(ids.KindStep, task.CreatedAt, 403)
	if _, _, err := prepareBlueprintReleaseNetworks(updated, artifact, "attach-provisioned"); err == nil {
		t.Fatal("accepted changed published network step identity")
	}
}

func TestBlueprintNetworkStepJournalRejectsMissingOrReorderedPreparation(t *testing.T) {
	_, task := blueprintPlanTestState(t)
	stepID := ids.NewAt(ids.KindStep, task.CreatedAt, 410)
	task.Params[blueprintNetworkPrepareStepsParam] = stepID
	task.Steps = []testtaskjournal.TaskStepRecord{{Kind: testtaskjournal.TaskStepOperation, ID: stepID}}
	for offset := int64(411); offset < 415; offset++ {
		task.Steps = append(
			task.Steps,
			testtaskjournal.TaskStepRecord{
				Kind: testtaskjournal.TaskStepOperation,
				ID:   ids.NewAt(ids.KindStep, task.CreatedAt, offset),
			},
		)
	}
	members := []testreleaserender.ReleaseTaskRenderMember{{}}
	if _, next, err := blueprintReleaseProcedureStepIDs(task, etcd.ReleaseTaskRenderInput{Members: members}, 0); err != nil ||
		next != len(task.Steps) {
		t.Fatalf("valid durable procedure rejected: next=%d error=%v", next, err)
	}
	task.Steps[0], task.Steps[1] = task.Steps[1], task.Steps[0]
	if _, _, err := blueprintReleaseProcedureStepIDs(task, etcd.ReleaseTaskRenderInput{Members: members}, 0); err == nil {
		t.Fatal("accepted unbound network preparation journal")
	}
	for _, encoded := range []string{"", stepID + "," + stepID, "invalid"} {
		task.Params[blueprintNetworkPrepareStepsParam] = encoded
		if _, err := blueprintReleaseNetworkStepIDs(task); err == nil {
			t.Fatalf("accepted malformed authority %q", encoded)
		}
	}
}
