package taskplanning

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

type blueprintVolumeUnitPlanReader struct {
	*blueprintPlanReader
	desired    testenvironmentprojection.EnvironmentDesiredInput
	identities testenvironmentprojection.EnvironmentOwnedIdentities
}

func (reader *blueprintVolumeUnitPlanReader) GetEnvironmentDesiredInputRevision(
	context.Context,
	string,
	string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentDesiredInput], bool, error) {
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentDesiredInput]{Record: reader.desired}, true, nil
}

func (reader *blueprintVolumeUnitPlanReader) GetEnvironmentOwnedIdentitiesRevision(
	context.Context,
	string,
	string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentOwnedIdentities], bool, error) {
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentOwnedIdentities]{
		Record: reader.identities,
	}, true, nil
}

// BP-10 / VOL-01: a private newborn Volume child must reconstruct the same
// directory-first, single-Volume plan from its immutable parent revision after
// Controller restart, without including any workload or network effect.
func TestBlueprintVolumeUnitReconstructsPinnedAuthoredVolume(t *testing.T) {
	base, parent := blueprintPlanTestState(t)
	environmentID := base.environment.ID
	volumeID := base.projection.Volumes[0].ID
	normalized := []byte(`name: authored
volumes:
  app-data:
    x-gp-slug: app-data
`)
	reader := &blueprintVolumeUnitPlanReader{
		blueprintPlanReader: base,
		desired: testenvironmentprojection.EnvironmentDesiredInput{
			EnvironmentID: environmentID, RevisionID: parent.ID, RenderGeneration: 1,
			Input: core.BlueprintDesiredInput{NormalizedCompose: normalized},
		},
		identities: testenvironmentprojection.EnvironmentOwnedIdentities{
			EnvironmentID: environmentID, RevisionID: parent.ID, RenderGeneration: 1,
			Volumes: []testenvironmentprojection.OwnedIdentity{{
				ID: volumeID, Name: "app-data", Slug: "app-data", BirthRevisionID: parent.ID,
			}},
		},
	}
	owner, err := testtaskjournal.EnvironmentTaskOwner(base.project, base.environment)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 25, 12, 30, 0, 0, time.UTC)
	intent := sha256.Sum256([]byte("protected Blueprint intent"))
	task := etcd.TaskRecord{
		ID: ids.NewAt(ids.KindTask, at, 10), OperationID: ids.NewAt(ids.KindOperation, at, 11),
		Owner: owner, Actor: testtaskjournal.TaskActorSystem, Executor: testtaskjournal.TaskExecutorAgent,
		PlanID: ids.NewAt(ids.KindPlan, at, 12), RenderGeneration: 1,
		Type: testtaskjournal.TaskUpdate, Target: volumeID,
		Params: map[string]string{
			testblueprints.EnvironmentDesiredRevisionParam: parent.ID,
			testtaskjournal.TaskBlueprintParentParam:       parent.ID,
			testtaskjournal.TaskBlueprintVolumeUnitParam:   volumeID,
			testtaskjournal.TaskBlueprintVolumeModeParam:   testtaskjournal.TaskBlueprintVolumeModeCreate,
			testtaskjournal.TaskBlueprintVolumeIntentParam: hex.EncodeToString(intent[:]),
		},
		TimeoutSeconds: 120, Status: testtaskjournal.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: at, UpdatedAt: at,
	}
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	prepared, first, err := resolver.PrepareBlueprintVolumeUnit(context.Background(), task)
	if err != nil {
		t.Fatalf("PrepareBlueprintVolumeUnit() error = %v", err)
	}
	second, err := resolver.ResolveExecutionPlan(context.Background(), prepared)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	directories := first.GetSteps()[0].GetManagedVolumeDirectoriesEnsure()
	ensure := first.GetSteps()[1].GetManagedVolumeEnsure()
	artifact := first.GetArtifacts()[0]
	if !bytes.Equal(first.GetPlanHash(), second.GetPlanHash()) || !proto.Equal(first, second) ||
		first.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY ||
		first.GetTargetId() != environmentID || len(first.GetArtifacts()) != 1 ||
		len(artifact.GetVolumes()) != 1 || len(artifact.GetServices()) != 0 || len(artifact.GetNetworks()) != 0 ||
		len(first.GetSteps()) != 2 || directories.GetArtifactId() != artifact.GetArtifactId() ||
		len(directories.GetVolumeIds()) != 1 || directories.GetVolumeIds()[0] != volumeID ||
		!bytes.Equal(directories.GetIntentSha256(), intent[:]) || ensure.GetArtifactId() != artifact.GetArtifactId() ||
		ensure.GetVolumeId() != volumeID || ensure.GetRequireExisting() ||
		first.GetSteps()[1].GetPrerequisiteStepId() != first.GetSteps()[0].GetStepId() {
		t.Fatalf("Blueprint Volume plans diverged: first=%#v second=%#v", first, second)
	}
}
