package controller

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

var managedConfigContributionTestTime = time.Unix(1_700_000_000, 0).UTC()

func TestBuildEnvironmentComponentTaskContributionOmitsAbsentCandidate(t *testing.T) {
	apply := EnvironmentManagedConfigApplyInput{
		RevisionID:       ids.NewAt(ids.KindTask, managedConfigContributionTestTime, 1),
		RenderGeneration: 1,
		Artifact: &agentpb.ComposeArtifact{
			ArtifactId: ids.NewAt(ids.KindConfig, managedConfigContributionTestTime, 2),
			OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
			OwnerId:    ids.NewAt(ids.KindEnvironment, managedConfigContributionTestTime, 3),
		},
	}
	allocated := false
	steps, records, err := BuildEnvironmentComponentTaskContribution(EnvironmentComponentTaskContributionInput{
		Apply: apply,
		AllocateStep: func() string {
			allocated = true
			return ids.NewAt(ids.KindStep, managedConfigContributionTestTime, 4)
		},
		TimeoutSeconds: 120,
	})
	if err != nil {
		t.Fatalf("BuildEnvironmentComponentTaskContribution() error = %v", err)
	}
	if allocated || len(steps) != 0 || len(records) != 0 {
		t.Fatalf("absent managed-config contribution = allocated %t, %d steps, %d records", allocated, len(steps), len(records))
	}
}

// Rationale: router configuration must never activate until the reconciled
// service topology has completed its Compose apply step.
func TestAppendEnvironmentComponentTaskContributionRequiresComposeFirst(t *testing.T) {
	t.Parallel()

	composeID := ids.NewAt(ids.KindStep, managedConfigContributionTestTime, 5)
	activateID := ids.NewAt(ids.KindStep, managedConfigContributionTestTime, 6)
	compose := &agentpb.ExecutionStep{
		StepId: composeID,
		Payload: &agentpb.ExecutionStep_ComposeApply{
			ComposeApply: &agentpb.ComposeApply{ArtifactId: "cfg_01M0ZJAQH5YE4KVB81DW6G0M3W"},
		},
	}
	activate := &agentpb.ExecutionStep{StepId: activateID}

	steps, records, err := AppendEnvironmentComponentTaskContribution(
		[]*agentpb.ExecutionStep{compose},
		[]etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: composeID}},
		[]*agentpb.ExecutionStep{activate},
		[]etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: activateID}},
	)
	if err != nil {
		t.Fatalf("AppendEnvironmentComponentTaskContribution() error = %v", err)
	}
	if len(steps) != 2 || steps[0].GetComposeApply() == nil || steps[1].GetStepId() != activateID ||
		len(records) != 2 || records[0].ID != composeID || records[1].ID != activateID {
		t.Fatal("managed configuration did not remain after Compose apply")
	}
}

func TestBuildEnvironmentComponentTaskContributionRequiresStepAllocator(t *testing.T) {
	_, _, err := BuildEnvironmentComponentTaskContribution(EnvironmentComponentTaskContributionInput{TimeoutSeconds: 120})
	if err == nil {
		t.Fatal("BuildEnvironmentComponentTaskContribution() accepted missing step allocator")
	}
}
