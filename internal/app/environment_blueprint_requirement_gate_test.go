package app

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestEnvironmentBlueprintRequirementsUsesFixedSnapshot(t *testing.T) {
	// Rationale: apply binds the label before staging and retains its fixed revision.
	t.Parallel()
	at := time.Date(2026, 9, 2, 14, 0, 0, 0, time.UTC)
	attachID := ids.NewAt(ids.KindAttach, at, 1)
	taskID := ids.NewAt(ids.KindTask, at, 2)
	requirements, err := environmentBlueprintRequirements(
		[]core.Requirement{{
			Target:    core.RequirementTarget{Kind: core.RequirementTargetBackingAttach, Name: "api-db"},
			Condition: core.RequirementReady, Phases: []core.RequirementPhase{core.RequirementPhaseDeploy},
		}},
		nil,
		[]etcd.Versioned[etcd.AttachRecord]{{
			Record:   etcd.AttachRecord{ID: attachID, Name: "api-db", TaskID: taskID},
			Revision: 71, ReadRevision: 71,
		}},
		71,
	)
	if err != nil {
		t.Fatal(err)
	}
	if requirements.ResolutionRevision != 71 || requirements.Resolved[0].Target.ID != attachID ||
		requirements.Resolved[0].Target.TaskID != taskID || requirements.Resolved[0].Target.Revision != 71 {
		t.Fatalf("requirements = %#v", requirements)
	}
}

func TestEnvironmentBlueprintRequirementsRejectsSelfDependency(t *testing.T) {
	// Rationale: a requirement cannot wait on an Attach that the same Task creates.
	t.Parallel()
	_, err := environmentBlueprintRequirements(
		[]core.Requirement{{
			Target:    core.RequirementTarget{Kind: core.RequirementTargetBackingAttach, Name: "api-db"},
			Condition: core.RequirementReady, Phases: []core.RequirementPhase{core.RequirementPhaseDeploy},
		}},
		map[string]core.AttachmentSpec{"api-db": {}}, nil, 33,
	)
	if err == nil {
		t.Fatal("self dependency accepted")
	}
}
