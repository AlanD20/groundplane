package blueprint

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
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
		[]testkeyvalue.Versioned[testattachments.Record]{{
			Record:   testattachments.Record{ID: attachID, Name: "api-db", TaskID: taskID},
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

// TASK-127: inherited dependencies must come from the native captured snapshot,
// not a gate the new publisher never writes or a later descendant resolution.
func TestEnvironmentBlueprintRequirementsInheritsNativeCapturedGraph(t *testing.T) {
	at := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	environment := ids.NewAt(ids.KindEnvironment, at, 1)
	root := ids.NewAt(ids.KindTask, at, 2)
	producer := ids.NewAt(ids.KindTask, at, 3)
	ancestor := ids.NewAt(ids.KindTask, at, 4)
	oldest := ids.NewAt(ids.KindTask, at, 5)
	requirements := func(taskID string) core.BlueprintRequirements {
		authored := core.Requirement{
			Target:    core.RequirementTarget{Kind: core.RequirementTargetBackingAttach, Name: "db"},
			Condition: core.RequirementReady,
			Phases:    []core.RequirementPhase{core.RequirementPhaseDeploy},
		}
		return core.BlueprintRequirements{ResolutionRevision: 37, Authored: []core.Requirement{authored},
			Resolved: []core.ResolvedRequirement{{Target: core.ResolvedRequirementTarget{
				Kind: authored.Target.Kind, Name: authored.Target.Name, ID: ids.NewAt(ids.KindAttach, at, 6),
				TaskID: taskID, Revision: 31}, Condition: authored.Condition, Phases: authored.Phases}}}
	}
	for _, cycle := range []bool{false, true} {
		name := "captured ancestry"
		predecessor := ancestor
		inherited := []core.BlueprintRequirementTaskEdge{{PrerequisiteTaskID: oldest, DependentTaskID: ancestor}}
		if cycle {
			name, predecessor, inherited = "cycle", root, nil
		}
		t.Run(name, func(t *testing.T) {
			authority, err := etcd.NewBlueprintPrerequisiteAuthority(
				environment,
				producer,
				requirements(predecessor),
				inherited,
			)
			if err != nil {
				t.Fatal(err)
			}
			repository := &nativePrerequisiteSnapshotReader{environment: environment, taskID: producer, revision: 37,
				authority: authority}
			edges, err := environmentBlueprintRequirementTaskEdges(
				t.Context(),
				repository,
				environment,
				root,
				requirements(producer),
				37,
			)
			if cycle {
				if err == nil {
					t.Fatal("native transitive dependency cycle was ignored")
				}
				return
			}
			if err != nil || len(edges) != 2 || repository.calls != 1 ||
				!slices.Contains(
					edges,
					core.BlueprintRequirementTaskEdge{PrerequisiteTaskID: ancestor, DependentTaskID: producer},
				) ||
				!slices.Contains(
					edges,
					core.BlueprintRequirementTaskEdge{PrerequisiteTaskID: oldest, DependentTaskID: ancestor},
				) {
				t.Fatalf("native captured ancestry was lost or reread: %v, calls=%d", err, repository.calls)
			}
		})
	}
}

type nativePrerequisiteSnapshotReader struct {
	environmentBlueprintRepository
	environment string
	taskID      string
	revision    int64
	authority   etcd.BlueprintPrerequisiteAuthority
	calls       int
}

func (reader *nativePrerequisiteSnapshotReader) GetBlueprintPrerequisiteAuthority(_ context.Context,
	environment, taskID string, revision int64,
) (testkeyvalue.Versioned[etcd.BlueprintPrerequisiteAuthority], bool, error) {
	reader.calls++
	if environment != reader.environment || taskID != reader.taskID || revision != reader.revision {
		return testkeyvalue.Versioned[etcd.BlueprintPrerequisiteAuthority]{}, false,
			errs.New(errs.KindStateConflict, "prerequisite escaped its captured Environment or revision")
	}
	return testkeyvalue.Versioned[etcd.BlueprintPrerequisiteAuthority]{Record: reader.authority,
		Revision: 32, ReadRevision: revision}, true, nil
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
