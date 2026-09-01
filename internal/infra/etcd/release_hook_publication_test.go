package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
)

// Rationale: an initial Release has no predecessor execution from which to
// recover Script-set ownership. Its post-deploy hook must bind that immutable
// storage identity from the same fixed-revision sources used to build the plan.
func TestPrepareReleaseHookPublicationBindsInitialPostDeployScriptSetGeneration(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	record := scriptCheckpointTestRecord(at)
	generationID := record.ID
	revision := int64(41)
	script, err := NewScriptRecord(record.EnvironmentID, record.ServiceID, core.Script{
		ID: record.ScriptID, Slug: "migrate", ServiceName: "api",
		When: core.ScriptPostDeploy, Body: "exit 0",
	})
	if err != nil {
		t.Fatal(err)
	}
	script.ScriptSetGeneration = generationID
	sources := ScriptExecutionSources{
		Revision: revision,
		Tenant: Versioned[TenantRecord]{ReadRevision: revision},
		Project: Versioned[ProjectRecord]{ReadRevision: revision},
		Environment: Versioned[EnvironmentRecord]{
			Record: EnvironmentRecord{ID: record.EnvironmentID}, ReadRevision: revision,
		},
		Service: Versioned[ServiceRecord]{
			Record: ServiceRecord{Desired: core.Service{ID: record.ServiceID}}, ReadRevision: revision,
		},
		ScriptSet: Versioned[ScriptSetGenerationRecord]{
			Record: ScriptSetGenerationRecord{EnvironmentID: record.EnvironmentID, GenerationID: generationID},
			Revision: 31, ReadRevision: revision,
		},
		Script: Versioned[ScriptRecord]{Record: script, Revision: 32, ReadRevision: revision},
		BodyGeneration: Versioned[ScriptBodyGenerationRecord]{
			Record: ScriptBodyGenerationRecord{
				ScriptID: record.ScriptID, Generation: record.ScriptGeneration, BodySHA256: record.BodySHA256,
			},
			Revision: 33, ReadRevision: revision,
		},
		Release: CurrentSuccessfulRelease{
			Intent: domain.Intent{ID: record.ReleaseID}, Revision: revision,
		},
		RenderInput: Versioned[ReleaseRenderInput]{
			Record: ReleaseRenderInput{
				ReleaseID: record.ReleaseID,
				Projection: EnvironmentComposeProjection{RenderGeneration: record.RenderGeneration},
			},
			ReadRevision: revision,
		},
		DesiredHead: Versioned[EnvironmentBlueprintHead]{
			Record: EnvironmentBlueprintHead{EnvironmentID: record.EnvironmentID, RevisionID: record.CurrentTaskID},
			Revision: 34, ReadRevision: revision,
		},
		DesiredProjection: Versioned[EnvironmentComposeProjection]{
			Record: EnvironmentComposeProjection{
				EnvironmentID: record.EnvironmentID, RevisionID: record.CurrentTaskID,
				RenderGeneration: record.RenderGeneration,
			},
			Revision: 35, ReadRevision: revision,
		},
	}
	evidence := ReleasePublicationEvidence{
		Manifest: VersionedReleaseManifest{ReadRevision: revision},
		Task: TaskRecord{
			ID: record.CurrentTaskID, OperationID: record.OperationID, PlanHash: record.PlanHash,
			Params: map[string]string{ReleaseHookStepExecutionParam(record.StepID): record.ID},
		},
		Hooks: []ReleaseHookExecutionPublication{{Sources: sources, Execution: record}},
	}
	fragment, err := prepareReleaseHookPublicationFragment(evidence)
	if err != nil {
		t.Fatalf("prepareReleaseHookPublicationFragment() error = %v", err)
	}
	defer clearReleaseHookPublicationFragment(fragment)
	if len(fragment.mutations) == 0 || fragment.mutations[0].Key != scriptExecutionKey(record.ID) {
		t.Fatalf("release hook mutations = %#v", fragment.mutations)
	}
	stored, err := decodeEnvelope[ScriptExecutionRecord](fragment.mutations[0].Value, "script-execution")
	if err != nil {
		t.Fatalf("decode stored Script execution: %v", err)
	}
	if stored.ScriptSetGeneration != generationID || record.ScriptSetGeneration != "" {
		t.Fatalf("stored Script-set generation = %q; source record = %q", stored.ScriptSetGeneration, record.ScriptSetGeneration)
	}
}
