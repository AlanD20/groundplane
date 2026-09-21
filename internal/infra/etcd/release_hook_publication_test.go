package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testreleasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testscriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	testscriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

// Rationale: an initial Release has no predecessor execution from which to
// recover Script-set ownership. Its post-deploy hook must bind that immutable
// storage identity from the same fixed-revision sources used to build the plan.
func TestPrepareReleaseHookPublicationBindsInitialPostDeployScriptSetGeneration(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	record := withScriptContextSnapshot(t, scriptCheckpointTestRecord(at))
	generationID := record.ID
	revision := int64(41)
	script, err := testscripts.NewRecord(record.EnvironmentID, record.ServiceID, core.Script{
		ID: record.ScriptID, Slug: "migrate", ServiceName: "api",
		When: core.ScriptPostDeploy, Body: "exit 0",
	})
	if err != nil {
		t.Fatal(err)
	}
	script.ScriptSetGeneration = generationID
	sources := testscriptsourcequeries.ScriptExecutionSources{
		Revision: revision,
		Tenant:   testkeyvalue.Versioned[testhierarchy.TenantRecord]{ReadRevision: revision},
		Project:  testkeyvalue.Versioned[testhierarchy.ProjectRecord]{ReadRevision: revision},
		Environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{ID: record.EnvironmentID}, ReadRevision: revision,
		},
		Service: testkeyvalue.Versioned[testservices.ServiceRecord]{
			Record: testservices.ServiceRecord{Desired: core.Service{ID: record.ServiceID}}, ReadRevision: revision,
		},
		ScriptSet: testkeyvalue.Versioned[testscripts.SetGenerationRecord]{
			Record:   testscripts.SetGenerationRecord{EnvironmentID: record.EnvironmentID, GenerationID: generationID},
			Revision: 31, ReadRevision: revision,
		},
		Script: testkeyvalue.Versioned[testscripts.Record]{Record: script, Revision: 32, ReadRevision: revision},
		BodyGeneration: testkeyvalue.Versioned[testscripts.BodyGenerationRecord]{
			Record: testscripts.BodyGenerationRecord{
				ScriptID: record.ScriptID, Generation: record.ScriptGeneration, BodySHA256: record.BodySHA256,
			},
			Revision: 33, ReadRevision: revision,
		},
		Release: testreleasequeries.ServingRelease{
			Intent: domain.Intent{ID: record.ReleaseID}, Revision: revision,
		},
		RenderInput: testkeyvalue.Versioned[testreleaserender.ReleaseRenderInput]{
			Record: testreleaserender.ReleaseRenderInput{
				ReleaseID: record.ReleaseID,
				Projection: testenvironmentprojection.EnvironmentComposeProjection{
					RenderGeneration: record.RenderGeneration,
				},
			},
			ReadRevision: revision,
		},
		DesiredHead: testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintHead]{
			Record: testblueprints.EnvironmentBlueprintHead{
				EnvironmentID: record.EnvironmentID,
				RevisionID:    record.CurrentTaskID,
			},
			Revision: 34, ReadRevision: revision,
		},
		DesiredProjection: testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
			Record: testenvironmentprojection.EnvironmentComposeProjection{
				EnvironmentID: record.EnvironmentID, RevisionID: record.CurrentTaskID,
				RenderGeneration: record.RenderGeneration,
			},
			Revision: 35, ReadRevision: revision,
		},
	}
	evidence := ReleasePublicationEvidence{
		Manifest: testreleases.VersionedReleaseManifest{ReadRevision: revision},
		Task: TaskRecord{
			ID: record.CurrentTaskID, OperationID: record.OperationID, PlanHash: record.PlanHash,
			Params: map[string]string{testreleaserender.ReleaseHookStepExecutionParam(record.StepID): record.ID},
		},
		Hooks: []ReleaseHookExecutionPublication{{Sources: sources, Execution: record}},
	}
	fragment, err := prepareReleaseHookPublicationFragment(evidence)
	if err != nil {
		t.Fatalf("prepareReleaseHookPublicationFragment() error = %v", err)
	}
	defer clearReleaseHookPublicationFragment(fragment)
	if len(fragment.mutations) == 0 || fragment.mutations[0].Key != testscriptexecutions.ScriptExecutionKey(record.ID) {
		t.Fatalf("release hook mutations = %#v", fragment.mutations)
	}
	stored, err := testrecordcodec.Decode[testscriptexecutions.ScriptExecutionRecord](
		fragment.mutations[0].Value,
		"script-execution",
	)
	if err != nil {
		t.Fatalf("decode stored Script execution: %v", err)
	}
	if stored.ScriptSetGeneration != generationID || record.ScriptSetGeneration != "" {
		t.Fatalf(
			"stored Script-set generation = %q; source record = %q",
			stored.ScriptSetGeneration,
			record.ScriptSetGeneration,
		)
	}
}
