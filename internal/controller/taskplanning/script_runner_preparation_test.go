package taskplanning

import (
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testlocalagents "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	testscriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type scriptPreparationAgent struct{ calls int }

func (agent *scriptPreparationAgent) GetSingleton(
	context.Context,
) (testkeyvalue.Versioned[testlocalagents.LocalAgentRecord], error) {
	agent.calls++
	return testkeyvalue.Versioned[testlocalagents.LocalAgentRecord]{
		Record: testlocalagents.LocalAgentRecord{ID: "setup-agent"},
	}, nil
}

type scriptPreparationImages struct {
	calls     int
	reference string
	fail      bool
	corrupt   bool
}

func (images *scriptPreparationImages) ResolveWorkloadImages(
	_ context.Context, agentID string, selectors []*agentpb.WorkloadImageSelector,
) (*agentpb.WorkloadImageResolutionResult, error) {
	images.calls++
	if agentID != "setup-agent" || len(selectors) != 1 {
		return nil, errs.New(errs.KindInternal, "unexpected setup image request")
	}
	images.reference = selectors[0].GetRequestedReference()
	if images.fail {
		return nil, errs.New(errs.KindWorkloadImageResolutionUnavailable, "fixture image unavailable")
	}
	selector := proto.CloneOf(selectors[0])
	if images.corrupt {
		selector = nil
	}
	return &agentpb.WorkloadImageResolutionResult{
		RequestId: strings.Repeat("1", 32),
		Outcome: &agentpb.WorkloadImageResolutionResult_Success{Success: &agentpb.WorkloadImageResolutions{
			Resolutions: []*agentpb.WorkloadImageResolution{
				{Selector: selector, LocalImageId: "sha256:" + strings.Repeat("c", 64)},
			},
		}},
	}, nil
}

func scriptPreparationSources() testscriptsourcequeries.ScriptExecutionSources {
	sources := explicitScriptEntrySources()
	sources.Revision, sources.Script.Revision, sources.Script.ReadRevision = 23, 17, 23
	sources.DesiredProjection.Revision, sources.DesiredProjection.ReadRevision = 19, 23
	return sources
}

// Rationale: only a closed authenticated image response can prepare an explicit
// runner; invalid resources and unavailable images cannot reach secret reads.
func TestScriptRunnerPreparationAuthenticatesBeforeEntryReads(t *testing.T) {
	for _, scenario := range []string{"success", "inherited", "invalid resource", "missing image", "corrupt image", "mixed revision"} {
		t.Run(scenario, func(t *testing.T) {
			sources := scriptPreparationSources()
			agent, images, values := &scriptPreparationAgent{}, &scriptPreparationImages{}, &explicitScriptEntryValues{}
			switch scenario {
			case "inherited":
				sources.Script.Record.Desired.Execution = &core.ScriptExecution{Mode: core.ScriptExecutionInherited}
			case "invalid resource":
				sources.DesiredProjection.Record.Volumes = nil
			case "missing image":
				images.fail = true
			case "corrupt image":
				images.corrupt = true
			case "mixed revision":
				sources.Script.ReadRevision++
			}
			service, err := NewScriptRunnerPreparationService(&ScriptArtifactService{values: values}, agent, images)
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := service.Prepare(context.Background(), sources)
			success := scenario == "success" || scenario == "inherited"
			if !success {
				if err == nil || len(values.ids) != 0 || prepared.sourceDigest != ([32]byte{}) {
					t.Fatalf("failed preparation yielded authority or read values: %v", err)
				}
				if (scenario == "invalid resource" || scenario == "mixed revision") && images.calls != 0 {
					t.Fatal("invalid sources reached image resolution")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := prepared.validateSources(sources); err != nil {
				t.Fatal(err)
			}
			if scenario == "inherited" {
				if agent.calls != 0 || images.calls != 0 || prepared.explicit != nil || len(prepared.bindings) != 3 {
					t.Fatal("inherited preparation acquired a new image or changed Entry access")
				}
			} else if images.calls != 1 || images.reference != sources.Script.Record.Desired.Execution.Image ||
				prepared.image.LocalImageID != "sha256:"+strings.Repeat("c", 64) || len(prepared.bindings) != 2 {
				t.Fatal("explicit image or Entry selection was substituted")
			}
		})
	}
}

// Rationale: copying an opaque preparation into another capture, or changing its
// mutable source metadata after preparation, must never transfer authority.
func TestScriptRunnerPreparationBindsExactSource(t *testing.T) {
	for _, edit := range []func(*testscriptsourcequeries.ScriptExecutionSources){
		func(s *testscriptsourcequeries.ScriptExecutionSources) {
			s.Script.Record.Desired.Execution.Image = "example/other@sha256:" + strings.Repeat("d", 64)
		},
		func(s *testscriptsourcequeries.ScriptExecutionSources) {
			s.Script.Record.Desired.Execution.User = "1:1"
		},
		func(s *testscriptsourcequeries.ScriptExecutionSources) {
			s.Script.Record.Desired.ID = "scr_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		},
		func(s *testscriptsourcequeries.ScriptExecutionSources) { s.Script.Revision++ },
		func(s *testscriptsourcequeries.ScriptExecutionSources) {
			s.Revision++
			s.Script.ReadRevision++
			s.DesiredProjection.ReadRevision++
		},
		func(s *testscriptsourcequeries.ScriptExecutionSources) {
			s.DesiredProjection.Record.Entries[0].CurrentValueGenerationID = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		},
		func(s *testscriptsourcequeries.ScriptExecutionSources) {
			s.DesiredProjection.Record.Entries[1].Entry.Path = "etc/tls/other.conf"
		},
	} {
		sources := scriptPreparationSources()
		service, err := NewScriptRunnerPreparationService(
			&ScriptArtifactService{values: &explicitScriptEntryValues{}},
			&scriptPreparationAgent{},
			&scriptPreparationImages{},
		)
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := service.Prepare(context.Background(), sources)
		if err != nil {
			t.Fatal(err)
		}
		edit(&sources)
		if prepared.validateSources(sources) == nil {
			t.Fatal("preparation accepted a changed source")
		}
	}
	if (ScriptRunnerPreparation{}).validateSources(scriptPreparationSources()) == nil {
		t.Fatal("unprepared explicit sources acquired authority")
	}
}
