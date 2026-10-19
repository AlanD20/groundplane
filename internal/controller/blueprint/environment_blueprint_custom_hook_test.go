package blueprint

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

// QA: BACK-15, TASK-104. The ordinary candidate builder cannot substitute a
// network-only Attach for a Custom hook. Authored hook-generation capture is
// a separate native path, not prohibited by this lower-level refusal.
func TestBlueprintCandidateBuilderRefusesCustomHookSubstitution(t *testing.T) {
	registerAdapters()
	now := time.Date(2026, 9, 20, 18, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	consumerID := ids.NewAt(ids.KindService, now, 2)
	backingProjectID := ids.NewAt(ids.KindProject, now, 3)
	backingEnvironmentID := ids.NewAt(ids.KindEnvironment, now, 4)
	backingServiceID := ids.NewAt(ids.KindService, now, 5)
	repository := &environmentBlueprintAttachRepositoryStub{
		backingProject: testkeyvalue.Versioned[testhierarchy.ProjectRecord]{Record: testhierarchy.ProjectRecord{
			ID: backingProjectID, Kind: testhierarchy.ProjectKindBacking,
		}},
		backingEnvironment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{
				ID: backingEnvironmentID, ProjectID: backingProjectID, Name: "main",
				ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
			},
		},
		backingService: testkeyvalue.Versioned[testservices.ServiceRecord]{Record: testservices.ServiceRecord{
			EnvironmentID: backingEnvironmentID, BackingNetworkID: ids.NewAt(ids.KindNetwork, now, 6),
			Desired: core.Service{ID: backingServiceID, Name: "shared", Adapter: "custom",
				Hooks: &backinghook.Configuration{Attach: &backinghook.Definition{
					Command: []string{"/opt/provision"}, TimeoutSeconds: 30,
				}}},
			Runtime: core.ServiceRuntime{RuntimeIntent: core.ServiceRuntimeIntentRunning},
		}},
	}
	service := &Service{repository: repository}
	prepared, err := service.prepareBlueprintAttachCandidates(context.Background(), environmentID,
		ids.NewAt(ids.KindTask, now, 7), map[string]core.AttachmentSpec{
			"shared": {BackingProject: "shared", BackingService: "shared", Service: "worker",
				Credential: core.AttachmentCredentialSpec{Mode: "new"}},
		}, []testblueprints.EnvironmentBlueprintServiceChange{{Record: testservices.ServiceRecord{
			EnvironmentID: environmentID, Desired: core.Service{ID: consumerID, Name: "worker"},
		}}}, nil, func(ids.Kind, string) string {
			t.Fatal("rejected hook Attach allocated candidate identity")
			return ""
		}, now)
	defer prepared.clear()
	if err == nil || !strings.Contains(err.Error(), "create the custom Attach directly") {
		t.Fatalf("expected standalone-first guidance, got %v", err)
	}
	if len(prepared.candidates) != 0 || len(prepared.effective) != 0 {
		t.Fatal("rejected hook Attach produced publication or execution work")
	}
}
