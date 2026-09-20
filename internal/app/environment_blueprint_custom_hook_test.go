package app

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// QA: BACK-15. Rationale: Blueprint must not publish a network-only substitute
// for an Attach that requires an operator hook, or allocate incomplete facts.
func TestBlueprintRejectsNewCustomHookAttachBeforePreparation(t *testing.T) {
	registerAdapters()
	now := time.Date(2026, 9, 20, 18, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	consumerID := ids.NewAt(ids.KindService, now, 2)
	backingProjectID := ids.NewAt(ids.KindProject, now, 3)
	backingEnvironmentID := ids.NewAt(ids.KindEnvironment, now, 4)
	backingServiceID := ids.NewAt(ids.KindService, now, 5)
	repository := &environmentBlueprintAttachRepositoryStub{
		backingProject: etcd.Versioned[etcd.ProjectRecord]{Record: etcd.ProjectRecord{
			ID: backingProjectID, Kind: etcd.ProjectKindBacking,
		}},
		backingEnvironment: etcd.Versioned[etcd.EnvironmentRecord]{Record: etcd.EnvironmentRecord{
			ID: backingEnvironmentID, ProjectID: backingProjectID, Name: "main",
			ProvisioningState: etcd.EnvironmentProvisioningReady,
		}},
		backingService: etcd.Versioned[etcd.ServiceRecord]{Record: etcd.ServiceRecord{
			EnvironmentID: backingEnvironmentID, BackingNetworkID: ids.NewAt(ids.KindNetwork, now, 6),
			Desired: core.Service{ID: backingServiceID, Name: "shared", Adapter: "custom",
				Hooks: &backinghook.Configuration{Attach: &backinghook.Definition{
					Command: []string{"/opt/provision"}, TimeoutSeconds: 30,
				}}},
			Runtime: core.ServiceRuntime{RuntimeIntent: core.ServiceRuntimeIntentRunning},
		}},
	}
	service := &environmentBlueprintService{repository: repository}
	prepared, err := service.prepareBlueprintAttaches(context.Background(), environmentID,
		ids.NewAt(ids.KindTask, now, 7), map[string]core.AttachmentSpec{
			"shared": {BackingProject: "shared", BackingService: "shared", Service: "worker",
				Credential: core.AttachmentCredentialSpec{Mode: "new"}},
		}, []etcd.EnvironmentBlueprintServiceChange{{Record: etcd.ServiceRecord{
			EnvironmentID: environmentID, Desired: core.Service{ID: consumerID, Name: "worker"},
		}}}, nil, func(ids.Kind, string) string {
			t.Fatal("rejected hook Attach allocated candidate identity")
			return ""
		}, false, now)
	defer prepared.clear()
	if err == nil || !strings.Contains(err.Error(), "create the custom Attach directly") {
		t.Fatalf("expected standalone-first guidance, got %v", err)
	}
	if len(prepared.publication.Intent.Candidates) != 0 || len(prepared.procedures) != 0 {
		t.Fatal("rejected hook Attach produced publication or execution work")
	}
}
