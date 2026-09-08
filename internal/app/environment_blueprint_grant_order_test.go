package app

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: grants between new owners must not depend on their authored name order.
func TestEnvironmentBlueprintNewGrantTargetSurvivesPreparationOrder(t *testing.T) {
	registerAdapters()
	for _, targetName := range []string{"a-target", "z-target"} {
		t.Run(targetName, func(t *testing.T) {
			now := time.Date(2026, 9, 5, 16, 0, 0, 0, time.UTC)
			environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
			taskID := ids.NewAt(ids.KindTask, now, 2)
			projectID := ids.NewAt(ids.KindProject, now, 3)
			backingID := ids.NewAt(ids.KindEnvironment, now, 4)
			serviceID := ids.NewAt(ids.KindService, now, 5)
			crypt := attachFactTestCrypt{}
			protector, err := secretvalue.NewProtector(crypt, crypt)
			if err != nil {
				t.Fatal(err)
			}
			facts, err := NewAttachFactService(&attachFactTestRepository{}, protector)
			if err != nil {
				t.Fatal(err)
			}
			repository := &environmentBlueprintAttachRepositoryStub{
				backingProject: etcd.Versioned[etcd.ProjectRecord]{
					Record:   etcd.ProjectRecord{ID: projectID, Slug: "database", Kind: etcd.ProjectKindBacking},
					Revision: 21, ReadRevision: 41,
				},
				backingEnvironment: etcd.Versioned[etcd.EnvironmentRecord]{
					Record: etcd.EnvironmentRecord{ID: backingID, ProjectID: projectID, Name: "main",
						ProvisioningState: etcd.EnvironmentProvisioningReady},
					Revision: 22, ReadRevision: 41,
				},
				backingService: etcd.Versioned[etcd.ServiceRecord]{
					Record: etcd.ServiceRecord{EnvironmentID: backingID,
						BackingNetworkID: ids.NewAt(ids.KindNetwork, now, 6),
						Desired:          core.Service{ID: serviceID, Name: "postgres", Adapter: "postgres:16"},
						Runtime: core.ServiceRuntime{
							ServiceID:     serviceID,
							RuntimeIntent: core.ServiceRuntimeIntentRunning,
						}},
					Revision: 23, ReadRevision: 41,
				},
			}
			service := &environmentBlueprintService{
				repository: repository, attachFacts: facts, random: strings.NewReader(strings.Repeat("x", 256)),
			}
			specs := map[string]core.AttachmentSpec{
				targetName: {BackingProject: "database", BackingService: "postgres", Service: "target",
					Credential: core.AttachmentCredentialSpec{Mode: "new"}},
				"m-reader": {BackingProject: "database", BackingService: "postgres", Service: "reader",
					Credential: core.AttachmentCredentialSpec{Mode: "new"}, Grants: []string{targetName}},
			}
			changes := []etcd.EnvironmentBlueprintServiceChange{
				{Record: etcd.ServiceRecord{EnvironmentID: environmentID,
					Desired: core.Service{ID: ids.NewAt(ids.KindService, now, 7), Name: "target"}}},
				{Record: etcd.ServiceRecord{EnvironmentID: environmentID,
					Desired: core.Service{ID: ids.NewAt(ids.KindService, now, 8), Name: "reader"}}},
			}
			prepared, err := service.prepareBlueprintAttaches(context.Background(), environmentID, taskID,
				specs, changes, nil, func(kind ids.Kind, purpose string) string {
					return ids.DeriveAt(kind, now, taskID, purpose)
				}, false, now)
			if err != nil {
				t.Fatalf("new grant preparation: %v", err)
			}
			defer prepared.clear()
			if len(prepared.procedures) != 2 {
				t.Fatal("expected both credential owners")
			}
			targetDatabase := ""
			for _, procedure := range prepared.procedures {
				if procedure.record.Name == targetName {
					targetDatabase = procedure.identity.Database
				}
			}
			for _, procedure := range prepared.procedures {
				if procedure.record.Name == "m-reader" && (len(procedure.identity.Grants) != 1 ||
					procedure.identity.Grants[0].Database != targetDatabase || targetDatabase == "") {
					t.Fatal("grant does not retain target database identity")
				}
			}
		})
	}
}
