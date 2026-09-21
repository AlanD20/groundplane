package blueprint

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

// QA: ATT-12; local Blueprint fact preparation, not live Docker DNS or authentication proof.
// Rationale: Blueprint-created owner and grant fact sets must select the same stable backing endpoint as the
// backing runtime alias, including while another same-name backing network remains attached.
func TestEnvironmentBlueprintAttachFactsUseStableBackingEndpoint(t *testing.T) {
	registerAdapters()
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	taskID := ids.NewAt(ids.KindTask, now, 2)
	projectID := ids.NewAt(ids.KindProject, now, 3)
	backingEnvironmentID := ids.NewAt(ids.KindEnvironment, now, 4)
	backingServiceID := ids.NewAt(ids.KindService, now, 5)
	crypt := blueprintTestCrypt{}
	protector, err := secretvalue.NewProtector(crypt, crypt)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := attachments.NewFactService(&blueprintAttachFactRepository{}, protector)
	if err != nil {
		t.Fatal(err)
	}
	repository := &environmentBlueprintAttachRepositoryStub{
		backingProject: testkeyvalue.Versioned[testhierarchy.ProjectRecord]{
			Record: testhierarchy.ProjectRecord{
				ID:   projectID,
				Slug: "database",
				Kind: testhierarchy.ProjectKindBacking,
			},
			Revision: 21, ReadRevision: 41,
		},
		backingEnvironment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{
				ID: backingEnvironmentID, ProjectID: projectID, Name: "main",
				ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
			},
			Revision: 22, ReadRevision: 41,
		},
		backingService: testkeyvalue.Versioned[testservices.ServiceRecord]{
			Record: testservices.ServiceRecord{
				EnvironmentID:    backingEnvironmentID,
				BackingNetworkID: ids.NewAt(ids.KindNetwork, now, 6),
				Desired: core.Service{
					ID: backingServiceID, Name: "postgres", Adapter: "postgres:16",
				},
				Runtime: core.ServiceRuntime{
					ServiceID: backingServiceID, RuntimeIntent: core.ServiceRuntimeIntentRunning,
				},
			},
			Revision: 23, ReadRevision: 41,
		},
	}
	service := &Service{
		repository: repository, attachFacts: facts, random: strings.NewReader(strings.Repeat("x", 256)),
	}
	specs := map[string]core.AttachmentSpec{
		"target": {
			BackingProject: "database", BackingService: "postgres", Service: "target",
			Credential: core.AttachmentCredentialSpec{Mode: "new"},
		},
		"reader": {
			BackingProject: "database", BackingService: "postgres", Service: "reader",
			Credential: core.AttachmentCredentialSpec{Mode: "new"}, Grants: []string{"target"},
		},
	}
	changes := []testblueprints.EnvironmentBlueprintServiceChange{
		{Record: testservices.ServiceRecord{
			EnvironmentID: environmentID,
			Desired:       core.Service{ID: ids.NewAt(ids.KindService, now, 7), Name: "target"},
		}},
		{Record: testservices.ServiceRecord{
			EnvironmentID: environmentID,
			Desired:       core.Service{ID: ids.NewAt(ids.KindService, now, 8), Name: "reader"},
		}},
	}
	prepared, err := service.prepareBlueprintAttaches(
		context.Background(), environmentID, taskID, specs, changes, nil,
		func(kind ids.Kind, purpose string) string { return ids.DeriveAt(kind, now, taskID, purpose) },
		false,
		now,
	)
	if err != nil {
		t.Fatalf("prepareBlueprintAttaches() error = %v", err)
	}
	defer prepared.clear()

	wantHost := "gp-svc-01m2fwkng0smhx4kvh89teeh1m"
	for _, grant := range []string{"", "target"} {
		host := resolvePreparedBlueprintFact(t, prepared.facts, environmentID, core.FactRef{
			Attach: "reader", Grant: grant, Key: "pg16_HOST",
		}, false)
		url := resolvePreparedBlueprintFact(t, prepared.facts, environmentID, core.FactRef{
			Attach: "reader", Grant: grant, Key: "pg16_URL",
		}, true)
		if host != wantHost {
			t.Fatalf("grant %q HOST = %q, want endpoint %q", grant, host, wantHost)
		}
		if !strings.Contains(url, "@"+wantHost+":5432/") {
			t.Fatalf("grant %q URL does not use endpoint %q", grant, wantHost)
		}
	}
}

func resolvePreparedBlueprintFact(
	t *testing.T,
	facts *blueprintAttachFactOverlay,
	environmentID string,
	reference core.FactRef,
	secret bool,
) string {
	t.Helper()
	value := ""
	err := facts.ResolveFact(context.Background(), environmentID, reference, secret, func(plaintext []byte) error {
		value = string(plaintext)
		return nil
	})
	if err != nil {
		t.Fatalf("ResolveFact(%#v) error = %v", reference, err)
	}
	return value
}
