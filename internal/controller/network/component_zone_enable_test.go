package network

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/controller/component"
	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

// Rationale: the guided create-Zone then enable journey advances the desired
// head to a mutation revision, which intentionally has no uploaded file audit.
// The Component action must consume current authoring and keep its revision.
func TestComponentEnableAfterOrdinaryZoneCreation(t *testing.T) {
	at := time.Date(2026, 9, 6, 8, 0, 0, 0, time.UTC)
	environmentID, projectID := ids.NewAt(ids.KindEnvironment, at, 1), ids.NewAt(ids.KindProject, at, 2)
	repository := &fakeZoneCreationRepository{
		environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{Record: testhierarchy.EnvironmentRecord{
			ID: environmentID, ProjectID: projectID, NetworkPool: "10.34.0.0/16",
			ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
		}, Revision: 7, ReadRevision: 9},
		project: testkeyvalue.Versioned[testhierarchy.ProjectRecord]{Record: testhierarchy.ProjectRecord{
			ID: projectID, Kind: testhierarchy.ProjectKindTenant,
		}, Revision: 8, ReadRevision: 9},
		projection: zoneCreationProjectionForTest(t, environmentID, at),
	}
	creator, err := newZoneCreationService(repository, &fakeZoneCreationIdempotency{
		evidence:   zoneCreationEvidence{durable: networkTestProtectedIntent()},
		resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := creator.CreateZone(t.Context(), apiTypes.ZoneCreate{
		EnvironmentID: environmentID, Name: "secondary", Subnet: "10.34.20.0/24",
	}, "create-component-zone"); err != nil {
		t.Fatal(err)
	}
	if repository.claim.SourceKind != testblueprints.EnvironmentBlueprintSourceMutation {
		t.Fatal("ordinary Zone creation did not publish mutation authority")
	}
	projection := repository.publication.Projection
	componentID := ids.NewAt(ids.KindComponent, at, 6)
	document, err := blueprintparser.MarshalAuthoringDocument(blueprintparser.AuthoringDocument{
		Envelope: core.Envelope{Kind: core.KindDocEnvironment, Schema: core.EnvelopeSchema,
			Metadata: core.EnvelopeMetadata{Tenant: "tenant", Project: "project", Environment: "production"}},
		NetworkPool: "10.34.0.0/16", Compose: projection.NormalizedCompose,
		Components: map[string]core.ComponentSpec{"http-router": {Implementation: core.ComponentKindIngressCaddy}},
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture := &componentZoneAuthoring{
		component: testcomponents.Record{Desired: testcomponents.DesiredRecord{
			ID: componentID, Owner: core.ComponentOwnerEnvironment, OwnerID: environmentID,
			Kind: core.ComponentKindIngressCaddy,
		}},
		document: apiTypes.EnvironmentBlueprintDocument{
			EnvironmentID: environmentID, Revision: projection.RevisionID, Document: string(document),
		},
	}
	mutations, err := component.NewMutationService(fixture, fixture, fixture, fixture)
	if err != nil {
		t.Fatal(err)
	}
	zoneID := repository.publication.Zone.Desired.ID
	config := &apiTypes.ComponentConfigMutationInput{
		Caddy: &apiTypes.CaddyComponentConfigMutationInput{ZoneIDs: []string{zoneID}},
	}
	if _, err := mutations.EnableComponent(t.Context(), componentID,
		apiTypes.ComponentEnableRequest{Config: config}, "enable-component-zone"); err != nil {
		t.Fatal(err)
	}
	if fixture.calls != 1 || fixture.expected != projection.RevisionID {
		t.Fatalf("enable calls/revision = %d/%s", fixture.calls, fixture.expected)
	}
	var authored struct {
		Components map[string]core.ComponentSpec `yaml:"x-gp-components"`
		Networks   map[string]yaml.Node          `yaml:"networks"`
		Services   map[string]yaml.Node          `yaml:"services"`
	}
	if err := yaml.Unmarshal(fixture.bundle.Files[0].Content, &authored); err != nil {
		t.Fatal(err)
	}
	router := authored.Components["http-router"]
	if !router.Enabled || len(router.Settings.ZoneIDs) != 1 || router.Settings.ZoneIDs[0] != zoneID ||
		len(authored.Networks) != 1 || len(authored.Services) != 1 {
		t.Fatalf("enable lost current decisions: %#v", authored)
	}
	fixture.document.Revision = ids.NewAt(ids.KindTask, at, 7)
	fixture.reject = true
	if _, err := mutations.EnableComponent(t.Context(), componentID,
		apiTypes.ComponentEnableRequest{Config: config}, "enable-stale-zone"); err == nil {
		t.Fatal("enable swallowed the publication revision conflict")
	}
}

// Only the authoring/publication and unused platform/credential side-effect
// ports are doubled; Zone creation and Component mutation are real producers.
type componentZoneAuthoring struct {
	component testcomponents.Record
	document  apiTypes.EnvironmentBlueprintDocument
	bundle    core.BlueprintBundle
	expected  string
	calls     int
	reject    bool
}

func (f *componentZoneAuthoring) GetComponent(
	context.Context,
	string,
) (testkeyvalue.Versioned[testcomponents.Record], error) {
	return testkeyvalue.Versioned[testcomponents.Record]{Record: f.component}, nil
}
func (f *componentZoneAuthoring) GetBlueprint(context.Context, string) (apiTypes.EnvironmentBlueprintDocument, error) {
	return f.document, nil
}
func (f *componentZoneAuthoring) ApplyComponentBlueprint(_ context.Context, _, _ string,
	bundle core.BlueprintBundle, expected, _ string) (testidempotency.IdempotencyResponse, error) {
	f.calls++
	f.bundle, f.expected = bundle, expected
	if f.reject {
		return testidempotency.IdempotencyResponse{}, errs.New(errs.KindStateConflict, "authoring revision changed")
	}
	return testidempotency.IdempotencyResponse{Status: 202}, nil
}
func (*componentZoneAuthoring) ResolveCredentialReference(context.Context, string,
	component.OpaqueSecretReferenceInput, string) (string, error) {
	panic("unexpected credential mutation")
}

func (*componentZoneAuthoring) EnablePlatformComponent(
	context.Context, string, string,

) (testidempotency.IdempotencyResponse, error) {
	panic("unexpected platform mutation")
}

func (*componentZoneAuthoring) DisablePlatformComponent(
	context.Context, string, string,

) (testidempotency.IdempotencyResponse, error) {
	panic("unexpected platform mutation")
}

func (*componentZoneAuthoring) UpdatePlatformComponent(
	context.Context, string, string,

) (testidempotency.IdempotencyResponse, error) {
	panic("unexpected platform mutation")
}
func (*componentZoneAuthoring) ReplacePlatformComponentConfig(context.Context, string,
	apiTypes.ComponentConfigMutationRequest, string) (testidempotency.IdempotencyResponse, error) {
	panic("unexpected platform mutation")
}
