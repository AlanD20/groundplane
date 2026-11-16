package network

import (
	"context"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/acceptedactions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
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
		scope: workexecution.Scope{
			EnvironmentID: environmentID,
			ProjectID:     projectID,
			TenantID:      ids.NewAt(ids.KindTenant, at, 3),
		},
		component: testcomponents.Record{Desired: testcomponents.DesiredRecord{
			ID: componentID, Owner: core.ComponentOwnerEnvironment, OwnerID: environmentID,
			Kind: core.ComponentKindIngressCaddy,
		}},
		document: apiTypes.EnvironmentBlueprintDocument{
			EnvironmentID: environmentID, Revision: projection.RevisionID, Document: string(document),
		},
	}
	submitter, queue := newComponentZoneSubmitter(t)
	mutations, err := component.NewMutationService(fixture, fixture, fixture, fixture, submitter)
	if err != nil {
		t.Fatal(err)
	}
	zoneID := repository.publication.Zone.Desired.ID
	config := &apiTypes.ComponentConfigMutationInput{
		Caddy: &apiTypes.CaddyComponentConfigMutationInput{ZoneIDs: []string{zoneID}},
	}
	response, err := mutations.EnableComponent(
		t.Context(),
		componentID,
		apiTypes.ComponentEnableRequest{Config: config},
		ids.NewULID(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.calls != 0 {
		t.Fatal("pure acceptance captured current authoring")
	}
	capture := func(response testidempotency.IdempotencyResponse) error {
		var accepted apiTypes.TaskAccepted
		if err := json.Unmarshal(response.Body, &accepted); err != nil {
			return err
		}
		task, revision, err := queue.ReadTask(t.Context(), accepted.TaskID, 0)
		if err != nil {
			return err
		}
		_, err = mutations.AcceptedPreparations()[0].Owner.PrepareAccepted(
			t.Context(),
			task,
			revision,
			time.Now().UTC(),
		)
		return err
	}
	if err := capture(response); err != nil {
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
	response, err = mutations.EnableComponent(
		t.Context(),
		componentID,
		apiTypes.ComponentEnableRequest{Config: config},
		ids.NewULID(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := capture(response); err == nil {
		t.Fatal("deferred enable swallowed the publication revision conflict")
	}

}

// Only the authoring/publication and unused platform/credential side-effect
// ports are doubled; Zone creation and Component mutation are real producers.
type componentZoneAuthoring struct {
	scope     workexecution.Scope
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

func (f *componentZoneAuthoring) TaskAcceptanceIdentity(
	_ context.Context,
	environmentID string,
) (workexecution.Scope, string, time.Duration, error) {
	return f.scope, "production", time.Hour, nil
}

func (f *componentZoneAuthoring) PrepareAcceptedComponentBlueprint(
	_ context.Context,
	task workqueue.TaskSnapshot,
	bundle core.BlueprintBundle,
	expected string,
	_ testidempotency.IdempotencyLocator,
	_ testidempotency.ProtectedIntentRecord,
) (taskdependencies.Prepared, error) {
	f.calls++
	f.bundle, f.expected = bundle, expected
	if f.reject {
		return taskdependencies.Prepared{}, errs.New(errs.KindStateConflict, "authoring revision changed")
	}
	return taskdependencies.Prepared{Task: task.Task}, nil
}
func (*componentZoneAuthoring) ResolveCredentialReference(context.Context, string,
	component.OpaqueSecretReferenceInput, string) (string, error) {
	panic("unexpected credential mutation")
}

func (*componentZoneAuthoring) EnablePlatformComponent(
	context.Context, string, apiTypes.TaskRunAfterRequest, string,

) (testidempotency.IdempotencyResponse, error) {
	panic("unexpected platform mutation")
}

func (*componentZoneAuthoring) DisablePlatformComponent(
	context.Context, string, apiTypes.TaskRunAfterRequest, string,

) (testidempotency.IdempotencyResponse, error) {
	panic("unexpected platform mutation")
}

func (*componentZoneAuthoring) UpdatePlatformComponent(
	context.Context, string, apiTypes.TaskRunAfterRequest, string,

) (testidempotency.IdempotencyResponse, error) {
	panic("unexpected platform mutation")
}
func (*componentZoneAuthoring) ReplacePlatformComponentConfig(context.Context, string,
	apiTypes.ComponentConfigMutationRequest, string) (testidempotency.IdempotencyResponse, error) {
	panic("unexpected platform mutation")
}

func newComponentZoneSubmitter(t *testing.T) (*acceptedaction.Submitter, *workqueue.Repository) {
	t.Helper()
	store := &componentZoneStore{revision: 1, values: make(map[string]*testkeyvalue.KeyValue)}
	payloadRepository, err := acceptedactions.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	protector, err := secretvalue.NewProtector(componentZoneCipher{}, componentZoneCipher{})
	if err != nil {
		t.Fatal(err)
	}
	payloads, err := acceptedaction.NewPayloads(payloadRepository, protector)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := etcd.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := idempotentintent.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	submitter, err := acceptedaction.NewSubmitter(payloads, queue, replay, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	return submitter, queue
}

type componentZoneCipher struct{}

func (componentZoneCipher) Seal(_ context.Context, input []byte) ([]byte, error) {
	return append([]byte("sealed:"), input...), nil
}
func (componentZoneCipher) Open(_ context.Context, input []byte) ([]byte, error) {
	return append([]byte(nil), input[len("sealed:"):]...), nil
}
