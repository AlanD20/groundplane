package blueprintparser

import (
	"reflect"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"gopkg.in/yaml.v3"
)

// Rationale: the canonical authoring projection retains concrete extension
// values after native Compose fields and in the grammar's fixed field order.
func TestMarshalAuthoringDocumentEmitsTypedExtensionsInCanonicalOrder(t *testing.T) {
	t.Parallel()
	uid := uint32(1000)
	input := AuthoringDocument{
		Envelope: core.Envelope{
			Kind: core.KindDocEnvironment, Schema: core.EnvelopeSchema,
			Metadata: core.EnvelopeMetadata{Tenant: "acme", Project: "shop", Environment: "production"},
		},
		NetworkPool: "10.40.0.0/16",
		Compose:     []byte("name: native\nservices:\n  web:\n    image: example/web\nx-gp-stale: ignored\n"),
		Requires: []core.Requirement{{
			Target:    core.RequirementTarget{Kind: core.RequirementTargetBackingAttach, Name: "database"},
			Condition: core.RequirementReady, Phases: []core.RequirementPhase{core.RequirementPhaseDeploy},
		}},
		Attachments: map[string]core.AttachmentSpec{
			"database": {BackingProject: "data", BackingService: "postgres", Service: "web"},
		},
		Entries: map[string]core.EntrySpec{
			"settings": {
				Kind: core.EntryKindFile, Path: "/run/settings", UID: &uid,
				Source: core.EntrySourceSpec{Literal: "enabled=true"}, Exposure: []string{"web"},
			},
		},
		Routes: []core.RouteSpec{{
			Hostname: "app.example.test", Path: "/", Target: "web", TargetPort: 8080, Exposure: "public",
		}},
		Scripts: map[string]core.ScriptSpec{
			"migrate": {Slug: "migrate", Service: "web", When: core.ScriptPreDeploy, Script: "echo ready\n"},
		},
		Components: map[string]core.ComponentSpec{
			"http-router": {Implementation: core.ComponentKindIngressCaddy, Enabled: true},
		},
		Backup: &core.BackupSpec{
			Enabled: true, Frequency: "0 3 * * *", Keep: 7, Encryption: "controller", Connector: "archive",
			Sources: []core.BackupSourceSpec{{Kind: core.BackupSourceConfig}},
		},
		ReleaseGroups: map[string]core.ReleaseGroupSpec{
			"application": {Services: []string{"web"}, Tag: "stable", OnFailure: core.OnFailureSwitchBack},
		},
	}

	encoded, err := MarshalAuthoringDocument(input)
	if err != nil {
		t.Fatalf("MarshalAuthoringDocument() error = %v", err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(encoded, &document); err != nil {
		t.Fatalf("decode authored document: %v", err)
	}
	root := document.Content[0]
	keys := make([]string, 0, len(root.Content)/2)
	for index := 0; index+1 < len(root.Content); index += 2 {
		keys = append(keys, root.Content[index].Value)
	}
	wantKeys := []string{
		"kind", "schema", "metadata", "x-gp-network-pool", "services",
		"x-gp-requires", "x-gp-attachments", "x-gp-entry", "x-gp-routes",
		"x-gp-scripts", "x-gp-components", "x-gp-backup", "x-gp-release-groups",
	}
	if !slices.Equal(keys, wantKeys) {
		t.Fatalf("authored field order = %v, want %v", keys, wantKeys)
	}
	assertAuthoringExtensionRoundTrip(t, encoded, input)
}

// Rationale: empty optional collections are absence, while native Compose
// scalar and sequence values retain their authored YAML types.
func TestMarshalAuthoringDocumentOmitsEmptyTypedExtensions(t *testing.T) {
	t.Parallel()
	input := AuthoringDocument{
		Envelope: core.Envelope{
			Kind: core.KindDocEnvironment, Schema: core.EnvelopeSchema,
			Metadata: core.EnvelopeMetadata{Tenant: "acme", Project: "shop", Environment: "production"},
		},
		NetworkPool: "10.40.0.0/16",
		Compose:     []byte("services: {}\nvolumes: []\nenabled: false\n"),
		Requires:    []core.Requirement{}, Attachments: map[string]core.AttachmentSpec{},
		Entries: map[string]core.EntrySpec{}, Routes: []core.RouteSpec{}, Scripts: map[string]core.ScriptSpec{},
		Components: map[string]core.ComponentSpec{}, ReleaseGroups: map[string]core.ReleaseGroupSpec{},
	}

	encoded, err := MarshalAuthoringDocument(input)
	if err != nil {
		t.Fatalf("MarshalAuthoringDocument() error = %v", err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(encoded, &document); err != nil {
		t.Fatalf("decode authored document: %v", err)
	}
	root := document.Content[0]
	keys := make([]string, 0, len(root.Content)/2)
	for index := 0; index+1 < len(root.Content); index += 2 {
		keys = append(keys, root.Content[index].Value)
	}
	wantKeys := []string{"kind", "schema", "metadata", "x-gp-network-pool", "services", "volumes", "enabled"}
	if !slices.Equal(keys, wantKeys) {
		t.Fatalf("authored fields = %v, want %v", keys, wantKeys)
	}
	if root.Content[9].Kind != yaml.MappingNode || root.Content[11].Kind != yaml.SequenceNode ||
		root.Content[13].Tag != "!!bool" || root.Content[13].Value != "false" {
		t.Fatalf("native scalar semantics changed: %#v", root.Content)
	}
}

// Rationale: invalid envelope and native Compose inputs remain internal
// authoring failures rather than partially emitted documents.
func TestMarshalAuthoringDocumentRejectsInvalidInputs(t *testing.T) {
	t.Parallel()
	valid := AuthoringDocument{
		Envelope: core.Envelope{
			Kind: core.KindDocEnvironment, Schema: core.EnvelopeSchema,
			Metadata: core.EnvelopeMetadata{Tenant: "acme", Project: "shop", Environment: "production"},
		},
		NetworkPool: "10.40.0.0/16", Compose: []byte("services: {}\n"),
	}
	invalidEnvelope := valid
	invalidEnvelope.NetworkPool = ""
	invalidCompose := valid
	invalidCompose.Compose = []byte("- not-a-mapping\n")
	for name, input := range map[string]AuthoringDocument{"envelope": invalidEnvelope, "compose": invalidCompose} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if encoded, err := MarshalAuthoringDocument(input); err == nil || encoded != nil {
				t.Fatalf("MarshalAuthoringDocument() = %q, %v; want nil error result", encoded, err)
			}
		})
	}
}

func assertAuthoringExtensionRoundTrip(t *testing.T, encoded []byte, want AuthoringDocument) {
	t.Helper()
	var got struct {
		core.Envelope `yaml:",inline"`
		NetworkPool   string                           `yaml:"x-gp-network-pool"`
		Requires      []core.Requirement               `yaml:"x-gp-requires"`
		Attachments   map[string]core.AttachmentSpec   `yaml:"x-gp-attachments"`
		Entries       map[string]core.EntrySpec        `yaml:"x-gp-entry"`
		Routes        []core.RouteSpec                 `yaml:"x-gp-routes"`
		Scripts       map[string]core.ScriptSpec       `yaml:"x-gp-scripts"`
		Components    map[string]core.ComponentSpec    `yaml:"x-gp-components"`
		Backup        *core.BackupSpec                 `yaml:"x-gp-backup"`
		ReleaseGroups map[string]core.ReleaseGroupSpec `yaml:"x-gp-release-groups"`
	}
	if err := yaml.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("decode typed authored extensions: %v", err)
	}
	if got.Envelope != want.Envelope || got.NetworkPool != want.NetworkPool ||
		!reflect.DeepEqual(got.Requires, want.Requires) || !reflect.DeepEqual(got.Attachments, want.Attachments) ||
		!reflect.DeepEqual(got.Entries, want.Entries) || !reflect.DeepEqual(got.Routes, want.Routes) ||
		!reflect.DeepEqual(got.Scripts, want.Scripts) || !reflect.DeepEqual(got.Components, want.Components) ||
		!reflect.DeepEqual(got.Backup, want.Backup) || !reflect.DeepEqual(got.ReleaseGroups, want.ReleaseGroups) {
		t.Fatalf("typed authored round trip = %#v, want %#v", got, want)
	}
}
