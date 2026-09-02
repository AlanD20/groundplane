// envelope.go represents the AUTHORED Blueprint shape — the Controller
// envelope plus the Compose body and its x-gp-* extensions, exactly as
// blueprint.md defines it. This is the outer layer of the pipeline
// sketched in blueprint.md's "The layers":
//
//	Blueprint envelope + Compose body + x-gp-* extensions
//	                         |
//	                         v
//	              typed Controller desired state   <- model.go's types
//	                         |
//	                         v
//	           durable records + render/task plan
//
// Parsing an Envelope into model.go's typed structs (Environment,
// Service, Attach, …) is internal/controller's job (the Controller is
// the ONLY interpreter — blueprint.md, "The laws", #3); this file only
// carries the authored shape and the envelope-level parse/validate
// entry point.
package core

import (
	"fmt"
	"sort"

	"gopkg.in/yaml.v3"
)

// Kind is the Blueprint envelope's document discriminator.
type Kind string

const (
	KindDocEnvironment Kind = "environment"
	KindDocBacking     Kind = "backing"
	KindDocConnector   Kind = "connector"
	KindDocProject     Kind = "project"
	KindDocTenant      Kind = "tenant"
)

// EnvelopeSchema is the current Groundplane envelope/extension grammar
// version this Controller understands. A schema bump is required when a
// change cannot be accepted without changing interpretation (blueprint.md,
// "Envelope and placement").
const EnvelopeSchema = 1

// Envelope is the Controller-only wrapper stripped before the remaining
// document is passed to the Compose parser (blueprint.md, "Envelope and
// placement").
type Envelope struct {
	Kind     Kind             `yaml:"kind"`
	Schema   int              `yaml:"schema"`
	Metadata EnvelopeMetadata `yaml:"metadata"`
}

// EnvelopeMetadata addresses the document by human-facing slugs; the
// Controller resolves these to stable ids before storing anything (see
// model.go's Tenant/Project/Environment.ID). Which fields are required
// depends on Kind: environment and connector documents set the complete
// tenant/project/environment chain.
type EnvelopeMetadata struct {
	Tenant      string `yaml:"tenant,omitempty"`
	Project     string `yaml:"project,omitempty"`
	Environment string `yaml:"environment,omitempty"`
	Name        string `yaml:"name,omitempty"` // connector documents: the immutable Connector name
}

// ConnectorDocument is a `kind: connector` Blueprint document — see
// blueprint.md's envelope example. It compiles to model.go's Connector.
type ConnectorDocument struct {
	Envelope  `yaml:",inline"`
	Connector ConnectorBody `yaml:"connector"`
}

type ConnectorBody struct {
	Kind        string                   `yaml:"kind"` // "s3-compatible", …
	Endpoint    string                   `yaml:"endpoint,omitempty"`
	Bucket      string                   `yaml:"bucket,omitempty"`
	Prefix      string                   `yaml:"prefix,omitempty"`
	Region      string                   `yaml:"region,omitempty"`
	PathStyle   *bool                    `yaml:"path_style"`
	Credentials map[string]CredentialRef `yaml:"credentials,omitempty"`
}

// CredentialRef is either a secret-store reference or (less commonly) a
// direct value baked in and stored encrypted — never emitted into
// Compose either way (blueprint.md: "Connector credentials are
// references or encrypted direct values and are never emitted into
// Compose").
type CredentialRef struct {
	SecretRef string `yaml:"secret_ref,omitempty"`
	Value     string `yaml:"value,omitempty"` // stored encrypted at rest; never round-tripped back into an authored document
}

func (r CredentialRef) Validate() error {
	if (r.SecretRef == "") == (r.Value == "") {
		return fmt.Errorf("exactly one of secret_ref or value is required")
	}
	return nil
}

// Requirement is x-gp-requires' authored shape — a Controller-level
// prerequisite, possibly crossing Compose project boundaries.
// `condition` is exactly exists|ready|completed_successfully; `phases` is a
// nonempty subset of start|deploy|rollback|always. See blueprint.md,
// "x-gp-requires".
type Requirement struct {
	Target    RequirementTarget    `yaml:"target"`
	Condition RequirementCondition `yaml:"condition" json:"condition"`
	Phases    []RequirementPhase   `yaml:"phases,omitempty" json:"phases"`
}

type RequirementTarget struct {
	Kind RequirementTargetKind `yaml:"kind" json:"kind"`
	Name string                `yaml:"name" json:"name"`
}

// ServiceExtensionSpec is the typed authored Groundplane input attached to one
// native Compose Service. The native Service remains the topology authority;
// these fields carry only Controller behavior that Compose cannot express.
type ServiceExtensionSpec struct {
	Release   *ServiceReleaseSpec
	DependsOn map[string]ServiceDependency
}

// ServiceReleaseSpec is the authored subset of x-gp-release. Current release
// state is Controller-generated from the immutable release ledger and is not
// accepted at the Blueprint boundary.
type ServiceReleaseSpec struct {
	DefaultStrategy Strategy  `yaml:"default_strategy"`
	OnFailure       OnFailure `yaml:"on_failure,omitempty"`
}

// AttachmentSpec is one x-gp-attachments entry, keyed by attach name.
// See blueprint.md, "x-gp-attachments".
type AttachmentSpec struct {
	BackingProject string                   `yaml:"backing_project"`
	BackingService string                   `yaml:"backing_service"`
	Service        string                   `yaml:"service"`
	Credential     AttachmentCredentialSpec `yaml:"credential"`
	Grants         []string                 `yaml:"grants,omitempty"` // other attach names/ids; new credentials only
}

type AttachmentCredentialSpec struct {
	Mode   string `yaml:"mode"`
	Attach string `yaml:"attach,omitempty"`
}

// EntrySpec is one x-gp-entry entry, keyed by entry name. Mirrors
// EnvEntry (model.go) in authored form — kind/source/exposure/secret.
// See blueprint.md, "x-gp-entry".
type EntrySpec struct {
	Kind     EntryKind       `yaml:"kind"`
	Path     string          `yaml:"path,omitempty"` // Kind=file only
	UID      *uint32         `yaml:"uid,omitempty"`  // Kind=file only; explicit, never inferred
	GID      *uint32         `yaml:"gid,omitempty"`  // Kind=file only; explicit, never inferred
	Source   EntrySourceSpec `yaml:"source"`
	Exposure []string        `yaml:"exposure"`
	Secret   bool            `yaml:"secret,omitempty"`
}

// EntrySourceSpec is the authored YAML shape of a fact/secret_ref/
// literal source — a nested object per kind, e.g.:
//
//	source:
//	  fact: {attach: api-db, key: pg16_URL}
//
// or `source: {secret_ref: sec_01J...}`, or a bare literal value.
type EntrySourceSpec struct {
	Fact      *FactRef `yaml:"fact,omitempty"`
	SecretRef string   `yaml:"secret_ref,omitempty"`
	Literal   string   `yaml:"literal,omitempty"`
}

// RouteSpec is one x-gp-routes entry. See blueprint.md, "x-gp-route".
type RouteSpec struct {
	Hostname   string `yaml:"hostname,omitempty"`
	Path       string `yaml:"path,omitempty"`
	Target     string `yaml:"target"`
	TargetPort uint16 `yaml:"target_port"`
	Exposure   string `yaml:"exposure"` // "public" | "internal"
}

// ScriptSpec is one x-gp-scripts entry keyed by immutable reconciliation key.
type ScriptSpec struct {
	Slug    string     `yaml:"slug"`
	Service string     `yaml:"service"`
	When    ScriptHook `yaml:"when"`
	Script  string     `yaml:"script"`
}

// ComponentSpec is one x-gp-components entry keyed by Component Capability.
type ComponentSpec struct {
	Implementation       ComponentKind                 `yaml:"implementation"`
	Enabled              bool                          `yaml:"enabled"`
	Settings             ComponentCapabilitySettings   `yaml:"settings"`
	ImplementationConfig ComponentImplementationConfig `yaml:"implementation_config,omitempty"`
}

type ComponentCapabilitySettings struct {
	ZoneID   string `yaml:"zone_id,omitempty"`
	SecretID string `yaml:"secret_id,omitempty"`
}

type ComponentImplementationConfig struct {
	CaddyfileTemplate string `yaml:"caddyfile_template,omitempty"`
}

// BackupSpec is x-gp-backup's authored shape. See blueprint.md,
// "x-gp-backup".
type BackupSpec struct {
	Enabled    bool               `yaml:"enabled"`
	Frequency  string             `yaml:"frequency,omitempty"`
	Keep       int64              `yaml:"keep,omitempty"`
	Encryption string             `yaml:"encryption,omitempty"`
	Connector  string             `yaml:"connector,omitempty"`
	Sources    []BackupSourceSpec `yaml:"sources,omitempty"`
}

type BackupSourceSpec struct {
	Kind BackupSourceKind `yaml:"kind"`
	Ref  string           `yaml:"ref,omitempty"`
}

// ReleaseGroupSpec is one x-gp-release-groups entry. The enclosing map key is
// the canonical group name, so the entry does not duplicate it. See
// blueprint.md, "x-gp-release-groups".
type ReleaseGroupSpec struct {
	Services  []string  `yaml:"services"`
	Order     []string  `yaml:"order,omitempty"`
	Tag       string    `yaml:"tag,omitempty"`
	OnFailure OnFailure `yaml:"on_failure,omitempty"`

	orderPresent bool
}

// UnmarshalYAML retains whether order was authored. A nil slice alone cannot
// distinguish omission, which selects the services-order default, from an
// explicit null or empty sequence, both of which are invalid decisions.
func (spec *ReleaseGroupSpec) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("release group must be a mapping")
	}

	orderPresent := false
	for index := 0; index+1 < len(node.Content); index += 2 {
		key := node.Content[index]
		if key.Kind != yaml.ScalarNode {
			return fmt.Errorf("release group field must be a scalar")
		}
		switch key.Value {
		case "services", "tag", "on_failure":
		case "order":
			orderPresent = true
		default:
			return fmt.Errorf("release group contains an unknown field")
		}
	}

	type plainReleaseGroupSpec ReleaseGroupSpec
	var decoded plainReleaseGroupSpec
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	*spec = ReleaseGroupSpec(decoded)
	spec.orderPresent = orderPresent
	return nil
}

// ParseEnvelope reads just the envelope fields (kind/schema/metadata)
// from a raw Blueprint document — the first step of blueprint.md's
// lifecycle ("Parse the envelope and Compose body"). Callers dispatch on
// Kind to decode the rest (EnvironmentDocument, ConnectorDocument, …).
func ParseEnvelope(raw []byte) (Envelope, error) {
	var env Envelope
	if err := yaml.Unmarshal(raw, &env); err != nil {
		return Envelope{}, fmt.Errorf("envelope: %w", err)
	}
	if env.Schema == 0 {
		env.Schema = EnvelopeSchema
	}
	return env, nil
}

// ParseConnectorDocument decodes a full `kind: connector` document.
func ParseConnectorDocument(raw []byte) (ConnectorDocument, error) {
	var doc ConnectorDocument
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return ConnectorDocument{}, fmt.Errorf("connector document: %w", err)
	}
	if doc.Kind != KindDocConnector {
		return ConnectorDocument{}, fmt.Errorf(
			"connector document: envelope kind is %q, want %q",
			doc.Kind,
			KindDocConnector,
		)
	}
	if doc.Schema != EnvelopeSchema {
		return ConnectorDocument{}, fmt.Errorf(
			"connector document: schema is %d, want %d",
			doc.Schema,
			EnvelopeSchema,
		)
	}
	if doc.Metadata.Name == "" || doc.Metadata.Tenant == "" || doc.Metadata.Project == "" ||
		doc.Metadata.Environment == "" {
		return ConnectorDocument{}, fmt.Errorf(
			"connector document: metadata.name, tenant, project, and environment are required",
		)
	}
	if doc.Connector.Kind == "" {
		return ConnectorDocument{}, fmt.Errorf("connector document: connector.kind is required")
	}
	if doc.Connector.PathStyle == nil {
		return ConnectorDocument{}, fmt.Errorf("connector document: connector.path_style is required")
	}
	credentialNames := make([]string, 0, len(doc.Connector.Credentials))
	for name := range doc.Connector.Credentials {
		credentialNames = append(credentialNames, name)
	}
	sort.Strings(credentialNames)
	for _, name := range credentialNames {
		if name == "" {
			return ConnectorDocument{}, fmt.Errorf(
				"connector document: credential name is required",
			)
		}
		if err := doc.Connector.Credentials[name].Validate(); err != nil {
			return ConnectorDocument{}, fmt.Errorf(
				"connector document: credential %q: %w",
				name,
				err,
			)
		}
	}
	return doc, nil
}
