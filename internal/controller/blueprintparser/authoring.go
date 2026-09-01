package blueprintparser

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

// AuthoringDocument is the complete reproducible Environment Blueprint input.
// Runtime identity, observations, generated paths, and secret plaintext have no
// representation at this boundary.
type AuthoringDocument struct {
	Envelope      core.Envelope
	NetworkPool   string
	Compose       []byte
	Requires      []core.Requirement
	Attachments   map[string]core.AttachmentSpec
	Entries       map[string]core.EntrySpec
	Routes        []core.RouteSpec
	Scripts       map[string]core.ScriptSpec
	Components    map[string]core.ComponentSpec
	Backup        *core.BackupSpec
	ReleaseGroups map[string]core.ReleaseGroupSpec
}

// MarshalAuthoringDocument emits one canonical single-file Blueprint. The
// source bundle layout, YAML comments, aliases, and generated state are not
// desired-state decisions and are deliberately not reconstructed.
func MarshalAuthoringDocument(input AuthoringDocument) ([]byte, error) {
	if input.Envelope.Kind != core.KindDocEnvironment || input.Envelope.Schema != core.EnvelopeSchema ||
		input.Envelope.Metadata.Tenant == "" || input.Envelope.Metadata.Project == "" ||
		input.Envelope.Metadata.Environment == "" || input.NetworkPool == "" {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint authoring envelope is invalid")
	}
	var document yaml.Node
	decoder := yaml.NewDecoder(bytes.NewReader(input.Compose))
	if err := decoder.Decode(&document); err != nil || len(document.Content) != 1 ||
		document.Content[0].Kind != yaml.MappingNode {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint authored Compose is invalid")
	}

	native := document.Content[0]
	root := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	if err := appendAuthoringField(root, "kind", input.Envelope.Kind); err != nil {
		return nil, err
	}
	if err := appendAuthoringField(root, "schema", input.Envelope.Schema); err != nil {
		return nil, err
	}
	if err := appendAuthoringField(root, "metadata", input.Envelope.Metadata); err != nil {
		return nil, err
	}
	if err := appendAuthoringField(root, "x-gp-network-pool", input.NetworkPool); err != nil {
		return nil, err
	}
	for index := 0; index+1 < len(native.Content); index += 2 {
		key := native.Content[index].Value
		if key == "kind" || key == "schema" || key == "metadata" || key == "x-gp-network-pool" ||
			len(key) >= 5 && key[:5] == "x-gp-" {
			continue
		}
		root.Content = append(root.Content, native.Content[index], native.Content[index+1])
	}
	for _, field := range []struct {
		name  string
		value any
		emit  bool
	}{
		{"x-gp-requires", input.Requires, len(input.Requires) != 0},
		{"x-gp-attachments", input.Attachments, len(input.Attachments) != 0},
		{"x-gp-entry", input.Entries, len(input.Entries) != 0},
		{"x-gp-routes", input.Routes, len(input.Routes) != 0},
		{"x-gp-scripts", input.Scripts, len(input.Scripts) != 0},
		{"x-gp-components", input.Components, len(input.Components) != 0},
		{"x-gp-backup", input.Backup, input.Backup != nil},
		{"x-gp-release-groups", input.ReleaseGroups, len(input.ReleaseGroups) != 0},
	} {
		if field.emit {
			if err := appendAuthoringField(root, field.name, field.value); err != nil {
				return nil, err
			}
		}
	}
	markScriptBodiesLiteral(root)
	encoded, err := yaml.Marshal(&yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{root}})
	if err != nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint authoring projection failed")
	}
	return encoded, nil
}

func appendAuthoringField(root *yaml.Node, name string, value any) error {
	node := &yaml.Node{}
	if err := node.Encode(value); err != nil {
		return errs.New(errs.KindInternal, "Environment Blueprint authoring value is invalid")
	}
	root.Content = append(root.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: name},
		node,
	)
	return nil
}

func markScriptBodiesLiteral(root *yaml.Node) {
	for index := 0; index+1 < len(root.Content); index += 2 {
		if root.Content[index].Value != "x-gp-scripts" {
			continue
		}
		scripts := root.Content[index+1]
		for scriptIndex := 0; scriptIndex+1 < len(scripts.Content); scriptIndex += 2 {
			spec := scripts.Content[scriptIndex+1]
			for fieldIndex := 0; fieldIndex+1 < len(spec.Content); fieldIndex += 2 {
				if spec.Content[fieldIndex].Value == "script" {
					spec.Content[fieldIndex+1].Style = yaml.LiteralStyle
				}
			}
		}
		return
	}
}
