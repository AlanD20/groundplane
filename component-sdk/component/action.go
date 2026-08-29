package component

import (
	"crypto/sha256"
	"fmt"
	"strings"
)

type ActionID string

type ActionDefinition struct {
	id         ActionID
	capability Capability
	operation  Operation
}

func NewActionDefinition(id ActionID, capability Capability, operation Operation) (ActionDefinition, error) {
	if err := validateToken("action id", string(id)); err != nil {
		return ActionDefinition{}, err
	}
	if !capability.Valid() {
		return ActionDefinition{}, fmt.Errorf("component: action %q has invalid capability %q", id, capability)
	}
	if !operation.Valid() {
		return ActionDefinition{}, fmt.Errorf("component: action %q has invalid operation %q", id, operation)
	}
	return ActionDefinition{id: id, capability: capability, operation: operation}, nil
}

func (a ActionDefinition) Validate() error {
	_, err := NewActionDefinition(a.id, a.capability, a.operation)
	return err
}

func (a ActionDefinition) ID() ActionID {
	return a.id
}

func (a ActionDefinition) Capability() Capability {
	return a.capability
}

func (a ActionDefinition) Operation() Operation {
	return a.operation
}

type ComponentID struct {
	value string
}

func NewComponentID(value string) (ComponentID, error) {
	if err := validateOpaqueID("component id", value); err != nil {
		return ComponentID{}, err
	}
	return ComponentID{value: value}, nil
}

func (id ComponentID) String() string {
	return id.value
}

type ArtifactID struct {
	value string
}

func NewArtifactID(value string) (ArtifactID, error) {
	if err := validateOpaqueID("artifact id", value); err != nil {
		return ArtifactID{}, err
	}
	return ArtifactID{value: value}, nil
}

func (id ArtifactID) String() string {
	return id.value
}

type ArtifactReference struct {
	id     ArtifactID
	digest [sha256.Size]byte
}

func NewArtifactReference(id ArtifactID, digest [sha256.Size]byte) (ArtifactReference, error) {
	if id.value == "" {
		return ArtifactReference{}, fmt.Errorf("component: artifact id is required")
	}
	if zeroDigest(digest) {
		return ArtifactReference{}, fmt.Errorf("component: artifact digest is required")
	}
	return ArtifactReference{id: id, digest: digest}, nil
}

func (a ArtifactReference) ID() ArtifactID {
	return a.id
}

func (a ArtifactReference) Digest() [sha256.Size]byte {
	return a.digest
}

type ActionEnvelopeInput struct {
	ComponentID      ComponentID
	DefinitionDigest [sha256.Size]byte
	CatalogDigest    [sha256.Size]byte
	ActionID         ActionID
	Artifact         ArtifactReference
	Generation       uint64
}

type ActionEnvelope struct {
	componentID      ComponentID
	definitionDigest [sha256.Size]byte
	catalogDigest    [sha256.Size]byte
	actionID         ActionID
	artifact         ArtifactReference
	generation       uint64
}

func NewActionEnvelope(input ActionEnvelopeInput) (ActionEnvelope, error) {
	if input.ComponentID.value == "" {
		return ActionEnvelope{}, fmt.Errorf("component: action Component id is required")
	}
	if zeroDigest(input.DefinitionDigest) {
		return ActionEnvelope{}, fmt.Errorf("component: action definition digest is required")
	}
	if zeroDigest(input.CatalogDigest) {
		return ActionEnvelope{}, fmt.Errorf("component: action catalog digest is required")
	}
	if err := validateToken("action id", string(input.ActionID)); err != nil {
		return ActionEnvelope{}, err
	}
	if input.Artifact.id.value == "" || zeroDigest(input.Artifact.digest) {
		return ActionEnvelope{}, fmt.Errorf("component: action artifact is required")
	}
	if input.Generation == 0 {
		return ActionEnvelope{}, fmt.Errorf("component: action generation must be positive")
	}
	return ActionEnvelope{
		componentID:      input.ComponentID,
		definitionDigest: input.DefinitionDigest,
		catalogDigest:    input.CatalogDigest,
		actionID:         input.ActionID,
		artifact:         input.Artifact,
		generation:       input.Generation,
	}, nil
}

func (a ActionEnvelope) ComponentID() ComponentID {
	return a.componentID
}

func (a ActionEnvelope) DefinitionDigest() [sha256.Size]byte {
	return a.definitionDigest
}

func (a ActionEnvelope) CatalogDigest() [sha256.Size]byte {
	return a.catalogDigest
}

func (a ActionEnvelope) ActionID() ActionID {
	return a.actionID
}

func (a ActionEnvelope) Artifact() ArtifactReference {
	return a.artifact
}

func (a ActionEnvelope) Generation() uint64 {
	return a.generation
}

func validateOpaqueID(field, value string) error {
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value {
		return fmt.Errorf("component: %s is invalid", field)
	}
	for _, character := range value {
		if character < '!' || character > '~' {
			return fmt.Errorf("component: %s is invalid", field)
		}
	}
	return nil
}

func zeroDigest(digest [sha256.Size]byte) bool {
	return digest == [sha256.Size]byte{}
}
