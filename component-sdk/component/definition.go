package component

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
)

type Capability string

const (
	CapabilityServices       Capability = "services"
	CapabilityRoutes         Capability = "routes"
	CapabilityVolumes        Capability = "volumes"
	CapabilitySecrets        Capability = "secrets"
	CapabilityScripts        Capability = "scripts"
	CapabilityBackups        Capability = "backups"
	CapabilityNetworks       Capability = "networks"
	CapabilityEntries        Capability = "entries"
	CapabilityTasks          Capability = "tasks"
	CapabilityHTTPRouter     Capability = "http-router"
	CapabilityEdgeTunnel     Capability = "edge-tunnel"
	CapabilityDNSResolver    Capability = "dns-resolver"
	CapabilityManagedConfig  Capability = "managed-config"
	CapabilityHostResolution Capability = "host-resolution"
	CapabilityHostTrust      Capability = "host-trust"
)

type Operation string

const (
	OperationList        Operation = "list"
	OperationRead        Operation = "read"
	OperationCreate      Operation = "create"
	OperationEdit        Operation = "edit"
	OperationRemove      Operation = "remove"
	OperationRun         Operation = "run"
	OperationBind        Operation = "bind"
	OperationConfigure   Operation = "configure"
	OperationActivate    Operation = "activate"
	OperationObserve     Operation = "observe"
	OperationMaterialize Operation = "materialize"
)

type OwnerScope string

const (
	OwnerScopeEnvironment OwnerScope = "environment"
	OwnerScopePlatform    OwnerScope = "platform"
)

type ImplementationKey string

type ConfigVariant string

type Grant struct {
	capability Capability
	operations []Operation
}

func NewGrant(capability Capability, operations ...Operation) (Grant, error) {
	if !capability.Valid() {
		return Grant{}, fmt.Errorf("component: invalid granted capability %q", capability)
	}
	if len(operations) == 0 {
		return Grant{}, fmt.Errorf("component: capability %q requires at least one operation", capability)
	}
	canonical := append([]Operation(nil), operations...)
	sort.Slice(canonical, func(i, j int) bool { return canonical[i] < canonical[j] })
	for index, operation := range canonical {
		if !operation.Valid() {
			return Grant{}, fmt.Errorf("component: capability %q has invalid operation %q", capability, operation)
		}
		if index > 0 && operation == canonical[index-1] {
			return Grant{}, fmt.Errorf("component: capability %q repeats operation %q", capability, operation)
		}
	}
	return Grant{capability: capability, operations: canonical}, nil
}

func (g Grant) Capability() Capability {
	return g.capability
}

func (g Grant) Operations() []Operation {
	return append([]Operation(nil), g.operations...)
}

type DefinitionInput struct {
	Implementation ImplementationKey
	ConfigVariant  ConfigVariant
	Provides       []Capability
	Grants         []Grant
	OwnerScopes    []OwnerScope
	Actions        []ActionDefinition
}

type Definition struct {
	implementation ImplementationKey
	configVariant  ConfigVariant
	provides       []Capability
	grants         []Grant
	ownerScopes    []OwnerScope
	actions        []ActionDefinition
	digest         [sha256.Size]byte
}

func NewDefinition(input DefinitionInput) (Definition, error) {
	if err := validateToken("implementation", string(input.Implementation)); err != nil {
		return Definition{}, err
	}
	if err := validateToken("config variant", string(input.ConfigVariant)); err != nil {
		return Definition{}, err
	}
	if len(input.Provides) == 0 {
		return Definition{}, fmt.Errorf("component: at least one provided capability is required")
	}
	if len(input.OwnerScopes) == 0 {
		return Definition{}, fmt.Errorf("component: at least one owner scope is required")
	}

	definition := Definition{
		implementation: input.Implementation,
		configVariant:  input.ConfigVariant,
		provides:       append([]Capability(nil), input.Provides...),
		grants:         cloneGrants(input.Grants),
		ownerScopes:    append([]OwnerScope(nil), input.OwnerScopes...),
		actions:        append([]ActionDefinition(nil), input.Actions...),
	}
	if err := canonicalizeDefinition(&definition); err != nil {
		return Definition{}, err
	}
	definition.digest = definitionDigest(definition)
	return definition, nil
}

func (d Definition) Validate() error {
	rebuilt, err := NewDefinition(DefinitionInput{
		Implementation: d.implementation,
		ConfigVariant:  d.configVariant,
		Provides:       d.provides,
		Grants:         d.grants,
		OwnerScopes:    d.ownerScopes,
		Actions:        d.actions,
	})
	if err != nil {
		return err
	}
	if rebuilt.digest != d.digest {
		return fmt.Errorf("component: definition digest is invalid")
	}
	return nil
}

func (d Definition) Implementation() ImplementationKey {
	return d.implementation
}

func (d Definition) ConfigVariant() ConfigVariant {
	return d.configVariant
}

func (d Definition) Provides() []Capability {
	return append([]Capability(nil), d.provides...)
}

func (d Definition) Grants() []Grant {
	return cloneGrants(d.grants)
}

func (d Definition) OwnerScopes() []OwnerScope {
	return append([]OwnerScope(nil), d.ownerScopes...)
}

func (d Definition) Actions() []ActionDefinition {
	return append([]ActionDefinition(nil), d.actions...)
}

func (d Definition) FindAction(id ActionID) (ActionDefinition, bool) {
	index := sort.Search(len(d.actions), func(index int) bool {
		return d.actions[index].ID() >= id
	})
	if index == len(d.actions) || d.actions[index].ID() != id {
		return ActionDefinition{}, false
	}
	return d.actions[index], true
}

func (d Definition) Digest() [sha256.Size]byte {
	return d.digest
}

func (c Capability) Valid() bool {
	switch c {
	case CapabilityServices, CapabilityRoutes, CapabilityVolumes, CapabilitySecrets,
		CapabilityScripts, CapabilityBackups, CapabilityNetworks, CapabilityEntries,
		CapabilityTasks, CapabilityHTTPRouter, CapabilityEdgeTunnel,
		CapabilityDNSResolver, CapabilityManagedConfig, CapabilityHostResolution,
		CapabilityHostTrust:
		return true
	default:
		return false
	}
}

func (o Operation) Valid() bool {
	switch o {
	case OperationList, OperationRead, OperationCreate, OperationEdit, OperationRemove,
		OperationRun, OperationBind, OperationConfigure, OperationActivate,
		OperationObserve, OperationMaterialize:
		return true
	default:
		return false
	}
}

func (o OwnerScope) Valid() bool {
	return o == OwnerScopeEnvironment || o == OwnerScopePlatform
}

func canonicalizeDefinition(definition *Definition) error {
	sort.Slice(definition.provides, func(i, j int) bool { return definition.provides[i] < definition.provides[j] })
	for index, capability := range definition.provides {
		if !capability.Valid() {
			return fmt.Errorf("component: invalid provided capability %q", capability)
		}
		if index > 0 && capability == definition.provides[index-1] {
			return fmt.Errorf("component: repeated provided capability %q", capability)
		}
	}

	sort.Slice(definition.grants, func(i, j int) bool {
		return definition.grants[i].capability < definition.grants[j].capability
	})
	for index, grant := range definition.grants {
		canonical, err := NewGrant(grant.capability, grant.operations...)
		if err != nil {
			return err
		}
		definition.grants[index] = canonical
		if index > 0 && grant.capability == definition.grants[index-1].capability {
			return fmt.Errorf("component: repeated grant for capability %q", grant.capability)
		}
	}

	sort.Slice(definition.ownerScopes, func(i, j int) bool {
		return definition.ownerScopes[i] < definition.ownerScopes[j]
	})
	for index, scope := range definition.ownerScopes {
		if !scope.Valid() {
			return fmt.Errorf("component: invalid owner scope %q", scope)
		}
		if index > 0 && scope == definition.ownerScopes[index-1] {
			return fmt.Errorf("component: repeated owner scope %q", scope)
		}
	}

	sort.Slice(definition.actions, func(i, j int) bool {
		return definition.actions[i].ID() < definition.actions[j].ID()
	})
	for index, action := range definition.actions {
		if err := action.Validate(); err != nil {
			return err
		}
		if index > 0 && action.ID() == definition.actions[index-1].ID() {
			return fmt.Errorf("component: repeated action %q", action.ID())
		}
	}
	return nil
}

func definitionDigest(definition Definition) [sha256.Size]byte {
	encoded := appendString(nil, "groundplane-component-definition-v1")
	encoded = appendString(encoded, string(definition.implementation))
	encoded = appendString(encoded, string(definition.configVariant))
	encoded = appendCount(encoded, len(definition.provides))
	for _, capability := range definition.provides {
		encoded = appendString(encoded, string(capability))
	}
	encoded = appendCount(encoded, len(definition.grants))
	for _, grant := range definition.grants {
		encoded = appendString(encoded, string(grant.capability))
		encoded = appendCount(encoded, len(grant.operations))
		for _, operation := range grant.operations {
			encoded = appendString(encoded, string(operation))
		}
	}
	encoded = appendCount(encoded, len(definition.ownerScopes))
	for _, scope := range definition.ownerScopes {
		encoded = appendString(encoded, string(scope))
	}
	encoded = appendCount(encoded, len(definition.actions))
	for _, action := range definition.actions {
		encoded = appendString(encoded, string(action.ID()))
		encoded = appendString(encoded, string(action.Capability()))
		encoded = appendString(encoded, string(action.Operation()))
	}
	return sha256.Sum256(encoded)
}

func appendString(target []byte, value string) []byte {
	target = appendCount(target, len(value))
	return append(target, value...)
}

func appendCount(target []byte, value int) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], uint32(value))
	return append(target, encoded[:]...)
}

func cloneGrants(grants []Grant) []Grant {
	cloned := make([]Grant, len(grants))
	for index, grant := range grants {
		cloned[index] = Grant{
			capability: grant.capability,
			operations: append([]Operation(nil), grant.operations...),
		}
	}
	return cloned
}

func validateToken(field, value string) error {
	if value == "" || len(value) > 64 || value[0] < 'a' || value[0] > 'z' {
		return fmt.Errorf("component: %s must be a lowercase token", field)
	}
	previousDash := false
	for _, character := range value {
		valid := character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-'
		if !valid || character == '-' && previousDash {
			return fmt.Errorf("component: %s must be a lowercase token", field)
		}
		previousDash = character == '-'
	}
	if strings.HasSuffix(value, "-") {
		return fmt.Errorf("component: %s must be a lowercase token", field)
	}
	return nil
}
