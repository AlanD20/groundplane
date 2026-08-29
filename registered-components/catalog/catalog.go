package catalog

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

type Catalog struct {
	definitions         []component.Definition
	managedConfigActions []registeredManagedConfigAction
	containerConfigActions []registeredContainerConfigAction
	digest              [sha256.Size]byte
}

type Registration struct {
	Definition           component.Definition
	ManagedConfigActions []ManagedConfigActionRecipe
	ContainerConfigActions []ContainerConfigActionRecipe
}

type ManagedConfigActionRecipe struct {
	actionID     component.ActionID
	relativePath string
}

type registeredManagedConfigAction struct {
	implementation component.ImplementationKey
	recipe         ManagedConfigActionRecipe
}

type ContainerConfigActionRecipe struct {
	actionID       component.ActionID
	relativePath   string
	containerPath  string
	validateArgs   []string
	activateArgs   []string
}

type registeredContainerConfigAction struct {
	implementation component.ImplementationKey
	recipe         ContainerConfigActionRecipe
}

func NewManagedConfigActionRecipe(
	actionID component.ActionID,
	relativePath string,
) (ManagedConfigActionRecipe, error) {
	if actionID == "" || !validManagedConfigRelativePath(relativePath) {
		return ManagedConfigActionRecipe{}, fmt.Errorf("component catalog: managed-config action recipe is invalid")
	}
	return ManagedConfigActionRecipe{actionID: actionID, relativePath: relativePath}, nil
}

func (recipe ManagedConfigActionRecipe) ActionID() component.ActionID {
	return recipe.actionID
}

func (recipe ManagedConfigActionRecipe) RelativePath() string {
	return recipe.relativePath
}

func NewContainerConfigActionRecipe(
	actionID component.ActionID,
	relativePath string,
	containerPath string,
	validateArgs []string,
	activateArgs []string,
) (ContainerConfigActionRecipe, error) {
	if actionID == "" || !validManagedConfigRelativePath(relativePath) ||
		!path.IsAbs(containerPath) || path.Clean(containerPath) != containerPath ||
		!validContainerCommand(validateArgs) || !validContainerCommand(activateArgs) {
		return ContainerConfigActionRecipe{}, fmt.Errorf("component catalog: container-config action recipe is invalid")
	}
	return ContainerConfigActionRecipe{
		actionID: actionID, relativePath: relativePath, containerPath: containerPath,
		validateArgs: append([]string(nil), validateArgs...),
		activateArgs: append([]string(nil), activateArgs...),
	}, nil
}

func (recipe ContainerConfigActionRecipe) ActionID() component.ActionID { return recipe.actionID }
func (recipe ContainerConfigActionRecipe) RelativePath() string { return recipe.relativePath }
func (recipe ContainerConfigActionRecipe) ContainerPath() string { return recipe.containerPath }
func (recipe ContainerConfigActionRecipe) ValidateArgs() []string {
	return append([]string(nil), recipe.validateArgs...)
}
func (recipe ContainerConfigActionRecipe) ActivateArgs() []string {
	return append([]string(nil), recipe.activateArgs...)
}

func New(definitions ...component.Definition) (Catalog, error) {
	registrations := make([]Registration, len(definitions))
	for index, definition := range definitions {
		registrations[index] = Registration{Definition: definition}
	}
	return NewRegistered(registrations...)
}

func NewRegistered(registrations ...Registration) (Catalog, error) {
	if len(registrations) == 0 {
		return Catalog{}, fmt.Errorf("component catalog: at least one definition is required")
	}
	canonical := make([]component.Definition, len(registrations))
	actions := make([]registeredManagedConfigAction, 0)
	containerActions := make([]registeredContainerConfigAction, 0)
	for index, registration := range registrations {
		definition := registration.Definition
		if err := definition.Validate(); err != nil {
			return Catalog{}, fmt.Errorf("component catalog: %w", err)
		}
		canonical[index] = definition
		for _, recipe := range registration.ManagedConfigActions {
			action, found := definition.FindAction(recipe.actionID)
			if !found || action.Capability() != component.CapabilityManagedConfig ||
				action.Operation() != component.OperationActivate ||
				!validManagedConfigRelativePath(recipe.relativePath) {
				return Catalog{}, fmt.Errorf(
					"component catalog: invalid managed-config recipe for %q",
					definition.Implementation(),
				)
			}
			actions = append(actions, registeredManagedConfigAction{
				implementation: definition.Implementation(), recipe: recipe,
			})
		}
		for _, recipe := range registration.ContainerConfigActions {
			action, found := definition.FindAction(recipe.actionID)
			if !found || action.Capability() != component.CapabilityManagedConfig ||
				action.Operation() != component.OperationActivate ||
				!validManagedConfigRelativePath(recipe.relativePath) ||
				!path.IsAbs(recipe.containerPath) || path.Clean(recipe.containerPath) != recipe.containerPath ||
				!validContainerCommand(recipe.validateArgs) || !validContainerCommand(recipe.activateArgs) {
				return Catalog{}, fmt.Errorf(
					"component catalog: invalid container-config recipe for %q",
					definition.Implementation(),
				)
			}
			containerActions = append(containerActions, registeredContainerConfigAction{
				implementation: definition.Implementation(), recipe: recipe,
			})
		}
	}
	sort.Slice(canonical, func(i, j int) bool {
		return canonical[i].Implementation() < canonical[j].Implementation()
	})
	for index := 1; index < len(canonical); index++ {
		if canonical[index].Implementation() == canonical[index-1].Implementation() {
			return Catalog{}, fmt.Errorf(
				"component catalog: repeated implementation %q",
				canonical[index].Implementation(),
			)
		}
	}
	sort.Slice(actions, func(left, right int) bool {
		if actions[left].implementation != actions[right].implementation {
			return actions[left].implementation < actions[right].implementation
		}
		return actions[left].recipe.actionID < actions[right].recipe.actionID
	})
	for index := 1; index < len(actions); index++ {
		if actions[index].implementation == actions[index-1].implementation &&
			actions[index].recipe.actionID == actions[index-1].recipe.actionID {
			return Catalog{}, fmt.Errorf(
				"component catalog: repeated managed-config recipe %q for %q",
				actions[index].recipe.actionID,
				actions[index].implementation,
			)
		}
	}
	sort.Slice(containerActions, func(left, right int) bool {
		if containerActions[left].implementation != containerActions[right].implementation {
			return containerActions[left].implementation < containerActions[right].implementation
		}
		return containerActions[left].recipe.actionID < containerActions[right].recipe.actionID
	})
	for index := 1; index < len(containerActions); index++ {
		if containerActions[index].implementation == containerActions[index-1].implementation &&
			containerActions[index].recipe.actionID == containerActions[index-1].recipe.actionID {
			return Catalog{}, fmt.Errorf(
				"component catalog: repeated container-config recipe %q for %q",
				containerActions[index].recipe.actionID,
				containerActions[index].implementation,
			)
		}
	}
	catalog := Catalog{
		definitions: canonical,
		managedConfigActions: actions,
		containerConfigActions: containerActions,
	}
	catalog.digest = catalogDigest(canonical, actions, containerActions)
	return catalog, nil
}

func (c Catalog) Definitions() []component.Definition {
	return append([]component.Definition(nil), c.definitions...)
}

func (c Catalog) Digest() [sha256.Size]byte {
	return c.digest
}

func (c Catalog) Find(implementation component.ImplementationKey) (component.Definition, bool) {
	index := sort.Search(len(c.definitions), func(index int) bool {
		return c.definitions[index].Implementation() >= implementation
	})
	if index == len(c.definitions) || c.definitions[index].Implementation() != implementation {
		return component.Definition{}, false
	}
	return c.definitions[index], true
}

func (c Catalog) FindAction(
	implementation component.ImplementationKey,
	actionID component.ActionID,
) (component.Definition, component.ActionDefinition, bool) {
	definition, found := c.Find(implementation)
	if !found {
		return component.Definition{}, component.ActionDefinition{}, false
	}
	action, found := definition.FindAction(actionID)
	if !found {
		return component.Definition{}, component.ActionDefinition{}, false
	}
	return definition, action, true
}

func (c Catalog) ResolveActionEnvelope(
	envelope component.ActionEnvelope,
) (component.Definition, component.ActionDefinition, error) {
	if envelope.CatalogDigest() != c.digest {
		return component.Definition{}, component.ActionDefinition{}, fmt.Errorf(
			"component catalog: action catalog digest does not match the compiled catalog",
		)
	}
	definition, found := c.findDefinitionByDigest(envelope.DefinitionDigest())
	if !found {
		return component.Definition{}, component.ActionDefinition{}, fmt.Errorf(
			"component catalog: action definition digest is not registered",
		)
	}
	action, found := definition.FindAction(envelope.ActionID())
	if !found {
		return component.Definition{}, component.ActionDefinition{}, fmt.Errorf(
			"component catalog: action %q is not registered for definition %q",
			envelope.ActionID(),
			definition.Implementation(),
		)
	}
	return definition, action, nil
}

func (c Catalog) ResolveManagedConfigActionEnvelope(
	envelope component.ActionEnvelope,
) (component.Definition, component.ActionDefinition, ManagedConfigActionRecipe, error) {
	definition, action, err := c.ResolveActionEnvelope(envelope)
	if err != nil {
		return component.Definition{}, component.ActionDefinition{}, ManagedConfigActionRecipe{}, err
	}
	index := sort.Search(len(c.managedConfigActions), func(index int) bool {
		candidate := c.managedConfigActions[index]
		return candidate.implementation > definition.Implementation() ||
			candidate.implementation == definition.Implementation() && candidate.recipe.actionID >= action.ID()
	})
	if index == len(c.managedConfigActions) ||
		c.managedConfigActions[index].implementation != definition.Implementation() ||
		c.managedConfigActions[index].recipe.actionID != action.ID() {
		return component.Definition{}, component.ActionDefinition{}, ManagedConfigActionRecipe{}, fmt.Errorf(
			"component catalog: managed-config recipe is not registered for action %q",
			action.ID(),
		)
	}
	return definition, action, c.managedConfigActions[index].recipe, nil
}

func (c Catalog) ResolveContainerConfigActionEnvelope(
	envelope component.ActionEnvelope,
) (component.Definition, component.ActionDefinition, ContainerConfigActionRecipe, error) {
	definition, action, err := c.ResolveActionEnvelope(envelope)
	if err != nil {
		return component.Definition{}, component.ActionDefinition{}, ContainerConfigActionRecipe{}, err
	}
	index := sort.Search(len(c.containerConfigActions), func(index int) bool {
		candidate := c.containerConfigActions[index]
		return candidate.implementation > definition.Implementation() ||
			candidate.implementation == definition.Implementation() && candidate.recipe.actionID >= action.ID()
	})
	if index == len(c.containerConfigActions) ||
		c.containerConfigActions[index].implementation != definition.Implementation() ||
		c.containerConfigActions[index].recipe.actionID != action.ID() {
		return component.Definition{}, component.ActionDefinition{}, ContainerConfigActionRecipe{}, fmt.Errorf(
			"component catalog: container-config recipe is not registered for action %q",
			action.ID(),
		)
	}
	return definition, action, c.containerConfigActions[index].recipe, nil
}

func (c Catalog) findDefinitionByDigest(digest [sha256.Size]byte) (component.Definition, bool) {
	for _, definition := range c.definitions {
		if definition.Digest() == digest {
			return definition, true
		}
	}
	return component.Definition{}, false
}

func catalogDigest(
	definitions []component.Definition,
	actions []registeredManagedConfigAction,
	containerActions []registeredContainerConfigAction,
) [sha256.Size]byte {
	encoded := appendLength(nil, len("groundplane-component-catalog-v3"))
	encoded = append(encoded, "groundplane-component-catalog-v3"...)
	encoded = appendLength(encoded, len(definitions))
	for _, definition := range definitions {
		implementation := string(definition.Implementation())
		encoded = appendLength(encoded, len(implementation))
		encoded = append(encoded, implementation...)
		digest := definition.Digest()
		encoded = append(encoded, digest[:]...)
	}
	encoded = appendLength(encoded, len(actions))
	for _, action := range actions {
		implementation := string(action.implementation)
		encoded = appendLength(encoded, len(implementation))
		encoded = append(encoded, implementation...)
		actionID := string(action.recipe.actionID)
		encoded = appendLength(encoded, len(actionID))
		encoded = append(encoded, actionID...)
		encoded = appendLength(encoded, len(action.recipe.relativePath))
		encoded = append(encoded, action.recipe.relativePath...)
	}
	encoded = appendLength(encoded, len(containerActions))
	for _, action := range containerActions {
		implementation := string(action.implementation)
		encoded = appendLength(encoded, len(implementation))
		encoded = append(encoded, implementation...)
		actionID := string(action.recipe.actionID)
		encoded = appendLength(encoded, len(actionID))
		encoded = append(encoded, actionID...)
		for _, value := range []string{action.recipe.relativePath, action.recipe.containerPath} {
			encoded = appendLength(encoded, len(value))
			encoded = append(encoded, value...)
		}
		for _, command := range [][]string{action.recipe.validateArgs, action.recipe.activateArgs} {
			encoded = appendLength(encoded, len(command))
			for _, argument := range command {
				encoded = appendLength(encoded, len(argument))
				encoded = append(encoded, argument...)
			}
		}
	}
	return sha256.Sum256(encoded)
}

func validManagedConfigRelativePath(value string) bool {
	return value != "" && len(value) <= 240 && !path.IsAbs(value) && path.Clean(value) == value &&
		value != "." && !strings.HasPrefix(value, "../") && !strings.ContainsRune(value, 0)
}

func validContainerCommand(arguments []string) bool {
	if len(arguments) == 0 || len(arguments) > 32 {
		return false
	}
	for _, argument := range arguments {
		if argument == "" || len(argument) > 1024 || strings.ContainsRune(argument, 0) {
			return false
		}
	}
	return true
}

func appendLength(target []byte, value int) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], uint32(value))
	return append(target, encoded[:]...)
}
