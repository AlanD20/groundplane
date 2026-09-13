package catalog

import (
	"crypto/sha256"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

type Catalog struct {
	definitions             []component.Definition
	images                  []registeredImage
	managedConfigActions    []registeredManagedConfigAction
	containerConfigActions  []registeredContainerConfigAction
	dnsResolverObservations []registeredDNSResolverObservation
	digest                  [sha256.Size]byte
}

type Registration struct {
	Definition              component.Definition
	Images                  []component.OCIImage
	ManagedConfigActions    []ManagedConfigActionRecipe
	ContainerConfigActions  []ContainerConfigActionRecipe
	DNSResolverObservations []DNSResolverObservationRecipe
}

type registeredImage struct {
	implementation component.ImplementationKey
	image          component.OCIImage
}

type ManagedConfigActionRecipe struct {
	actionID     component.ActionID
	relativePath string
	validateArgs []string
	image        component.OCIImage
}

type registeredManagedConfigAction struct {
	implementation component.ImplementationKey
	recipe         ManagedConfigActionRecipe
}

type DNSResolverObservationRecipe struct {
	actionID       component.ActionID
	serviceName    string
	artifactTarget string
	image          component.OCIImage
	listenEndpoint string
	metricsURL     string
	reloadMetric   string
}

type registeredDNSResolverObservation struct {
	implementation component.ImplementationKey
	recipe         DNSResolverObservationRecipe
}

func NewDNSResolverObservationRecipe(
	actionID component.ActionID,
	serviceName string,
	artifactTarget string,
	image component.OCIImage,
	listenEndpoint string,
	metricsURL string,
	reloadMetric string,
) (DNSResolverObservationRecipe, error) {
	if actionID == "" || !safeToken(serviceName) || !path.IsAbs(artifactTarget) ||
		path.Clean(artifactTarget) != artifactTarget || image.Validate() != nil ||
		listenEndpoint != "127.0.0.1:53" || metricsURL != "http://127.0.0.1:9153/metrics" ||
		!safeToken(reloadMetric) {
		return DNSResolverObservationRecipe{}, fmt.Errorf(
			"component catalog: DNS resolver observation recipe is invalid",
		)
	}
	return DNSResolverObservationRecipe{
		actionID: actionID, serviceName: serviceName, artifactTarget: artifactTarget,
		image: cloneOCIImage(image), listenEndpoint: listenEndpoint,
		metricsURL: metricsURL, reloadMetric: reloadMetric,
	}, nil
}

func (recipe DNSResolverObservationRecipe) ActionID() component.ActionID { return recipe.actionID }
func (recipe DNSResolverObservationRecipe) ServiceName() string          { return recipe.serviceName }
func (recipe DNSResolverObservationRecipe) ArtifactTarget() string       { return recipe.artifactTarget }
func (recipe DNSResolverObservationRecipe) Image() component.OCIImage {
	return cloneOCIImage(recipe.image)
}
func (recipe DNSResolverObservationRecipe) ListenEndpoint() string { return recipe.listenEndpoint }
func (recipe DNSResolverObservationRecipe) MetricsURL() string     { return recipe.metricsURL }
func (recipe DNSResolverObservationRecipe) ReloadMetric() string   { return recipe.reloadMetric }

type ContainerConfigActionRecipe struct {
	actionID      component.ActionID
	relativePath  string
	containerPath string
	preflightArgs []string
	validateArgs  []string
	activateArgs  []string
	image         component.OCIImage
}

type registeredContainerConfigAction struct {
	implementation component.ImplementationKey
	recipe         ContainerConfigActionRecipe
}

func NewManagedConfigActionRecipe(
	actionID component.ActionID,
	relativePath string,
	validateArgs []string,
	image component.OCIImage,
) (ManagedConfigActionRecipe, error) {
	if actionID == "" || !validManagedConfigRelativePath(relativePath) ||
		!validContainerCommand(validateArgs) || image.Validate() != nil {
		return ManagedConfigActionRecipe{}, fmt.Errorf("component catalog: managed-config action recipe is invalid")
	}
	return ManagedConfigActionRecipe{
		actionID: actionID, relativePath: relativePath,
		validateArgs: append([]string(nil), validateArgs...), image: cloneOCIImage(image),
	}, nil
}

func (recipe ManagedConfigActionRecipe) ActionID() component.ActionID {
	return recipe.actionID
}

func (recipe ManagedConfigActionRecipe) RelativePath() string {
	return recipe.relativePath
}

func (recipe ManagedConfigActionRecipe) ValidateArgs() []string {
	return append([]string(nil), recipe.validateArgs...)
}

func (recipe ManagedConfigActionRecipe) Image() component.OCIImage {
	return cloneOCIImage(recipe.image)
}

func NewContainerConfigActionRecipe(
	actionID component.ActionID,
	relativePath string,
	containerPath string,
	preflightArgs []string,
	validateArgs []string,
	activateArgs []string,
	image component.OCIImage,
) (ContainerConfigActionRecipe, error) {
	if actionID == "" || !validManagedConfigRelativePath(relativePath) ||
		!path.IsAbs(containerPath) || path.Clean(containerPath) != containerPath ||
		!validContainerCommand(
			preflightArgs,
		) || !validContainerCommand(validateArgs) || !validContainerCommand(activateArgs) ||
		image.Validate() != nil {
		return ContainerConfigActionRecipe{}, fmt.Errorf("component catalog: container-config action recipe is invalid")
	}
	return ContainerConfigActionRecipe{
		actionID: actionID, relativePath: relativePath, containerPath: containerPath,
		preflightArgs: append([]string(nil), preflightArgs...),
		validateArgs:  append([]string(nil), validateArgs...),
		activateArgs:  append([]string(nil), activateArgs...),
		image:         cloneOCIImage(image),
	}, nil
}

func (recipe ContainerConfigActionRecipe) ActionID() component.ActionID { return recipe.actionID }
func (recipe ContainerConfigActionRecipe) RelativePath() string         { return recipe.relativePath }
func (recipe ContainerConfigActionRecipe) ContainerPath() string        { return recipe.containerPath }
func (recipe ContainerConfigActionRecipe) PreflightArgs() []string {
	return append([]string(nil), recipe.preflightArgs...)
}
func (recipe ContainerConfigActionRecipe) ValidateArgs() []string {
	return append([]string(nil), recipe.validateArgs...)
}
func (recipe ContainerConfigActionRecipe) ActivateArgs() []string {
	return append([]string(nil), recipe.activateArgs...)
}
func (recipe ContainerConfigActionRecipe) Image() component.OCIImage {
	return cloneOCIImage(recipe.image)
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
	observations := make([]registeredDNSResolverObservation, 0)
	images := make([]registeredImage, 0)
	for index, registration := range registrations {
		definition := registration.Definition
		if err := definition.Validate(); err != nil {
			return Catalog{}, fmt.Errorf("component catalog: %w", err)
		}
		canonical[index] = definition
		imageRepositories := make(map[string]struct{}, len(registration.Images))
		for _, image := range registration.Images {
			if image.Validate() != nil {
				return Catalog{}, fmt.Errorf("component catalog: invalid image for %q", definition.Implementation())
			}
			if _, duplicate := imageRepositories[image.Repository]; duplicate {
				return Catalog{}, fmt.Errorf(
					"component catalog: repeated image repository %q for %q",
					image.Repository,
					definition.Implementation(),
				)
			}
			imageRepositories[image.Repository] = struct{}{}
			images = append(
				images,
				registeredImage{implementation: definition.Implementation(), image: cloneOCIImage(image)},
			)
		}
		for _, recipe := range registration.ManagedConfigActions {
			action, found := definition.FindAction(recipe.actionID)
			if !found || action.Capability() != component.CapabilityManagedConfig ||
				action.Operation() != component.OperationActivate ||
				!validManagedConfigRelativePath(recipe.relativePath) ||
				!validContainerCommand(recipe.validateArgs) || recipe.image.Validate() != nil ||
				!registeredImageMatches(images, definition.Implementation(), recipe.image) {
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
				!validContainerCommand(
					recipe.preflightArgs,
				) || !validContainerCommand(recipe.validateArgs) || !validContainerCommand(recipe.activateArgs) ||
				recipe.image.Validate() != nil ||
				!registeredImageMatches(images, definition.Implementation(), recipe.image) {
				return Catalog{}, fmt.Errorf(
					"component catalog: invalid container-config recipe for %q",
					definition.Implementation(),
				)
			}
			containerActions = append(containerActions, registeredContainerConfigAction{
				implementation: definition.Implementation(), recipe: recipe,
			})
		}
		for _, recipe := range registration.DNSResolverObservations {
			action, found := definition.FindAction(recipe.actionID)
			if !found || action.Capability() != component.CapabilityHostResolution ||
				action.Operation() != component.OperationObserve || !safeToken(recipe.serviceName) ||
				!path.IsAbs(recipe.artifactTarget) || path.Clean(recipe.artifactTarget) != recipe.artifactTarget ||
				recipe.image.Validate() != nil || !registeredImageMatches(images, definition.Implementation(), recipe.image) ||
				recipe.listenEndpoint != "127.0.0.1:53" ||
				recipe.metricsURL != "http://127.0.0.1:9153/metrics" || !safeToken(recipe.reloadMetric) {
				return Catalog{}, fmt.Errorf(
					"component catalog: invalid DNS resolver observation recipe for %q",
					definition.Implementation(),
				)
			}
			observations = append(observations, registeredDNSResolverObservation{
				implementation: definition.Implementation(), recipe: recipe,
			})
		}
	}
	sort.Slice(canonical, func(i, j int) bool {
		return canonical[i].Implementation() < canonical[j].Implementation()
	})
	sort.Slice(images, func(left, right int) bool {
		if images[left].implementation != images[right].implementation {
			return images[left].implementation < images[right].implementation
		}
		return images[left].image.Repository < images[right].image.Repository
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
	sort.Slice(observations, func(left, right int) bool {
		if observations[left].implementation != observations[right].implementation {
			return observations[left].implementation < observations[right].implementation
		}
		return observations[left].recipe.actionID < observations[right].recipe.actionID
	})
	for index := 1; index < len(observations); index++ {
		if observations[index].implementation == observations[index-1].implementation &&
			observations[index].recipe.actionID == observations[index-1].recipe.actionID {
			return Catalog{}, fmt.Errorf(
				"component catalog: repeated DNS resolver observation recipe %q for %q",
				observations[index].recipe.actionID,
				observations[index].implementation,
			)
		}
	}
	catalog := Catalog{
		definitions:             canonical,
		images:                  images,
		managedConfigActions:    actions,
		containerConfigActions:  containerActions,
		dnsResolverObservations: observations,
	}
	catalog.digest = catalogDigest(canonical, images, actions, containerActions, observations)
	return catalog, nil
}

func (c Catalog) ValidateEnvironmentPlanImages(
	implementation component.ImplementationKey,
	plan component.EnvironmentPlan,
) error {
	if _, found := c.Find(implementation); !found {
		return fmt.Errorf("component catalog: implementation %q is not registered", implementation)
	}
	for _, service := range plan.Services {
		if service.Image.Validate() != nil || !registeredImageMatches(c.images, implementation, service.Image) {
			return fmt.Errorf("component catalog: planner emitted an unregistered managed image for %q", implementation)
		}
	}
	return nil
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

func (c Catalog) ResolveDNSResolverObservation(
	implementation component.ImplementationKey,
	actionID component.ActionID,
) (DNSResolverObservationRecipe, bool) {
	index := sort.Search(len(c.dnsResolverObservations), func(index int) bool {
		candidate := c.dnsResolverObservations[index]
		return candidate.implementation > implementation ||
			candidate.implementation == implementation && candidate.recipe.actionID >= actionID
	})
	if index == len(c.dnsResolverObservations) ||
		c.dnsResolverObservations[index].implementation != implementation ||
		c.dnsResolverObservations[index].recipe.actionID != actionID {
		return DNSResolverObservationRecipe{}, false
	}
	return c.dnsResolverObservations[index].recipe, true
}

func (c Catalog) ResolveDNSResolverObservationActionEnvelope(
	envelope component.ActionEnvelope,
) (component.Definition, component.ActionDefinition, DNSResolverObservationRecipe, error) {
	definition, action, err := c.ResolveActionEnvelope(envelope)
	if err != nil {
		return component.Definition{}, component.ActionDefinition{}, DNSResolverObservationRecipe{}, err
	}
	recipe, found := c.ResolveDNSResolverObservation(definition.Implementation(), action.ID())
	if !found {
		return component.Definition{}, component.ActionDefinition{}, DNSResolverObservationRecipe{}, fmt.Errorf(
			"component catalog: DNS resolver observation recipe is not registered for action %q", action.ID(),
		)
	}
	return definition, action, recipe, nil
}

func (c Catalog) findDefinitionByDigest(digest [sha256.Size]byte) (component.Definition, bool) {
	for _, definition := range c.definitions {
		if definition.Digest() == digest {
			return definition, true
		}
	}
	return component.Definition{}, false
}

func cloneOCIImage(image component.OCIImage) component.OCIImage {
	image.Platforms = append([]component.OCIPlatform(nil), image.Platforms...)
	return image
}

func registeredImageMatches(
	images []registeredImage,
	implementation component.ImplementationKey,
	image component.OCIImage,
) bool {
	for _, registered := range images {
		if registered.implementation == implementation && registered.image.Equal(image) {
			return true
		}
	}
	return false
}

func validImmutableImageReference(value string) bool {
	separator := strings.LastIndex(value, "@sha256:")
	if separator <= 0 || len(value)-separator != len("@sha256:")+sha256.Size*2 {
		return false
	}
	for _, character := range value[separator+len("@sha256:"):] {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
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

func safeToken(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character != '-' && character != '_' && character != '.' &&
			(character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}
