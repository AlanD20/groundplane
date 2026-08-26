package blueprintparser

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/compose-spec/compose-go/v2/loader"
	"github.com/compose-spec/compose-go/v2/types"
	"gopkg.in/yaml.v3"
)

const maxResolvedResources = 512

// EnvironmentScope is the route-resolved owner of an Environment Blueprint.
// Human labels authenticate the envelope target; only EnvironmentID determines runtime identity.
type EnvironmentScope struct {
	EnvironmentID string
	Tenant        string
	Project       string
	Environment   string
}

// Extensions is the typed document-level Groundplane input stripped from the
// root before native Compose loading.
type Extensions struct {
	Requires      []core.Requirement
	Attachments   map[string]core.AttachmentSpec
	Entries       map[string]core.EntrySpec
	Routes        []core.RouteSpec
	Components    map[string]core.ComponentSpec
	Backup        *core.BackupSpec
	ReleaseGroups map[string]core.ReleaseGroupSpec
}

// Result is the closed, typed output of Blueprint bundle parsing.
type Result struct {
	Envelope          core.Envelope
	Extensions        Extensions
	ServiceExtensions map[string]core.ServiceExtensionSpec
	Project           *types.Project
}

type rootDocument struct {
	core.Envelope `yaml:",inline"`
	Requires      []core.Requirement               `yaml:"x-gp-requires,omitempty"`
	Attachments   map[string]core.AttachmentSpec   `yaml:"x-gp-attachments,omitempty"`
	Entries       map[string]core.EntrySpec        `yaml:"x-gp-entry,omitempty"`
	Routes        []core.RouteSpec                 `yaml:"x-gp-routes,omitempty"`
	Components    map[string]core.ComponentSpec    `yaml:"x-gp-components,omitempty"`
	Backup        *authoredBackupSpec              `yaml:"x-gp-backup,omitempty"`
	ReleaseGroups map[string]core.ReleaseGroupSpec `yaml:"x-gp-release-groups,omitempty"`
}

// authoredBackupSpec retains Keep presence long enough to distinguish an
// omitted value on the disabled-unconfigured shape from an explicitly authored
// zero. Presence is parser input evidence, not part of desired state.
type authoredBackupSpec struct {
	Enabled    bool                    `yaml:"enabled"`
	Frequency  string                  `yaml:"frequency,omitempty"`
	Keep       *int64                  `yaml:"keep,omitempty"`
	Encryption string                  `yaml:"encryption,omitempty"`
	Connector  string                  `yaml:"connector,omitempty"`
	Sources    []core.BackupSourceSpec `yaml:"sources,omitempty"`
}

var rootGroundplaneFields = map[string]struct{}{
	"kind": {}, "schema": {}, "metadata": {},
	"x-gp-requires": {}, "x-gp-attachments": {}, "x-gp-entry": {}, "x-gp-routes": {},
	"x-gp-components": {}, "x-gp-backup": {}, "x-gp-release-groups": {},
}

// Parse validates and loads a closed Blueprint bundle for one already-resolved Environment without ambient input.
func Parse(ctx context.Context, scope EnvironmentScope, bundle core.BlueprintBundle) (Result, error) {
	if ctx == nil {
		return Result{}, errs.New(errs.KindInternal, "blueprint parser context is required")
	}
	if ids.Validate(ids.KindEnvironment, scope.EnvironmentID) != nil || scope.Tenant == "" || scope.Project == "" ||
		scope.Environment == "" {
		return Result{}, errs.New(errs.KindInternal, "blueprint parser environment scope is invalid")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if err := bundle.Validate(); err != nil {
		return Result{}, validationError("blueprint bundle is invalid")
	}
	if err := Preflight(ctx, bundle); err != nil {
		return Result{}, err
	}

	rootFile, _ := bundle.File(bundle.RootPath)
	envelope, extensions, rootCompose, err := parseRoot(rootFile.Content)
	if err != nil {
		return Result{}, err
	}
	if envelope.Metadata.Tenant != scope.Tenant || envelope.Metadata.Project != scope.Project ||
		envelope.Metadata.Environment != scope.Environment {
		return Result{}, validationError("blueprint metadata does not match the resolved environment")
	}
	projectName := "gp-" + strings.ToLower(scope.EnvironmentID)

	workspace, err := os.MkdirTemp("", "groundplane-blueprint-")
	if err != nil {
		return Result{}, errs.New(errs.KindInternal, "blueprint workspace creation failed")
	}
	defer func() {
		// Cleanup is best-effort: the private workspace contains no durable
		// state, and a cleanup failure must not replace the parse result.
		_ = os.RemoveAll(workspace)
	}()
	emptyEnv, err := createEmptyEnvironmentFile(workspace)
	if err != nil {
		return Result{}, errs.New(errs.KindInternal, "blueprint workspace creation failed")
	}

	plan := newParsePlan(bundle, rootCompose, projectName, emptyEnv)
	baseDir := path.Dir(bundle.RootPath)
	for index, source := range bundle.ComposeSources {
		if err := plan.scan(ctx, scanRequest{
			filename: source,
			baseDir:  baseDir,
			depth:    0,
			root:     index == 0,
			env:      interpolationMapping(bundle.Interpolation),
		}); err != nil {
			return Result{}, err
		}
	}

	expanded := bundle
	expanded.ComposeSources = sortedComposePaths(plan.compose, bundle.ComposeSources)
	if err := Preflight(ctx, expanded); err != nil {
		return Result{}, err
	}
	if err := plan.materialize(workspace); err != nil {
		return Result{}, errs.New(errs.KindInternal, "blueprint workspace materialization failed")
	}

	configFiles := make([]types.ConfigFile, 0, len(bundle.ComposeSources))
	for _, source := range bundle.ComposeSources {
		configFiles = append(configFiles, types.ConfigFile{
			Filename: filepath.Join(workspace, filepath.FromSlash(source)),
			Content:  plan.compose[source],
		})
	}
	project, err := loader.LoadWithContext(ctx, types.ConfigDetails{
		WorkingDir:  filepath.Join(workspace, filepath.FromSlash(baseDir)),
		ConfigFiles: configFiles,
		Environment: interpolationMapping(bundle.Interpolation),
	}, func(options *loader.Options) {
		options.ResolvePaths = true
		options.MaxNodeVisits = maxYAMLNodes
		options.SetProjectName(projectName, true)
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return Result{}, err
		}
		return Result{}, validationError("blueprint Compose project is invalid")
	}
	if resolvedResourceCount(project) > maxResolvedResources {
		return Result{}, validationError("blueprint resolved Compose resource limit exceeded")
	}
	serviceExtensions, err := normalizeServiceExtensions(project)
	if err != nil {
		return Result{}, err
	}
	if err := validateReleaseGroupReferences(extensions.ReleaseGroups, project); err != nil {
		return Result{}, err
	}
	normalizeProjectPaths(project, workspace)

	return Result{
		Envelope: envelope, Extensions: extensions,
		ServiceExtensions: serviceExtensions, Project: project,
	}, nil
}

func parseRoot(content []byte) (core.Envelope, Extensions, []byte, error) {
	document, err := decodeSingleDocument(content)
	if err != nil {
		return core.Envelope{}, Extensions{}, nil, err
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return core.Envelope{}, Extensions{}, nil, validationError("blueprint root must be a mapping")
	}

	root := document.Content[0]
	typed := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	composeNodes := make([]*yaml.Node, 0, len(root.Content))
	for index := 0; index+1 < len(root.Content); index += 2 {
		key := root.Content[index].Value
		if _, known := rootGroundplaneFields[key]; known {
			typed.Content = append(typed.Content, root.Content[index], root.Content[index+1])
			continue
		}
		if strings.HasPrefix(key, "x-gp-") {
			return core.Envelope{}, Extensions{}, nil, validationError(
				"blueprint root has an unknown Groundplane extension",
			)
		}
		composeNodes = append(composeNodes, root.Content[index], root.Content[index+1])
	}

	var authored rootDocument
	if err := decodeKnownFields(typed, &authored); err != nil {
		return core.Envelope{}, Extensions{}, nil, validationError("blueprint root extension is invalid")
	}
	if authored.Kind != core.KindDocEnvironment || authored.Schema != core.EnvelopeSchema {
		return core.Envelope{}, Extensions{}, nil, validationError("blueprint root envelope is invalid")
	}
	if authored.Metadata.Tenant == "" || authored.Metadata.Project == "" || authored.Metadata.Environment == "" {
		return core.Envelope{}, Extensions{}, nil, validationError("blueprint root metadata is incomplete")
	}
	authored.ReleaseGroups, err = normalizeReleaseGroups(authored.ReleaseGroups)
	if err != nil {
		return core.Envelope{}, Extensions{}, nil, err
	}
	authored.Routes, err = normalizeRoutes(authored.Routes)
	if err != nil {
		return core.Envelope{}, Extensions{}, nil, err
	}
	backup, err := normalizeBackupSpec(authored.Backup)
	if err != nil {
		return core.Envelope{}, Extensions{}, nil, err
	}

	root.Content = composeNodes
	compose, err := yaml.Marshal(document)
	if err != nil {
		return core.Envelope{}, Extensions{}, nil, errs.New(errs.KindInternal, "blueprint root preparation failed")
	}
	extensions := Extensions{
		Requires: authored.Requires, Attachments: authored.Attachments, Entries: authored.Entries,
		Routes: authored.Routes, Components: authored.Components, Backup: backup,
		ReleaseGroups: authored.ReleaseGroups,
	}
	return authored.Envelope, extensions, compose, nil
}

func normalizeBackupSpec(authored *authoredBackupSpec) (*core.BackupSpec, error) {
	if authored == nil {
		return nil, nil
	}
	unconfigured := authored.Frequency == "" && authored.Keep == nil && authored.Encryption == "" &&
		authored.Connector == "" && len(authored.Sources) == 0
	if !authored.Enabled && unconfigured {
		return &core.BackupSpec{}, nil
	}
	configured := authored.Frequency != "" && authored.Keep != nil && authored.Encryption != "" &&
		authored.Connector != "" && len(authored.Sources) != 0
	if !configured {
		return nil, validationError("Blueprint Backup configuration is incomplete")
	}
	if !apiTypes.ValidBackupPolicyKeep(*authored.Keep) {
		return nil, validationError("Blueprint Backup keep must be between 1 and 9007199254740991")
	}
	return &core.BackupSpec{
		Enabled: authored.Enabled, Frequency: authored.Frequency, Keep: *authored.Keep,
		Encryption: authored.Encryption, Connector: authored.Connector,
		Sources: append([]core.BackupSourceSpec(nil), authored.Sources...),
	}, nil
}

func normalizeRoutes(routes []core.RouteSpec) ([]core.RouteSpec, error) {
	normalized := append([]core.RouteSpec(nil), routes...)
	for index := range normalized {
		route := &normalized[index]
		if route.Path == "" {
			route.Path = "/"
		}
		if route.Target == "" {
			return nil, validationError("Blueprint Route target is required")
		}
		if err := (core.Route{
			Host: route.Hostname, Path: route.Path, TargetServiceID: "authored",
			TargetPort: route.TargetPort, Exposure: route.Exposure,
		}).Validate(); err != nil {
			return nil, validationError("Blueprint Route is invalid")
		}
	}
	return normalized, nil
}

func normalizeReleaseGroups(
	groups map[string]core.ReleaseGroupSpec,
) (map[string]core.ReleaseGroupSpec, error) {
	if len(groups) == 0 {
		return groups, nil
	}
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	sort.Strings(names)
	normalized := make(map[string]core.ReleaseGroupSpec, len(groups))
	for _, name := range names {
		group := groups[name]
		if err := group.Validate(name); err != nil {
			return nil, validationError("blueprint release group is invalid")
		}
		if len(group.Order) == 0 {
			group.Order = append([]string(nil), group.Services...)
		}
		group.OnFailure = group.OnFailure.WithDefault()
		normalized[name] = group
	}
	return normalized, nil
}

func validateReleaseGroupReferences(
	groups map[string]core.ReleaseGroupSpec,
	project *types.Project,
) error {
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		for _, service := range groups[name].Services {
			if _, exists := project.Services[service]; !exists {
				return validationError("blueprint release group references a service that is not enabled")
			}
		}
	}
	return nil
}

func decodeKnownFields(node *yaml.Node, destination any) error {
	var encoded bytes.Buffer
	encoder := yaml.NewEncoder(&encoded)
	if err := encoder.Encode(node); err != nil {
		return err
	}
	if err := encoder.Close(); err != nil {
		return err
	}
	decoder := yaml.NewDecoder(&encoded)
	decoder.KnownFields(true)
	return decoder.Decode(destination)
}

func createEmptyEnvironmentFile(workspace string) (string, error) {
	file, err := os.CreateTemp(workspace, ".empty-include-env-")
	if err != nil {
		return "", err
	}
	name := file.Name()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	return name, nil
}

func interpolationMapping(values map[string]string) types.Mapping {
	result := make(types.Mapping, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func normalizeProjectPaths(project *types.Project, workspace string) {
	project.WorkingDir = ""
	project.ComposeFiles = nil
	project.Services = normalizeServicePaths(project.Services, workspace)
	project.DisabledServices = normalizeServicePaths(project.DisabledServices, workspace)
	for name, config := range project.Configs {
		config.File = relativeWorkspacePath(workspace, config.File)
		project.Configs[name] = config
	}
	for name, volume := range project.Volumes {
		if device, exists := volume.DriverOpts["device"]; exists {
			volume.DriverOpts["device"] = relativeWorkspacePath(workspace, device)
			project.Volumes[name] = volume
		}
	}
}

func normalizeServicePaths(services types.Services, workspace string) types.Services {
	for name, service := range services {
		for index := range service.EnvFiles {
			service.EnvFiles[index].Path = relativeWorkspacePath(workspace, service.EnvFiles[index].Path)
		}
		for index := range service.LabelFiles {
			service.LabelFiles[index] = relativeWorkspacePath(workspace, service.LabelFiles[index])
		}
		for index := range service.Volumes {
			if service.Volumes[index].Type == types.VolumeTypeBind {
				service.Volumes[index].Source = relativeWorkspacePath(workspace, service.Volumes[index].Source)
			}
		}
		if service.CredentialSpec != nil {
			service.CredentialSpec.File = relativeWorkspacePath(workspace, service.CredentialSpec.File)
		}
		if service.Develop != nil {
			for index := range service.Develop.Watch {
				service.Develop.Watch[index].Path = relativeWorkspacePath(workspace, service.Develop.Watch[index].Path)
			}
		}
		services[name] = service
	}
	return services
}

func relativeWorkspacePath(workspace, value string) string {
	if value == "" || !filepath.IsAbs(value) {
		return filepath.ToSlash(path.Clean(value))
	}
	relative, err := filepath.Rel(workspace, value)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return value
	}
	return filepath.ToSlash(relative)
}

func resolvedResourceCount(project *types.Project) int {
	return len(project.Services) + len(project.DisabledServices) + len(project.Networks) + len(project.Volumes) +
		len(project.Configs) + len(project.Secrets)
}

func sortedComposePaths(compose map[string][]byte, primary []string) []string {
	result := append([]string(nil), primary...)
	seen := make(map[string]struct{}, len(result))
	for _, name := range result {
		seen[name] = struct{}{}
	}
	extra := make([]string, 0)
	for name := range compose {
		if _, ok := seen[name]; !ok {
			extra = append(extra, name)
		}
	}
	sort.Strings(extra)
	return append(result, extra...)
}
