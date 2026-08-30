package blueprintparser

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/compose-spec/compose-go/v2/dotenv"
	"github.com/compose-spec/compose-go/v2/format"
	"github.com/compose-spec/compose-go/v2/loader"
	composepaths "github.com/compose-spec/compose-go/v2/paths"
	"github.com/compose-spec/compose-go/v2/types"
	"gopkg.in/yaml.v3"
)

type parsePlan struct {
	files       map[string][]byte
	compose     map[string][]byte
	prepared    map[string]bool
	referenced  map[string]struct{}
	runtime     map[string]struct{}
	directories map[string]struct{}
	visiting    map[scanKey]bool
	visitedAt   map[scanKey]int
	projectName string
	emptyEnv    string
}
type scanRequest struct {
	filename string
	baseDir  string
	service  string
	depth    int
	root     bool
	env      types.Mapping
}
type scanKey struct {
	filename string
	baseDir  string
	service  string
	env      string
}

var knownGroundplaneExtensions = map[string]struct{}{
	"x-gp-resource": {}, "x-gp-release": {}, "x-gp-release-groups": {}, "x-gp-adapter": {},
	"x-gp-network": {}, "x-gp-attach": {}, "x-gp-attachments": {}, "x-gp-fact": {},
	"x-gp-entry": {}, "x-gp-exposure": {}, "x-gp-depends_on": {}, "x-gp-requires": {},
	"x-gp-route": {}, "x-gp-routes": {}, "x-gp-components": {}, "x-gp-backup": {}, "x-gp-network-pool": {},
	"x-gp-execution": {}, "x-gp-managed": {},
}

var generatedGroundplaneExtensions = map[string]struct{}{
	"x-gp-resource": {}, "x-gp-execution": {}, "x-gp-managed": {},
}

func newParsePlan(
	bundle core.BlueprintBundle,
	rootCompose []byte,
	projectName, emptyEnv string,
) *parsePlan {
	files := make(map[string][]byte, len(bundle.Files))
	for _, file := range bundle.Files {
		files[file.Path] = file.Content
	}
	return &parsePlan{
		files:       files,
		compose:     map[string][]byte{bundle.RootPath: rootCompose},
		prepared:    make(map[string]bool),
		referenced:  make(map[string]struct{}),
		runtime:     make(map[string]struct{}),
		directories: make(map[string]struct{}),
		visiting:    make(map[scanKey]bool),
		visitedAt:   make(map[scanKey]int),
		projectName: projectName,
		emptyEnv:    emptyEnv,
	}
}

func (p *parsePlan) scan(ctx context.Context, request scanRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if request.depth > maxAliasDepth {
		return validationError("blueprint include or extends depth exceeded")
	}
	key := scanKey{
		filename: request.filename, baseDir: request.baseDir, service: request.service,
		env: mappingFingerprint(request.env),
	}
	if p.visiting[key] {
		return validationError("blueprint include or extends cycle detected")
	}
	if previous, ok := p.visitedAt[key]; ok && previous >= request.depth {
		return nil
	}
	if content, exists := p.files[request.filename]; exists {
		if err := validateNoAuthoredProjectName(content); err != nil {
			return err
		}
	}
	content, err := p.sanitizedCompose(ctx, request.filename)
	if err != nil {
		return err
	}
	if err := validateAuthoredGroundplanePresence(request.filename, content); err != nil {
		return err
	}
	model, err := p.loadModel(ctx, request.filename, content, request.env, true)
	if err != nil {
		return err
	}
	if hasGroundplaneExtension(model, "x-gp-release-groups") {
		return validationError("blueprint release groups are allowed only at the root")
	}
	if !request.root && hasDocumentGroundplaneField(model) {
		return validationError("blueprint Groundplane envelope is allowed only in the root")
	}
	if err := validateAuthoredGroundplaneSource(request.filename, model); err != nil {
		return err
	}

	p.referenced[request.filename] = struct{}{}
	p.visiting[key] = true
	defer delete(p.visiting, key)

	children := make([]scanRequest, 0)
	if request.service == "" {
		projectChildren, err := p.inspectProject(request, model)
		if err != nil {
			return err
		}
		children = append(children, projectChildren...)
		services, _ := stringMap(model["services"])
		for _, name := range sortedMapKeys(services) {
			serviceChildren, err := p.inspectService(request, services[name])
			if err != nil {
				return err
			}
			children = append(children, serviceChildren...)
		}
	} else {
		services, _ := stringMap(model["services"])
		service, exists := services[request.service]
		if !exists {
			return validationError("blueprint extended Compose service is missing")
		}
		serviceChildren, err := p.inspectService(request, service)
		if err != nil {
			return err
		}
		children = append(children, serviceChildren...)
	}

	for _, child := range children {
		if err := p.scan(ctx, child); err != nil {
			return err
		}
	}
	p.visitedAt[key] = request.depth
	return nil
}

func (p *parsePlan) sanitizedCompose(ctx context.Context, filename string) ([]byte, error) {
	if p.prepared[filename] {
		return p.compose[filename], nil
	}
	content, exists := p.compose[filename]
	if !exists {
		content, exists = p.files[filename]
	}
	if !exists {
		return nil, validationError("blueprint Compose reference is undeclared")
	}
	model, err := p.loadModel(ctx, filename, content, nil, false)
	if err != nil {
		return nil, err
	}
	if err := makeIncludeEnvironmentExplicit(model, p.emptyEnv); err != nil {
		return nil, err
	}
	sanitized, err := yaml.Marshal(model)
	if err != nil {
		return nil, errs.New(errs.KindInternal, "blueprint Compose source preparation failed")
	}
	p.compose[filename] = sanitized
	p.prepared[filename] = true
	return sanitized, nil
}

func (p *parsePlan) loadModel(
	ctx context.Context,
	filename string,
	content []byte,
	environment types.Mapping,
	interpolate bool,
) (map[string]any, error) {
	model, err := loader.LoadModelWithContext(ctx, types.ConfigDetails{
		WorkingDir:  ".",
		ConfigFiles: []types.ConfigFile{{Filename: filename, Content: content}},
		Environment: environment.Clone(),
	}, func(options *loader.Options) {
		options.ResolvePaths = false
		options.SkipInclude = true
		options.SkipExtends = true
		options.SkipNormalization = true
		options.SkipConsistencyCheck = true
		options.SkipDefaultValues = true
		options.SkipValidation = true
		options.MaxNodeVisits = maxYAMLNodes
		options.SkipInterpolation = !interpolate
		options.SetProjectName(p.projectName, true)
	})
	if err != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, validationError("blueprint Compose source is invalid")
	}
	return model, nil
}

func makeIncludeEnvironmentExplicit(model map[string]any, emptyEnv string) error {
	value, exists := model["include"]
	if !exists {
		return nil
	}
	entries, ok := anyList(value)
	if !ok {
		return validationError("blueprint include is invalid")
	}
	for index, raw := range entries {
		entry, ok := stringMap(raw)
		if !ok {
			pathValue, scalar := raw.(string)
			if !scalar {
				return validationError("blueprint include is invalid")
			}
			entry = map[string]any{"path": pathValue}
			entries[index] = entry
		}
		envFiles, exists := entry["env_file"]
		if !exists {
			entry["env_file"] = []any{emptyEnv}
			continue
		}
		list, ok := anyList(envFiles)
		if !ok {
			return validationError("blueprint include env_file is invalid")
		}
		if len(list) == 0 {
			entry["env_file"] = []any{emptyEnv}
		}
	}
	model["include"] = entries
	return nil
}

func validateNoAuthoredProjectName(content []byte) error {
	document, err := decodeSingleDocument(content)
	if err != nil {
		return err
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return validationError("blueprint Compose source is invalid")
	}
	root := document.Content[0]
	for index := 0; index+1 < len(root.Content); index += 2 {
		if root.Content[index].Value == "name" {
			return validationError("blueprint Compose project name is Controller-generated")
		}
	}
	return nil
}

func (p *parsePlan) inspectProject(
	request scanRequest,
	model map[string]any,
) ([]scanRequest, error) {
	children, err := p.inspectIncludes(request, model["include"])
	if err != nil {
		return nil, err
	}
	if err := p.inspectConfigs(request.baseDir, model["configs"]); err != nil {
		return nil, err
	}
	if err := inspectSecrets(model["secrets"]); err != nil {
		return nil, err
	}
	if err := p.inspectVolumes(request.baseDir, model["volumes"]); err != nil {
		return nil, err
	}
	return children, nil
}

func (p *parsePlan) inspectIncludes(request scanRequest, raw any) ([]scanRequest, error) {
	if raw == nil {
		return nil, nil
	}
	entries, ok := anyList(raw)
	if !ok {
		return nil, validationError("blueprint include is invalid")
	}
	children := make([]scanRequest, 0)
	for _, rawEntry := range entries {
		entry, ok := stringMap(rawEntry)
		if !ok {
			return nil, validationError("blueprint include is invalid")
		}
		pathValues, ok := stringList(entry["path"])
		if !ok || len(pathValues) == 0 {
			return nil, validationError("blueprint include path is required")
		}
		resolvedPaths := make([]string, 0, len(pathValues))
		for _, value := range pathValues {
			resolved, exists, err := p.requireReference(request.baseDir, value, false, false)
			if err != nil {
				return nil, err
			}
			if !exists {
				return nil, validationError("blueprint include path is undeclared")
			}
			resolvedPaths = append(resolvedPaths, resolved)
		}

		projectDir := path.Dir(resolvedPaths[0])
		if value, exists := entry["project_directory"]; exists {
			directory, ok := value.(string)
			if !ok {
				return nil, validationError("blueprint include project_directory is invalid")
			}
			resolved, _, err := p.requireReference(request.baseDir, directory, true, false)
			if err != nil {
				return nil, err
			}
			projectDir = resolved
		}

		childEnv := request.env.Clone()
		envValues, ok := stringList(entry["env_file"])
		if !ok {
			return nil, validationError("blueprint include env_file is invalid")
		}
		envFromFiles := types.Mapping{}
		for _, value := range envValues {
			if value == p.emptyEnv {
				continue
			}
			resolved, exists, err := p.requireReference(request.baseDir, value, false, false)
			if err != nil {
				return nil, err
			}
			if !exists {
				return nil, validationError("blueprint include env_file is undeclared")
			}
			parsed, err := dotenv.ParseWithLookup(
				bytes.NewReader(p.files[resolved]),
				func(key string) (string, bool) {
					if value, ok := childEnv[key]; ok {
						return value, true
					}
					value, ok := envFromFiles[key]
					return value, ok
				},
			)
			if err != nil {
				return nil, validationError("blueprint include env_file is invalid")
			}
			for key, value := range parsed {
				envFromFiles[key] = value
			}
		}
		childEnv.Merge(envFromFiles)

		for _, filename := range resolvedPaths {
			children = append(children, scanRequest{
				filename: filename, baseDir: projectDir, depth: request.depth + 1, env: childEnv,
			})
		}
	}
	return children, nil
}

func (p *parsePlan) inspectService(request scanRequest, raw any) ([]scanRequest, error) {
	service, ok := stringMap(raw)
	if !ok {
		return nil, validationError("blueprint Compose service is invalid")
	}
	if _, exists := service["build"]; exists {
		return nil, validationError("blueprint service build is not supported")
	}
	if value, exists := service["ports"]; exists && !isEmptyCollection(value) {
		return nil, validationError("blueprint tenant service ports are not supported")
	}
	if value, exists := service["devices"]; exists && !isEmptyCollection(value) {
		return nil, validationError("blueprint host devices are forbidden")
	}
	if err := p.inspectEnvFiles(request.baseDir, service["env_file"]); err != nil {
		return nil, err
	}
	if err := p.inspectRequiredFiles(request.baseDir, service["label_file"]); err != nil {
		return nil, err
	}
	if err := p.inspectServiceVolumes(request.baseDir, service["volumes"]); err != nil {
		return nil, err
	}
	if credential, ok := stringMap(service["credential_spec"]); ok {
		if file, exists := credential["file"]; exists {
			value, ok := file.(string)
			if !ok {
				return nil, validationError("blueprint credential_spec file is invalid")
			}
			if _, exists, err := p.requireReference(
				request.baseDir,
				value,
				false,
				false,
			); err != nil ||
				!exists {
				if err != nil {
					return nil, err
				}
				return nil, validationError("blueprint credential_spec file is undeclared")
			}
		}
	}
	if develop, ok := stringMap(service["develop"]); ok {
		watch, _ := anyList(develop["watch"])
		for _, rawTrigger := range watch {
			trigger, ok := stringMap(rawTrigger)
			if !ok {
				return nil, validationError("blueprint develop watch is invalid")
			}
			value, ok := trigger["path"].(string)
			if !ok {
				return nil, validationError("blueprint develop watch path is invalid")
			}
			if _, _, err := p.requireReference(request.baseDir, value, true, false); err != nil {
				return nil, err
			}
		}
	}

	extends, exists := service["extends"]
	if !exists {
		return nil, nil
	}
	var referencedService string
	var referencedFile string
	switch value := extends.(type) {
	case string:
		referencedService = value
	case map[string]any:
		var valid bool
		referencedService, valid = value["service"].(string)
		if !valid || referencedService == "" {
			return nil, validationError("blueprint service extends is invalid")
		}
		if file, exists := value["file"]; exists {
			referencedFile, valid = file.(string)
			if !valid {
				return nil, validationError("blueprint service extends file is invalid")
			}
		}
	default:
		return nil, validationError("blueprint service extends is invalid")
	}
	child := scanRequest{
		filename: request.filename, baseDir: request.baseDir, service: referencedService,
		depth: request.depth + 1, env: request.env,
	}
	if referencedFile != "" {
		resolved, exists, err := p.requireReference(request.baseDir, referencedFile, false, false)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, validationError("blueprint service extends file is undeclared")
		}
		child.filename = resolved
		child.baseDir = path.Dir(resolved)
	}
	return []scanRequest{child}, nil
}

func (p *parsePlan) inspectEnvFiles(baseDir string, raw any) error {
	if raw == nil {
		return nil
	}
	entries, ok := anyList(raw)
	if !ok {
		return validationError("blueprint env_file is invalid")
	}
	for _, rawEntry := range entries {
		entry, ok := stringMap(rawEntry)
		if !ok {
			return validationError("blueprint env_file is invalid")
		}
		value, ok := entry["path"].(string)
		if !ok {
			return validationError("blueprint env_file path is invalid")
		}
		required := true
		if configured, exists := entry["required"]; exists {
			var valid bool
			required, valid = configured.(bool)
			if !valid {
				return validationError("blueprint env_file required flag is invalid")
			}
		}
		_, exists, err := p.requireReference(baseDir, value, false, !required)
		if err != nil {
			return err
		}
		if required && !exists {
			return validationError("blueprint env_file is undeclared")
		}
	}
	return nil
}

func (p *parsePlan) inspectRequiredFiles(baseDir string, raw any) error {
	if raw == nil {
		return nil
	}
	values, ok := stringList(raw)
	if !ok {
		return validationError("blueprint file reference is invalid")
	}
	for _, value := range values {
		_, exists, err := p.requireReference(baseDir, value, false, false)
		if err != nil {
			return err
		}
		if !exists {
			return validationError("blueprint file reference is undeclared")
		}
	}
	return nil
}

func (p *parsePlan) inspectConfigs(baseDir string, raw any) error {
	configs, ok := stringMap(raw)
	if raw == nil || !ok {
		return nil
	}
	for _, rawConfig := range configs {
		config, ok := stringMap(rawConfig)
		if !ok {
			continue
		}
		if file, exists := config["file"]; exists {
			value, ok := file.(string)
			if !ok {
				return validationError("blueprint config file is invalid")
			}
			_, declared, err := p.requireReference(baseDir, value, false, false)
			if err != nil {
				return err
			}
			if !declared {
				return validationError("blueprint config file is undeclared")
			}
		}
	}
	return nil
}

func inspectSecrets(raw any) error {
	secrets, ok := stringMap(raw)
	if raw == nil || !ok {
		return nil
	}
	for _, rawSecret := range secrets {
		secret, ok := stringMap(rawSecret)
		if !ok {
			continue
		}
		for _, source := range []string{"file", "content", "environment"} {
			if _, exists := secret[source]; exists {
				return validationError("blueprint authored secret sources are forbidden")
			}
		}
	}
	return nil
}

func (p *parsePlan) inspectServiceVolumes(baseDir string, raw any) error {
	if raw == nil {
		return nil
	}
	volumes, ok := anyList(raw)
	if !ok {
		return validationError("blueprint service volumes are invalid")
	}
	for _, rawVolume := range volumes {
		kind, source, readOnly, err := canonicalServiceVolume(rawVolume)
		if err != nil {
			return err
		}
		if kind != types.VolumeTypeBind {
			continue
		}
		if source == "" {
			return validationError("blueprint bind source is required")
		}
		if !readOnly {
			return validationError("blueprint bind source must be read-only")
		}
		if err := p.requireRuntimeReference(baseDir, source); err != nil {
			return err
		}
	}
	return nil
}

func canonicalServiceVolume(raw any) (string, string, bool, error) {
	switch volume := raw.(type) {
	case string:
		parsed, err := format.ParseVolume(volume)
		if err != nil {
			return "", "", false, validationError("blueprint service volume is invalid")
		}
		return parsed.Type, parsed.Source, parsed.ReadOnly, nil
	case map[string]any:
		kind, _ := volume["type"].(string)
		source, _ := volume["source"].(string)
		readOnly, _ := volume["read_only"].(bool)
		return kind, source, readOnly, nil
	default:
		return "", "", false, validationError("blueprint service volume is invalid")
	}
}

func (p *parsePlan) inspectVolumes(baseDir string, raw any) error {
	volumes, ok := stringMap(raw)
	if raw == nil || !ok {
		return nil
	}
	for _, rawVolume := range volumes {
		volume, ok := stringMap(rawVolume)
		if !ok {
			continue
		}
		if _, authoredName := volume["name"]; authoredName {
			return validationError("blueprint volume runtime name is Controller-generated")
		}
		driver, _ := volume["driver"].(string)
		if driver != "" && driver != "local" {
			return validationError("blueprint non-local volume drivers are forbidden")
		}
		opts, _ := stringMap(volume["driver_opts"])
		options, _ := opts["o"].(string)
		if driver == "local" && optionContains(options, "bind") {
			if !optionContains(options, "ro") {
				return validationError("blueprint local bind volume must be read-only")
			}
			device, ok := opts["device"].(string)
			if !ok || device == "" {
				return validationError("blueprint local bind volume device is required")
			}
			if err := p.requireRuntimeReference(baseDir, device); err != nil {
				return err
			}
		}
		if driver == "local" && isNetworkFilesystem(opts, options) {
			return validationError("blueprint network volume resources are forbidden")
		}
	}
	return nil
}

func isNetworkFilesystem(options map[string]any, mountOptions string) bool {
	filesystem, _ := options["type"].(string)
	switch strings.ToLower(filesystem) {
	case "nfs", "nfs4", "cifs", "smb", "smb3":
		return true
	}
	return strings.Contains(mountOptions, "addr=")
}

func optionContains(options, expected string) bool {
	for _, option := range strings.Split(options, ",") {
		if strings.TrimSpace(option) == expected {
			return true
		}
	}
	return false
}

func (p *parsePlan) requireReference(
	baseDir string,
	reference string,
	allowDirectory bool,
	allowMissing bool,
) (string, bool, error) {
	if reference == p.emptyEnv {
		return reference, true, nil
	}
	if reference == "" || strings.Contains(reference, `\`) || strings.HasPrefix(reference, "~") ||
		path.IsAbs(reference) || filepath.IsAbs(reference) || composepaths.IsWindowsAbs(reference) {
		return "", false, validationError("blueprint file reference must be bundle-relative")
	}
	parsed, err := url.Parse(reference)
	if err != nil || parsed.Scheme != "" || strings.HasPrefix(reference, "git@") {
		return "", false, validationError("blueprint remote file reference is forbidden")
	}
	for _, segment := range strings.Split(reference, "/") {
		if segment == ".." {
			return "", false, validationError("blueprint file reference contains traversal")
		}
	}

	resolved := path.Clean(path.Join(baseDir, path.Clean(reference)))
	if resolved == ".." || strings.HasPrefix(resolved, "../") {
		return "", false, validationError("blueprint file reference escapes the bundle")
	}
	if _, exists := p.files[resolved]; exists {
		p.referenced[resolved] = struct{}{}
		return resolved, true, nil
	}
	if allowDirectory {
		prefix := ""
		if resolved != "." {
			prefix = strings.TrimSuffix(resolved, "/") + "/"
		}
		found := false
		for filename := range p.files {
			if prefix == "" || strings.HasPrefix(filename, prefix) {
				p.referenced[filename] = struct{}{}
				found = true
			}
		}
		if found || resolved == baseDir {
			p.directories[resolved] = struct{}{}
			return resolved, true, nil
		}
	}
	if allowMissing {
		return resolved, false, nil
	}
	return "", false, validationError("blueprint file reference is undeclared")
}

func (p *parsePlan) requireRuntimeReference(baseDir string, reference string) error {
	resolved, _, err := p.requireReference(baseDir, reference, true, false)
	if err != nil {
		return err
	}
	if _, exists := p.files[resolved]; exists {
		p.runtime[resolved] = struct{}{}
		return nil
	}
	prefix := ""
	if resolved != "." {
		prefix = strings.TrimSuffix(resolved, "/") + "/"
	}
	for filename := range p.files {
		if prefix == "" || strings.HasPrefix(filename, prefix) {
			p.runtime[filename] = struct{}{}
		}
	}
	return nil
}

func (p *parsePlan) runtimeBlueprintFiles() []core.BlueprintFile {
	files := make([]core.BlueprintFile, 0, len(p.runtime))
	for filename := range p.runtime {
		files = append(files, core.BlueprintFile{
			Path: filename, Content: append([]byte(nil), p.files[filename]...),
		})
	}
	sort.Slice(files, func(left int, right int) bool { return files[left].Path < files[right].Path })
	return files
}

func (p *parsePlan) materialize(workspace string) error {
	directories := make([]string, 0, len(p.directories))
	for directory := range p.directories {
		directories = append(directories, directory)
	}
	sort.Strings(directories)
	for _, directory := range directories {
		if err := os.MkdirAll(
			filepath.Join(workspace, filepath.FromSlash(directory)),
			0o700,
		); err != nil {
			return err
		}
	}
	files := make([]string, 0, len(p.referenced))
	for filename := range p.referenced {
		files = append(files, filename)
	}
	sort.Strings(files)
	for _, filename := range files {
		content := p.files[filename]
		if compose, ok := p.compose[filename]; ok {
			content = compose
		}
		fullPath := filepath.Join(workspace, filepath.FromSlash(filename))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(fullPath, content, 0o600); err != nil {
			return err
		}
	}
	return nil
}

func validateGroundplaneExtensionNames(value any) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if strings.HasPrefix(key, "x-gp-") {
				if _, known := knownGroundplaneExtensions[key]; !known {
					return validationError("blueprint has an unknown Groundplane extension")
				}
				if _, generated := generatedGroundplaneExtensions[key]; generated {
					return validationError("blueprint contains Controller-generated metadata")
				}
			}
			if err := validateGroundplaneExtensionNames(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range typed {
			if err := validateGroundplaneExtensionNames(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func hasGroundplaneExtension(value any, expected string) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if key == expected || hasGroundplaneExtension(child, expected) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if hasGroundplaneExtension(child, expected) {
				return true
			}
		}
	}
	return false
}

func hasDocumentGroundplaneField(model map[string]any) bool {
	for name := range rootGroundplaneFields {
		if _, exists := model[name]; exists {
			return true
		}
	}
	return false
}

func mappingFingerprint(mapping types.Mapping) string {
	keys := make([]string, 0, len(mapping))
	for key := range mapping {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	hash := sha256.New()
	for _, key := range keys {
		_, _ = fmt.Fprintf(hash, "%d:%s%d:%s", len(key), key, len(mapping[key]), mapping[key])
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func stringMap(value any) (map[string]any, bool) {
	result, ok := value.(map[string]any)
	return result, ok
}

func anyList(value any) ([]any, bool) {
	switch typed := value.(type) {
	case []any:
		return typed, true
	case nil:
		return nil, false
	default:
		return []any{typed}, true
	}
}

func stringList(value any) ([]string, bool) {
	items, ok := anyList(value)
	if !ok {
		return nil, false
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		text, ok := item.(string)
		if !ok {
			return nil, false
		}
		result = append(result, text)
	}
	return result, true
}

func sortedMapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func isEmptyCollection(value any) bool {
	switch typed := value.(type) {
	case []any:
		return len(typed) == 0
	case map[string]any:
		return len(typed) == 0
	case nil:
		return true
	default:
		return false
	}
}
