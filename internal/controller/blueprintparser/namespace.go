package blueprintparser

import (
	"bytes"
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/compose-spec/compose-go/v2/dotenv"
	"github.com/compose-spec/compose-go/v2/types"
	"path"
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
	"x-gp-route": {}, "x-gp-routes": {}, "x-gp-components": {}, "x-gp-backup": {}, "x-gp-scripts": {}, "x-gp-network-pool": {},
	"x-gp-slug": {}, "x-gp-execution": {}, "x-gp-managed": {},
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
