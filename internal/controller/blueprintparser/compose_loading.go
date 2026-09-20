package blueprintparser

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/compose-spec/compose-go/v2/loader"
	"github.com/compose-spec/compose-go/v2/types"
)

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
