package handlers

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

func taskStepKindResponse(step etcd.TaskStepRecord) (apiTypes.TaskStepKind, error) {
	switch step.Kind {
	case etcd.TaskStepOperation:
		if step.ScriptID != "" || step.ScriptSlug != "" {
			return "", errs.New(errs.KindInternal, "Task operation step carries Script identity")
		}
		return apiTypes.TaskStepOperation, nil
	case etcd.TaskStepScript:
		if step.ScriptID == "" || step.ScriptSlug == "" {
			return "", errs.New(errs.KindInternal, "Task Script step identity is incomplete")
		}
		return apiTypes.TaskStepScript, nil
	default:
		return "", errs.New(errs.KindInternal, "Task step kind is invalid")
	}
}

func taskStepOpenAPISchema(registry huma.Registry) *huma.Schema {
	reference := openAPISchema[apiTypes.TaskStep](registry, "TaskStep")
	schema := registry.SchemaFromRef(reference.Ref)
	if schema == nil {
		return reference
	}
	schema.DependentRequired = map[string][]string{
		"script_id":   {"script_slug"},
		"script_slug": {"script_id"},
	}
	schema.OneOf = []*huma.Schema{
		{
			Properties: map[string]*huma.Schema{
				"kind": {Const: string(apiTypes.TaskStepOperation)},
			},
			Not: &huma.Schema{AnyOf: []*huma.Schema{
				{Required: []string{"script_id"}},
				{Required: []string{"script_slug"}},
			}},
		},
		{
			Properties: map[string]*huma.Schema{
				"kind": {Const: string(apiTypes.TaskStepScript)},
			},
			Required: []string{"script_id", "script_slug"},
		},
	}
	return reference
}
