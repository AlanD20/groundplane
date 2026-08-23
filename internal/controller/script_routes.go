package controller

import (
	"context"
	"log/slog"
	"net/http"
	"reflect"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type ScriptReader interface {
	GetScript(context.Context, string) (etcd.Versioned[etcd.ScriptRecord], error)
	ListScripts(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ScriptRecord], error)
}

type ScriptMutator interface {
	CreateScript(context.Context, apiTypes.ScriptCreate, string) (etcd.IdempotencyResponse, error)
	EditScript(context.Context, string, apiTypes.ScriptEdit, string) (etcd.IdempotencyResponse, error)
}

type scriptListInput struct {
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Limit       int    `query:"limit" required:"false"`
	Cursor      string `query:"cursor" required:"false"`
}

type scriptShowInput struct {
	ID string `path:"id" pattern:"^scr_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type scriptCreateInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.ScriptCreate
}

type scriptEditInput struct {
	ID             string `path:"id" pattern:"^scr_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.ScriptEdit
}

type scriptOutput struct{ Body apiTypes.Script }
type scriptPageOutput struct {
	Body apiTypes.Page[apiTypes.Script]
}
type scriptMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerScripts() {
	scriptSchema := s.API.OpenAPI().Components.Schemas.Schema(reflect.TypeFor[apiTypes.Script](), true, "Script")
	huma.Register(s.API, huma.Operation{
		OperationID: "script.list", Method: http.MethodGet, Path: "/scripts",
		Summary: "List scripts", Tags: []string{"Script"},
	}, s.listScripts)
	huma.Register(s.API, huma.Operation{
		OperationID: "script.show", Method: http.MethodGet, Path: "/scripts/{id}",
		Summary: "Show a script", Tags: []string{"Script"},
	}, s.showScript)
	huma.Register(s.API, huma.Operation{
		OperationID: "script.create", Method: http.MethodPost, Path: "/scripts",
		Summary: "Create a script", Tags: []string{"Script"}, DefaultStatus: http.StatusCreated,
		Responses: map[string]*huma.Response{
			"201": {Description: http.StatusText(http.StatusCreated), Content: map[string]*huma.MediaType{
				"application/json": {Schema: scriptSchema},
			}},
		},
	}, s.createScript)
	huma.Register(s.API, huma.Operation{
		OperationID: "script.edit", Method: http.MethodPatch, Path: "/scripts/{id}",
		Summary: "Edit a script", Tags: []string{"Script"}, DefaultStatus: http.StatusOK,
		Responses: map[string]*huma.Response{
			"200": {Description: http.StatusText(http.StatusOK), Content: map[string]*huma.MediaType{
				"application/json": {Schema: scriptSchema},
			}},
		},
	}, s.editScript)
	s.setRoutePolicy("POST /api/v1/scripts", routePolicy{body: jsonBody})
	s.setRoutePolicy("PATCH /api/v1/scripts/{id}", routePolicy{body: jsonBody})
}

func (s *Server) listScripts(ctx context.Context, request *scriptListInput) (*scriptPageOutput, error) {
	if s.scriptReads == nil {
		return nil, errs.New(errs.KindInternal, "Script reader is not configured")
	}
	pageRequest, err := scriptListRequest(request.Environment, request.Limit, request.Cursor)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	page, err := s.scriptReads.ListScripts(ctx, request.Environment, pageRequest)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response := apiTypes.Page[apiTypes.Script]{
		Items:      make([]apiTypes.Script, len(page.Items)),
		NextCursor: page.NextCursor,
	}
	for index, item := range page.Items {
		response.Items[index] = scriptResponse(item.Record)
	}
	return &scriptPageOutput{Body: response}, nil
}

func (s *Server) showScript(ctx context.Context, request *scriptShowInput) (*scriptOutput, error) {
	if s.scriptReads == nil {
		return nil, errs.New(errs.KindInternal, "Script reader is not configured")
	}
	script, err := s.scriptReads.GetScript(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &scriptOutput{Body: scriptResponse(script.Record)}, nil
}

func (s *Server) createScript(ctx context.Context, request *scriptCreateInput) (*scriptMutationOutput, error) {
	if s.scriptMutations == nil {
		return nil, errs.New(errs.KindInternal, "Script mutator is not configured")
	}
	response, err := s.scriptMutations.CreateScript(ctx, request.Body, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.scriptMutationResponse(response), nil
}

func (s *Server) editScript(ctx context.Context, request *scriptEditInput) (*scriptMutationOutput, error) {
	if s.scriptMutations == nil {
		return nil, errs.New(errs.KindInternal, "Script mutator is not configured")
	}
	response, err := s.scriptMutations.EditScript(ctx, request.ID, request.Body, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.scriptMutationResponse(response), nil
}

func (s *Server) scriptMutationResponse(response etcd.IdempotencyResponse) *scriptMutationOutput {
	return &scriptMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, err := ctx.BodyWriter().Write(response.Body); err != nil && s.Logger != nil {
				s.Logger.Error("controller: write Script mutation response", slog.Any("error", err))
			}
		},
	}
}

func scriptListRequest(environmentID string, limit int, cursor string) (etcd.PageRequest, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcd.PageRequest{}, errs.New(errs.KindValidationFailed, "Script list requires a stable Environment id")
	}
	if limit < 0 {
		return etcd.PageRequest{}, errs.New(errs.KindValidationFailed, "Script list limit must be a positive integer")
	}
	return etcd.PageRequest{Limit: limit, Cursor: cursor}, nil
}

func scriptResponse(record etcd.ScriptRecord) apiTypes.Script {
	return apiTypes.Script{
		ID: record.Desired.ID, Name: record.Desired.Name, ServiceName: record.Desired.ServiceName,
		Body: record.Desired.Body, When: string(record.Desired.When),
	}
}
