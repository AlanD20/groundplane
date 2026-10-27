package handlers

import (
	"context"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
	"net/http"
)

type taskAlertScopeInput struct {
	Environment string `query:"environment" maxLength:"64"`
	Platform    bool   `query:"platform"`
}

func (input taskAlertScopeInput) scope() (string, error) {
	if input.Platform && input.Environment == "" {
		return "platform", nil
	}
	if !input.Platform && input.Environment != "" {
		return input.Environment, nil
	}
	return "", errs.New(errs.KindValidationFailed, "Select platform=true or one Environment, never both")
}

type taskAlertWriteInput struct {
	Environment string `query:"environment" maxLength:"64"`
	Platform    bool   `query:"platform"`
	Key         string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body        apiTypes.TaskAlertSettingsWrite
}
type taskAlertTestInput struct {
	Environment string `query:"environment" maxLength:"64"`
	Platform    bool   `query:"platform"`
	Key         string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body        apiTypes.TaskAlertTestRequest
}
type taskAlertOutput struct{ Body apiTypes.TaskAlertSettings }

func (server *Server) registerTaskAlerts() {
	schemas := server.API.OpenAPI().Components.Schemas
	config := openAPISchema[apiTypes.TaskAlertSettings](schemas, "TaskAlertSettings")
	test := openAPISchema[apiTypes.TaskAlertTestAccepted](schemas, "TaskAlertTestAccepted")
	response := func(schema *huma.Schema) map[string]*huma.Response {
		return map[string]*huma.Response{
			"200": {
				Description: "Protected alert operation result",
				Content:     map[string]*huma.MediaType{"application/json": {Schema: schema}},
			},
		}
	}
	huma.Register(
		server.API,
		huma.Operation{OperationID: "task-alert.show", Method: http.MethodGet, Path: "/task-alerts",
			Summary: "Read scoped Discord Task alert settings and last delivery status", Tags: []string{"Task alerts"}},
		server.showTaskAlerts,
	)
	huma.Register(
		server.API,
		huma.Operation{OperationID: "task-alert.set", Method: http.MethodPut, Path: "/task-alerts",
			Summary: "Save scoped Discord Task alerts; webhook value is write-only", Tags: []string{"Task alerts"}, Responses: response(config)},
		server.saveTaskAlerts,
	)
	huma.Register(
		server.API,
		huma.Operation{OperationID: "task-alert.test", Method: http.MethodPost, Path: "/task-alerts/test",
			Summary: "Queue a test Discord notification using the saved destination", Tags: []string{"Task alerts"}, Responses: response(test)},
		server.testTaskAlerts,
	)
	server.setRoutePolicy("PUT /api/v1/task-alerts", routePolicy{body: jsonBody})
	server.setRoutePolicy("POST /api/v1/task-alerts/test", routePolicy{body: jsonBody})
}
func (server *Server) showTaskAlerts(ctx context.Context, input *taskAlertScopeInput) (*taskAlertOutput, error) {
	if server.taskAlerts == nil {
		return nil, errs.New(errs.KindInternal, "Task alerting is not configured")
	}
	scope, err := input.scope()
	if err != nil {
		return nil, err
	}
	result, err := server.taskAlerts.Show(ctx, scope)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &taskAlertOutput{Body: result}, nil
}

func (server *Server) saveTaskAlerts(
	ctx context.Context,
	input *taskAlertWriteInput,
) (*componentMutationOutput, error) {
	if server.taskAlerts == nil {
		return nil, errs.New(errs.KindInternal, "Task alerting is not configured")
	}
	scope, err := (taskAlertScopeInput{Environment: input.Environment, Platform: input.Platform}).scope()
	if err != nil {
		return nil, err
	}
	result, err := server.taskAlerts.Save(ctx, scope, input.Key, input.Body)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return componentMutationResponse(result), nil
}
func (server *Server) testTaskAlerts(ctx context.Context, input *taskAlertTestInput) (*componentMutationOutput, error) {
	if server.taskAlerts == nil {
		return nil, errs.New(errs.KindInternal, "Task alerting is not configured")
	}
	scope, err := (taskAlertScopeInput{Environment: input.Environment, Platform: input.Platform}).scope()
	if err != nil {
		return nil, err
	}
	result, err := server.taskAlerts.Test(ctx, scope, input.Key, input.Body)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return componentMutationResponse(result), nil
}
