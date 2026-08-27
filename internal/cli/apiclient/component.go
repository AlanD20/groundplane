package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) ListComponents(
	ctx context.Context,
	environmentID string,
	platform bool,
	kind string,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.Component], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.Component]{}, err
	}
	params := &generated.ComponentListParams{}
	if environmentID != "" {
		params.Environment = &environmentID
	}
	if platform {
		params.Platform = &platform
	}
	if kind != "" {
		params.Kind = &kind
	}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.ComponentListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.Page[apiTypes.Component]{}, generatedCallError(
			ctx, http.MethodGet, "/api/v1/components", err,
		)
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/components", response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Page[apiTypes.Component]{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.PageComponent{}
		if err := decodeSingleJSON(
			http.MethodGet, "/api/v1/components", bytes.NewReader(response.Body), parsed,
		); err != nil {
			return apiTypes.Page[apiTypes.Component]{}, err
		}
	}
	items := []generated.Component(nil)
	if parsed.Items != nil {
		items = *parsed.Items
	}
	page := apiTypes.Page[apiTypes.Component]{Items: make([]apiTypes.Component, len(items))}
	if parsed.NextCursor != nil {
		page.NextCursor = *parsed.NextCursor
	}
	for index, component := range items {
		page.Items[index] = componentFromGenerated(component)
	}
	return page, nil
}

func (c *Client) ShowComponent(ctx context.Context, id string) (apiTypes.Component, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Component{}, err
	}
	path := "/api/v1/components/" + id
	response, err := client.ComponentShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.Component{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Component{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.Component{}
		if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.Component{}, err
		}
	}
	return componentFromGenerated(*parsed), nil
}

func (c *Client) ShowComponentConfig(ctx context.Context, id string) (apiTypes.ComponentConfig, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.ComponentConfig{}, err
	}
	path := "/api/v1/components/" + id + "/config"
	response, err := client.ComponentConfigShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.ComponentConfig{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.ComponentConfig{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.ComponentConfig{}
		if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.ComponentConfig{}, err
		}
	}
	return componentConfigFromGenerated(*parsed), nil
}

func (c *Client) SetComponentConfig(
	ctx context.Context,
	id string,
	config apiTypes.ComponentConfig,
) (apiTypes.ComponentConfigMutationResult, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.ComponentConfigMutationResult{}, err
	}
	path := "/api/v1/components/" + id + "/config"
	configMap := copyComponentConfig(config.Config)
	response, err := client.ComponentConfigSetWithResponse(
		ctx,
		id,
		&generated.ComponentConfigSetParams{IdempotencyKey: ids.NewULID()},
		generated.ComponentConfigSetJSONRequestBody{Config: &configMap},
	)
	if err != nil {
		return apiTypes.ComponentConfigMutationResult{}, generatedCallError(ctx, http.MethodPut, path, err)
	}
	if err := generatedResponseError(
		http.MethodPut, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.ComponentConfigMutationResult{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.ComponentConfigMutationResult{}
		if err := decodeSingleJSON(http.MethodPut, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.ComponentConfigMutationResult{}, err
		}
	}
	return apiTypes.ComponentConfigMutationResult{
		Resource:        componentConfigFromGenerated(parsed.Resource),
		ReconcileTaskID: parsed.ReconcileTaskId,
	}, nil
}

func (c *Client) EnableComponent(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/components/" + id + "/enable"
	response, err := client.ComponentEnableWithResponse(
		ctx, id, &generated.ComponentEnableParams{IdempotencyKey: ids.NewULID()},
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(
		http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodPost, path, response.Body, response.JSON202)
}

func (c *Client) DisableComponent(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/components/" + id + "/disable"
	response, err := client.ComponentDisableWithResponse(
		ctx, id, &generated.ComponentDisableParams{IdempotencyKey: ids.NewULID()},
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(
		http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodPost, path, response.Body, response.JSON202)
}

func (c *Client) UpdateComponent(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/components/" + id + "/update"
	response, err := client.ComponentUpdateWithResponse(
		ctx, id, &generated.ComponentUpdateParams{IdempotencyKey: ids.NewULID()},
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(
		http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodPost, path, response.Body, response.JSON202)
}

func componentFromGenerated(component generated.Component) apiTypes.Component {
	ownerID := ""
	if component.OwnerId != nil {
		ownerID = *component.OwnerId
	}
	pinnedIPv4 := ""
	if component.PinnedIpv4 != nil {
		pinnedIPv4 = *component.PinnedIpv4
	}
	services := []string(nil)
	if component.GeneratedServices != nil {
		services = append(services, (*component.GeneratedServices)...)
	}
	var config map[string]any
	if component.Config != nil {
		config = copyComponentConfig(*component.Config)
	}
	return apiTypes.Component{
		ID: component.Id, Owner: string(component.Owner), OwnerID: ownerID,
		EnvironmentID: component.EnvironmentId, Kind: component.Kind, Enabled: component.Enabled,
		Config: config, GeneratedServices: services, PinnedIPv4: pinnedIPv4,
		Healthy: component.Healthy, Status: string(component.Status),
	}
}

func componentConfigFromGenerated(config generated.ComponentConfig) apiTypes.ComponentConfig {
	if config.Config == nil {
		return apiTypes.ComponentConfig{}
	}
	return apiTypes.ComponentConfig{Config: copyComponentConfig(*config.Config)}
}

func copyComponentConfig(config map[string]any) map[string]any {
	result := make(map[string]any, len(config))
	for key, value := range config {
		result[key] = value
	}
	return result
}
