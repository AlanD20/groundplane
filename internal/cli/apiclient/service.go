package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) ListServices(
	ctx context.Context,
	environmentID string,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.Service], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.Service]{}, err
	}
	params := &generated.ServiceListParams{Environment: environmentID}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.ServiceListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.Page[apiTypes.Service]{}, generatedCallError(ctx, http.MethodGet, "/api/v1/services", err)
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/services", response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Page[apiTypes.Service]{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.PageService{}
		if err := decodeSingleJSON(
			http.MethodGet,
			"/api/v1/services",
			bytes.NewReader(response.Body),
			parsed,
		); err != nil {
			return apiTypes.Page[apiTypes.Service]{}, err
		}
	}
	items := []generated.Service(nil)
	if parsed.Items != nil {
		items = *parsed.Items
	}
	page := apiTypes.Page[apiTypes.Service]{Items: make([]apiTypes.Service, len(items))}
	if parsed.NextCursor != nil {
		page.NextCursor = *parsed.NextCursor
	}
	for index, service := range items {
		page.Items[index] = serviceFromGenerated(service)
	}
	return page, nil
}

func serviceFromGenerated(service generated.Service) apiTypes.Service {
	result := apiTypes.Service{
		ID: service.Id, Name: service.Name, Image: service.Image,
		RuntimeIntent: apiTypes.ServiceRuntimeIntent(service.RuntimeIntent),
	}
	if service.Zones != nil {
		result.Zones = append([]string(nil), (*service.Zones)...)
	}
	if service.Strategy != nil {
		result.Strategy = *service.Strategy
	}
	if service.OnFailure != nil {
		result.OnFailure = apiTypes.OnFailure(*service.OnFailure)
	}
	if service.Replicas != nil {
		result.Replicas = int(*service.Replicas)
	}
	if service.Adapter != nil {
		result.Adapter = *service.Adapter
	}
	if service.FactsPrefix != nil {
		result.FactsPrefix = *service.FactsPrefix
	}
	if service.BackingNetworkId != nil {
		result.BackingNetworkID = *service.BackingNetworkId
	}
	return result
}
