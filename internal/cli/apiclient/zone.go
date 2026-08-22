package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) CreateZone(ctx context.Context, input apiTypes.ZoneCreate) (apiTypes.Zone, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Zone{}, err
	}
	params := &generated.ZoneCreateParams{IdempotencyKey: ids.NewULID()}
	body := generated.ZoneCreateJSONRequestBody{
		EnvironmentId: input.EnvironmentID, Name: input.Name,
		Subnet: input.Subnet, Internal: input.Internal,
	}
	response, err := client.ZoneCreateWithResponse(ctx, params, body)
	if err != nil {
		return apiTypes.Zone{}, generatedCallError(ctx, http.MethodPost, "/api/v1/zones", err)
	}
	if err := generatedResponseError(
		http.MethodPost, "/api/v1/zones", response.HTTPResponse, response.Body, http.StatusCreated,
	); err != nil {
		return apiTypes.Zone{}, err
	}
	parsed := response.JSON201
	if parsed == nil {
		parsed = &generated.Zone{}
		if err := decodeSingleJSON(
			http.MethodPost,
			"/api/v1/zones",
			bytes.NewReader(response.Body),
			parsed,
		); err != nil {
			return apiTypes.Zone{}, err
		}
	}
	return zoneFromGenerated(*parsed), nil
}

func (c *Client) ListZones(
	ctx context.Context,
	environmentID string,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.Zone], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.Zone]{}, err
	}
	params := &generated.ZoneListParams{Environment: environmentID}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.ZoneListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.Page[apiTypes.Zone]{}, generatedCallError(ctx, http.MethodGet, "/api/v1/zones", err)
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/zones", response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Page[apiTypes.Zone]{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.PageZone{}
		if err := decodeSingleJSON(
			http.MethodGet,
			"/api/v1/zones",
			bytes.NewReader(response.Body),
			parsed,
		); err != nil {
			return apiTypes.Page[apiTypes.Zone]{}, err
		}
	}
	items := []generated.Zone(nil)
	if parsed.Items != nil {
		items = *parsed.Items
	}
	page := apiTypes.Page[apiTypes.Zone]{Items: make([]apiTypes.Zone, len(items))}
	if parsed.NextCursor != nil {
		page.NextCursor = *parsed.NextCursor
	}
	for index, zone := range items {
		page.Items[index] = zoneFromGenerated(zone)
	}
	return page, nil
}

func (c *Client) GetZone(ctx context.Context, id string) (apiTypes.Zone, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Zone{}, err
	}
	path := "/api/v1/zones/" + id
	response, err := client.ZoneShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.Zone{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Zone{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.Zone{}
		if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.Zone{}, err
		}
	}
	return zoneFromGenerated(*parsed), nil
}

func (c *Client) GetZoneRemovalImpact(ctx context.Context, id string) (apiTypes.ZoneRemovalImpact, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.ZoneRemovalImpact{}, err
	}
	path := "/api/v1/zones/" + id + "/removal-impact"
	response, err := client.ZoneRemovalImpactWithResponse(ctx, id)
	if err != nil {
		return apiTypes.ZoneRemovalImpact{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.ZoneRemovalImpact{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.ZoneRemovalImpact{}
		if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.ZoneRemovalImpact{}, err
		}
	}
	attaches := []generated.ZoneRemovalImpactAttach{}
	if parsed.Attaches != nil {
		attaches = *parsed.Attaches
	}
	services := []generated.ZoneRemovalImpactService{}
	if parsed.Services != nil {
		services = *parsed.Services
	}
	databases := []generated.ZoneRemovalImpactDatabase{}
	if parsed.Databases != nil {
		databases = *parsed.Databases
	}
	impact := apiTypes.ZoneRemovalImpact{
		ZoneID: parsed.ZoneId, ZoneName: parsed.ZoneName,
		Mode: apiTypes.ZoneRemovalImpactMode(parsed.Mode), ImpactToken: parsed.ImpactToken,
		Attaches:  make([]apiTypes.ZoneRemovalImpactAttach, len(attaches)),
		Services:  make([]apiTypes.ZoneRemovalImpactService, len(services)),
		Databases: make([]apiTypes.ZoneRemovalImpactDatabase, len(databases)),
	}
	for index, item := range attaches {
		database := ""
		if item.Database != nil {
			database = *item.Database
		}
		impact.Attaches[index] = apiTypes.ZoneRemovalImpactAttach{
			ID: item.Id, Name: item.Name, EnvironmentID: item.EnvironmentId,
			ServiceID: item.ServiceId, Database: database, Status: item.Status,
		}
	}
	for index, item := range services {
		impact.Services[index] = apiTypes.ZoneRemovalImpactService{
			ID: item.Id, Name: item.Name, EnvironmentID: item.EnvironmentId,
		}
	}
	for index, item := range databases {
		impact.Databases[index] = apiTypes.ZoneRemovalImpactDatabase{AttachID: item.AttachId, Name: item.Name}
	}
	return impact, nil
}

func (c *Client) RemoveZone(ctx context.Context, id string, impactToken string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/zones/" + id
	params := &generated.ZoneRemoveParams{IdempotencyKey: ids.NewULID()}
	if impactToken != "" {
		params.ImpactToken = &impactToken
	}
	response, err := client.ZoneRemoveWithResponse(ctx, id, params)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodDelete, path, err)
	}
	if err := generatedResponseError(
		http.MethodDelete, path, response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodDelete, path, response.Body, response.JSON202)
}

func zoneFromGenerated(zone generated.Zone) apiTypes.Zone {
	return apiTypes.Zone{
		ID: zone.Id, EnvironmentID: zone.EnvironmentId, Name: zone.Name,
		Subnet: zone.Subnet, Internal: zone.Internal,
		OwnerKind: apiTypes.ZoneOwnerKind(zone.OwnerKind), OwnerID: zone.OwnerId,
	}
}
