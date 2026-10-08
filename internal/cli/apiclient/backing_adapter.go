package apiclient

import (
	"bytes"
	"context"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"net/http"
)

func (client *Client) BackingAdapters(ctx context.Context) (apiTypes.BackingAdapterCatalog, error) {
	generated, err := client.generatedHumanClient()
	if err != nil {
		return apiTypes.BackingAdapterCatalog{}, err
	}
	path := "/api/v1/backing-service-adapters"
	response, err := generated.BackingServiceAdaptersWithResponse(ctx)
	if err != nil {
		return apiTypes.BackingAdapterCatalog{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.BackingAdapterCatalog{}, err
	}
	var result apiTypes.BackingAdapterCatalog
	err = decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &result)
	return result, err
}
