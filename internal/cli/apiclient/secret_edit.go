package apiclient

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (client *Client) EditSecret(ctx context.Context, id string, input apiTypes.SecretEditRequest, key string) (apiTypes.Secret, error) {
	if key == "" {
		key = ids.NewULID()
	}
	transport, err := client.generatedHumanClient()
	if err != nil {
		return apiTypes.Secret{}, err
	}
	path := "/api/v1/secrets/" + id
	response, err := transport.SecretEditWithResponse(ctx, id, &generated.SecretEditParams{IdempotencyKey: key}, generated.SecretEditJSONRequestBody{Revision: input.Revision, Value: input.Value})
	if err != nil {
		return apiTypes.Secret{}, generatedCallError(ctx, http.MethodPatch, path, err)
	}
	if err := generatedResponseError(http.MethodPatch, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.Secret{}, err
	}
	if response.JSON200 == nil {
		return apiTypes.Secret{}, generatedCallError(ctx, http.MethodPatch, path, http.ErrMissingFile)
	}
	return secretFromGenerated(*response.JSON200), nil
}
