package apiclient

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) RemoveRecoveryPoint(
	ctx context.Context,
	environmentID string,
	pointID string,
	idempotencyKey string,
) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	if idempotencyKey == "" {
		idempotencyKey = ids.NewULID()
	}
	path := "/api/v1/environments/" + environmentID + "/recovery-points/" + pointID
	response, err := client.BackupPointsRemoveWithResponse(
		ctx,
		environmentID,
		pointID,
		&generated.BackupPointsRemoveParams{IdempotencyKey: idempotencyKey},
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodDelete, path, err)
	}
	if err := generatedResponseError(
		http.MethodDelete,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusAccepted,
	); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodDelete, path, response.Body, response.JSON202)
}
