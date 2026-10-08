package apiclient

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (c *Client) RestoreBackup(ctx context.Context, environmentID, idempotencyKey string,
	input apiTypes.RestoreRequest,
) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	if idempotencyKey == "" {
		idempotencyKey = ids.NewULID()
	}
	body := generated.RestoreRequest{SourceId: input.SourceID}
	if input.VersionReviewSHA256 != "" {
		body.VersionReviewSha256 = &input.VersionReviewSHA256
	}
	if input.AcknowledgeVersionDifference {
		body.AcknowledgeVersionDifference = &input.AcknowledgeVersionDifference
	}
	if input.RecoveryPointID != "" {
		body.RecoveryPointId = &input.RecoveryPointID
	}
	if input.AgeIdentity != "" {
		body.AgeIdentity = &input.AgeIdentity
	}
	path := "/api/v1/environments/" + environmentID + "/restore"
	response, err := client.BackupRestoreWithResponse(ctx, environmentID,
		&generated.BackupRestoreParams{IdempotencyKey: idempotencyKey}, body)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodPost, path, response.Body, response.JSON202)
}

func (c *Client) PreviewRestoreBackup(
	ctx context.Context,
	environmentID string,
	input apiTypes.RestoreRequest,
) (apiTypes.RestorePreview, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.RestorePreview{}, err
	}
	body := generated.RestoreRequest{SourceId: input.SourceID}
	if input.RecoveryPointID != "" {
		body.RecoveryPointId = &input.RecoveryPointID
	}
	if input.AgeIdentity != "" {
		body.AgeIdentity = &input.AgeIdentity
	}
	path := "/api/v1/environments/" + environmentID + "/restore/preview"
	response, err := client.BackupRestorePreviewWithResponse(ctx, environmentID, body)
	if err != nil {
		return apiTypes.RestorePreview{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.RestorePreview{}, err
	}
	var result apiTypes.RestorePreview
	if err := json.Unmarshal(response.Body, &result); err != nil {
		return apiTypes.RestorePreview{}, errs.Wrap(errs.KindInternal, err)
	}
	return result, nil
}
