package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (c *Client) CheckBackingAccess(
	ctx context.Context,
	environmentID, attachID, grantID string,
) (apiTypes.TaskAccepted, error) {
	var revision int64
	cursor := ""
	for {
		page, err := c.ListAttaches(ctx, environmentID, 200, cursor)
		if err != nil {
			return apiTypes.TaskAccepted{}, err
		}
		for _, attach := range page.Items {
			if attach.ID == attachID {
				revision = attach.Revision
				break
			}
		}
		if revision > 0 || page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if revision <= 0 {
		return apiTypes.TaskAccepted{}, errs.New(
			errs.KindAttachNotFound,
			"Attach snapshot was not found in this Environment",
		)
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	body := generated.AttachCheckJSONRequestBody{AttachId: attachID, Revision: revision}
	if grantID != "" {
		body.GrantId = &grantID
	}
	response, err := client.AttachCheckWithResponse(ctx, &generated.AttachCheckParams{
		Environment: environmentID, IdempotencyKey: ids.NewULID()}, body)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, "/api/v1/attaches/checks", err)
	}
	if err := generatedResponseError(http.MethodPost, "/api/v1/attaches/checks", response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodPost, "/api/v1/attaches/checks", response.Body, response.JSON202)
}

func (c *Client) ShowBackingCheck(
	ctx context.Context,
	environmentID, taskID, attachID, grantID string,
) (apiTypes.BackingCheckResult, error) {
	if (taskID == "") == (attachID == "") || taskID != "" && grantID != "" {
		return apiTypes.BackingCheckResult{}, errs.New(
			errs.KindValidationFailed,
			"access check lookup requires a Task or an Attach with an optional grant",
		)
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.BackingCheckResult{}, err
	}
	params := &generated.AttachCheckShowParams{Environment: environmentID}
	if taskID != "" {
		params.Task = &taskID
	} else {
		params.Attach = &attachID
	}
	if grantID != "" {
		params.Grant = &grantID
	}
	response, err := client.AttachCheckShowWithResponse(ctx, params)
	if err != nil {
		return apiTypes.BackingCheckResult{}, generatedCallError(ctx, http.MethodGet, "/api/v1/attaches/checks", err)
	}
	if err := generatedResponseError(http.MethodGet, "/api/v1/attaches/checks", response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.BackingCheckResult{}, err
	}
	var result apiTypes.BackingCheckResult
	err = decodeSingleJSON(http.MethodGet, "/api/v1/attaches/checks", bytes.NewReader(response.Body), &result)
	return result, err
}
