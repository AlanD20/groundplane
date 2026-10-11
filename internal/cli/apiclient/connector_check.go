package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CheckConnector captures a current revision when checking a saved Connector;
// any supplied Changes remain unsaved. Drafts have no saved owner or revision.
func (c *Client) CheckConnector(ctx context.Context, environmentID string,
	input apiTypes.ConnectorCheckRequest,
) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	if input.ConnectorID != "" && input.Revision == 0 {
		snapshot, err := client.ConnectorShowWithResponse(ctx, input.ConnectorID)
		if err != nil {
			return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodGet, "/api/v1/connectors/{id}", err)
		}
		if err := generatedResponseError(http.MethodGet, "/api/v1/connectors/{id}", snapshot.HTTPResponse, snapshot.Body, http.StatusOK); err != nil {
			return apiTypes.TaskAccepted{}, err
		}
		text, err := strconv.Unquote(snapshot.HTTPResponse.Header.Get("ETag"))
		if err != nil {
			return apiTypes.TaskAccepted{}, errs.New(errs.KindInternal, "connector response has no valid revision ETag")
		}
		input.Revision, err = strconv.ParseInt(text, 10, 64)
		if err != nil || input.Revision <= 0 {
			return apiTypes.TaskAccepted{}, errs.New(errs.KindInternal, "connector response revision is invalid")
		}
	}
	body, err := json.Marshal(input)
	if err != nil {
		return apiTypes.TaskAccepted{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response, err := client.ConnectorCheckWithBodyWithResponse(ctx,
		&generated.ConnectorCheckParams{Environment: environmentID, IdempotencyKey: ids.NewULID()},
		"application/json", bytes.NewReader(body))
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, "/api/v1/connectors/checks", err)
	}
	if err := generatedResponseError(http.MethodPost, "/api/v1/connectors/checks", response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	var accepted apiTypes.TaskAccepted
	err = decodeSingleJSON(http.MethodPost, "/api/v1/connectors/checks", bytes.NewReader(response.Body), &accepted)
	return accepted, err
}

func (c *Client) ShowConnectorCheck(
	ctx context.Context,
	environmentID, taskID, connectorID string,
) (apiTypes.ConnectorCheckResult, error) {
	if (taskID == "") == (connectorID == "") {
		return apiTypes.ConnectorCheckResult{}, errs.New(
			errs.KindValidationFailed,
			"connection check lookup requires exactly one Task or Connector",
		)
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.ConnectorCheckResult{}, err
	}
	params := &generated.ConnectorCheckShowParams{Environment: environmentID}
	if taskID != "" {
		params.Task = &taskID
	} else {
		params.Connector = &connectorID
	}
	response, err := client.ConnectorCheckShowWithResponse(ctx, params)
	if err != nil {
		return apiTypes.ConnectorCheckResult{}, generatedCallError(
			ctx,
			http.MethodGet,
			"/api/v1/connectors/checks",
			err,
		)
	}
	if err := generatedResponseError(http.MethodGet, "/api/v1/connectors/checks", response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.ConnectorCheckResult{}, err
	}
	var result apiTypes.ConnectorCheckResult
	err = decodeSingleJSON(http.MethodGet, "/api/v1/connectors/checks", bytes.NewReader(response.Body), &result)
	return result, err
}
