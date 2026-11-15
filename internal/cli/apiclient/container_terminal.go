package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (c *Client) OpenContainerTerminal(
	ctx context.Context,
	serviceID string,
	options apiTypes.ContainerTerminalOpen,
) (apiTypes.TerminalSession, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TerminalSession{}, err
	}
	body, err := json.Marshal(options)
	if err != nil {
		return apiTypes.TerminalSession{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	path := "/api/v1/services/" + serviceID + "/terminal"
	key := ids.NewULID()
	var response *generated.ServiceTerminalResponse
	for attempt := 0; attempt < 2; attempt++ {
		response, err = client.ServiceTerminalWithBodyWithResponse(ctx, serviceID,
			&generated.ServiceTerminalParams{IdempotencyKey: key}, "application/json", bytes.NewReader(body))
		if err == nil {
			break
		}
		if ctx.Err() != nil || attempt == 1 {
			return apiTypes.TerminalSession{}, generatedCallError(ctx, http.MethodPost, path, err)
		}
	}
	if err := generatedResponseError(http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusCreated); err != nil {
		return apiTypes.TerminalSession{}, err
	}
	var session apiTypes.TerminalSession
	if err := decodeSingleJSON(http.MethodPost, path, bytes.NewReader(response.Body), &session); err != nil {
		return session, err
	}
	return session, nil
}
