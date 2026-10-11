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

func (client *Client) CheckDNS(
	ctx context.Context,
	componentID string,
	queries []apiTypes.DNSCheckQuery,
) (apiTypes.TaskAccepted, error) {
	component, err := client.ShowComponent(ctx, componentID)
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	if component.Revision <= 0 {
		return apiTypes.TaskAccepted{}, errs.New(
			errs.KindStateConflict,
			"CoreDNS snapshot has no valid revision; refresh before testing",
		)
	}
	transport, err := client.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	body := generated.ComponentDnsCheckJSONRequestBody{Revision: component.Revision}
	if len(queries) > 0 {
		selected := make([]generated.DNSCheckQuery, 0, len(queries))
		for _, query := range queries {
			kind := generated.DNSCheckQueryType(query.Type)
			if !kind.Valid() {
				return apiTypes.TaskAccepted{}, errs.New(errs.KindValidationFailed, "DNS query type must be A or NS")
			}
			selected = append(selected, generated.DNSCheckQuery{Domain: query.Domain, Name: query.Name, Type: kind})
		}
		body.Queries = &selected
	}
	path := "/api/v1/components/" + componentID + "/dns-checks"
	response, err := transport.ComponentDnsCheckWithResponse(
		ctx,
		componentID,
		&generated.ComponentDnsCheckParams{IdempotencyKey: ids.NewULID()},
		body,
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodPost, path, response.Body, response.JSON202)
}

func (client *Client) ShowDNSCheck(ctx context.Context, componentID, taskID string) (apiTypes.DNSCheckResult, error) {
	transport, err := client.generatedHumanClient()
	if err != nil {
		return apiTypes.DNSCheckResult{}, err
	}
	params := &generated.ComponentDnsCheckShowParams{}
	if taskID != "" {
		params.Task = &taskID
	}
	path := "/api/v1/components/" + componentID + "/dns-checks"
	response, err := transport.ComponentDnsCheckShowWithResponse(ctx, componentID, params)
	if err != nil {
		return apiTypes.DNSCheckResult{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.DNSCheckResult{}, err
	}
	var result apiTypes.DNSCheckResult
	err = decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &result)
	return result, err
}
