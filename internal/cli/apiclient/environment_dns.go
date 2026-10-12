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

func (c *Client) GetEnvironmentDNS(ctx context.Context, environmentID string) (apiTypes.EnvironmentDNS, error) {
	var result apiTypes.EnvironmentDNS
	client, err := c.generatedHumanClient()
	if err != nil {
		return result, err
	}
	path := "/api/v1/environments/" + environmentID + "/dns-records"
	response, err := client.EnvironmentDnsListWithResponse(ctx, environmentID)
	if err != nil {
		return result, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return result, err
	}
	if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &result); err != nil {
		return result, err
	}
	if result.EnvironmentID != environmentID || !generated.EnvironmentDNSState(result.State).Valid() ||
		(result.Revision != apiTypes.EnvironmentBlueprintInitialRevision && ids.Validate(ids.KindTask, result.Revision) != nil) {
		return result, errs.New(errs.KindInternal, "Controller returned invalid Environment DNS status")
	}
	for _, record := range result.Records {
		if ids.Validate(ids.KindDNSRecord, record.ID) != nil ||
			!generated.EnvironmentDNSRecordState(record.State).Valid() ||
			!generated.EnvironmentDNSRecordScope(record.Scope).Valid() {
			return result, errs.New(errs.KindInternal, "Controller returned an invalid Environment DNS record")
		}
	}
	return result, nil
}

func (c *Client) CreateEnvironmentDNS(
	ctx context.Context,
	environmentID, revision string,
	input apiTypes.EnvironmentDNSWrite,
) (apiTypes.EnvironmentDNSSaved, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.EnvironmentDNSSaved{}, err
	}
	body, err := environmentDNSBody(input)
	if err != nil {
		return apiTypes.EnvironmentDNSSaved{}, err
	}
	path := "/api/v1/environments/" + environmentID + "/dns-records"
	response, err := client.EnvironmentDnsCreateWithResponse(ctx, environmentID,
		&generated.EnvironmentDnsCreateParams{IdempotencyKey: ids.NewULID(), IfMatch: "\"" + revision + "\""}, body)
	if err != nil {
		return apiTypes.EnvironmentDNSSaved{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	return environmentDNSSaved(
		http.MethodPost,
		path,
		environmentID,
		response.HTTPResponse,
		response.Body,
		http.StatusCreated,
	)
}

func (c *Client) EditEnvironmentDNS(
	ctx context.Context,
	environmentID, recordID, revision string,
	input apiTypes.EnvironmentDNSWrite,
) (apiTypes.EnvironmentDNSSaved, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.EnvironmentDNSSaved{}, err
	}
	body, err := environmentDNSBody(input)
	if err != nil {
		return apiTypes.EnvironmentDNSSaved{}, err
	}
	path := "/api/v1/environments/" + environmentID + "/dns-records/" + recordID
	response, err := client.EnvironmentDnsEditWithResponse(ctx, environmentID, recordID,
		&generated.EnvironmentDnsEditParams{IdempotencyKey: ids.NewULID(), IfMatch: "\"" + revision + "\""}, body)
	if err != nil {
		return apiTypes.EnvironmentDNSSaved{}, generatedCallError(ctx, http.MethodPut, path, err)
	}
	return environmentDNSSaved(http.MethodPut, path, environmentID, response.HTTPResponse, response.Body, http.StatusOK)
}

func (c *Client) RemoveEnvironmentDNS(
	ctx context.Context,
	environmentID, recordID, revision string,
) error {
	client, err := c.generatedHumanClient()
	if err != nil {
		return err
	}
	path := "/api/v1/environments/" + environmentID + "/dns-records/" + recordID
	response, err := client.EnvironmentDnsRemoveWithResponse(ctx, environmentID, recordID,
		&generated.EnvironmentDnsRemoveParams{IdempotencyKey: ids.NewULID(), IfMatch: "\"" + revision + "\""})
	if err != nil {
		return generatedCallError(ctx, http.MethodDelete, path, err)
	}
	return generatedResponseError(http.MethodDelete, path, response.HTTPResponse, response.Body, http.StatusNoContent)
}

func environmentDNSBody(input apiTypes.EnvironmentDNSWrite) (generated.EnvironmentDNSWrite, error) {
	result := generated.EnvironmentDNSWrite{Label: input.Label}
	if input.Address != "" {
		result.Address = &input.Address
	}
	if input.Service != "" {
		result.Service = &input.Service
	}
	if input.Zone != "" {
		result.Zone = &input.Zone
	}
	if input.Scope != nil {
		scope := generated.EnvironmentDNSWriteScope(*input.Scope)
		if !scope.Valid() {
			return result, errs.New(errs.KindValidationFailed, "DNS scope must be environment, project or tenant")
		}
		result.Scope = &scope
	}
	return result, nil
}

func environmentDNSSaved(
	method, path, environmentID string,
	response *http.Response,
	body []byte,
	status int,
) (apiTypes.EnvironmentDNSSaved, error) {
	var result apiTypes.EnvironmentDNSSaved
	if err := generatedResponseError(method, path, response, body, status); err != nil {
		return result, err
	}
	if err := decodeSingleJSON(method, path, bytes.NewReader(body), &result); err != nil {
		return result, err
	}
	if result.EnvironmentID != environmentID || ids.Validate(ids.KindDNSRecord, result.RecordID) != nil {
		return result, errs.New(errs.KindInternal, "Controller returned an invalid saved DNS identity")
	}
	return result, nil
}
