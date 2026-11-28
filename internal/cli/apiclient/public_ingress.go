package apiclient

import (
	"bytes"
	"context"
	"net/http"
	"strconv"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (c *Client) ShowPublicIngress(ctx context.Context, hostID string) (apiTypes.PublicIngressState, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.PublicIngressState{}, err
	}
	const method = http.MethodGet
	path := publicIngressPath(hostID)
	response, err := client.PublicIngressShowWithResponse(ctx, hostID)
	if err != nil {
		return apiTypes.PublicIngressState{}, generatedCallError(ctx, method, path, err)
	}
	if err := generatedResponseError(method, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.PublicIngressState{}, err
	}
	var state apiTypes.PublicIngressState
	if err := decodeSingleJSON(method, path, bytes.NewReader(response.Body), &state); err != nil {
		return apiTypes.PublicIngressState{}, err
	}
	if state.Configuration.Revision == "" ||
		response.HTTPResponse.Header.Get("ETag") != strconv.Quote(state.Configuration.Revision) {
		return apiTypes.PublicIngressState{}, errs.New(
			errs.KindInternal,
			"public ingress response has an invalid revision ETag",
		)
	}
	return state, nil
}

func (c *Client) PreflightPublicIngress(
	ctx context.Context, hostID, revision string, draft apiTypes.PublicIngressDraft,
) (apiTypes.PublicIngressReview, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.PublicIngressReview{}, err
	}
	const method = http.MethodPost
	path := publicIngressPath(hostID) + "/preflight"
	response, err := client.PublicIngressPreflightWithResponse(
		ctx, hostID, &generated.PublicIngressPreflightParams{IfMatch: strconv.Quote(revision)},
		generated.PublicIngressPreflightJSONRequestBody{
			Enabled: draft.Enabled, Addresses: publicIngressStrings(draft.Addresses), RouteIds: publicIngressStrings(draft.RouteIDs),
		},
	)
	if err != nil {
		return apiTypes.PublicIngressReview{}, generatedCallError(ctx, method, path, err)
	}
	if err := generatedResponseError(method, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.PublicIngressReview{}, err
	}
	var review apiTypes.PublicIngressReview
	if err := decodeSingleJSON(method, path, bytes.NewReader(response.Body), &review); err != nil {
		return apiTypes.PublicIngressReview{}, err
	}
	return review, nil
}

func (c *Client) ApplyPublicIngress(
	ctx context.Context, hostID, revision string, input apiTypes.PublicIngressApplyRequest, key string,
) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	if key == "" {
		key = ids.NewULID()
	}
	if !idempotencyKeyPattern.MatchString(key) {
		return apiTypes.TaskAccepted{}, errs.New(errs.KindValidationFailed, "public ingress idempotency key is invalid")
	}
	const method = http.MethodPost
	path := publicIngressPath(hostID) + "/apply"
	response, err := client.PublicIngressApplyWithResponse(
		ctx, hostID, &generated.PublicIngressApplyParams{IfMatch: strconv.Quote(revision), IdempotencyKey: key},
		generated.PublicIngressApplyJSONRequestBody{
			Enabled: input.Enabled, Addresses: publicIngressStrings(input.Addresses), RouteIds: publicIngressStrings(input.RouteIDs),
			ReviewToken: input.ReviewToken,
		},
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, method, path, err)
	}
	if err := generatedResponseError(method, path, response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(method, path, response.Body, response.JSON202)
}

func publicIngressPath(hostID string) string { return "/api/v1/hosts/" + hostID + "/public-ingress" }

func publicIngressStrings(values []string) *[]string {
	if values == nil {
		values = []string{}
	}
	return &values
}
