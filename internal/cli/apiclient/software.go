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

func softwareKey(key string) (string, error) {
	if key == "" {
		key = ids.NewULID()
	}
	if !idempotencyKeyPattern.MatchString(key) {
		return "", errs.New(errs.KindValidationFailed, "software idempotency key is invalid")
	}
	return key, nil
}

func (c *Client) PrepareSoftware(
	ctx context.Context,
	input apiTypes.SoftwarePreparationRequest,
	key string,
) (apiTypes.TaskAccepted, error) {
	key, err := softwareKey(key)
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	const path = "/api/v1/software/preparations"
	response, err := client.SoftwarePrepareWithResponse(
		ctx,
		&generated.SoftwarePrepareParams{IdempotencyKey: key},
		generated.SoftwarePrepareJSONRequestBody{
			Selection:  generated.SoftwarePreparationRequestSelection(input.Selection),
			SourceKind: generated.SoftwarePreparationRequestSourceKind(input.SourceKind),
			Ref:        input.Ref,
		},
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodPost, path, response.Body, response.JSON202)
}

func (c *Client) ApplySoftware(ctx context.Context, taskID, key string) (apiTypes.TaskAccepted, error) {
	if err := ids.Validate(ids.KindTask, taskID); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	key, err := softwareKey(key)
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/software/preparations/" + taskID + "/apply"
	response, err := client.SoftwareApplyWithResponse(ctx, taskID, &generated.SoftwareApplyParams{IdempotencyKey: key})
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodPost, path, response.Body, response.JSON202)
}

func (c *Client) ShowSoftwarePreparation(ctx context.Context, taskID string) (apiTypes.SoftwarePreparation, error) {
	if err := ids.Validate(ids.KindTask, taskID); err != nil {
		return apiTypes.SoftwarePreparation{}, err
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.SoftwarePreparation{}, err
	}
	path := "/api/v1/software/preparations/" + taskID
	response, err := client.SoftwarePreparationShowWithResponse(ctx, taskID)
	if err != nil {
		return apiTypes.SoftwarePreparation{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.SoftwarePreparation{}, err
	}
	var result apiTypes.SoftwarePreparation
	err = decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &result)
	return result, err
}

func (c *Client) ListSoftwarePreparations(
	ctx context.Context,
	limit int,
	cursor string,
) (apiTypes.SoftwarePreparationPage, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.SoftwarePreparationPage{}, err
	}
	const path = "/api/v1/software/preparations"
	pageLimit := int64(limit)
	response, err := client.SoftwarePreparationsWithResponse(
		ctx,
		&generated.SoftwarePreparationsParams{Limit: &pageLimit, Cursor: &cursor},
	)
	if err != nil {
		return apiTypes.SoftwarePreparationPage{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.SoftwarePreparationPage{}, err
	}
	var result apiTypes.SoftwarePreparationPage
	err = decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &result)
	return result, err
}

func (c *Client) SoftwareReleases(
	ctx context.Context,
	selection apiTypes.SoftwareSelection,
) (apiTypes.SoftwareReleaseCatalog, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.SoftwareReleaseCatalog{}, err
	}
	const path = "/api/v1/software/releases"
	response, err := client.SoftwareReleasesWithResponse(
		ctx,
		&generated.SoftwareReleasesParams{Selection: generated.SoftwareReleasesParamsSelection(selection)},
	)
	if err != nil {
		return apiTypes.SoftwareReleaseCatalog{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.SoftwareReleaseCatalog{}, err
	}
	var result apiTypes.SoftwareReleaseCatalog
	err = decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &result)
	return result, err
}

func (c *Client) ShowSoftwareActivation(ctx context.Context, taskID string) (apiTypes.SoftwareActivation, error) {
	if err := ids.Validate(ids.KindTask, taskID); err != nil {
		return apiTypes.SoftwareActivation{}, err
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.SoftwareActivation{}, err
	}
	path := "/api/v1/software/activations/" + taskID
	response, err := client.SoftwareActivationShowWithResponse(ctx, taskID)
	if err != nil {
		return apiTypes.SoftwareActivation{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.SoftwareActivation{}, err
	}
	var result apiTypes.SoftwareActivation
	err = decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &result)
	return result, err
}
