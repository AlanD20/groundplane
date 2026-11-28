package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const remoteHostsPath = "/api/v1/remote-hosts"

func (c *Client) ListRemoteHosts(ctx context.Context, limit int, cursor string) (api.Page[api.RemoteHost], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return api.Page[api.RemoteHost]{}, err
	}
	input := &generated.RemoteHostListParams{}
	if limit != 0 {
		value := int64(limit)
		input.Limit = &value
	}
	if cursor != "" {
		input.Cursor = &cursor
	}
	response, err := client.RemoteHostListWithResponse(ctx, input)
	if err != nil {
		return api.Page[api.RemoteHost]{}, generatedCallError(ctx, http.MethodGet, remoteHostsPath, err)
	}
	return decodeRemoteHostResponse[api.Page[api.RemoteHost]](
		http.MethodGet, remoteHostsPath, response.HTTPResponse, response.Body, http.StatusOK)
}

func (c *Client) ShowRemoteHost(ctx context.Context, id string) (api.RemoteHost, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return api.RemoteHost{}, err
	}
	path := remoteHostsPath + "/" + id
	response, err := client.RemoteHostShowWithResponse(ctx, id)
	if err != nil {
		return api.RemoteHost{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	return decodeRemoteHostResponse[api.RemoteHost](
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	)
}

func (c *Client) CreateRemoteHost(ctx context.Context, name, key string) (api.RemoteHost, error) {
	key, err := remoteHostKey(key)
	if err != nil {
		return api.RemoteHost{}, err
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return api.RemoteHost{}, err
	}
	response, err := client.RemoteHostCreateWithResponse(ctx,
		&generated.RemoteHostCreateParams{IdempotencyKey: key}, generated.RemoteHostCreateJSONRequestBody{Name: name})
	if err != nil {
		return api.RemoteHost{}, generatedCallError(ctx, http.MethodPost, remoteHostsPath, err)
	}
	return decodeRemoteHostResponse[api.RemoteHost](
		http.MethodPost, remoteHostsPath, response.HTTPResponse, response.Body, http.StatusCreated)
}

func (c *Client) StageRemoteHostEnrollment(
	ctx context.Context,
	id string,
	input api.RemoteHostEnrollmentRequest,
	key string,
) (
	api.RemoteHostEnrollment, error,
) {
	key, err := remoteHostKey(key)
	if err != nil {
		return api.RemoteHostEnrollment{}, err
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return api.RemoteHostEnrollment{}, err
	}
	body, err := json.Marshal(input)
	if err != nil {
		return api.RemoteHostEnrollment{}, errs.Wrap(errs.KindInternal, err)
	}
	path := remoteHostsPath + "/" + id + "/enrollment"
	response, err := client.RemoteHostStageEnrollmentWithBodyWithResponse(ctx, id,
		&generated.RemoteHostStageEnrollmentParams{IdempotencyKey: key}, "application/json", bytes.NewReader(body))
	if err != nil {
		return api.RemoteHostEnrollment{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	return decodeRemoteHostResponse[api.RemoteHostEnrollment](
		http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusOK)
}

func (c *Client) RevokeRemoteHostEnrollment(
	ctx context.Context,
	id string,
	generation uint64,
	key string,
) error {
	if generation == 0 || generation > math.MaxInt64 {
		return errs.New(errs.KindValidationFailed, "Remote Agent generation is invalid")
	}
	key, err := remoteHostKey(key)
	if err != nil {
		return err
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return err
	}
	path := remoteHostsPath + "/" + id + "/enrollment"
	response, err := client.RemoteHostRevokeEnrollmentWithResponse(ctx, id,
		&generated.RemoteHostRevokeEnrollmentParams{IdempotencyKey: key, Generation: int64(generation)})
	if err != nil {
		return generatedCallError(ctx, http.MethodDelete, path, err)
	}
	return generatedResponseError(http.MethodDelete, path, response.HTTPResponse, response.Body, http.StatusNoContent)
}

func remoteHostKey(key string) (string, error) {
	if key == "" {
		return ids.NewULID(), nil
	}
	if !idempotencyKeyPattern.MatchString(key) {
		return "", errs.New(errs.KindValidationFailed, "Remote Host idempotency key is invalid")
	}
	return key, nil
}

func decodeRemoteHostResponse[T any](method, path string, response *http.Response, body []byte, status int) (T, error) {
	var output T
	if err := generatedResponseError(method, path, response, body, status); err != nil {
		return output, err
	}
	err := decodeSingleJSON(method, path, bytes.NewReader(body), &output)
	return output, err
}
