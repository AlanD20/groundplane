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

func (c *Client) CreateEntry(
	ctx context.Context,
	input apiTypes.EntryCreateRequest,
) (apiTypes.Entry, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Entry{}, err
	}
	exposure := append([]string(nil), input.Exposure...)
	body := generated.EntryCreateRequest{
		EnvironmentId: input.EnvironmentID,
		Type:          input.Type,
		Source:        entrySourceToGenerated(input.Source),
		Exposure:      &exposure,
		Secret:        input.Secret,
	}
	if input.Key != "" {
		body.Key = &input.Key
	}
	if input.Path != "" {
		body.Path = &input.Path
	}
	if input.UID != nil {
		value := *input.UID
		body.Uid = &value
	}
	if input.GID != nil {
		value := *input.GID
		body.Gid = &value
	}
	response, err := client.EntryCreateWithResponse(
		ctx,
		&generated.EntryCreateParams{IdempotencyKey: ids.NewULID()},
		body,
	)
	if err != nil {
		return apiTypes.Entry{}, generatedCallError(ctx, http.MethodPost, "/api/v1/entries", err)
	}
	if err := generatedResponseError(
		http.MethodPost,
		"/api/v1/entries",
		response.HTTPResponse,
		response.Body,
		http.StatusCreated,
	); err != nil {
		return apiTypes.Entry{}, err
	}
	parsed := response.JSON201
	if parsed == nil {
		parsed = &generated.Entry{}
		if err := decodeSingleJSON(
			http.MethodPost,
			"/api/v1/entries",
			bytes.NewReader(response.Body),
			parsed,
		); err != nil {
			return apiTypes.Entry{}, err
		}
	}
	return entryFromGenerated(*parsed)
}

func (c *Client) BulkUpsertEntries(
	ctx context.Context,
	input apiTypes.EntryBulkUpsertRequest,
) (apiTypes.EntryBulkUpsertResult, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.EntryBulkUpsertResult{}, err
	}
	entries := make([]generated.EntryBulkItem, len(input.Entries))
	for index, entry := range input.Entries {
		entries[index] = generated.EntryBulkItem{Key: entry.Key, Value: entry.Value}
	}
	exposure := append([]string(nil), input.Exposure...)
	body := generated.EntryBulkUpsertRequest{
		EnvironmentId: input.EnvironmentID,
		Entries:       &entries,
		Exposure:      &exposure,
		Secret:        input.Secret,
	}
	response, err := client.EntryBulkUpsertWithResponse(
		ctx,
		&generated.EntryBulkUpsertParams{IdempotencyKey: ids.NewULID()},
		body,
	)
	if err != nil {
		return apiTypes.EntryBulkUpsertResult{}, generatedCallError(
			ctx, http.MethodPost, "/api/v1/entries/bulk", err,
		)
	}
	if err := generatedResponseError(
		http.MethodPost,
		"/api/v1/entries/bulk",
		response.HTTPResponse,
		response.Body,
		http.StatusAccepted,
	); err != nil {
		return apiTypes.EntryBulkUpsertResult{}, err
	}
	parsed := response.JSON202
	if parsed == nil {
		parsed = &generated.EntryBulkUpsertResult{}
		if err := decodeSingleJSON(
			http.MethodPost,
			"/api/v1/entries/bulk",
			bytes.NewReader(response.Body),
			parsed,
		); err != nil {
			return apiTypes.EntryBulkUpsertResult{}, err
		}
	}
	result := apiTypes.EntryBulkUpsertResult{TaskID: parsed.TaskId}
	if parsed.Entries == nil {
		return apiTypes.EntryBulkUpsertResult{}, errs.New(
			errs.KindInternal, "Entry bulk upsert response entries are missing",
		)
	}
	result.Entries = make([]apiTypes.Entry, len(*parsed.Entries))
	for index, entry := range *parsed.Entries {
		converted, err := entryFromGenerated(entry)
		if err != nil {
			return apiTypes.EntryBulkUpsertResult{}, err
		}
		result.Entries[index] = converted
	}
	return result, nil
}

func (c *Client) EditEntry(
	ctx context.Context,
	id string,
	input apiTypes.EntryEditRequest,
) (apiTypes.Entry, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Entry{}, err
	}
	exposure := append([]string(nil), input.Exposure...)
	body := generated.EntryEditRequest{
		Source:   entrySourceToGenerated(input.Source),
		Exposure: &exposure,
	}
	path := "/api/v1/entries/" + id
	response, err := client.EntryEditWithResponse(
		ctx, id, &generated.EntryEditParams{IdempotencyKey: ids.NewULID()}, body,
	)
	if err != nil {
		return apiTypes.Entry{}, generatedCallError(ctx, http.MethodPatch, path, err)
	}
	if err := generatedResponseError(
		http.MethodPatch, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Entry{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.Entry{}
		if err := decodeSingleJSON(http.MethodPatch, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.Entry{}, err
		}
	}
	return entryFromGenerated(*parsed)
}

func (c *Client) RemoveEntry(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/entries/" + id
	response, err := client.EntryRemoveWithResponse(
		ctx,
		id,
		&generated.EntryRemoveParams{IdempotencyKey: ids.NewULID()},
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodDelete, path, err)
	}
	if err := generatedResponseError(
		http.MethodDelete, path, response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodDelete, path, response.Body, response.JSON202)
}

func (c *Client) ListEntries(
	ctx context.Context,
	environmentID string,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.Entry], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.Entry]{}, err
	}
	params := &generated.EntryListParams{Environment: environmentID}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.EntryListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.Page[apiTypes.Entry]{}, generatedCallError(ctx, http.MethodGet, "/api/v1/entries", err)
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/entries", response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Page[apiTypes.Entry]{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.PageEntry{}
		if err := decodeSingleJSON(
			http.MethodGet,
			"/api/v1/entries",
			bytes.NewReader(response.Body),
			parsed,
		); err != nil {
			return apiTypes.Page[apiTypes.Entry]{}, err
		}
	}
	items := []generated.Entry(nil)
	if parsed.Items != nil {
		items = *parsed.Items
	}
	page := apiTypes.Page[apiTypes.Entry]{Items: make([]apiTypes.Entry, len(items))}
	if parsed.NextCursor != nil {
		page.NextCursor = *parsed.NextCursor
	}
	for index, entry := range items {
		converted, convertErr := entryFromGenerated(entry)
		if convertErr != nil {
			return apiTypes.Page[apiTypes.Entry]{}, convertErr
		}
		page.Items[index] = converted
	}
	return page, nil
}

func (c *Client) ShowEntry(ctx context.Context, id string) (apiTypes.Entry, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Entry{}, err
	}
	path := "/api/v1/entries/" + id
	response, err := client.EntryShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.Entry{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.Entry{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.Entry{}
		if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.Entry{}, err
		}
	}
	return entryFromGenerated(*parsed)
}

func entryFromGenerated(entry generated.Entry) (apiTypes.Entry, error) {
	result := apiTypes.Entry{
		ID: entry.Id, Type: entry.Type, Source: entrySourceFromGenerated(entry.Source), Secret: entry.Secret,
	}
	if entry.Key != nil {
		result.Key = *entry.Key
	}
	if entry.Path != nil {
		result.Path = *entry.Path
	}
	if entry.Uid != nil {
		if *entry.Uid < 0 || *entry.Uid > 1<<32-2 {
			return apiTypes.Entry{}, errs.New(errs.KindInternal, "Entry response uid is outside the supported range")
		}
		value := *entry.Uid
		result.UID = &value
	}
	if entry.Gid != nil {
		if *entry.Gid < 0 || *entry.Gid > 1<<32-2 {
			return apiTypes.Entry{}, errs.New(errs.KindInternal, "Entry response gid is outside the supported range")
		}
		value := *entry.Gid
		result.GID = &value
	}
	if entry.Exposure == nil {
		return apiTypes.Entry{}, errs.New(errs.KindInternal, "Entry response exposure is missing")
	}
	result.Exposure = append([]string(nil), (*entry.Exposure)...)
	return result, nil
}

func entrySourceToGenerated(source apiTypes.EntrySource) generated.EntrySource {
	result := generated.EntrySource{Kind: source.Kind}
	if source.Kind == "literal" {
		result.Literal = &source.Literal
	}
	if source.SecretRef != "" {
		result.SecretRef = &source.SecretRef
	}
	if source.AttachID != "" {
		result.AttachId = &source.AttachID
	}
	if source.GrantAttachID != "" {
		result.GrantAttachId = &source.GrantAttachID
	}
	if source.Fact != "" {
		result.Fact = &source.Fact
	}
	return result
}

func entrySourceFromGenerated(source generated.EntrySource) apiTypes.EntrySource {
	result := apiTypes.EntrySource{Kind: source.Kind}
	if source.Literal != nil {
		result.Literal = *source.Literal
	}
	if source.SecretRef != nil {
		result.SecretRef = *source.SecretRef
	}
	if source.AttachId != nil {
		result.AttachID = *source.AttachId
	}
	if source.GrantAttachId != nil {
		result.GrantAttachID = *source.GrantAttachId
	}
	if source.Fact != nil {
		result.Fact = *source.Fact
	}
	return result
}
