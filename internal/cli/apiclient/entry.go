package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

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
		if *entry.Uid < 0 {
			return apiTypes.Entry{}, errs.New(errs.KindInternal, "Entry response uid is negative")
		}
		value := uint32(*entry.Uid)
		result.UID = &value
	}
	if entry.Gid != nil {
		if *entry.Gid < 0 {
			return apiTypes.Entry{}, errs.New(errs.KindInternal, "Entry response gid is negative")
		}
		value := uint32(*entry.Gid)
		result.GID = &value
	}
	if entry.Exposure == nil {
		return apiTypes.Entry{}, errs.New(errs.KindInternal, "Entry response exposure is missing")
	}
	result.Exposure = append([]string(nil), (*entry.Exposure)...)
	return result, nil
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
