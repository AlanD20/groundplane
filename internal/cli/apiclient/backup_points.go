package apiclient

import (
	"bytes"
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) ListRecoveryPoints(
	ctx context.Context,
	environmentID string,
	cursor string,
) (apiTypes.RecoveryPointPage, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.RecoveryPointPage{}, err
	}
	params := &generated.BackupPointsListParams{}
	if cursor != "" {
		params.Cursor = &cursor
	}
	path := "/api/v1/environments/" + environmentID + "/recovery-points"
	response, err := client.BackupPointsListWithResponse(ctx, environmentID, params)
	if err != nil {
		return apiTypes.RecoveryPointPage{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.RecoveryPointPage{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.RecoveryPointPage{}
		if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.RecoveryPointPage{}, err
		}
	}
	items := []generated.RecoveryPoint(nil)
	if parsed.Items != nil {
		items = *parsed.Items
	}
	page := apiTypes.RecoveryPointPage{Items: make([]apiTypes.RecoveryPoint, len(items))}
	if parsed.NextCursor != nil {
		page.NextCursor = *parsed.NextCursor
	}
	for index, point := range items {
		converted := apiTypes.RecoveryPoint{
			ID:         point.Id,
			SourceID:   point.SourceId,
			SourceKind: apiTypes.BackupSourceKind(point.SourceKind),
			TargetID:   point.TargetId,
			CreatedAt:  point.CreatedAt.UTC().Format(time.RFC3339),
			SizeBytes:  point.SizeBytes,
			Encrypted:  point.Encrypted,
			Status:     apiTypes.RecoveryPointStatus(point.Status),
		}
		if point.KeyEra != nil {
			converted.KeyEra = int(*point.KeyEra)
		}
		page.Items[index] = converted
	}
	return page, nil
}
