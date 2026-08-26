package apiclient

import (
	"bytes"
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) ShowBackupPolicy(ctx context.Context, environmentID string) (apiTypes.BackupPolicy, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.BackupPolicy{}, err
	}
	path := "/api/v1/environments/" + environmentID + "/backup-policy"
	response, err := client.BackupPolicyShowWithResponse(ctx, environmentID)
	if err != nil {
		return apiTypes.BackupPolicy{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.BackupPolicy{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.BackupPolicy{}
		if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.BackupPolicy{}, err
		}
	}
	return backupPolicyFromGenerated(*parsed), nil
}

func (c *Client) SetBackupPolicy(
	ctx context.Context,
	environmentID string,
	input apiTypes.BackupPolicyReplacementRequest,
) (apiTypes.BackupPolicy, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.BackupPolicy{}, err
	}
	path := "/api/v1/environments/" + environmentID + "/backup-policy"
	sources := make([]generated.BackupSourceInput, len(input.Sources))
	for index, source := range input.Sources {
		sources[index] = generated.BackupSourceInput{
			Kind:     generated.BackupSourceInputKind(source.Kind),
			TargetId: source.TargetID,
		}
	}
	body := generated.BackupPolicyReplacementRequest{
		Enabled: input.Enabled,
		Sources: sources,
	}
	if input.Frequency != "" {
		body.Frequency = &input.Frequency
	}
	if input.Keep != 0 {
		keep := input.Keep
		body.Keep = &keep
	}
	if input.Encryption != "" {
		encryption := generated.BackupPolicyReplacementRequestEncryption(input.Encryption)
		body.Encryption = &encryption
	}
	if input.ConnectorID != "" {
		body.ConnectorId = &input.ConnectorID
	}
	params := &generated.BackupPolicySetParams{IdempotencyKey: ids.NewULID()}
	response, err := client.BackupPolicySetWithResponse(ctx, environmentID, params, body)
	if err != nil {
		return apiTypes.BackupPolicy{}, generatedCallError(ctx, http.MethodPut, path, err)
	}
	if err := generatedResponseError(
		http.MethodPut,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.BackupPolicy{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.BackupPolicy{}
		if err := decodeSingleJSON(http.MethodPut, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.BackupPolicy{}, err
		}
	}
	return backupPolicyFromGenerated(*parsed), nil
}

func backupPolicyFromGenerated(policy generated.BackupPolicy) apiTypes.BackupPolicy {
	converted := apiTypes.BackupPolicy{
		Enabled: policy.Enabled,
	}
	if policy.Frequency != nil {
		converted.Frequency = *policy.Frequency
	}
	if policy.Keep != nil {
		converted.Keep = *policy.Keep
	}
	if policy.Encryption != nil {
		converted.Encryption = apiTypes.BackupEncryption(*policy.Encryption)
	}
	if policy.ConnectorId != nil {
		converted.ConnectorID = *policy.ConnectorId
	}
	converted.Sources = make([]apiTypes.BackupSource, len(policy.Sources))
	for index, source := range policy.Sources {
		converted.Sources[index] = apiTypes.BackupSource{
			ID: source.Id, Kind: apiTypes.BackupSourceKind(source.Kind), TargetID: source.TargetId,
		}
	}
	if policy.AgeRecipient != nil {
		converted.AgeRecipient = *policy.AgeRecipient
	}
	if policy.KeyEra != nil {
		converted.KeyEra = int(*policy.KeyEra)
	}
	if policy.KeyCreatedAt != nil {
		converted.KeyCreatedAt = policy.KeyCreatedAt.Format(time.RFC3339)
	}
	if policy.KeyRotatedAt != nil {
		converted.KeyRotatedAt = policy.KeyRotatedAt.Format(time.RFC3339)
	}
	return converted
}
