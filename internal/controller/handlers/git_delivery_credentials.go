package handlers

import (
	"bytes"
	"encoding/json"
	"io"
	"unicode/utf8"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateGitHubCredentialFields(input apiTypes.GitHubConnectionCredentialsWrite) error {
	webhookLength := len(input.WebhookSecret)
	if len(input.PrivateKeyPEM) == 0 || len(input.PrivateKeyPEM) > 16384 ||
		(webhookLength != 0 && (webhookLength < 32 || webhookLength > 256)) {
		return errs.New(errs.KindValidationFailed, "GitHub App credential lengths are invalid")
	}
	return nil
}

// Handle secret-bearing input without returning validation evidence derived
// from the private key or webhook secret. RawBody is cleared by the caller.
func decodeGitCredentials[T any](body []byte) (T, error) {
	var zero T
	malformed := errs.New(errs.KindMalformedRequest, "GitHub App credentials require one valid JSON object")
	if !utf8.Valid(body) {
		return zero, malformed
	}
	reader := json.NewDecoder(bytes.NewReader(body))
	reader.UseNumber()
	if err := scanConnectorJSONValue(reader); err != nil {
		return zero, malformed
	}
	if _, err := reader.Token(); err != io.EOF {
		return zero, malformed
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var result *T
	if err := decoder.Decode(&result); err != nil || result == nil {
		return zero, malformed
	}
	return *result, nil
}
