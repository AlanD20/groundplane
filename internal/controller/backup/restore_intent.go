package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *RestoreService) prepareRestoreIntent(ctx context.Context, environmentID string,
	request apiTypes.RestoreRequest,
) ([]byte, requestidempotency.ProtectedEvidence, error) {
	if err := ctx.Err(); err != nil {
		return nil, requestidempotency.ProtectedEvidence{}, err
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindBackupSource, request.SourceID) != nil ||
		request.RecoveryPointID != "" && ids.Validate(ids.KindRecoveryPoint, request.RecoveryPointID) != nil ||
		len(
			request.AgeIdentity,
		) > int(
			executionplan.MaximumBackupSecretIdentityBytes,
		) || !utf8.ValidString(request.AgeIdentity) {
		return nil, requestidempotency.ProtectedEvidence{}, errs.New(
			errs.KindValidationFailed,
			"Restore request is invalid",
		)
	}
	identity := []byte(request.AgeIdentity)
	if len(identity) != 0 &&
		(len(bytes.TrimSpace(identity)) == 0 || bytes.ContainsAny(bytes.TrimSpace(identity), "\r\n\x00")) {
		clear(identity)
		return nil, requestidempotency.ProtectedEvidence{}, errs.New(
			errs.KindValidationFailed,
			"Restore identity must be one UTF-8 line",
		)
	}
	identityDigest := ""
	if len(identity) != 0 {
		digest := sha256.Sum256(identity)
		identityDigest = hex.EncodeToString(digest[:])
	}
	// Only the protected intent digest survives canonicalization. Neither the
	// private identity nor an exposed plaintext verifier enters the marker.
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: "POST", Route: restoreRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: environmentID}}, Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "source_id", Value: requestidempotency.String(request.SourceID)},
			requestidempotency.Field{
				Name:  "recovery_point_id",
				Value: requestidempotency.String(request.RecoveryPointID),
			},
			requestidempotency.Field{Name: "identity_digest", Value: requestidempotency.String(identityDigest)},
		)),
	})
	if err != nil {
		clear(identity)
		return nil, requestidempotency.ProtectedEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		clear(identity)
		return nil, requestidempotency.ProtectedEvidence{}, err
	}
	return identity, candidate, nil
}
