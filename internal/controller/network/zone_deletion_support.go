package network

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func backingZoneCascadePlanHash(intent environmentchanges.ZoneRemovalIntent, impactToken string) (string, error) {
	value, err := json.Marshal(struct {
		Version                               int `json:"version"`
		Type, ZoneID, ImpactToken, RevisionID string
		RenderGeneration                      uint64 `json:"render_generation"`
	}{Version: 2, Type: "backing_zone_cascade", ZoneID: intent.ZoneID, ImpactToken: impactToken,
		RevisionID: intent.Claim.RevisionID, RenderGeneration: intent.CandidateProjection.RenderGeneration})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	clear(value)
	return hex.EncodeToString(digest[:]), nil
}

func (service *zoneDeletionService) replayZoneDeletion(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	target idempotencyrecord.IdempotencyReplayTarget,
	impactToken string,
) (idempotencyrecord.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, locator, target.ID, impactToken)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !existing || resolution.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Zone deletion replay target is inconsistent",
		)
	}
	return cloneIdempotencyResponse(resolution.Response), nil
}

func isUnknownZoneDeletionOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
