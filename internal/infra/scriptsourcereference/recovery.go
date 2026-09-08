package scriptsourcereference

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
)

// RecoverPreparations runs during single-Controller startup, before accepting
// mutations. It abandons only private, unpublished source preparations. It is
// not a live janitor: a caller must not race fresh request preparation against
// this startup sweep. Publication racing abandonment still loses or wins via
// the descriptor CAS; a published operation's root is never released here.
func (repository *Repository) RecoverPreparations(ctx context.Context) error {
	if ctx == nil || repository == nil || repository.store == nil {
		return validation("source preparation recovery dependencies are missing")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := repository.store.Range(ctx, PreparationPrefix, normalReleaseWindowSize)
		if err != nil {
			return err
		}
		if page == nil || len(page.Values) > normalReleaseWindowSize || (page.More && len(page.Values) == 0) {
			return corruption("source preparation recovery page is invalid")
		}
		if len(page.Values) == 0 {
			return nil
		}
		for _, value := range page.Values {
			descriptor, err := decodePreparation(value.Value)
			if err != nil || !validRecoverablePreparation(descriptor) ||
				value.Key != PreparationKey(descriptor.OperationID) ||
				value.ModRevision <= 0 {
				return corruption("source preparation recovery descriptor is invalid")
			}
			if err := repository.abandonPreparation(ctx, descriptor); err != nil {
				return err
			}
		}
	}
}

func validRecoverablePreparation(descriptor Preparation) bool {
	if descriptor.OperationID == "" || descriptor.MembershipCount == 0 ||
		descriptor.PreparationCursor > descriptor.MembershipCount || descriptor.ReleaseCursor > descriptor.PreparationCursor ||
		len(descriptor.MembershipSHA256) != hex.EncodedLen(sha256.Size) {
		return false
	}
	digest, err := hex.DecodeString(descriptor.MembershipSHA256)
	if err != nil || hex.EncodeToString(digest) != descriptor.MembershipSHA256 {
		return false
	}
	switch descriptor.Phase {
	case PreparationPreparing:
		return descriptor.ReleaseCursor == 0
	case PreparationSealed:
		return descriptor.ReleaseCursor == 0 && descriptor.PreparationCursor == descriptor.MembershipCount
	case PreparationAbandoning:
		return true
	default:
		return false
	}
}
