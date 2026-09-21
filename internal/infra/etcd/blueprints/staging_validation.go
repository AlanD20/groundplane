package blueprints

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/ids"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
	"time"
	"unicode/utf8"
)

func ValidateEnvironmentBlueprintStageClaim(claim EnvironmentBlueprintStageClaim) error {
	if ids.Validate(ids.KindTask, "task_"+claim.DescriptorID) != nil ||
		ids.Validate(ids.KindEnvironment, claim.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, claim.RevisionID) != nil || ids.Validate(ids.KindTask, claim.TaskID) != nil ||
		claim.BaselineHeadRevision < 0 || claim.RenderGeneration == 0 || claim.ProjectionSchema == 0 ||
		!ValidBlueprintRecordTime(claim.CreatedAt) || idempotencyrecord.ValidateProtectedIntent(claim.Intent) != nil {
		return errs.New(errs.KindValidationFailed, "Blueprint staging claim is invalid")
	}
	if claim.SourceKind != EnvironmentBlueprintSourceApply && claim.SourceKind != EnvironmentBlueprintSourceMutation {
		return errs.New(errs.KindValidationFailed, "Blueprint staging source kind is invalid")
	}
	if claim.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		claim.Locator.ScopeID != claim.EnvironmentID {
		return errs.New(errs.KindValidationFailed, "Blueprint staging locator must belong to its Environment")
	}
	if _, _, err := EnvironmentBlueprintLocatorKey(claim.Locator); err != nil {
		return err
	}
	return nil
}

func validateEnvironmentBlueprintStageDescriptor(value EnvironmentBlueprintStageDescriptor) error {
	if err := ValidateEnvironmentBlueprintStageClaim(value.Claim); err != nil ||
		!ValidBlueprintRecordTime(value.UpdatedAt) ||
		value.UpdatedAt.Before(value.Claim.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Blueprint staging descriptor is invalid")
	}
	if !value.Bound {
		if (value.State != EnvironmentBlueprintStageOpen && value.State != EnvironmentBlueprintStageAbandoned) ||
			value.AuditChunks != 0 || value.AuditBytes != 0 ||
			value.ProjectionChunks != 0 || value.ProjectionBytes != 0 || value.ProjectionResources != 0 ||
			value.NextAuditChunk != 0 || value.NextProjectionChunk != 0 || !zeroDigest(value.AuditSHA256) ||
			!zeroDigest(value.ProjectionSHA256) || !zeroDigest(value.DependencyDigest) {
			return errs.New(errs.KindValidationFailed, "unbound Blueprint staging descriptor has stream authority")
		}
		return nil
	}
	if value.AuditBytes == 0 || value.AuditBytes > environmentBlueprintMaximumAuditBytes ||
		value.ProjectionBytes == 0 || value.ProjectionBytes > projectionrecord.EnvironmentBlueprintProjectionMaxBytes ||
		value.AuditChunks != ChunkCount32(int(value.AuditBytes)) ||
		value.ProjectionChunks != ChunkCount32(int(value.ProjectionBytes)) ||
		value.AuditChunks > environmentBlueprintMaximumAuditChunks ||
		value.ProjectionChunks > environmentBlueprintMaximumProjectionChunks ||
		value.AuditChunks+value.ProjectionChunks > environmentBlueprintMaximumChunks ||
		value.ProjectionResources > 512 || value.NextAuditChunk > value.AuditChunks ||
		value.NextProjectionChunk > value.ProjectionChunks || zeroDigest(value.AuditSHA256) ||
		zeroDigest(value.ProjectionSHA256) || zeroDigest(value.DependencyDigest) {
		return errs.New(errs.KindValidationFailed, "Blueprint staging stream authority is invalid")
	}
	switch value.State {
	case EnvironmentBlueprintStageOpen:
	case EnvironmentBlueprintStageSealed, EnvironmentBlueprintStagePublished:
		if value.NextAuditChunk != value.AuditChunks || value.NextProjectionChunk != value.ProjectionChunks {
			return errs.New(errs.KindValidationFailed, "sealed Blueprint staging descriptor is incomplete")
		}
	case EnvironmentBlueprintStageAbandoned:
	default:
		return errs.New(errs.KindValidationFailed, "Blueprint staging descriptor state is invalid")
	}
	return nil
}

func ValidBlueprintRecordTime(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC && value.UnixNano() >= 0 &&
		value.Equal(time.Unix(0, value.UnixNano()).UTC())
}

func zeroDigest(value [sha256.Size]byte) bool { return value == [sha256.Size]byte{} }

func SameEnvironmentBlueprintStageClaim(left, right EnvironmentBlueprintStageClaim) bool {
	return left.DescriptorID == right.DescriptorID && left.EnvironmentID == right.EnvironmentID &&
		left.RevisionID == right.RevisionID && left.TaskID == right.TaskID && left.Locator == right.Locator &&
		left.Intent.EnvelopeVersion == right.Intent.EnvelopeVersion && left.Intent.Cipher == right.Intent.Cipher &&
		left.Intent.DigestAlgorithm == right.Intent.DigestAlgorithm &&
		left.Intent.CiphertextDigest == right.Intent.CiphertextDigest &&
		bytes.Equal(left.Intent.Ciphertext, right.Intent.Ciphertext) &&
		left.BaselineHeadRevision == right.BaselineHeadRevision && left.SourceKind == right.SourceKind &&
		left.RenderGeneration == right.RenderGeneration && left.ProjectionSchema == right.ProjectionSchema &&
		left.CreatedAt.Equal(right.CreatedAt)
}

func CloneEnvironmentBlueprintStageClaim(value EnvironmentBlueprintStageClaim) EnvironmentBlueprintStageClaim {
	value.Intent.Ciphertext = append([]byte(nil), value.Intent.Ciphertext...)
	return value
}

func validBlueprintString(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && utf8.ValidString(value) && !strings.ContainsRune(value, '\x00')
}

func CorruptEnvironmentBlueprintStage() error {
	return errs.New(errs.KindInternal, "Blueprint staging authority is corrupt")
}

func blueprintChunkLabel(family uint8) string {
	if family == EnvironmentBlueprintChunkAudit {
		return "audit"
	}
	if family == EnvironmentBlueprintChunkProjection {
		return "projection"
	}
	return fmt.Sprintf("invalid-%d", family)
}
