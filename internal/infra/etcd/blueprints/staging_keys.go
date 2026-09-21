package blueprints

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
)

func EnvironmentBlueprintDescriptorKeyByID(descriptorID string) string {
	return EnvironmentBlueprintDescriptorPrefix + recordcodec.EncodeKeySegment(descriptorID)
}

func EnvironmentBlueprintRootKey(environmentID string, revisionID string) string {
	return EnvironmentBlueprintRevisionPrefixFinal(environmentID, revisionID) + "root"
}

func EnvironmentBlueprintChunkKeyFor(environmentID, revisionID string, family uint8, index uint32) string {
	familyName := "audit"
	if family == EnvironmentBlueprintChunkProjection {
		familyName = "projection"
	}
	sequence := make([]byte, 4)
	binary.BigEndian.PutUint32(sequence, index)
	return EnvironmentBlueprintRevisionPrefixFinal(environmentID, revisionID) +
		"chunks/" + familyName + "/" + encodeBlueprintDynamicBytes(sequence)
}

func environmentBlueprintChunkKey(environmentID, revisionID, family string, index int) string {
	familyID := EnvironmentBlueprintChunkAudit
	if family == "projection" {
		familyID = EnvironmentBlueprintChunkProjection
	}
	return EnvironmentBlueprintChunkKeyFor(environmentID, revisionID, familyID, uint32(index))
}

func EnvironmentBlueprintRevisionPrefixFinal(environmentID, revisionID string) string {
	return environmentBlueprintRevisionRoot + recordcodec.EncodeKeySegment(environmentID) + "/" +
		recordcodec.EncodeKeySegment(revisionID) + "/"
}

func EnvironmentBlueprintLocatorKey(locator idempotencyrecord.IdempotencyLocator) (string, [sha256.Size]byte, error) {
	if _, err := idempotencyrecord.IdempotencyMarkerKey(locator); err != nil {
		return "", [sha256.Size]byte{}, err
	}
	scope := canonicalBlueprintLocatorScope(locator)
	keyDigest := sha256.Sum256([]byte(locator.Key))
	key := environmentBlueprintLocatorPrefix + encodeBlueprintDynamicBytes(scope) + "/" +
		encodeBlueprintDynamicBytes(keyDigest[:])
	if len(key) > EnvironmentBlueprintKeyMaxBytes {
		return "", [sha256.Size]byte{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint staging locator key exceeds 2 KiB",
		)
	}
	return key, keyDigest, nil
}

func canonicalBlueprintLocatorScope(locator idempotencyrecord.IdempotencyLocator) []byte {
	fields := []string{string(locator.ScopeKind), locator.ScopeID, locator.Method, locator.Route}
	length := 0
	for _, field := range fields {
		length += 4 + len(field)
	}
	result := make([]byte, 0, length)
	for _, field := range fields {
		encodedLength := make([]byte, 4)
		binary.BigEndian.PutUint32(encodedLength, uint32(len(field)))
		result = append(result, encodedLength...)
		result = append(result, field...)
	}
	return result
}

func encodeBlueprintDynamicBytes(value []byte) string {
	return "~" + base64.RawURLEncoding.EncodeToString(value)
}

func DecodeBlueprintDynamicBytes(segment string) ([]byte, error) {
	if !strings.HasPrefix(segment, "~") || len(segment) == 1 {
		return nil, CorruptEnvironmentBlueprintStage()
	}
	decoded, err := base64.RawURLEncoding.DecodeString(segment[1:])
	if err != nil || encodeBlueprintDynamicBytes(decoded) != segment {
		return nil, CorruptEnvironmentBlueprintStage()
	}
	return decoded, nil
}
