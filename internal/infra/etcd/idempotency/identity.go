package idempotency

import (
	"encoding/base64"
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

func IdempotencyMarkerKey(locator IdempotencyLocator) (string, error) {
	if err := ValidateIdempotencyLocator(locator); err != nil {
		return "", err
	}
	key := IdempotencyMarkerPrefix + string(locator.ScopeKind) + "/" + locator.ScopeID + "/" +
		recordcodec.EncodeKeySegment(locator.Method) + "/" + recordcodec.EncodeKeySegment(locator.Route) + "/" +
		recordcodec.EncodeKeySegment(locator.Key)
	if len(key) > maximumMarkerKeyBytes {
		return "", errs.New(errs.KindValidationFailed, "idempotency lookup exceeds key limit")
	}
	return key, nil
}

func ValidateIdempotencyLocator(locator IdempotencyLocator) error {
	var ownerKind ids.Kind
	switch locator.ScopeKind {
	case IdempotencyScopePlatform:
		if locator.ScopeID != "-" {
			return errs.New(errs.KindValidationFailed, "platform idempotency scope id must be -")
		}
	case IdempotencyScopeTenant:
		ownerKind = ids.KindTenant
	case IdempotencyScopeProject:
		ownerKind = ids.KindProject
	case IdempotencyScopeEnvironment:
		ownerKind = ids.KindEnvironment
	default:
		return errs.New(errs.KindValidationFailed, "idempotency scope kind is invalid")
	}
	if ownerKind != "" {
		if err := ids.Validate(ownerKind, locator.ScopeID); err != nil {
			return errs.New(errs.KindValidationFailed, "idempotency scope id is invalid")
		}
	}
	if locator.Method != strings.ToUpper(locator.Method) {
		return errs.New(errs.KindValidationFailed, "idempotency method must be uppercase")
	}
	switch locator.Method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
	default:
		return errs.New(errs.KindValidationFailed, "idempotency method is not mutating")
	}
	if locator.Route == "" || !strings.HasPrefix(locator.Route, "/") || !utf8.ValidString(locator.Route) {
		return errs.New(errs.KindValidationFailed, "idempotency route is invalid")
	}
	if !idempotencyKeyPattern.MatchString(locator.Key) {
		return errs.New(errs.KindValidationFailed, "idempotency key is invalid")
	}
	return nil
}

func validateIdempotencyReplayTarget(target IdempotencyReplayTarget) error {
	switch target.Kind {
	case IdempotencyReplayTargetAttach:
		if ids.Validate(ids.KindAttach, target.ID) != nil {
			return errs.New(errs.KindValidationFailed, "idempotency replay target id is invalid")
		}
	case IdempotencyReplayTargetConnector:
		if ids.Validate(ids.KindConnector, target.ID) != nil {
			return errs.New(errs.KindValidationFailed, "idempotency replay target id is invalid")
		}
	case IdempotencyReplayTargetEntry:
		if ids.Validate(ids.KindEnvEntry, target.ID) != nil {
			return errs.New(errs.KindValidationFailed, "idempotency replay target id is invalid")
		}
	case IdempotencyReplayTargetRoute:
		if ids.Validate(ids.KindRoute, target.ID) != nil {
			return errs.New(errs.KindValidationFailed, "idempotency replay target id is invalid")
		}
	case IdempotencyReplayTargetRunner:
		if ids.Validate(ids.KindRunner, target.ID) != nil {
			return errs.New(errs.KindValidationFailed, "idempotency replay target id is invalid")
		}
	case IdempotencyReplayTargetScript:
		if ids.Validate(ids.KindScript, target.ID) != nil {
			return errs.New(errs.KindValidationFailed, "idempotency replay target id is invalid")
		}
	case IdempotencyReplayTargetReleaseGroup:
		if ids.Validate(ids.KindReleaseGroup, target.ID) != nil {
			return errs.New(errs.KindValidationFailed, "idempotency replay target id is invalid")
		}
	case IdempotencyReplayTargetSecret:
		if ids.Validate(ids.KindSecret, target.ID) != nil {
			return errs.New(errs.KindValidationFailed, "idempotency replay target id is invalid")
		}
	case IdempotencyReplayTargetZone:
		if ids.Validate(ids.KindNetwork, target.ID) != nil {
			return errs.New(errs.KindValidationFailed, "idempotency replay target id is invalid")
		}
	case IdempotencyReplayTargetService:
		if ids.Validate(ids.KindService, target.ID) != nil {
			return errs.New(errs.KindValidationFailed, "idempotency replay target id is invalid")
		}
	case IdempotencyReplayTargetVolume:
		if ids.Validate(ids.KindVolume, target.ID) != nil {
			return errs.New(errs.KindValidationFailed, "idempotency replay target id is invalid")
		}
	case IdempotencyReplayTargetTenant:
		if ids.Validate(ids.KindTenant, target.ID) != nil {
			return errs.New(errs.KindValidationFailed, "idempotency replay target id is invalid")
		}
	case IdempotencyReplayTargetProject, IdempotencyReplayTargetBacking:
		if ids.Validate(ids.KindProject, target.ID) != nil {
			return errs.New(errs.KindValidationFailed, "idempotency replay target id is invalid")
		}
	case IdempotencyReplayTargetEnvironment:
		if ids.Validate(ids.KindEnvironment, target.ID) != nil {
			return errs.New(errs.KindValidationFailed, "idempotency replay target id is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "idempotency replay target kind is invalid")
	}
	return nil
}

func IdempotencyReplayTargetKey(
	target IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (string, error) {
	if err := validateIdempotencyReplayTarget(target); err != nil {
		return "", err
	}
	request := IdempotencyLocator{
		ScopeKind: IdempotencyScopePlatform, ScopeID: "-", Method: method, Route: route, Key: key,
	}
	if err := ValidateIdempotencyLocator(request); err != nil {
		return "", err
	}
	value := IdempotencyReplayTargetPrefix + string(target.Kind) + "/" + target.ID + "/" +
		recordcodec.EncodeKeySegment(method) + "/" + recordcodec.EncodeKeySegment(route) + "/" + recordcodec.EncodeKeySegment(key)
	if len(value) > maximumMarkerKeyBytes {
		return "", errs.New(errs.KindValidationFailed, "idempotency replay target lookup exceeds key limit")
	}
	return value, nil
}

func ParseIdempotencyMarkerKey(key string) (IdempotencyLocator, error) {
	if !strings.HasPrefix(key, IdempotencyMarkerPrefix) || len(key) > maximumMarkerKeyBytes {
		return IdempotencyLocator{}, CorruptIdempotencyMarker()
	}
	segments := strings.Split(strings.TrimPrefix(key, IdempotencyMarkerPrefix), "/")
	if len(segments) != 5 {
		return IdempotencyLocator{}, CorruptIdempotencyMarker()
	}
	method, err := decodeDynamicIdempotencySegment(segments[2])
	if err != nil {
		return IdempotencyLocator{}, err
	}
	route, err := decodeDynamicIdempotencySegment(segments[3])
	if err != nil {
		return IdempotencyLocator{}, err
	}
	idempotencyKey, err := decodeDynamicIdempotencySegment(segments[4])
	if err != nil {
		return IdempotencyLocator{}, err
	}
	locator := IdempotencyLocator{
		ScopeKind: IdempotencyScopeKind(segments[0]), ScopeID: segments[1],
		Method: method, Route: route, Key: idempotencyKey,
	}
	want, err := IdempotencyMarkerKey(locator)
	if err != nil || want != key {
		return IdempotencyLocator{}, CorruptIdempotencyMarker()
	}
	return locator, nil
}

func decodeDynamicIdempotencySegment(segment string) (string, error) {
	if !strings.HasPrefix(segment, "~") {
		return "", CorruptIdempotencyMarker()
	}
	decoded, err := decodeRawBase64(strings.TrimPrefix(segment, "~"))
	if err != nil || !utf8.Valid(decoded) || recordcodec.EncodeKeySegment(string(decoded)) != segment {
		return "", CorruptIdempotencyMarker()
	}
	return string(decoded), nil
}

func formatOptionalTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Format(time.RFC3339Nano)
}

func parseRequiredMarkerTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || parsed.Location() != time.UTC || parsed.Format(time.RFC3339Nano) != value {
		return time.Time{}, fmt.Errorf("invalid marker timestamp")
	}
	return parsed, nil
}

func parseOptionalMarkerTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	return parseRequiredMarkerTime(value)
}

func decodeRawBase64(value string) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != value {
		return nil, fmt.Errorf("invalid raw base64url")
	}
	return decoded, nil
}

func IdempotencyRetentionKey(markerKey string, retainUntil time.Time) (string, error) {
	if !recordcodec.IsCanonicalUTC(retainUntil) || retainUntil.UnixNano() < 0 ||
		!strings.HasPrefix(markerKey, IdempotencyMarkerPrefix) {
		return "", CorruptIdempotencyMarker()
	}
	return IdempotencyRetentionPrefix + fmt.Sprintf("%020d", retainUntil.UnixNano()) + "/" +
		recordcodec.EncodeKeySegment(markerKey), nil
}

func ValidateIdempotencyRetentionKey(key string, markerKey string, retainUntil time.Time) error {
	want, err := IdempotencyRetentionKey(markerKey, retainUntil)
	if err != nil || key != want {
		return CorruptIdempotencyMarker()
	}
	segment := strings.TrimPrefix(key, IdempotencyRetentionPrefix)
	separator := strings.IndexByte(segment, '/')
	if separator != 20 {
		return CorruptIdempotencyMarker()
	}
	nanoseconds, err := strconv.ParseInt(segment[:separator], 10, 64)
	if err != nil || !time.Unix(0, nanoseconds).UTC().Equal(retainUntil) {
		return CorruptIdempotencyMarker()
	}
	decoded, err := decodeRawBase64(strings.TrimPrefix(segment[separator+1:], "~"))
	if err != nil || string(decoded) != markerKey || recordcodec.EncodeKeySegment(string(decoded)) != segment[separator+1:] {
		return CorruptIdempotencyMarker()
	}
	return nil
}

func ParseIdempotencyRetentionKey(key string) (string, time.Time, error) {
	if !strings.HasPrefix(key, IdempotencyRetentionPrefix) {
		return "", time.Time{}, CorruptIdempotencyMarker()
	}
	segment := strings.TrimPrefix(key, IdempotencyRetentionPrefix)
	separator := strings.IndexByte(segment, '/')
	if separator != 20 {
		return "", time.Time{}, CorruptIdempotencyMarker()
	}
	nanoseconds, err := strconv.ParseInt(segment[:separator], 10, 64)
	if err != nil || nanoseconds < 0 || fmt.Sprintf("%020d", nanoseconds) != segment[:separator] {
		return "", time.Time{}, CorruptIdempotencyMarker()
	}
	markerKey, err := decodeDynamicIdempotencySegment(segment[separator+1:])
	if err != nil || !strings.HasPrefix(markerKey, IdempotencyMarkerPrefix) {
		return "", time.Time{}, CorruptIdempotencyMarker()
	}
	retainUntil := time.Unix(0, nanoseconds).UTC()
	if err := ValidateIdempotencyRetentionKey(key, markerKey, retainUntil); err != nil {
		return "", time.Time{}, err
	}
	return markerKey, retainUntil, nil
}
