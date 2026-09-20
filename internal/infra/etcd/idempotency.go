package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	idempotencyMarkerPrefix       = "/v1/runtime/idempotency/"
	idempotencyRetentionPrefix    = "/v1/indexes/idempotency/by-retain-until/"
	idempotencyReplayTargetPrefix = "/v1/indexes/idempotency/by-replay-target/"
	maximumMarkerKeyBytes         = 2 << 10
	maximumIntentCiphertext       = 4 << 10
	maximumReplayBody             = 128 << 10
	maximumMarkerBytes            = 256 << 10
	maximumPruneMarkers           = 16
	maximumPruneCASAttempts       = 3
	markerRetention               = 90 * 24 * time.Hour
)

var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{16,128}$`)

type IdempotencyScopeKind string

const (
	IdempotencyScopePlatform    IdempotencyScopeKind = "platform"
	IdempotencyScopeTenant      IdempotencyScopeKind = "tenant"
	IdempotencyScopeProject     IdempotencyScopeKind = "project"
	IdempotencyScopeEnvironment IdempotencyScopeKind = "environment"
)

type IdempotencyLocator struct {
	ScopeKind IdempotencyScopeKind `json:"scope_kind"`
	ScopeID   string               `json:"scope_id"`
	Method    string               `json:"method"`
	Route     string               `json:"route"`
	Key       string               `json:"key"`
}
type IdempotencyReplayTargetKind string

const (
	IdempotencyReplayTargetAttach         IdempotencyReplayTargetKind = "attach"
	IdempotencyReplayTargetConnector      IdempotencyReplayTargetKind = "connector"
	IdempotencyReplayTargetEntry          IdempotencyReplayTargetKind = "entry"
	IdempotencyReplayTargetRoute          IdempotencyReplayTargetKind = "route"
	IdempotencyReplayTargetRunner         IdempotencyReplayTargetKind = "runner"
	IdempotencyReplayTargetScript         IdempotencyReplayTargetKind = "script"
	IdempotencyReplayTargetReleaseGroup   IdempotencyReplayTargetKind = "release_group"
	IdempotencyReplayTargetSecret         IdempotencyReplayTargetKind = "secret"
	IdempotencyReplayTargetZone           IdempotencyReplayTargetKind = "zone"
	IdempotencyReplayTargetService        IdempotencyReplayTargetKind = "service"
	IdempotencyReplayTargetVolume         IdempotencyReplayTargetKind = "volume"
	IdempotencyReplayTargetTenant         IdempotencyReplayTargetKind = "tenant"
	IdempotencyReplayTargetProject        IdempotencyReplayTargetKind = "project"
	IdempotencyReplayTargetEnvironment    IdempotencyReplayTargetKind = "environment"
	IdempotencyReplayTargetBacking        IdempotencyReplayTargetKind = "backing-service"
	IdempotencyReplayTargetBackingService                             = IdempotencyReplayTargetBacking
)

type IdempotencyReplayTarget struct {
	Kind IdempotencyReplayTargetKind `json:"kind"`
	ID   string                      `json:"id"`
}
type ProtectedIntentRecord struct {
	EnvelopeVersion  uint8
	Cipher           string
	DigestAlgorithm  string
	CiphertextDigest string
	Ciphertext       []byte
}

func (value ProtectedIntentRecord) String() string   { return "ProtectedIntentRecord{redacted}" }
func (value ProtectedIntentRecord) GoString() string { return value.String() }

type IdempotencyResponse struct {
	Status      int
	ContentKind string
	Body        []byte
}

func (response IdempotencyResponse) String() string {
	return fmt.Sprintf(
		"IdempotencyResponse{status:%d,content_kind:%s,body:redacted}",
		response.Status,
		response.ContentKind,
	)
}
func (response IdempotencyResponse) GoString() string { return response.String() }

type IdempotencyMarkerKind string

const (
	IdempotencyMarkerDirect IdempotencyMarkerKind = "direct"
	IdempotencyMarkerTask   IdempotencyMarkerKind = "task"
)

type IdempotencyMarkerState string

const (
	IdempotencyMarkerPending   IdempotencyMarkerState = "pending"
	IdempotencyMarkerCompleted IdempotencyMarkerState = "completed"
	IdempotencyMarkerFailed    IdempotencyMarkerState = "failed"
)

type IdempotencyMarker struct {
	Kind         IdempotencyMarkerKind
	State        IdempotencyMarkerState
	Locator      IdempotencyLocator
	ReplayTarget *IdempotencyReplayTarget
	Intent       ProtectedIntentRecord
	Response     IdempotencyResponse
	TaskID       string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	TerminalAt   time.Time
	RetainUntil  time.Time
}

// NewCompletedDirectIdempotencyMarker fixes the lifecycle of a synchronous
// mutation marker: the exact public result is terminal at commit and retained
// for the accepted 90-day replay window.
func NewCompletedDirectIdempotencyMarker(
	locator IdempotencyLocator,
	intent ProtectedIntentRecord,
	response IdempotencyResponse,
	now time.Time,
) (IdempotencyMarker, error) {
	marker := IdempotencyMarker{
		Kind: IdempotencyMarkerDirect, State: IdempotencyMarkerCompleted,
		Locator: locator, Intent: intent, Response: response,
		CreatedAt: now, UpdatedAt: now, TerminalAt: now,
		RetainUntil: now.Add(markerRetention),
	}
	marker.Intent.Ciphertext = append([]byte(nil), intent.Ciphertext...)
	marker.Response.Body = append([]byte(nil), response.Body...)
	if err := validateIdempotencyMarker(marker); err != nil {
		clear(marker.Intent.Ciphertext)
		clear(marker.Response.Body)
		return IdempotencyMarker{}, err
	}
	return marker, nil
}
func (marker IdempotencyMarker) String() string {
	return fmt.Sprintf("IdempotencyMarker{kind:%s,state:%s,redacted}", marker.Kind, marker.State)
}
func (marker IdempotencyMarker) GoString() string { return marker.String() }

type IdempotencyEvidence struct {
	marker      IdempotencyMarker
	modRevision int64
}
type idempotencyIntentJSON struct {
	EnvelopeVersion  uint8  `json:"envelope_version"`
	Cipher           string `json:"cipher"`
	DigestAlgorithm  string `json:"digest_algorithm"`
	CiphertextDigest string `json:"ciphertext_digest"`
	Ciphertext       string `json:"ciphertext"`
}
type idempotencyResponseJSON struct {
	Status      int    `json:"status"`
	ContentKind string `json:"content_kind"`
	Body        string `json:"body"`
}
type idempotencyMarkerJSON struct {
	Schema         int                      `json:"schema"`
	Kind           IdempotencyMarkerKind    `json:"kind"`
	State          IdempotencyMarkerState   `json:"state"`
	Method         string                   `json:"method"`
	Route          string                   `json:"route"`
	ScopeKind      IdempotencyScopeKind     `json:"scope_kind"`
	ScopeID        string                   `json:"scope_id"`
	IdempotencyKey string                   `json:"idempotency_key"`
	ReplayTarget   *IdempotencyReplayTarget `json:"replay_target,omitempty"`
	Intent         idempotencyIntentJSON    `json:"intent"`
	Response       idempotencyResponseJSON  `json:"response"`
	TaskID         string                   `json:"task_id,omitempty"`
	CreatedAt      string                   `json:"created_at"`
	UpdatedAt      string                   `json:"updated_at"`
	TerminalAt     string                   `json:"terminal_at,omitempty"`
	RetainUntil    string                   `json:"retain_until,omitempty"`
}
type taskReferenceJSON struct {
	Schema   int    `json:"schema"`
	RecordID string `json:"record_id"`
}
type retentionReferenceJSON struct {
	Schema    int    `json:"schema"`
	MarkerKey string `json:"marker_key"`
}
type replayTargetReferenceJSON struct {
	Schema    int    `json:"schema"`
	MarkerKey string `json:"marker_key"`
}

func idempotencyMarkerKey(locator IdempotencyLocator) (string, error) {
	if err := validateIdempotencyLocator(locator); err != nil {
		return "", err
	}
	key := idempotencyMarkerPrefix + string(locator.ScopeKind) + "/" + locator.ScopeID + "/" +
		recordcodec.EncodeKeySegment(locator.Method) + "/" + recordcodec.EncodeKeySegment(locator.Route) + "/" +
		recordcodec.EncodeKeySegment(locator.Key)
	if len(key) > maximumMarkerKeyBytes {
		return "", errs.New(errs.KindValidationFailed, "idempotency lookup exceeds key limit")
	}
	return key, nil
}

func validateIdempotencyLocator(locator IdempotencyLocator) error {
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

func idempotencyReplayTargetKey(
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
	if err := validateIdempotencyLocator(request); err != nil {
		return "", err
	}
	value := idempotencyReplayTargetPrefix + string(target.Kind) + "/" + target.ID + "/" +
		recordcodec.EncodeKeySegment(method) + "/" + recordcodec.EncodeKeySegment(route) + "/" + recordcodec.EncodeKeySegment(key)
	if len(value) > maximumMarkerKeyBytes {
		return "", errs.New(errs.KindValidationFailed, "idempotency replay target lookup exceeds key limit")
	}
	return value, nil
}

func validateProtectedIntent(value ProtectedIntentRecord) error {
	if value.EnvelopeVersion != 1 || value.Cipher != "age-x25519" || value.DigestAlgorithm != "sha256" {
		return corruptIdempotencyMarker()
	}
	if len(value.Ciphertext) == 0 || len(value.Ciphertext) > maximumIntentCiphertext {
		return corruptIdempotencyMarker()
	}
	digest, err := hex.DecodeString(value.CiphertextDigest)
	if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != value.CiphertextDigest {
		return corruptIdempotencyMarker()
	}
	want := sha256.Sum256(value.Ciphertext)
	if subtle.ConstantTimeCompare(digest, want[:]) != 1 {
		return corruptIdempotencyMarker()
	}
	return nil
}

func validateIdempotencyMarker(marker IdempotencyMarker) error {
	if err := validateIdempotencyLocator(marker.Locator); err != nil {
		return corruptIdempotencyMarker()
	}
	if err := validateProtectedIntent(marker.Intent); err != nil {
		return err
	}
	if marker.ReplayTarget != nil {
		if err := validateIdempotencyReplayTarget(*marker.ReplayTarget); err != nil {
			return corruptIdempotencyMarker()
		}
	}
	if len(marker.Response.Body) > maximumReplayBody {
		return corruptIdempotencyMarker()
	}
	if !validMarkerTime(marker.CreatedAt) || !validMarkerTime(marker.UpdatedAt) ||
		marker.UpdatedAt.Before(marker.CreatedAt) {
		return corruptIdempotencyMarker()
	}
	switch marker.Kind {
	case IdempotencyMarkerDirect:
		if marker.State != IdempotencyMarkerCompleted || marker.TaskID != "" ||
			!validTerminalTimes(marker) || !validDirectResponse(marker.Locator.Method, marker.Response) {
			return corruptIdempotencyMarker()
		}
	case IdempotencyMarkerTask:
		if ids.Validate(ids.KindTask, marker.TaskID) != nil ||
			(!validTaskResponse(marker.Response, marker.TaskID) && !validEntryMutationTaskResponse(marker)) {
			return corruptIdempotencyMarker()
		}
		switch marker.State {
		case IdempotencyMarkerPending:
			if !marker.TerminalAt.IsZero() || !marker.RetainUntil.IsZero() {
				return corruptIdempotencyMarker()
			}
		case IdempotencyMarkerCompleted, IdempotencyMarkerFailed:
			if !validTerminalTimes(marker) {
				return corruptIdempotencyMarker()
			}
		default:
			return corruptIdempotencyMarker()
		}
	default:
		return corruptIdempotencyMarker()
	}
	return nil
}

func validEntryMutationTaskResponse(marker IdempotencyMarker) bool {
	status := http.StatusCreated
	if marker.Locator.Method == http.MethodPatch && marker.Locator.Route == "/entries/{id}" {
		status = http.StatusOK
	} else if marker.Locator.Method != http.MethodPost || marker.Locator.Route != "/entries" {
		return false
	}
	if marker.ReplayTarget == nil || marker.ReplayTarget.Kind != IdempotencyReplayTargetEntry ||
		ids.Validate(ids.KindEnvEntry, marker.ReplayTarget.ID) != nil || marker.Response.Status != status ||
		marker.Response.ContentKind != "application/json" {
		return false
	}
	var body struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(marker.Response.Body, &body) != nil || body.ID != marker.ReplayTarget.ID {
		return false
	}
	var compact bytes.Buffer
	if json.Compact(&compact, marker.Response.Body) != nil {
		return false
	}
	return bytes.Equal(marker.Response.Body, compact.Bytes())
}

func validMarkerTime(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC &&
		value.Format(time.RFC3339Nano) == value.UTC().Format(time.RFC3339Nano)
}

func validTerminalTimes(marker IdempotencyMarker) bool {
	return validMarkerTime(marker.TerminalAt) && validMarkerTime(marker.RetainUntil) &&
		!marker.TerminalAt.Before(marker.CreatedAt) && marker.UpdatedAt.Equal(marker.TerminalAt) &&
		marker.RetainUntil.Equal(marker.TerminalAt.Add(markerRetention))
}

func validDirectResponse(method string, response IdempotencyResponse) bool {
	switch method {
	case http.MethodPost:
		return (response.Status == http.StatusOK || response.Status == http.StatusCreated) &&
			response.ContentKind == "application/json" &&
			json.Valid(response.Body) || validRouteMutationAcceptedResponse(method, response)
	case http.MethodPut, http.MethodPatch:
		return response.Status == http.StatusOK && response.ContentKind == "application/json" &&
			json.Valid(response.Body) || validRouteMutationAcceptedResponse(method, response)
	case http.MethodDelete:
		return response.Status == http.StatusNoContent && response.ContentKind == "none" && len(response.Body) == 0
	default:
		return false
	}
}

func validRouteMutationAcceptedResponse(method string, response IdempotencyResponse) bool {
	if response.Status != http.StatusAccepted || response.ContentKind != "application/json" ||
		method != http.MethodPost && method != http.MethodPatch {
		return false
	}
	var body struct {
		Route struct {
			ID string `json:"id"`
		} `json:"route"`
		TaskID string `json:"task_id"`
	}
	if json.Unmarshal(response.Body, &body) != nil || ids.Validate(ids.KindRoute, body.Route.ID) != nil ||
		ids.Validate(ids.KindTask, body.TaskID) != nil {
		return false
	}
	var compact bytes.Buffer
	return json.Compact(&compact, response.Body) == nil && bytes.Equal(response.Body, compact.Bytes())
}

func validTaskResponse(response IdempotencyResponse, taskID string) bool {
	if response.Status != http.StatusOK && response.Status != http.StatusAccepted &&
		response.Status != http.StatusCreated ||
		response.ContentKind != "application/json" {
		return false
	}
	var body struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal(response.Body, &body); err != nil || body.TaskID != taskID {
		return false
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, response.Body); err != nil {
		return false
	}
	return bytes.Equal(response.Body, compact.Bytes())
}

func encodeIdempotencyMarker(marker IdempotencyMarker) ([]byte, error) {
	if err := validateIdempotencyMarker(marker); err != nil {
		return nil, err
	}
	value, err := json.Marshal(idempotencyMarkerJSON{
		Schema: 2, Kind: marker.Kind, State: marker.State,
		Method: marker.Locator.Method, Route: marker.Locator.Route,
		ScopeKind: marker.Locator.ScopeKind, ScopeID: marker.Locator.ScopeID,
		IdempotencyKey: marker.Locator.Key,
		ReplayTarget:   cloneIdempotencyReplayTarget(marker.ReplayTarget),
		Intent: idempotencyIntentJSON{
			EnvelopeVersion: marker.Intent.EnvelopeVersion, Cipher: marker.Intent.Cipher,
			DigestAlgorithm: marker.Intent.DigestAlgorithm, CiphertextDigest: marker.Intent.CiphertextDigest,
			Ciphertext: base64.RawURLEncoding.EncodeToString(marker.Intent.Ciphertext),
		},
		Response: idempotencyResponseJSON{
			Status: marker.Response.Status, ContentKind: marker.Response.ContentKind,
			Body: base64.RawURLEncoding.EncodeToString(marker.Response.Body),
		},
		TaskID: marker.TaskID, CreatedAt: marker.CreatedAt.Format(time.RFC3339Nano),
		UpdatedAt:  marker.UpdatedAt.Format(time.RFC3339Nano),
		TerminalAt: formatOptionalTime(marker.TerminalAt), RetainUntil: formatOptionalTime(marker.RetainUntil),
	})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(value) > maximumMarkerBytes {
		return nil, errs.New(errs.KindValidationFailed, "idempotency marker exceeds record limit")
	}
	return value, nil
}

func decodeIdempotencyMarker(value []byte, locator IdempotencyLocator) (IdempotencyMarker, error) {
	if len(value) == 0 || len(value) > maximumMarkerBytes || recordcodec.RejectDuplicateFields(value) != nil {
		return IdempotencyMarker{}, corruptIdempotencyMarker()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var data idempotencyMarkerJSON
	if err := decoder.Decode(&data); err != nil || recordcodec.RequireEOF(decoder) != nil || data.Schema != 2 {
		return IdempotencyMarker{}, corruptIdempotencyMarker()
	}
	ciphertext, err := decodeRawBase64(data.Intent.Ciphertext)
	if err != nil {
		return IdempotencyMarker{}, corruptIdempotencyMarker()
	}
	body, err := decodeRawBase64(data.Response.Body)
	if err != nil {
		clear(ciphertext)
		return IdempotencyMarker{}, corruptIdempotencyMarker()
	}
	keepBuffers := false
	defer func() {
		if !keepBuffers {
			clear(ciphertext)
			clear(body)
		}
	}()
	createdAt, err := parseRequiredMarkerTime(data.CreatedAt)
	if err != nil {
		return IdempotencyMarker{}, corruptIdempotencyMarker()
	}
	updatedAt, err := parseRequiredMarkerTime(data.UpdatedAt)
	if err != nil {
		return IdempotencyMarker{}, corruptIdempotencyMarker()
	}
	terminalAt, err := parseOptionalMarkerTime(data.TerminalAt)
	if err != nil {
		return IdempotencyMarker{}, corruptIdempotencyMarker()
	}
	retainUntil, err := parseOptionalMarkerTime(data.RetainUntil)
	if err != nil {
		return IdempotencyMarker{}, corruptIdempotencyMarker()
	}
	marker := IdempotencyMarker{
		Kind: data.Kind, State: data.State,
		Locator: IdempotencyLocator{ScopeKind: data.ScopeKind, ScopeID: data.ScopeID, Method: data.Method,
			Route: data.Route, Key: data.IdempotencyKey},
		ReplayTarget: cloneIdempotencyReplayTarget(data.ReplayTarget),
		Intent: ProtectedIntentRecord{EnvelopeVersion: data.Intent.EnvelopeVersion, Cipher: data.Intent.Cipher,
			DigestAlgorithm: data.Intent.DigestAlgorithm, CiphertextDigest: data.Intent.CiphertextDigest,
			Ciphertext: ciphertext},
		Response: IdempotencyResponse{Status: data.Response.Status, ContentKind: data.Response.ContentKind, Body: body},
		TaskID:   data.TaskID, CreatedAt: createdAt, UpdatedAt: updatedAt,
		TerminalAt: terminalAt, RetainUntil: retainUntil,
	}
	if marker.Locator != locator || validateIdempotencyMarker(marker) != nil {
		return IdempotencyMarker{}, corruptIdempotencyMarker()
	}
	keepBuffers = true
	return marker, nil
}

func parseIdempotencyMarkerKey(key string) (IdempotencyLocator, error) {
	if !strings.HasPrefix(key, idempotencyMarkerPrefix) || len(key) > maximumMarkerKeyBytes {
		return IdempotencyLocator{}, corruptIdempotencyMarker()
	}
	segments := strings.Split(strings.TrimPrefix(key, idempotencyMarkerPrefix), "/")
	if len(segments) != 5 {
		return IdempotencyLocator{}, corruptIdempotencyMarker()
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
	want, err := idempotencyMarkerKey(locator)
	if err != nil || want != key {
		return IdempotencyLocator{}, corruptIdempotencyMarker()
	}
	return locator, nil
}

func decodeDynamicIdempotencySegment(segment string) (string, error) {
	if !strings.HasPrefix(segment, "~") {
		return "", corruptIdempotencyMarker()
	}
	decoded, err := decodeRawBase64(strings.TrimPrefix(segment, "~"))
	if err != nil || !utf8.Valid(decoded) || recordcodec.EncodeKeySegment(string(decoded)) != segment {
		return "", corruptIdempotencyMarker()
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

func idempotencyRetentionKey(markerKey string, retainUntil time.Time) (string, error) {
	if !validMarkerTime(retainUntil) || retainUntil.UnixNano() < 0 ||
		!strings.HasPrefix(markerKey, idempotencyMarkerPrefix) {
		return "", corruptIdempotencyMarker()
	}
	return idempotencyRetentionPrefix + fmt.Sprintf("%020d", retainUntil.UnixNano()) + "/" +
		recordcodec.EncodeKeySegment(markerKey), nil
}

func validateIdempotencyRetentionKey(key string, markerKey string, retainUntil time.Time) error {
	want, err := idempotencyRetentionKey(markerKey, retainUntil)
	if err != nil || key != want {
		return corruptIdempotencyMarker()
	}
	segment := strings.TrimPrefix(key, idempotencyRetentionPrefix)
	separator := strings.IndexByte(segment, '/')
	if separator != 20 {
		return corruptIdempotencyMarker()
	}
	nanoseconds, err := strconv.ParseInt(segment[:separator], 10, 64)
	if err != nil || !time.Unix(0, nanoseconds).UTC().Equal(retainUntil) {
		return corruptIdempotencyMarker()
	}
	decoded, err := decodeRawBase64(strings.TrimPrefix(segment[separator+1:], "~"))
	if err != nil || string(decoded) != markerKey || recordcodec.EncodeKeySegment(string(decoded)) != segment[separator+1:] {
		return corruptIdempotencyMarker()
	}
	return nil
}

func parseIdempotencyRetentionKey(key string) (string, time.Time, error) {
	if !strings.HasPrefix(key, idempotencyRetentionPrefix) {
		return "", time.Time{}, corruptIdempotencyMarker()
	}
	segment := strings.TrimPrefix(key, idempotencyRetentionPrefix)
	separator := strings.IndexByte(segment, '/')
	if separator != 20 {
		return "", time.Time{}, corruptIdempotencyMarker()
	}
	nanoseconds, err := strconv.ParseInt(segment[:separator], 10, 64)
	if err != nil || nanoseconds < 0 || fmt.Sprintf("%020d", nanoseconds) != segment[:separator] {
		return "", time.Time{}, corruptIdempotencyMarker()
	}
	markerKey, err := decodeDynamicIdempotencySegment(segment[separator+1:])
	if err != nil || !strings.HasPrefix(markerKey, idempotencyMarkerPrefix) {
		return "", time.Time{}, corruptIdempotencyMarker()
	}
	retainUntil := time.Unix(0, nanoseconds).UTC()
	if err := validateIdempotencyRetentionKey(key, markerKey, retainUntil); err != nil {
		return "", time.Time{}, err
	}
	return markerKey, retainUntil, nil
}

func encodeTaskReference(taskID string) ([]byte, error) {
	if err := ids.Validate(ids.KindTask, taskID); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "task index record id is invalid")
	}
	return json.Marshal(taskReferenceJSON{Schema: 1, RecordID: taskID})
}

func decodeTaskReference(value []byte) (string, error) {
	if recordcodec.RejectDuplicateFields(value) != nil {
		return "", errs.New(errs.KindInternal, "task index record is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var data taskReferenceJSON
	if err := decoder.Decode(&data); err != nil || recordcodec.RequireEOF(decoder) != nil ||
		data.Schema != 1 || ids.Validate(ids.KindTask, data.RecordID) != nil {
		return "", errs.New(errs.KindInternal, "task index record is invalid")
	}
	return data.RecordID, nil
}

func decodeRetentionReference(value []byte, markerKey string) error {
	if len(value) == 0 || recordcodec.RejectDuplicateFields(value) != nil {
		return corruptIdempotencyMarker()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var data retentionReferenceJSON
	if err := decoder.Decode(&data); err != nil || recordcodec.RequireEOF(decoder) != nil ||
		data.Schema != 1 || data.MarkerKey != markerKey {
		return corruptIdempotencyMarker()
	}
	return nil
}

func encodeReplayTargetReference(markerKey string) ([]byte, error) {
	if _, err := parseIdempotencyMarkerKey(markerKey); err != nil {
		return nil, corruptIdempotencyMarker()
	}
	return json.Marshal(replayTargetReferenceJSON{Schema: 1, MarkerKey: markerKey})
}

func decodeReplayTargetReference(value []byte, markerKey string) error {
	if len(value) == 0 || recordcodec.RejectDuplicateFields(value) != nil {
		return corruptIdempotencyMarker()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var data replayTargetReferenceJSON
	if err := decoder.Decode(&data); err != nil || recordcodec.RequireEOF(decoder) != nil ||
		data.Schema != 1 || data.MarkerKey != markerKey {
		return corruptIdempotencyMarker()
	}
	return nil
}

type idempotencyPlanClassifier func(int64, []*etcdstore.KeyValue) error

type idempotencyMutationPlan struct {
	mu               sync.Mutex
	consumed         bool
	markerKind       IdempotencyMarkerKind
	conditions       []etcdstore.Condition
	mutations        []etcdstore.Mutation
	classify         idempotencyPlanClassifier
	validate         func([]etcdstore.Condition, []etcdstore.Mutation) error
	validateExisting func(context.Context, IdempotencyMarker, int64, int64) error
}

func (plan *idempotencyMutationPlan) enforceExistingReplay(
	validate func(context.Context, IdempotencyMarker, int64, int64) error,
) error {
	if plan == nil || validate == nil {
		return errs.New(errs.KindInternal, "idempotency replay validator is required")
	}
	plan.mu.Lock()
	defer plan.mu.Unlock()
	if plan.consumed || plan.validateExisting != nil {
		return errs.New(errs.KindInternal, "idempotency replay validator cannot be replaced")
	}
	plan.validateExisting = validate
	return nil
}

func (plan *idempotencyMutationPlan) existingReplayValidator() func(
	context.Context,
	IdempotencyMarker,
	int64,
	int64,
) error {
	if plan == nil {
		return nil
	}
	plan.mu.Lock()
	defer plan.mu.Unlock()
	return plan.validateExisting
}

// enforceTransactionBounds defers a domain envelope check until Apply has
// appended the real idempotency marker, replay target, and retention writes.
func (plan *idempotencyMutationPlan) enforceTransactionBounds(
	validate func([]etcdstore.Condition, []etcdstore.Mutation) error,
) error {
	if plan == nil || validate == nil {
		return errs.New(errs.KindInternal, "idempotency transaction validator is required")
	}
	plan.mu.Lock()
	defer plan.mu.Unlock()
	if plan.consumed || plan.validate != nil {
		return errs.New(errs.KindInternal, "idempotency transaction validator cannot be replaced")
	}
	plan.validate = validate
	return nil
}

func (plan *idempotencyMutationPlan) transactionValidator() func([]etcdstore.Condition, []etcdstore.Mutation) error {
	if plan == nil {
		return nil
	}
	plan.mu.Lock()
	defer plan.mu.Unlock()
	return plan.validate
}

func newIdempotencyMutationPlan(
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	classify idempotencyPlanClassifier,
) (*idempotencyMutationPlan, error) {
	return newIdempotencyMutationPlanForMarker(
		IdempotencyMarkerDirect,
		conditions,
		mutations,
		classify,
	)
}

func newTaskIdempotencyMutationPlan(
	record TaskRecord,
	initiation TaskInitiation,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	classify idempotencyPlanClassifier,
) (*idempotencyMutationPlan, error) {
	if err := validateTaskInitiation(record, initiation, true); err != nil {
		return nil, err
	}
	conditions, classify, err := prepareTaskInitiationFences(initiation, conditions, classify)
	if err != nil {
		return nil, err
	}
	conditions, mutations, classify, err = prepareTaskOwnerIndexPlan(record, conditions, mutations, classify)
	if err != nil {
		return nil, err
	}
	return newIdempotencyMutationPlanForMarker(
		IdempotencyMarkerTask,
		conditions,
		mutations,
		classify,
	)
}

func newIdempotencyMutationPlanForMarker(
	markerKind IdempotencyMarkerKind,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	classify idempotencyPlanClassifier,
) (*idempotencyMutationPlan, error) {
	if markerKind != IdempotencyMarkerDirect && markerKind != IdempotencyMarkerTask {
		return nil, errs.New(errs.KindInternal, "idempotency mutation plan marker kind is invalid")
	}
	if classify == nil || markerKind == IdempotencyMarkerTask && len(mutations) == 0 {
		return nil, errs.New(errs.KindInternal, "idempotency mutation plan is incomplete")
	}
	if err := validateIdempotencyPlanKeys(conditions, mutations); err != nil {
		return nil, err
	}
	return &idempotencyMutationPlan{
		markerKind: markerKind,
		conditions: append([]etcdstore.Condition(nil), conditions...),
		mutations:  cloneMutations(mutations),
		classify:   classify,
	}, nil
}

func validateIdempotencyPlanKeys(conditions []etcdstore.Condition, mutations []etcdstore.Mutation) error {
	compareKeys := make(map[string]struct{}, len(conditions))
	for _, condition := range conditions {
		if invalidIdempotencyPlanKey(condition.Key) {
			return errs.New(errs.KindInternal, "idempotency plan compare key is invalid")
		}
		if _, duplicate := compareKeys[condition.Key]; duplicate {
			return errs.New(errs.KindInternal, "idempotency plan contains a duplicate compare key")
		}
		compareKeys[condition.Key] = struct{}{}
	}
	mutationKeys := make(map[string]struct{}, len(mutations))
	for _, mutation := range mutations {
		if invalidIdempotencyPlanKey(mutation.Key) {
			return errs.New(errs.KindInternal, "idempotency plan mutation key is invalid")
		}
		if _, duplicate := mutationKeys[mutation.Key]; duplicate {
			return errs.New(errs.KindInternal, "idempotency plan contains a duplicate mutation key")
		}
		mutationKeys[mutation.Key] = struct{}{}
	}
	return nil
}

func invalidIdempotencyPlanKey(key string) bool {
	return key == "" || strings.HasPrefix(key, idempotencyMarkerPrefix) ||
		strings.HasPrefix(key, idempotencyRetentionPrefix) ||
		strings.HasPrefix(key, idempotencyReplayTargetPrefix)
}

func (plan *idempotencyMutationPlan) consume() ([]etcdstore.Condition, []etcdstore.Mutation, idempotencyPlanClassifier, error) {
	if plan == nil {
		return nil, nil, nil, errs.New(errs.KindInternal, "idempotency mutation plan is required")
	}
	plan.mu.Lock()
	defer plan.mu.Unlock()
	if plan.consumed {
		return nil, nil, nil, errs.New(errs.KindInternal, "idempotency mutation plan was already consumed")
	}
	plan.consumed = true
	conditions := append([]etcdstore.Condition(nil), plan.conditions...)
	mutations := cloneMutations(plan.mutations)
	classify := plan.classify
	clearMutationValues(plan.mutations)
	plan.conditions = nil
	plan.mutations = nil
	plan.classify = nil
	plan.validate = nil
	plan.validateExisting = nil
	return conditions, mutations, classify, nil
}

func cloneMutations(values []etcdstore.Mutation) []etcdstore.Mutation {
	result := make([]etcdstore.Mutation, len(values))
	for index, value := range values {
		result[index] = etcdstore.Mutation{Type: value.Type, Key: value.Key, Value: append([]byte(nil), value.Value...)}
	}
	return result
}

func clearMutationValues(values []etcdstore.Mutation) {
	for index := range values {
		clear(values[index].Value)
		values[index].Value = nil
	}
}

type IdempotencyTransactionResult struct {
	kind     idempotencyTransactionResultKind
	revision int64
	marker   IdempotencyMarker
	conflict error
}

type idempotencyTransactionResultKind uint8

const (
	idempotencyTransactionApplied idempotencyTransactionResultKind = iota + 1
	idempotencyTransactionExisting
	idempotencyTransactionConflict
)

type IdempotencyKnownOutcome uint8

const (
	IdempotencyKnownApplied IdempotencyKnownOutcome = iota + 1
	IdempotencyKnownExisting
	IdempotencyKnownConflict
)

func (result *IdempotencyTransactionResult) Classify() (
	IdempotencyKnownOutcome,
	IdempotencyMarker,
	error,
	error,
) {
	if result == nil || result.revision <= 0 {
		return 0, IdempotencyMarker{}, nil, corruptIdempotencyMarker()
	}
	switch result.kind {
	case idempotencyTransactionApplied:
		if result.conflict != nil || result.marker.Kind != "" {
			return 0, IdempotencyMarker{}, nil, corruptIdempotencyMarker()
		}
		return IdempotencyKnownApplied, IdempotencyMarker{}, nil, nil
	case idempotencyTransactionExisting:
		if result.conflict != nil || validateIdempotencyMarker(result.marker) != nil {
			return 0, IdempotencyMarker{}, nil, corruptIdempotencyMarker()
		}
		marker := cloneIdempotencyMarker(result.marker)
		clear(result.marker.Intent.Ciphertext)
		clear(result.marker.Response.Body)
		result.marker = IdempotencyMarker{}
		return IdempotencyKnownExisting, marker, nil, nil
	case idempotencyTransactionConflict:
		if result.conflict == nil || result.marker.Kind != "" {
			return 0, IdempotencyMarker{}, nil, corruptIdempotencyMarker()
		}
		return IdempotencyKnownConflict, IdempotencyMarker{}, result.conflict, nil
	default:
		return 0, IdempotencyMarker{}, nil, corruptIdempotencyMarker()
	}
}

func cloneIdempotencyMarker(marker IdempotencyMarker) IdempotencyMarker {
	marker.ReplayTarget = cloneIdempotencyReplayTarget(marker.ReplayTarget)
	marker.Intent.Ciphertext = append([]byte(nil), marker.Intent.Ciphertext...)
	marker.Response.Body = append([]byte(nil), marker.Response.Body...)
	return marker
}

func cloneIdempotencyReplayTarget(target *IdempotencyReplayTarget) *IdempotencyReplayTarget {
	if target == nil {
		return nil
	}
	cloned := *target
	return &cloned
}

type idempotencyRepositoryStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

type IdempotencyRepository struct{ store idempotencyRepositoryStore }

type idempotencyPruneCandidate struct {
	Marker                  IdempotencyEvidence
	RetentionKey            string
	RetentionValue          []byte
	RetentionModRevision    int64
	ReplayTargetKey         string
	ReplayTargetValue       []byte
	ReplayTargetModRevision int64
}

func NewIdempotencyRepository(store etcdstore.Store) (*IdempotencyRepository, error) {
	return newIdempotencyRepository(store)
}

func newIdempotencyRepository(store idempotencyRepositoryStore) (*IdempotencyRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "idempotency store is required")
	}
	return &IdempotencyRepository{store: store}, nil
}

func (repository *IdempotencyRepository) Apply(
	ctx context.Context,
	marker IdempotencyMarker,
	plan *idempotencyMutationPlan,
) (IdempotencyTransactionResult, error) {
	return repository.apply(ctx, marker, plan, repository.store.Transact)
}

func (repository *IdempotencyRepository) applyEnvironmentBlueprint(
	ctx context.Context,
	marker IdempotencyMarker,
	plan *idempotencyMutationPlan,
	transactions environmentBlueprintTransactionStore,
) (IdempotencyTransactionResult, error) {
	return repository.apply(ctx, marker, plan, func(
		ctx context.Context,
		conditions []etcdstore.Condition,
		mutations []etcdstore.Mutation,
	) (etcdstore.TransactionResult, error) {
		return executeEnvironmentBlueprintTransaction(ctx, transactions, conditions, mutations)
	})
}

func (repository *IdempotencyRepository) apply(
	ctx context.Context,
	marker IdempotencyMarker,
	plan *idempotencyMutationPlan,
	transact func(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error),
) (IdempotencyTransactionResult, error) {
	if ctx == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "idempotency context is required")
	}
	if plan == nil || plan.markerKind != marker.Kind ||
		(marker.Kind == IdempotencyMarkerTask && marker.State != IdempotencyMarkerPending) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"idempotency marker does not match its mutation plan",
		)
	}
	markerKey, err := idempotencyMarkerKey(marker.Locator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	markerValue, err := encodeIdempotencyMarker(marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(markerValue)
	validateTransaction := plan.transactionValidator()
	validateExisting := plan.existingReplayValidator()
	conditions, mutations, classify, err := plan.consume()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearMutationValues(mutations)
	conditions = append([]etcdstore.Condition{{Key: markerKey, ModRevision: 0}}, conditions...)
	mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: markerKey, Value: markerValue})
	if marker.ReplayTarget != nil {
		targetKey, targetErr := idempotencyReplayTargetKey(
			*marker.ReplayTarget, marker.Locator.Method, marker.Locator.Route, marker.Locator.Key,
		)
		if targetErr != nil {
			return IdempotencyTransactionResult{}, targetErr
		}
		targetValue, targetErr := encodeReplayTargetReference(markerKey)
		if targetErr != nil {
			return IdempotencyTransactionResult{}, targetErr
		}
		defer clear(targetValue)
		conditions = append(conditions[:1], append([]etcdstore.Condition{{Key: targetKey, ModRevision: 0}}, conditions[1:]...)...)
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: targetKey, Value: targetValue})
	}
	if !marker.RetainUntil.IsZero() {
		retentionKey, err := idempotencyRetentionKey(markerKey, marker.RetainUntil)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		retentionValue, err := json.Marshal(retentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
		if err != nil {
			return IdempotencyTransactionResult{}, errs.Wrap(errs.KindInternal, err)
		}
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: retentionKey, Value: retentionValue})
	}
	if validateTransaction != nil {
		if err := validateTransaction(conditions, mutations); err != nil {
			return IdempotencyTransactionResult{}, err
		}
	}
	result, err := transact(ctx, conditions, mutations)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearKeyValues(result.FailureReads)
	if result.Succeeded {
		return IdempotencyTransactionResult{
			kind: idempotencyTransactionApplied, revision: result.Revision,
		}, nil
	}
	if len(result.FailureReads) != len(conditions) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "idempotency compare evidence is incomplete")
	}
	if result.FailureReads[0] != nil {
		existing, err := decodeIdempotencyMarker(result.FailureReads[0].Value, marker.Locator)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		if validateExisting != nil {
			if err := validateExisting(
				ctx,
				existing,
				result.Revision,
				result.FailureReads[0].ModRevision,
			); err != nil {
				return IdempotencyTransactionResult{}, err
			}
		}
		return IdempotencyTransactionResult{
			kind: idempotencyTransactionExisting, revision: result.Revision, marker: existing,
		}, nil
	}
	planOffset := 1
	if marker.ReplayTarget != nil {
		if result.FailureReads[1] != nil {
			if err := decodeReplayTargetReference(result.FailureReads[1].Value, markerKey); err != nil {
				return IdempotencyTransactionResult{}, err
			}
			return IdempotencyTransactionResult{}, corruptIdempotencyMarker()
		}
		planOffset++
	}
	conflict := classify(result.Revision, result.FailureReads[planOffset:])
	if conflict == nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"idempotency plan conflict was not classified",
		)
	}
	return IdempotencyTransactionResult{
		kind: idempotencyTransactionConflict, revision: result.Revision, conflict: conflict,
	}, nil
}

func clearKeyValues(values []*etcdstore.KeyValue) {
	for _, value := range values {
		if value != nil {
			clear(value.Value)
			value.Value = nil
		}
	}
}

func (evidence *IdempotencyEvidence) Marker() (IdempotencyMarker, error) {
	if evidence == nil || evidence.modRevision <= 0 || validateIdempotencyMarker(evidence.marker) != nil {
		return IdempotencyMarker{}, corruptIdempotencyMarker()
	}
	marker := cloneIdempotencyMarker(evidence.marker)
	clear(evidence.marker.Intent.Ciphertext)
	clear(evidence.marker.Response.Body)
	evidence.marker = IdempotencyMarker{}
	return marker, nil
}

func corruptIdempotencyMarker() error {
	return errs.New(errs.KindInternal, "durable idempotency evidence is invalid")
}
