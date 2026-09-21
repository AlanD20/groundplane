package idempotency

import (
	"fmt"
	"github.com/AlanD20/groundplane/pkg/errs"
	"regexp"
	"time"
)

const (
	IdempotencyMarkerPrefix       = "/v1/runtime/idempotency/"
	IdempotencyRetentionPrefix    = "/v1/indexes/idempotency/by-retain-until/"
	IdempotencyReplayTargetPrefix = "/v1/indexes/idempotency/by-replay-target/"
	maximumMarkerKeyBytes         = 2 << 10
	MaximumIntentCiphertext       = 4 << 10
	maximumReplayBody             = 128 << 10
	maximumMarkerBytes            = 256 << 10

	MarkerRetention = 90 * 24 * time.Hour
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
		RetainUntil: now.Add(MarkerRetention),
	}
	marker.Intent.Ciphertext = append([]byte(nil), intent.Ciphertext...)
	marker.Response.Body = append([]byte(nil), response.Body...)
	if err := ValidateIdempotencyMarker(marker); err != nil {
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

func CloneIdempotencyMarker(marker IdempotencyMarker) IdempotencyMarker {
	marker.ReplayTarget = CloneIdempotencyReplayTarget(marker.ReplayTarget)
	marker.Intent.Ciphertext = append([]byte(nil), marker.Intent.Ciphertext...)
	marker.Response.Body = append([]byte(nil), marker.Response.Body...)
	return marker
}

func CloneIdempotencyReplayTarget(target *IdempotencyReplayTarget) *IdempotencyReplayTarget {
	if target == nil {
		return nil
	}
	cloned := *target
	return &cloned
}

func CorruptIdempotencyMarker() error {
	return errs.New(errs.KindInternal, "durable idempotency evidence is invalid")
}
