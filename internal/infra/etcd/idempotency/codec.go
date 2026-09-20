package idempotency

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

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
type RetentionReferenceJSON struct {
	Schema    int    `json:"schema"`
	MarkerKey string `json:"marker_key"`
}
type ReplayTargetReferenceJSON struct {
	Schema    int    `json:"schema"`
	MarkerKey string `json:"marker_key"`
}

func EncodeIdempotencyMarker(marker IdempotencyMarker) ([]byte, error) {
	if err := ValidateIdempotencyMarker(marker); err != nil {
		return nil, err
	}
	value, err := json.Marshal(idempotencyMarkerJSON{
		Schema: 2, Kind: marker.Kind, State: marker.State,
		Method: marker.Locator.Method, Route: marker.Locator.Route,
		ScopeKind: marker.Locator.ScopeKind, ScopeID: marker.Locator.ScopeID,
		IdempotencyKey: marker.Locator.Key,
		ReplayTarget:   CloneIdempotencyReplayTarget(marker.ReplayTarget),
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

func DecodeIdempotencyMarker(value []byte, locator IdempotencyLocator) (IdempotencyMarker, error) {
	if len(value) == 0 || len(value) > maximumMarkerBytes || recordcodec.RejectDuplicateFields(value) != nil {
		return IdempotencyMarker{}, CorruptIdempotencyMarker()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var data idempotencyMarkerJSON
	if err := decoder.Decode(&data); err != nil || recordcodec.RequireEOF(decoder) != nil || data.Schema != 2 {
		return IdempotencyMarker{}, CorruptIdempotencyMarker()
	}
	ciphertext, err := decodeRawBase64(data.Intent.Ciphertext)
	if err != nil {
		return IdempotencyMarker{}, CorruptIdempotencyMarker()
	}
	body, err := decodeRawBase64(data.Response.Body)
	if err != nil {
		clear(ciphertext)
		return IdempotencyMarker{}, CorruptIdempotencyMarker()
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
		return IdempotencyMarker{}, CorruptIdempotencyMarker()
	}
	updatedAt, err := parseRequiredMarkerTime(data.UpdatedAt)
	if err != nil {
		return IdempotencyMarker{}, CorruptIdempotencyMarker()
	}
	terminalAt, err := parseOptionalMarkerTime(data.TerminalAt)
	if err != nil {
		return IdempotencyMarker{}, CorruptIdempotencyMarker()
	}
	retainUntil, err := parseOptionalMarkerTime(data.RetainUntil)
	if err != nil {
		return IdempotencyMarker{}, CorruptIdempotencyMarker()
	}
	marker := IdempotencyMarker{
		Kind: data.Kind, State: data.State,
		Locator: IdempotencyLocator{ScopeKind: data.ScopeKind, ScopeID: data.ScopeID, Method: data.Method,
			Route: data.Route, Key: data.IdempotencyKey},
		ReplayTarget: CloneIdempotencyReplayTarget(data.ReplayTarget),
		Intent: ProtectedIntentRecord{EnvelopeVersion: data.Intent.EnvelopeVersion, Cipher: data.Intent.Cipher,
			DigestAlgorithm: data.Intent.DigestAlgorithm, CiphertextDigest: data.Intent.CiphertextDigest,
			Ciphertext: ciphertext},
		Response: IdempotencyResponse{Status: data.Response.Status, ContentKind: data.Response.ContentKind, Body: body},
		TaskID:   data.TaskID, CreatedAt: createdAt, UpdatedAt: updatedAt,
		TerminalAt: terminalAt, RetainUntil: retainUntil,
	}
	if marker.Locator != locator || ValidateIdempotencyMarker(marker) != nil {
		return IdempotencyMarker{}, CorruptIdempotencyMarker()
	}
	keepBuffers = true
	return marker, nil
}

func EncodeTaskReference(taskID string) ([]byte, error) {
	if err := ids.Validate(ids.KindTask, taskID); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "task index record id is invalid")
	}
	return json.Marshal(taskReferenceJSON{Schema: 1, RecordID: taskID})
}

func DecodeTaskReference(value []byte) (string, error) {
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

func DecodeRetentionReference(value []byte, markerKey string) error {
	if len(value) == 0 || recordcodec.RejectDuplicateFields(value) != nil {
		return CorruptIdempotencyMarker()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var data RetentionReferenceJSON
	if err := decoder.Decode(&data); err != nil || recordcodec.RequireEOF(decoder) != nil ||
		data.Schema != 1 || data.MarkerKey != markerKey {
		return CorruptIdempotencyMarker()
	}
	return nil
}

func EncodeReplayTargetReference(markerKey string) ([]byte, error) {
	if _, err := ParseIdempotencyMarkerKey(markerKey); err != nil {
		return nil, CorruptIdempotencyMarker()
	}
	return json.Marshal(ReplayTargetReferenceJSON{Schema: 1, MarkerKey: markerKey})
}

func DecodeReplayTargetReference(value []byte, markerKey string) error {
	if len(value) == 0 || recordcodec.RejectDuplicateFields(value) != nil {
		return CorruptIdempotencyMarker()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var data ReplayTargetReferenceJSON
	if err := decoder.Decode(&data); err != nil || recordcodec.RequireEOF(decoder) != nil ||
		data.Schema != 1 || data.MarkerKey != markerKey {
		return CorruptIdempotencyMarker()
	}
	return nil
}
