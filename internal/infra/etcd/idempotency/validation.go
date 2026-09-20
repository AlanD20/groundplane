package idempotency

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"net/http"
)

func ValidateProtectedIntent(value ProtectedIntentRecord) error {
	if value.EnvelopeVersion != 1 || value.Cipher != "age-x25519" || value.DigestAlgorithm != "sha256" {
		return CorruptIdempotencyMarker()
	}
	if len(value.Ciphertext) == 0 || len(value.Ciphertext) > MaximumIntentCiphertext {
		return CorruptIdempotencyMarker()
	}
	digest, err := hex.DecodeString(value.CiphertextDigest)
	if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != value.CiphertextDigest {
		return CorruptIdempotencyMarker()
	}
	want := sha256.Sum256(value.Ciphertext)
	if subtle.ConstantTimeCompare(digest, want[:]) != 1 {
		return CorruptIdempotencyMarker()
	}
	return nil
}

func ValidateIdempotencyMarker(marker IdempotencyMarker) error {
	if err := ValidateIdempotencyLocator(marker.Locator); err != nil {
		return CorruptIdempotencyMarker()
	}
	if err := ValidateProtectedIntent(marker.Intent); err != nil {
		return err
	}
	if marker.ReplayTarget != nil {
		if err := validateIdempotencyReplayTarget(*marker.ReplayTarget); err != nil {
			return CorruptIdempotencyMarker()
		}
	}
	if len(marker.Response.Body) > maximumReplayBody {
		return CorruptIdempotencyMarker()
	}
	if !recordcodec.IsCanonicalUTC(marker.CreatedAt) || !recordcodec.IsCanonicalUTC(marker.UpdatedAt) ||
		marker.UpdatedAt.Before(marker.CreatedAt) {
		return CorruptIdempotencyMarker()
	}
	switch marker.Kind {
	case IdempotencyMarkerDirect:
		if marker.State != IdempotencyMarkerCompleted || marker.TaskID != "" ||
			!validTerminalTimes(marker) || !validDirectResponse(marker.Locator.Method, marker.Response) {
			return CorruptIdempotencyMarker()
		}
	case IdempotencyMarkerTask:
		if ids.Validate(ids.KindTask, marker.TaskID) != nil ||
			(!ValidTaskResponse(marker.Response, marker.TaskID) && !validEntryMutationTaskResponse(marker)) {
			return CorruptIdempotencyMarker()
		}
		switch marker.State {
		case IdempotencyMarkerPending:
			if !marker.TerminalAt.IsZero() || !marker.RetainUntil.IsZero() {
				return CorruptIdempotencyMarker()
			}
		case IdempotencyMarkerCompleted, IdempotencyMarkerFailed:
			if !validTerminalTimes(marker) {
				return CorruptIdempotencyMarker()
			}
		default:
			return CorruptIdempotencyMarker()
		}
	default:
		return CorruptIdempotencyMarker()
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

func validTerminalTimes(marker IdempotencyMarker) bool {
	return recordcodec.IsCanonicalUTC(marker.TerminalAt) && recordcodec.IsCanonicalUTC(marker.RetainUntil) &&
		!marker.TerminalAt.Before(marker.CreatedAt) && marker.UpdatedAt.Equal(marker.TerminalAt) &&
		marker.RetainUntil.Equal(marker.TerminalAt.Add(MarkerRetention))
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

func ValidTaskResponse(response IdempotencyResponse, taskID string) bool {
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
