package s3connector

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ConnectionCheckCleanup identifies only the isolated provider state owned by
// one permission check. Its persistence owner retains the exact Connector and
// encrypted credential snapshot until cleanup is durably acknowledged.
type ConnectionCheckCleanup struct {
	OperationID      string   `json:"operation_id"`
	ObjectKey        string   `json:"object_key"`
	MultipartKey     string   `json:"multipart_key"`
	PayloadSHA256    string   `json:"payload_sha256"`
	PayloadSizeBytes int64    `json:"payload_size_bytes"`
	ObjectPending    bool     `json:"object_pending"`
	MultipartPending bool     `json:"multipart_pending"`
	UploadIDs        []string `json:"upload_ids,omitempty"`
}

func NewConnectionCheckCleanup(operationID, prefix string) (ConnectionCheckCleanup, error) {
	key := prefix + ".groundplane-checks/" + operationID
	body := ConnectionCheckPayload(operationID)
	digest := sha256.Sum256(body)
	state := ConnectionCheckCleanup{
		OperationID: operationID, ObjectKey: key, MultipartKey: key + ".multipart",
		PayloadSHA256: hex.EncodeToString(digest[:]), PayloadSizeBytes: int64(len(body)),
	}
	return state, state.Validate(prefix)
}

// ConnectionCheckPayload is used only when creating a fresh check. Cleanup
// consumes captured size/digest fields, never a regenerated current payload.
func ConnectionCheckPayload(operationID string) []byte {
	return []byte("Groundplane Connector permission check " + operationID)
}

func (state ConnectionCheckCleanup) Validate(prefix string) error {
	key := prefix + ".groundplane-checks/" + state.OperationID
	digest, digestErr := hex.DecodeString(state.PayloadSHA256)
	if !ValidPrefix(prefix) || ids.Validate(ids.KindOperation, state.OperationID) != nil ||
		state.ObjectKey != key || state.MultipartKey != key+".multipart" || len(state.UploadIDs) > 16 ||
		digestErr != nil || len(digest) != sha256.Size || state.PayloadSizeBytes <= 0 || state.PayloadSizeBytes > 4096 {
		return errs.New(errs.KindValidationFailed, "connector check cleanup authority is invalid")
	}
	for _, uploadID := range state.UploadIDs {
		if uploadID == "" || len(uploadID) > 4096 {
			return errs.New(errs.KindValidationFailed, "connector check upload identity is invalid")
		}
	}
	return nil
}
