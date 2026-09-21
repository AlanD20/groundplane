package taskconfiguration

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TaskSecretPinSet binds the operation's original owning Task and exact member
// identity through restart and Retry. It never contains Secret value bytes.
type TaskSecretPinSet struct {
	TaskID string `json:"task_id"`
	Count  uint64 `json:"count"`
	SHA256 string `json:"sha256"`
}

func ValidateTaskSecretPinSet(binding *TaskSecretPinSet) error {
	if binding == nil {
		return nil
	}
	digest, err := hex.DecodeString(binding.SHA256)
	if ids.Validate(ids.KindTask, binding.TaskID) != nil || binding.Count == 0 ||
		binding.Count > tasksecretpins.MaximumPins || err != nil || len(digest) != 32 ||
		hex.EncodeToString(digest) != binding.SHA256 {
		return errs.New(errs.KindValidationFailed, "Task Secret pin set identity is invalid")
	}
	return nil
}
