package blueprintunits

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// EntryWorkInput binds the decision to one immutable desired input and resource
// fingerprint. Values stay in their protected authored/generation storage.
func EntryWorkInput(task workexecution.Task, unit Unit) (workexecution.Input, error) {
	if task.Validate() != nil || task.Action != workexecution.ActionApply || task.Scope.EnvironmentID == "" ||
		!validUnit(unit) || unit.Target.Kind != ids.KindEnvEntry {
		return workexecution.Input{}, invalidRecord()
	}
	value, err := recordcodec.Encode("blueprint-entry-work-input", struct {
		TaskID  string              `json:"task_id"`
		Desired workexecution.Input `json:"desired"`
		Unit    Unit                `json:"unit"`
	}{task.ID, task.Intent, unit})
	if err != nil {
		return workexecution.Input{}, err
	}
	digest := sha256.Sum256(value)
	clear(value)
	return workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "entry-input/"+unit.Target.ID),
		SHA256: hex.EncodeToString(digest[:]),
	}, nil
}
