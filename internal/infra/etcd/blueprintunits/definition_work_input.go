package blueprintunits

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// DefinitionWorkInput captures metadata acknowledgement, not execution of a
// Script or a proxy reload. Those effects belong to their dependent operations.
func DefinitionWorkInput(task workexecution.Task, unit Unit) (workexecution.Input, error) {
	if task.Validate() != nil || task.Action != workexecution.ActionApply ||
		task.Scope.EnvironmentID == "" || !validUnit(unit) ||
		(unit.Target.Kind != ids.KindRoute && unit.Target.Kind != ids.KindScript && unit.Target.Kind != ids.KindDNSRecord) {
		return workexecution.Input{}, invalidRecord()
	}
	value, err := recordcodec.Encode("blueprint-definition-work-input", struct {
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
		ID: ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID,
			"definition-input/"+string(unit.Target.Kind)+"/"+unit.Target.ID),
		SHA256: hex.EncodeToString(digest[:]),
	}, nil
}
