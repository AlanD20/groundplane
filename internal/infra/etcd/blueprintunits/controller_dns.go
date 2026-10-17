package blueprintunits

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core/blueprintreconcile"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// DNSReceipt retains the exact resolver acknowledgement used to settle this
// unit. The binding is present for a retained record, including waiting records;
// removal instead proves withdrawal and retires its hostname claims atomically.
type DNSReceipt struct {
	ComponentID         string                         `json:"component_id"`
	TaskID              string                         `json:"task_id"`
	ObservationRevision int64                          `json:"observation_revision"`
	CorefileSHA256      string                         `json:"corefile_sha256"`
	Binding             *dnsrecords.EnvironmentBinding `json:"binding,omitempty"`
}

func validDNSReceipt(record AppliedRecord) bool {
	proof := record.ControllerDNS
	if proof == nil || ids.Validate(ids.KindComponent, proof.ComponentID) != nil ||
		ids.Validate(ids.KindTask, proof.TaskID) != nil || proof.ObservationRevision <= 0 ||
		!recordcodec.ValidSHA256(proof.CorefileSHA256) {
		return false
	}
	if record.State == Absent {
		return proof.Binding == nil
	}
	return record.State == Applied && proof.Binding != nil && proof.Binding.Validate() == nil &&
		proof.Binding.Record.ID == record.Target.ID && proof.Binding.Record.EnvironmentID == record.EnvironmentID
}

// The caller joins this receipt with the immutable authored input, native work
// decision fence, exact resolver observation/input, runtime and claim changes.
func PrepareControllerDNSSettlement(snapshot Snapshot, unit Unit, operationID string,
	authoredRevision int64, proof DNSReceipt,
) (MutationPlan, error) {
	if snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID ||
		unit.Target.Kind != ids.KindDNSRecord || ids.Validate(ids.KindOperation, operationID) != nil ||
		authoredRevision <= 0 || len(unit.Writes) != 1 || unit.Writes[0] != unit.Target {
		return MutationPlan{}, invalidRecord()
	}
	if !desiredControllerUnitMatches(snapshot.Desired.Record.Units, unit) {
		return MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint DNS unit changed")
	}
	selection, err := Select(snapshot)
	if err != nil {
		return MutationPlan{}, err
	}
	if !slices.Contains(selection.Ready, blueprintreconcile.ResourceKey{Kind: unit.Target.Kind, ID: unit.Target.ID}) {
		return MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint DNS unit is not ready")
	}
	state := Applied
	if unit.Removal {
		state = Absent
	}
	applied := AppliedRecord{
		EnvironmentID: snapshot.EnvironmentID, Target: unit.Target, State: state, Fingerprint: unit.Fingerprint,
		ParentTaskID: snapshot.HeadTaskID, ControllerOperationID: operationID,
		ControllerAuthoredRevision: authoredRevision, ControllerDNS: &proof,
	}
	plan, err := PrepareMutation(snapshot, []AppliedChange{{Target: unit.Target, Next: &applied}}, nil)
	if err != nil {
		return MutationPlan{}, err
	}
	plan.conditions = append(plan.conditions, keyvalue.Condition{
		Key: DesiredPlanKey(snapshot.EnvironmentID), ModRevision: snapshot.Desired.Revision,
	})
	return plan, nil
}
