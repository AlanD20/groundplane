package blueprintunits

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// MetadataDNSReceipt proves withdrawal after a direct declaration edit. The
// authored revision identifies metadata publication, not an execution or Task.
// NeverPublished requires positive bootstrap provenance and an absent native
// resolver intent compared atomically by the caller; missing runtime is not proof.
type MetadataDNSReceipt struct {
	AuthoredRevisionID string `json:"authored_revision_id"`
	AuthoredRevision   int64  `json:"authored_revision"`
	ComponentID        string `json:"component_id"`
	HostID             string `json:"host_id,omitempty"`
	AppliedRevision    int64  `json:"applied_revision"`
	CorefileSHA256     string `json:"corefile_sha256,omitempty"`
	ResolverEnabled    bool   `json:"resolver_enabled"`
	NeverPublished     bool   `json:"never_published"`
}

func validMetadataDNSReceipt(record AppliedRecord) bool {
	proof := record.MetadataDNS
	if proof == nil || record.Target.Kind != ids.KindDNSRecord || record.State != Absent ||
		ids.Validate(ids.KindTask, proof.AuthoredRevisionID) != nil || proof.AuthoredRevision <= 0 ||
		ids.Validate(ids.KindComponent, proof.ComponentID) != nil ||
		record.Fingerprint != "" || len(record.AffectedWrites) != 0 || record.ParentTaskID != "" || record.Execution != nil ||
		record.NativeOperationID != "" || record.ControllerOperationID != "" || record.ControllerAttachInputRevision != 0 ||
		record.ControllerAttachBindingSHA256 != "" || len(record.ControllerAttachReferences) != 0 ||
		record.ControllerValueGenerationID != "" || record.ControllerRuntimeRevision != 0 || record.ControllerAuthoredRevision != 0 ||
		record.ControllerComponentRecordRevision != 0 || record.ControllerComponentRuntimeRevision != 0 ||
		record.ControllerProxySelection || record.ControllerScriptSetRevision != 0 || record.ControllerDNS != nil ||
		record.ControllerServiceDeclarationAbsence != nil {
		return false
	}
	if proof.NeverPublished {
		return proof.AppliedRevision == 0 && proof.CorefileSHA256 == "" && !proof.ResolverEnabled &&
			(proof.HostID == "" || ids.Validate(ids.KindHost, proof.HostID) == nil)
	}
	return ids.Validate(ids.KindHost, proof.HostID) == nil && proof.AppliedRevision > 0 &&
		(!proof.ResolverEnabled || recordcodec.ValidSHA256(proof.CorefileSHA256)) &&
		(proof.CorefileSHA256 == "" || recordcodec.ValidSHA256(proof.CorefileSHA256))
}

// PrepareMetadataDNSAbsence is separate from Apply unit and host-execution
// settlement. The caller joins current metadata ownership, resolver proof and
// obsolete-name retirement. It cannot take a visible Apply's unit or a live claim.
func PrepareMetadataDNSAbsence(snapshot Snapshot, target ResourceKey, proof MetadataDNSReceipt) (MutationPlan, error) {
	if target.Kind != ids.KindDNSRecord || proof.AuthoredRevisionID != snapshot.HeadTaskID ||
		proof.AuthoredRevision != snapshot.HeadRevision ||
		snapshot.Desired != nil && snapshot.Desired.Record.ParentTaskID == snapshot.HeadTaskID {
		return MutationPlan{}, invalidRecord()
	}
	for _, execution := range snapshot.Executions {
		if slices.Contains(execution.Record.Unit.Reads, target) || slices.Contains(execution.Record.Unit.Writes, target) {
			return MutationPlan{}, errs.New(errs.KindStateConflict, "DNS declaration still has active Blueprint work")
		}
	}
	next := AppliedRecord{EnvironmentID: snapshot.EnvironmentID, Target: target, State: Absent, MetadataDNS: &proof}
	if !validMetadataDNSReceipt(next) {
		return MutationPlan{}, invalidRecord()
	}
	return PrepareMutation(snapshot, []AppliedChange{{Target: target, Next: &next}}, nil)
}
