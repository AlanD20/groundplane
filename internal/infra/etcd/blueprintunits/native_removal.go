package blueprintunits

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// NativeRemoval identifies an independently verified direct removal.
// The caller must join its receipt to the exact work settlement and protected metadata
// retirement; it is not a fabricated hidden child or a Blueprint parent.
type NativeRemoval struct {
	Target      ResourceKey
	OperationID string
	Execution   AppliedExecution
	DNS         *DNSReceipt
}

func PrepareNativeRemovalReceipt(snapshot Snapshot, source NativeRemoval) (MutationPlan, error) {
	target := source.Target
	if !nativeRemovalTarget(target) || source.Execution.Validate() != nil {
		return MutationPlan{}, invalidRecord()
	}
	for _, execution := range snapshot.Executions {
		if slices.Contains(execution.Record.Unit.Writes, target) ||
			slices.Contains(execution.Record.Unit.Reads, target) {
			return MutationPlan{}, errs.New(errs.KindStateConflict, "resource removal has unsettled Blueprint claims")
		}
	}
	absent := AppliedRecord{
		EnvironmentID: snapshot.EnvironmentID, Target: target, State: Absent,
		NativeOperationID: source.OperationID, Execution: &source.Execution,
		ControllerDNS: source.DNS,
	}
	if err := validateApplied(absent); err != nil {
		return MutationPlan{}, err
	}
	return prepareMutation(snapshot, []AppliedChange{{Target: target, Next: &absent}}, nil, source.OperationID)
}

func nativeRemovalTarget(target ResourceKey) bool {
	return (target.Kind == ids.KindService || target.Kind == ids.KindAttach || target.Kind == ids.KindNetwork || target.Kind == ids.KindDNSRecord) &&
		validKey(target)
}
