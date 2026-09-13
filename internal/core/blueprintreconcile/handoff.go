package blueprintreconcile

import (
	"slices"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type ExecutorStopState string

const (
	ExecutorUnconfirmed ExecutorStopState = "unconfirmed"
	ExecutorStopped     ExecutorStopState = "stopped"
)

type WriteOutcome string

const (
	WriteUnknown   WriteOutcome = "unknown"
	WriteUntouched WriteOutcome = "untouched"
	WriteChanged   WriteOutcome = "changed"
)

type WriteEffect struct {
	Resource ResourceKey
	Outcome  WriteOutcome
}

// HandoffReport contains Controller-verified evidence for one execution epoch.
// Stopped excludes delayed executor/daemon mutations, not only a cancelled context.
// Untouched requires proof of no effect; Changed requires accounted actual effects,
// not successful application. Missing runtime proof is represented as Unknown.
// Transport authentication and verification of the physical evidence belong to
// the caller; these flags cannot themselves prove a host process stopped.
type HandoffReport struct {
	PlanID   string
	TaskID   string
	Epoch    int64
	Executor ExecutorStopState
	Writes   []WriteEffect
}

type HandoffDecision struct {
	Settled       bool
	ChangedWrites []ResourceKey
}

// ResolveHandoff validates complete, exact evidence for a superseded Blueprint
// unit. It neither restores the predecessor nor promotes any input as applied.
// The caller must establish eligibility for the approved shared-configuration
// handoff; this function grants no exception to other recovery procedures.
// The persistence owner must atomically record the receipt, mark ChangedWrites
// diverged without discarding last-success fingerprints, and release this exact
// epoch's claims. It must fence the latest snapshot again before admitting work.
// Until Settled, all claims stay held. Duplicate terminal delivery belongs to the
// durable receipt path, not reactivation of an already released execution.
func ResolveHandoff(execution Execution, report HandoffReport) (HandoffDecision, error) {
	execution, err := normalizeExecution(execution)
	if err != nil {
		return HandoffDecision{}, err
	}
	if execution.State != Draining || report.PlanID != execution.PlanID ||
		report.TaskID != execution.TaskID || report.Epoch != execution.Epoch {
		return HandoffDecision{}, errs.New(errs.KindStateConflict, "superseded execution authority changed")
	}
	if report.Executor != ExecutorUnconfirmed && report.Executor != ExecutorStopped ||
		len(report.Writes) != len(execution.Unit.Writes) {
		return HandoffDecision{}, invalidSnapshot("blueprint handoff evidence is incomplete")
	}
	writes := slices.Clone(report.Writes)
	slices.SortFunc(writes, func(left, right WriteEffect) int { return compareKey(left.Resource, right.Resource) })
	settled := report.Executor == ExecutorStopped
	changed := make([]ResourceKey, 0, len(writes))
	for index, write := range writes {
		if write.Resource != execution.Unit.Writes[index] {
			return HandoffDecision{}, invalidSnapshot("blueprint handoff write scope differs from its execution")
		}
		switch write.Outcome {
		case WriteUnknown:
			settled = false
		case WriteUntouched:
		case WriteChanged:
			changed = append(changed, write.Resource)
		default:
			return HandoffDecision{}, invalidSnapshot("blueprint handoff write outcome is invalid")
		}
	}
	if !settled {
		return HandoffDecision{}, nil
	}
	return HandoffDecision{Settled: true, ChangedWrites: changed}, nil
}
