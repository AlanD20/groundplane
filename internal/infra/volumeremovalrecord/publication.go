package volumeremovalrecord

import "github.com/AlanD20/groundplane/pkg/errs"

// InitialPublication is the closed initial record set for the existing desired
// publisher. It has no storage authority; publishing its records separately
// from the desired head, Task, and removal ownership is not permitted.
type InitialPublication struct {
	runtime Runtime
}

// PrepareInitialPublication accepts only the original, already staged removal.
// The publisher must additionally bind its Task, marker, and sealed evidence.
func PrepareInitialPublication(runtime Runtime) (InitialPublication, error) {
	if err := ValidateRuntime(runtime); err != nil {
		return InitialPublication{}, err
	}
	if runtime.Checkpoint != DesiredPublished || runtime.AttemptOrdinal != 1 ||
		runtime.DesiredRevisionID != runtime.OriginTaskID || !runtime.UpdatedAt.Equal(runtime.CreatedAt) ||
		runtime.RootLocator.Method != "DELETE" || runtime.RootLocator.Route != "/volumes/{id}" {
		return InitialPublication{}, errs.New(
			errs.KindValidationFailed,
			"initial Volume removal publication is invalid",
		)
	}
	return InitialPublication{runtime: runtime}, nil
}

// Records returns the three initial records, never a caller-supplied checkpoint,
// cursor, successor attempt, or arbitrary store mutation. A zero preparation is
// invalid rather than an instruction to omit removal publication.
func (publication InitialPublication) Records() (Runtime, Attempt, Progress, error) {
	if publication.runtime.OperationID == "" {
		return Runtime{}, Attempt{}, Progress{}, errs.New(
			errs.KindValidationFailed, "initial Volume removal publication is not prepared",
		)
	}
	runtime := publication.runtime
	return runtime, Attempt{
			OperationID: runtime.OperationID, OriginTaskID: runtime.OriginTaskID,
			TaskID: runtime.OriginTaskID, Ordinal: 1, CreatedAt: runtime.CreatedAt,
		}, Progress{
			OperationID: runtime.OperationID, NextRequestOrdinal: 1, UpdatedAt: runtime.CreatedAt,
		}, nil
}
