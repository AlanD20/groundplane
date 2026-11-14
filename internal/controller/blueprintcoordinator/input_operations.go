package blueprintcoordinator

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// DeferredResourceOperation owns the closed mapping used by both admission and
// input-owner validation. A declared capability cannot silently create work for
// an operation that the Controller cannot prepare.
func DeferredResourceOperation(kind ids.Kind, removal bool) (workexecution.Operation, error) {
	switch kind {
	case ids.KindEnvEntry:
		if removal {
			return workexecution.EntryConfiguration, nil
		}
		return workexecution.EntrySettlement, nil
	case ids.KindService:
		if removal {
			return workexecution.ServiceRemoval, nil
		}
		return workexecution.Release, nil
	case ids.KindNetwork:
		return workexecution.Network, nil
	case ids.KindVolume:
		return workexecution.Volume, nil
	case ids.KindAttach:
		if removal {
			return workexecution.Detach, nil
		}
		return workexecution.Attach, nil
	case ids.KindComponent:
		return workexecution.Component, nil
	default:
		return "", errs.New(errs.KindInternal, "Resource has no native input operation")
	}
}

// ValidateInputPreparations is a composition-time capability check, before the
// API can accept any Apply. Every supported native operation is required at composition.
func ValidateInputPreparations(registrations []workdispatch.InputRegistration) error {
	provided := make(map[workexecution.Operation]bool, len(registrations))
	for _, registration := range registrations {
		if registration.Action != workexecution.ActionApply || registration.Owner == nil ||
			provided[registration.Operation] {
			return errs.New(errs.KindInternal, "Blueprint input preparation registration is invalid or duplicated")
		}
		provided[registration.Operation] = true
	}
	for _, kind := range []ids.Kind{ids.KindService, ids.KindEnvEntry, ids.KindNetwork, ids.KindVolume, ids.KindAttach, ids.KindComponent} {
		for _, removal := range []bool{false, true} {
			operation, err := DeferredResourceOperation(kind, removal)
			if err != nil {
				return err
			}
			if operation == workexecution.EntrySettlement {
				continue
			}
			if !provided[operation] {
				return errs.Newf(errs.KindInternal, "Blueprint %s work has no input-preparation owner", operation)
			}
		}
	}
	return nil
}
