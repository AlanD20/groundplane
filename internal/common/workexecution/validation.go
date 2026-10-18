package workexecution

import (
	"encoding/hex"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const MaximumDependencies = 64
const MaximumClaims = 32

func ValidOperation(operation Operation) bool {
	switch operation {
	case Release, ReleaseRestoration, ServiceLifecycle, ServiceRemoval, BackingProvision, Attach, Detach, Script, Network, Volume,
		Component, Environment, Hierarchy, RemovalBackupObject, Backup, BackupPrune, Restore, Image,
		Runner, ControllerSoftware, AgentSoftware, SoftwarePreparation, SoftwareStaging,
		ConnectorCheck, ConnectorRemoval, SecretRemoval, ScriptRemoval, BackingCheck, DNSCheck, EdgeCheck, RunnerCheck,
		SecretRotation, EntrySettlement, EntryConfiguration, ResourceDefinition,
		ReleaseGroup, AgentLifecycle, EtcdConfiguration:
		return true
	default:
		return false
	}
}

func (input Input) Validate() error {
	if ids.Validate(ids.KindPlan, input.ID) != nil || len(input.SHA256) != 64 ||
		strings.ToLower(input.SHA256) != input.SHA256 {
		return invalid("captured execution input is invalid")
	}
	if _, err := hex.DecodeString(input.SHA256); err != nil {
		return invalid("captured execution input digest is invalid")
	}
	return nil
}

func (placement Placement) Validate() error {
	if placement.Executor != Agent && placement.Executor != Controller ||
		placement.HostID != "" && ids.Validate(ids.KindHost, placement.HostID) != nil ||
		placement.Executor == Agent && placement.HostID == "" {
		return invalid("execution placement requires a valid executor and target host")
	}
	return nil
}

func (identity Identity) Validate() error {
	if err := (Placement{Executor: identity.Executor, HostID: identity.HostID}).Validate(); err != nil {
		return err
	}
	if identity.Generation == 0 ||
		identity.Executor == Agent && ids.Validate(ids.KindAgent, identity.AgentID) != nil ||
		identity.Executor == Controller && identity.AgentID != "" {
		return invalid("execution identity requires the current executor generation")
	}
	return nil
}

func (item Item) Validate() error {
	if ids.Validate(ids.KindWorkItem, item.ID) != nil || ids.Validate(ids.KindTask, item.TaskID) != nil ||
		!ValidOperation(item.Operation) || !validResourceID(item.TargetID) {
		return invalid("work item identity or operation is invalid")
	}
	if err := item.Placement.Validate(); err != nil {
		return err
	}
	if !validTime(item.CreatedAt) || !validTime(item.UpdatedAt) || !validTime(item.Deadline) ||
		item.UpdatedAt.Before(item.CreatedAt) || !item.Deadline.After(item.CreatedAt) {
		return invalid("work item timestamps are invalid")
	}
	if len(item.Dependencies) > MaximumDependencies || len(item.Claims) == 0 || len(item.Claims) > MaximumClaims {
		return invalid("work item dependency or claim count exceeds its bounds")
	}
	dependencies := make(map[string]struct{}, len(item.Dependencies))
	for _, dependency := range item.Dependencies {
		if ids.Validate(ids.KindWorkItem, dependency) != nil || dependency == item.ID {
			return invalid("work item dependency is invalid")
		}
		if _, exists := dependencies[dependency]; exists {
			return invalid("work item dependency is duplicated")
		}
		dependencies[dependency] = struct{}{}
	}
	claims := make(map[Claim]struct{}, len(item.Claims))
	for _, claim := range item.Claims {
		if !validResourceID(claim.ScopeID) || claim.HostID != "" && claim.HostID != item.Placement.HostID {
			return invalid("work item claim does not belong to its target host")
		}
		if _, exists := claims[claim]; exists {
			return invalid("work item claim is duplicated")
		}
		claims[claim] = struct{}{}
	}
	if item.Input != nil {
		if err := item.Input.Validate(); err != nil {
			return err
		}
	} else if item.Phase != Waiting && item.Phase != Settled {
		return invalid("work item is not prepared for execution")
	}
	if item.Outcome != "" && !validOutcome(item.Outcome) {
		return invalid("work item outcome is invalid")
	}
	if item.FirstFailure != nil {
		if err := item.FirstFailure.Validate(); err != nil {
			return err
		}
	}
	for _, result := range []*Input{item.Result, item.CleanupResult} {
		if result != nil {
			if err := result.Validate(); err != nil {
				return err
			}
		}
	}
	switch item.Cleanup {
	case CleanupNone, CleanupRequired, CleanupRunning, CleanupStopped, CleanupComplete:
	default:
		return invalid("work item cleanup state is invalid")
	}
	if item.Cleanup != CleanupNone && (item.Outcome == "" || item.Outcome == Succeeded) {
		return invalid("cleanup must retain the original unsuccessful outcome")
	}
	switch item.Phase {
	case Waiting:
		if item.ActiveAttemptID != "" || item.Wait == nil {
			return invalid("waiting work must identify its reason without an active attempt")
		}
		if err := item.Wait.Validate(); err != nil {
			return err
		}
	case Ready, Attention, Settled:
		if item.ActiveAttemptID != "" || item.Wait != nil {
			return invalid("inactive work cannot retain an active assignment or wait")
		}
	case Assigned, Running:
		if ids.Validate(ids.KindAttempt, item.ActiveAttemptID) != nil || item.AttemptNumber == 0 || item.Wait != nil {
			return invalid("active work requires one identified attempt")
		}
	default:
		return invalid("work item phase is invalid")
	}
	if item.Phase == Settled && (item.Outcome == "" || item.Cleanup != CleanupNone && item.Cleanup != CleanupComplete) {
		return invalid("settled work must have a known outcome and no unresolved cleanup")
	}
	return nil
}

func (wait Wait) Validate() error {
	switch wait.Reason {
	case WaitInput, WaitDependency, WaitConflict, WaitReservation, WaitExecutor, WaitCapacity, WaitRetry:
	default:
		return invalid("work wait reason is invalid")
	}
	if wait.WorkItemID != "" && ids.Validate(ids.KindWorkItem, wait.WorkItemID) != nil ||
		wait.TaskID != "" && ids.Validate(ids.KindTask, wait.TaskID) != nil ||
		wait.Until != nil && !validTime(*wait.Until) {
		return invalid("work wait target or timestamp is invalid")
	}
	if wait.Reason == WaitDependency && wait.WorkItemID == "" ||
		wait.Reason == WaitConflict && (wait.WorkItemID == "" || wait.TaskID == "") ||
		wait.Reason == WaitReservation && (wait.TaskID == "" || wait.WorkItemID != "") ||
		wait.Reason == WaitRetry && wait.Until == nil {
		return invalid("work wait reason requires its blocking identity or retry time")
	}
	return nil
}

func validOutcome(outcome Outcome) bool {
	return outcome == Succeeded || outcome == Failed || outcome == Cancelled || outcome == Superseded
}

func validTime(value time.Time) bool {
	return !value.IsZero() && value.Year() >= 1970 && value.Year() <= 9999
}

func validResourceID(value string) bool {
	if workloadimage.LocalIDValid(value) {
		return true
	}
	for _, kind := range []ids.Kind{ids.KindTenant, ids.KindProject, ids.KindEnvironment,
		ids.KindService, ids.KindEnvEntry, ids.KindVolume, ids.KindAttach, ids.KindRoute, ids.KindDNSRecord,
		ids.KindSecret, ids.KindConnector, ids.KindRunner, ids.KindScript, ids.KindBackupSource,
		ids.KindRecoveryPoint, ids.KindAgent, ids.KindHost, ids.KindNetwork,
		ids.KindBackingService, ids.KindReleaseGroup, ids.KindComponent, ids.KindConfig} {
		if ids.Validate(kind, value) == nil {
			return true
		}
	}
	return false
}

func invalid(message string) error {
	return errs.New(errs.KindValidationFailed, message)
}
