package hierarchydeletion

import (
	"crypto/sha256"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type ActionKind string

const (
	ActionAttachDeprovision         ActionKind = "attach.deprovision"
	ActionAttachFinalize            ActionKind = "attach.finalize"
	ActionEnvironmentAgentCleanup   ActionKind = "environment.agent-cleanup"
	ActionServiceRemove             ActionKind = "service.remove"
	ActionEntryRemove               ActionKind = "entry.remove"
	ActionRouteRemove               ActionKind = "route.remove"
	ActionComponentRemove           ActionKind = "component.remove"
	ActionScriptRemove              ActionKind = "script.remove"
	ActionReleaseGroupRemove        ActionKind = "release-group.remove"
	ActionReleaseFinalize           ActionKind = "release.finalize"
	ActionBackupPolicyFinalize      ActionKind = "backup-policy.finalize"
	ActionKeyMaterialRemove         ActionKind = "key-material.remove"
	ActionMaterializationRemove     ActionKind = "materialization.remove"
	ActionVolumeAgentCleanup        ActionKind = "volume.agent-cleanup"
	ActionVolumeFinalize            ActionKind = "volume.finalize"
	ActionZoneRemove                ActionKind = "zone.remove"
	ActionNetworkRemove             ActionKind = "network.remove"
	ActionReservationRelease        ActionKind = "reservation.release"
	ActionRecoveryPointRemove       ActionKind = "recovery-point.remove"
	ActionOrphanObjectRemove        ActionKind = "orphan-object.remove"
	ActionBackupSourceFinalize      ActionKind = "backup-source.finalize"
	ActionBackupScheduleFinalize    ActionKind = "backup-schedule.finalize"
	ActionBackupDueFinalize         ActionKind = "backup-due.finalize"
	ActionBackupRetentionFinalize   ActionKind = "backup-retention.finalize"
	ActionBackupRunDetach           ActionKind = "backup-run.detach"
	ActionBackupRestoreDetach       ActionKind = "backup-restore.detach"
	ActionBackupKeyRotationDetach   ActionKind = "backup-key-rotation.detach"
	ActionConnectorFinalize         ActionKind = "connector.finalize"
	ActionEnvironmentFinalize       ActionKind = "environment.finalize"
	ActionRunnerLocalRemove         ActionKind = "runner.local-remove"
	ActionProjectSecretRemove       ActionKind = "project-secret.remove"
	ActionBackingRuntimeReconstruct ActionKind = "backing.runtime-reconstruct"
	ActionBackingServiceFinalize    ActionKind = "backing-service.finalize"
	ActionProjectFinalize           ActionKind = "project.finalize"
	ActionTenantFinalize            ActionKind = "tenant.finalize"
)

func (k ActionKind) Valid() bool {
	switch k {
	case ActionAttachDeprovision,
		ActionAttachFinalize,
		ActionEnvironmentAgentCleanup,
		ActionServiceRemove,
		ActionEntryRemove,
		ActionRouteRemove,
		ActionComponentRemove,
		ActionScriptRemove,
		ActionReleaseGroupRemove,
		ActionReleaseFinalize,
		ActionBackupPolicyFinalize,
		ActionKeyMaterialRemove,
		ActionMaterializationRemove,
		ActionVolumeAgentCleanup,
		ActionVolumeFinalize,
		ActionZoneRemove,
		ActionNetworkRemove,
		ActionReservationRelease,
		ActionRecoveryPointRemove,
		ActionOrphanObjectRemove,
		ActionBackupSourceFinalize,
		ActionBackupScheduleFinalize,
		ActionBackupDueFinalize,
		ActionBackupRetentionFinalize,
		ActionBackupRunDetach,
		ActionBackupRestoreDetach,
		ActionBackupKeyRotationDetach,
		ActionConnectorFinalize,
		ActionEnvironmentFinalize,
		ActionRunnerLocalRemove,
		ActionProjectSecretRemove,
		ActionBackingRuntimeReconstruct,
		ActionBackingServiceFinalize,
		ActionProjectFinalize,
		ActionTenantFinalize:
		return true
	default:
		return false
	}
}

type ActionTargetKind string

const (
	ActionTargetTenant          ActionTargetKind = "tenant"
	ActionTargetProject         ActionTargetKind = "project"
	ActionTargetEnvironment     ActionTargetKind = "environment"
	ActionTargetService         ActionTargetKind = "service"
	ActionTargetAttach          ActionTargetKind = "attach"
	ActionTargetEntry           ActionTargetKind = "entry"
	ActionTargetRoute           ActionTargetKind = "route"
	ActionTargetComponent       ActionTargetKind = "component"
	ActionTargetScript          ActionTargetKind = "script"
	ActionTargetReleaseGroup    ActionTargetKind = "release-group"
	ActionTargetRelease         ActionTargetKind = "release"
	ActionTargetBackupPolicy    ActionTargetKind = "backup-policy"
	ActionTargetKeyMaterial     ActionTargetKind = "key-material"
	ActionTargetMaterialization ActionTargetKind = "materialization"
	ActionTargetVolume          ActionTargetKind = "volume"
	ActionTargetZone            ActionTargetKind = "zone"
	ActionTargetNetwork         ActionTargetKind = "network"
	ActionTargetReservation     ActionTargetKind = "reservation"
	ActionTargetRecoveryPoint   ActionTargetKind = "recovery-point"
	ActionTargetOrphanObject    ActionTargetKind = "orphan-object"
	ActionTargetBackupSource    ActionTargetKind = "backup-source"
	ActionTargetBackupSchedule  ActionTargetKind = "backup-schedule"
	ActionTargetBackupDue       ActionTargetKind = "backup-due"
	ActionTargetBackupRetention ActionTargetKind = "backup-retention"
	ActionTargetBackupRun       ActionTargetKind = "backup-run"
	ActionTargetBackupRestore   ActionTargetKind = "backup-restore"
	ActionTargetBackupRotation  ActionTargetKind = "backup-key-rotation"
	ActionTargetConnector       ActionTargetKind = "connector"
	ActionTargetRunner          ActionTargetKind = "runner"
	ActionTargetSecret          ActionTargetKind = "secret"
	ActionTargetBackingService  ActionTargetKind = "backing-service"
)

func (k ActionTargetKind) Valid() bool {
	switch k {
	case ActionTargetTenant,
		ActionTargetProject,
		ActionTargetEnvironment,
		ActionTargetService,
		ActionTargetAttach,
		ActionTargetEntry,
		ActionTargetRoute,
		ActionTargetComponent,
		ActionTargetScript,
		ActionTargetReleaseGroup,
		ActionTargetRelease,
		ActionTargetBackupPolicy,
		ActionTargetKeyMaterial,
		ActionTargetMaterialization,
		ActionTargetVolume,
		ActionTargetZone,
		ActionTargetNetwork,
		ActionTargetReservation,
		ActionTargetRecoveryPoint,
		ActionTargetOrphanObject,
		ActionTargetBackupSource,
		ActionTargetBackupSchedule,
		ActionTargetBackupDue,
		ActionTargetBackupRetention,
		ActionTargetBackupRun,
		ActionTargetBackupRestore,
		ActionTargetBackupRotation,
		ActionTargetConnector,
		ActionTargetRunner,
		ActionTargetSecret,
		ActionTargetBackingService:
		return true
	default:
		return false
	}
}

type ProcedureKind string

const (
	ProcedureAgentWork           ProcedureKind = "agent-work"
	ProcedureControllerFinalizer ProcedureKind = "controller-finalizer"
)

type AgentWorkInput struct {
	TypedProcedure string
	InputDigest    string
	Timeout        time.Duration
}
type ControllerFinalizerInput struct {
	Finalizer          string
	TargetKind         ActionTargetKind
	TargetID           string
	FixedInputRevision int64
	FixedInputDigest   string
	BatchOrdinal       int
	BatchCount         int
}
type ProcedureInput struct {
	Kind                ProcedureKind
	AgentWork           *AgentWorkInput
	ControllerFinalizer *ControllerFinalizerInput
}

type AgentWorkProcedure struct {
	TypedProcedure string
	InputDigest    string
	Timeout        time.Duration
}
type ControllerFinalizerProcedure struct {
	Finalizer                   string
	FixedInputRevision          int64
	CompareTemplateDigest       string
	MutationTemplateDigest      string
	PostconditionTemplateDigest string
}
type Procedure struct {
	Kind                ProcedureKind
	AgentWork           *AgentWorkProcedure
	ControllerFinalizer *ControllerFinalizerProcedure
}

var agentProcedures = map[ActionKind]string{
	ActionAttachDeprovision:         "attach.deprovision",
	ActionEnvironmentAgentCleanup:   "environment.cleanup",
	ActionMaterializationRemove:     "materialization.remove",
	ActionVolumeAgentCleanup:        "volume.cleanup",
	ActionNetworkRemove:             "network.remove",
	ActionBackingRuntimeReconstruct: "backing.runtime-reconstruct",
}

var controllerFinalizers = map[ActionKind]string{
	ActionAttachFinalize:          "attach.finalize",
	ActionServiceRemove:           "service.remove",
	ActionEntryRemove:             "entry.remove",
	ActionRouteRemove:             "route.remove",
	ActionComponentRemove:         "component.remove",
	ActionScriptRemove:            "script.remove",
	ActionReleaseGroupRemove:      "release-group.remove",
	ActionReleaseFinalize:         "release.finalize",
	ActionBackupPolicyFinalize:    "backup-policy.finalize",
	ActionKeyMaterialRemove:       "key-material.remove",
	ActionRecoveryPointRemove:     "recovery-point.remove",
	ActionOrphanObjectRemove:      "orphan-object.remove",
	ActionBackupSourceFinalize:    "backup-source.finalize",
	ActionBackupScheduleFinalize:  "backup-schedule.finalize",
	ActionBackupDueFinalize:       "backup-due.finalize",
	ActionBackupRetentionFinalize: "backup-retention.finalize",
	ActionBackupRunDetach:         "backup-run.detach",
	ActionBackupRestoreDetach:     "backup-restore.detach",
	ActionBackupKeyRotationDetach: "backup-key-rotation.detach",
	ActionVolumeFinalize:          "volume.finalize",
	ActionZoneRemove:              "zone.remove",
	ActionReservationRelease:      "reservation.release",
	ActionConnectorFinalize:       "connector.finalize",
	ActionEnvironmentFinalize:     "environment.finalize",
	ActionRunnerLocalRemove:       "runner.remove",
	ActionProjectSecretRemove:     "secret.remove",
	ActionBackingServiceFinalize:  "backing.finalize",
	ActionProjectFinalize:         "project.finalize",
	ActionTenantFinalize:          "tenant.finalize",
}

func (p ProcedureInput) Validate(
	action ActionKind,
	targetKind ActionTargetKind,
	targetID string,
	targetRevision int64,
) error {
	agentName, isAgent := agentProcedures[action]
	finalizer, isFinalizer := controllerFinalizers[action]
	switch p.Kind {
	case ProcedureAgentWork:
		if !isAgent || p.AgentWork == nil || p.ControllerFinalizer != nil {
			return errs.Newf(errs.KindInternal, "action %q requires exactly one agent-work input", action)
		}
		v := p.AgentWork
		if v.TypedProcedure != agentName || !canonicalDigest(v.InputDigest) || v.Timeout <= 0 {
			return errs.Newf(errs.KindInternal, "agent-work input for %q is incomplete", action)
		}
	case ProcedureControllerFinalizer:
		if !isFinalizer || p.ControllerFinalizer == nil || p.AgentWork != nil {
			return errs.Newf(errs.KindInternal, "action %q requires exactly one controller-finalizer input", action)
		}
		v := p.ControllerFinalizer
		if v.Finalizer != finalizer || v.TargetKind != targetKind || v.TargetID != targetID ||
			v.FixedInputRevision != targetRevision ||
			!canonicalDigest(v.FixedInputDigest) ||
			v.BatchOrdinal < 0 ||
			v.BatchCount <= 0 ||
			v.BatchOrdinal >= v.BatchCount {
			return errs.Newf(errs.KindInternal, "controller-finalizer input for %q is incomplete", action)
		}
	default:
		return errs.Newf(errs.KindInternal, "action %q procedure input kind %q is invalid", action, p.Kind)
	}
	return nil
}

func (p Procedure) Validate(action ActionKind) error {
	agentName, isAgent := agentProcedures[action]
	finalizer, isFinalizer := controllerFinalizers[action]
	switch p.Kind {
	case ProcedureAgentWork:
		if !isAgent || p.AgentWork == nil || p.ControllerFinalizer != nil {
			return errs.Newf(errs.KindInternal, "action %q requires exactly one agent-work procedure", action)
		}
		v := p.AgentWork
		if v.TypedProcedure != agentName ||
			!canonicalDigest(v.InputDigest) ||
			v.Timeout <= 0 {
			return errs.Newf(errs.KindInternal, "agent-work evidence for %q is incomplete", action)
		}
	case ProcedureControllerFinalizer:
		if !isFinalizer || p.ControllerFinalizer == nil || p.AgentWork != nil {
			return errs.Newf(errs.KindInternal, "action %q requires exactly one controller-finalizer procedure", action)
		}
		v := p.ControllerFinalizer
		if v.Finalizer != finalizer || v.FixedInputRevision <= 0 || !canonicalDigest(v.CompareTemplateDigest) ||
			!canonicalDigest(v.MutationTemplateDigest) ||
			!canonicalDigest(v.PostconditionTemplateDigest) {
			return errs.Newf(errs.KindInternal, "controller-finalizer evidence for %q is incomplete", action)
		}
	default:
		return errs.Newf(errs.KindInternal, "action %q procedure kind %q is invalid", action, p.Kind)
	}
	return nil
}

func validateBoundAction(planned PlannedAction, bound Action) error {
	if bound.ID != planned.ID || bound.NodeID != planned.NodeID || bound.Ordinal != planned.Ordinal ||
		bound.OperationID != planned.OperationID ||
		bound.Kind != planned.Kind ||
		bound.TargetKind != planned.TargetKind ||
		bound.TargetID != planned.TargetID ||
		bound.TargetRevision != planned.TargetRevision ||
		bound.State != ActionPending ||
		bound.Attempt != 0 ||
		len(bound.PrerequisiteOrdinals) != len(planned.PrerequisiteOrdinals) {
		return errs.Newf(errs.KindInternal, "bound hierarchy deletion action %s changed its plan identity", planned.ID)
	}
	for index := range planned.PrerequisiteOrdinals {
		if bound.PrerequisiteOrdinals[index] != planned.PrerequisiteOrdinals[index] {
			return errs.Newf(
				errs.KindInternal,
				"bound hierarchy deletion action %s changed its prerequisites",
				planned.ID,
			)
		}
	}
	if err := planned.ProcedureInput.Validate(planned.Kind, planned.TargetKind, planned.TargetID, planned.TargetRevision); err != nil {
		return err
	}
	if err := bound.Procedure.Validate(bound.Kind); err != nil {
		return err
	}
	if planned.ProcedureInput.Kind != bound.Procedure.Kind {
		return errs.Newf(errs.KindInternal, "bound hierarchy deletion action %s changed executor kind", planned.ID)
	}
	if planned.ProcedureInput.Kind == ProcedureAgentWork {
		input, procedure := planned.ProcedureInput.AgentWork, bound.Procedure.AgentWork
		if input.TypedProcedure != procedure.TypedProcedure ||
			input.InputDigest != procedure.InputDigest ||
			input.Timeout != procedure.Timeout {
			return errs.Newf(errs.KindInternal, "bound hierarchy deletion action %s changed agent input", planned.ID)
		}
		return nil
	}
	input, procedure := planned.ProcedureInput.ControllerFinalizer, bound.Procedure.ControllerFinalizer
	if input.Finalizer != procedure.Finalizer || input.FixedInputRevision != procedure.FixedInputRevision {
		return errs.Newf(errs.KindInternal, "bound hierarchy deletion action %s changed controller input", planned.ID)
	}
	return nil
}

func canonicalDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}
