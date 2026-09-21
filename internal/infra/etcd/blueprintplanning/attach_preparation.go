package blueprintplanning

import (
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EnvironmentBlueprintAttachCandidateInput is one fully resolved Attach that
// will be published by the same transaction as its owning Blueprint Task.
// Versioned backing records are private compare evidence, not desired state.
type EnvironmentBlueprintAttachCandidateInput struct {
	Record                  attachrecord.Record
	Facts                   *attachrecord.EncryptedFacts
	BackingProject          etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	BackingEnvironment      etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	BackingService          etcdstore.Versioned[servicerecord.ServiceRecord]
	RetainedCredentialOwner *etcdstore.Versioned[attachrecord.Record]
	RetainedGrantTargets    []etcdstore.Versioned[attachrecord.Record]
}

// BlueprintAttachTaskPreparation is immutable publication input. Ciphertext
// remains outside the durable intent so retries reuse the original fact set.
type BlueprintAttachTaskPreparation struct {
	Intent     attachrecord.BlueprintAttachTaskIntent
	candidates []EnvironmentBlueprintAttachCandidateInput
}

type EnvironmentBlueprintBackupPolicySourceInput struct {
	CandidateID string
	Kind        core.BackupSourceKind
	TargetID    string
}

type EnvironmentBlueprintBackupPolicyInput struct {
	EnvironmentID     string
	TaskID            string
	ReadRevision      int64
	Retain            bool
	Enabled           bool
	Frequency         string
	Keep              int64
	Encryption        string
	ConnectorName     string
	Sources           []EnvironmentBlueprintBackupPolicySourceInput
	Projection        projectionrecord.EnvironmentComposeProjection
	AttachPreparation BlueprintAttachTaskPreparation
	CreatedAt         time.Time
}

func PrepareEnvironmentBlueprintAttachTask(
	taskID string,
	environmentID string,
	inputs []EnvironmentBlueprintAttachCandidateInput,
	ownsEnvironmentFence bool,
	createdAt time.Time,
) (BlueprintAttachTaskPreparation, error) {
	if len(inputs) > attachrecord.MaximumEnvironmentBlueprintAttachCandidates {
		return BlueprintAttachTaskPreparation{}, errs.New(
			errs.KindValidationFailed, "Blueprint may introduce at most two Attaches",
		)
	}
	if len(inputs) == 0 {
		return BlueprintAttachTaskPreparation{}, nil
	}
	preparation := BlueprintAttachTaskPreparation{
		Intent: attachrecord.BlueprintAttachTaskIntent{
			TaskID: taskID, EnvironmentID: environmentID, Status: taskjournal.TaskStatusPending,
			OwnsEnvironmentFence: ownsEnvironmentFence, CreatedAt: createdAt.UTC(),
		},
		candidates: cloneEnvironmentBlueprintAttachCandidateInputs(inputs),
	}
	sort.Slice(preparation.candidates, func(left, right int) bool {
		return preparation.candidates[left].Record.ID < preparation.candidates[right].Record.ID
	})
	preparation.Intent.Candidates = make([]attachrecord.Record, 0, len(preparation.candidates))
	for _, input := range preparation.candidates {
		preparation.Intent.Candidates = append(preparation.Intent.Candidates, attachrecord.CloneAttachRecord(input.Record))
	}
	if err := validateBlueprintAttachTaskPreparation(preparation); err != nil {
		ClearBlueprintAttachTaskPreparation(&preparation)
		return BlueprintAttachTaskPreparation{}, err
	}
	return preparation, nil
}

func validateBlueprintAttachTaskPreparation(preparation BlueprintAttachTaskPreparation) error {
	if err := attachrecord.ValidateBlueprintAttachTaskIntent(preparation.Intent); err != nil {
		return err
	}
	if len(preparation.candidates) != len(preparation.Intent.Candidates) {
		return errs.New(errs.KindValidationFailed, "Blueprint Attach preparation is incomplete")
	}
	byID := make(map[string]EnvironmentBlueprintAttachCandidateInput, len(preparation.candidates))
	byName := make(map[string]string, len(preparation.candidates))
	for index, input := range preparation.candidates {
		if !attachrecord.SameBlueprintAttachCandidateRecord(input.Record, preparation.Intent.Candidates[index]) {
			return errs.New(errs.KindValidationFailed, "Blueprint Attach preparation changed its durable intent")
		}
		if input.BackingProject.Revision <= 0 || input.BackingEnvironment.Revision <= 0 ||
			input.BackingService.Revision <= 0 ||
			input.BackingProject.Record.ID != input.Record.BackingProjectID ||
			input.BackingEnvironment.Record.ID != input.Record.BackingEnvironmentID ||
			input.BackingEnvironment.Record.ProjectID != input.BackingProject.Record.ID ||
			input.BackingService.Record.EnvironmentID != input.BackingEnvironment.Record.ID ||
			input.BackingService.Record.Desired.ID != input.Record.BackingServiceID ||
			input.BackingService.Record.BackingNetworkID != input.Record.BackingNetworkID {
			return errs.New(errs.KindValidationFailed, "Blueprint Attach backing scope is inconsistent")
		}
		if input.Record.OwnsCredential() {
			if (input.Facts == nil) != (!input.Record.HookBundle && len(input.Record.FactSets) == 0) {
				return errs.New(errs.KindValidationFailed, "Blueprint Attach fact envelope is inconsistent")
			}
			if input.Facts != nil && input.Facts.AttachID != input.Record.ID {
				return errs.New(errs.KindValidationFailed, "Blueprint Attach facts changed owner")
			}
		} else if input.Facts != nil {
			return errs.New(errs.KindValidationFailed, "Existing-credential Blueprint Attach cannot own facts")
		}
		byID[input.Record.ID] = input
		byName[input.Record.Name] = input.Record.ID
	}
	if len(byID) != len(preparation.candidates) || len(byName) != len(preparation.candidates) {
		return errs.New(errs.KindValidationFailed, "Blueprint Attach identities must be unique")
	}
	retainedByID := make(map[string]etcdstore.Versioned[attachrecord.Record])
	retainedReadRevision := int64(0)
	validateRetained := func(retained etcdstore.Versioned[attachrecord.Record], candidate attachrecord.Record) error {
		if retained.Revision <= 0 || retained.ReadRevision <= 0 || retained.Revision > retained.ReadRevision ||
			attachrecord.ValidateAttachRecord(retained.Record) != nil || retained.Record.Status != core.AttachReady ||
			retained.Record.Operation != attachrecord.AttachOperationProvision || !retained.Record.OwnsCredential() ||
			retained.Record.EnvironmentID != candidate.EnvironmentID ||
			retained.Record.BackingProjectID != candidate.BackingProjectID ||
			retained.Record.BackingEnvironmentID != candidate.BackingEnvironmentID ||
			retained.Record.BackingServiceID != candidate.BackingServiceID ||
			retained.Record.BackingNetworkID != candidate.BackingNetworkID {
			return errs.New(errs.KindValidationFailed, "Blueprint Attach retained reference is inconsistent")
		}
		if retainedReadRevision == 0 {
			retainedReadRevision = retained.ReadRevision
		} else if retained.ReadRevision != retainedReadRevision {
			return errs.New(errs.KindValidationFailed, "Blueprint Attach retained references changed revision")
		}
		if existing, duplicate := retainedByID[retained.Record.ID]; duplicate {
			if existing.Revision != retained.Revision || existing.ReadRevision != retained.ReadRevision ||
				!attachrecord.SameBlueprintAttachCandidateRecord(existing.Record, retained.Record) {
				return errs.New(errs.KindValidationFailed, "Blueprint Attach retained reference is ambiguous")
			}
		} else {
			retainedByID[retained.Record.ID] = retained
		}
		return nil
	}
	for _, input := range preparation.candidates {
		record := input.Record
		owner, candidateOwner := byID[record.CredentialAttachID]
		if candidateOwner {
			if input.RetainedCredentialOwner != nil || !owner.Record.OwnsCredential() ||
				owner.Record.EnvironmentID != record.EnvironmentID ||
				owner.Record.BackingServiceID != record.BackingServiceID ||
				owner.Record.BackingNetworkID != record.BackingNetworkID {
				return errs.New(
					errs.KindValidationFailed,
					"Blueprint Attach candidate credential owner is inconsistent",
				)
			}
		} else {
			if input.RetainedCredentialOwner == nil ||
				input.RetainedCredentialOwner.Record.ID != record.CredentialAttachID {
				return errs.New(errs.KindValidationFailed, "Blueprint Attach retained credential owner is missing")
			}
			if err := validateRetained(*input.RetainedCredentialOwner, record); err != nil {
				return err
			}
		}
		retainedGrants := make(map[string]etcdstore.Versioned[attachrecord.Record], len(input.RetainedGrantTargets))
		for _, retained := range input.RetainedGrantTargets {
			if _, duplicate := retainedGrants[retained.Record.ID]; duplicate {
				return errs.New(errs.KindValidationFailed, "Blueprint Attach retained grant target is duplicated")
			}
			retainedGrants[retained.Record.ID] = retained
		}
		usedRetainedGrants := 0
		for _, grantID := range record.GrantAttachIDs {
			grant, candidateGrant := byID[grantID]
			retained, retainedGrant := retainedGrants[grantID]
			if candidateGrant == retainedGrant {
				return errs.New(errs.KindValidationFailed, "Blueprint Attach grant must resolve exactly once")
			}
			if candidateGrant {
				if !grant.Record.OwnsCredential() || grant.Record.EnvironmentID != record.EnvironmentID ||
					grant.Record.BackingServiceID != record.BackingServiceID ||
					grant.Record.BackingNetworkID != record.BackingNetworkID {
					return errs.New(
						errs.KindValidationFailed,
						"Blueprint Attach candidate grant target is inconsistent",
					)
				}
				continue
			}
			if err := validateRetained(retained, record); err != nil {
				return err
			}
			usedRetainedGrants++
		}
		if usedRetainedGrants != len(retainedGrants) {
			return errs.New(errs.KindValidationFailed, "Blueprint Attach retained grant evidence is unused")
		}
	}
	return nil
}

func BlueprintAttachTaskPreparationIsZero(preparation BlueprintAttachTaskPreparation) bool {
	return preparation.Intent.TaskID == "" && preparation.Intent.EnvironmentID == "" &&
		preparation.Intent.Status == "" && !preparation.Intent.OwnsEnvironmentFence && len(preparation.Intent.Candidates) == 0 &&
		preparation.Intent.CreatedAt.IsZero() && preparation.Intent.TerminalAt == nil && len(preparation.candidates) == 0
}

func cloneEnvironmentBlueprintAttachCandidateInputs(
	inputs []EnvironmentBlueprintAttachCandidateInput,
) []EnvironmentBlueprintAttachCandidateInput {
	cloned := make([]EnvironmentBlueprintAttachCandidateInput, len(inputs))
	for index, input := range inputs {
		cloned[index] = input
		cloned[index].Record = attachrecord.CloneAttachRecord(input.Record)
		if input.Facts != nil {
			facts := *input.Facts
			facts.Ciphertext = append([]byte(nil), input.Facts.Ciphertext...)
			cloned[index].Facts = &facts
		}
		if input.RetainedCredentialOwner != nil {
			owner := *input.RetainedCredentialOwner
			owner.Record = attachrecord.CloneAttachRecord(owner.Record)
			cloned[index].RetainedCredentialOwner = &owner
		}
		cloned[index].RetainedGrantTargets = append(
			[]etcdstore.Versioned[attachrecord.Record](nil), input.RetainedGrantTargets...,
		)
		for retainedIndex := range cloned[index].RetainedGrantTargets {
			cloned[index].RetainedGrantTargets[retainedIndex].Record = attachrecord.CloneAttachRecord(
				cloned[index].RetainedGrantTargets[retainedIndex].Record,
			)
		}
	}
	return cloned
}

// ClearBlueprintAttachTaskPreparation releases every encrypted fact copy held
// by application-side publication input after the atomic attempt returns.
func ClearBlueprintAttachTaskPreparation(preparation *BlueprintAttachTaskPreparation) {
	if preparation == nil {
		return
	}
	for index := range preparation.candidates {
		if preparation.candidates[index].Facts != nil {
			clear(preparation.candidates[index].Facts.Ciphertext)
			preparation.candidates[index].Facts.Ciphertext = nil
		}
	}
}

func ValidateBlueprintAttachTaskOwner(task TaskIdentity, intent attachrecord.BlueprintAttachTaskIntent) error {
	if task.ID != intent.TaskID || task.Target != intent.EnvironmentID {
		return errs.New(errs.KindStateConflict, "Blueprint Attach intent has the wrong Task owner")
	}
	return nil
}
