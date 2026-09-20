package etcd

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"slices"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	blueprintAttachTaskIntentPrefix             = "/v1/records/blueprint-attach-task-intents/"
	MaximumEnvironmentBlueprintAttachCandidates = 2
)

// EnvironmentBlueprintAttachCandidateInput is one fully resolved Attach that
// will be published by the same transaction as its owning Blueprint Task.
// Versioned backing records are private compare evidence, not desired state.
type EnvironmentBlueprintAttachCandidateInput struct {
	Record                  attachrecord.Record
	Facts                   *attachrecord.EncryptedFacts
	BackingProject          etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	BackingEnvironment      etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	BackingService          etcdstore.Versioned[ServiceRecord]
	RetainedCredentialOwner *etcdstore.Versioned[attachrecord.Record]
	RetainedGrantTargets    []etcdstore.Versioned[attachrecord.Record]
}

// BlueprintAttachTaskIntent is the non-secret, task-owned lifecycle manifest
// for every Attach introduced by one Blueprint application.
type BlueprintAttachTaskIntent struct {
	TaskID               string                `json:"task_id"`
	EnvironmentID        string                `json:"environment_id"`
	Status               TaskStatus            `json:"status"`
	OwnsEnvironmentFence bool                  `json:"owns_environment_fence"`
	Candidates           []attachrecord.Record `json:"candidates"`
	CreatedAt            time.Time             `json:"created_at"`
	TerminalAt           *time.Time            `json:"terminal_at,omitempty"`
}

// BlueprintAttachTaskPreparation is immutable publication input. Ciphertext
// remains outside the durable intent so retries reuse the original fact set.
type BlueprintAttachTaskPreparation struct {
	Intent     BlueprintAttachTaskIntent
	candidates []EnvironmentBlueprintAttachCandidateInput
}

type EnvironmentBlueprintBackupPolicy struct {
	Enabled     bool                                     `json:"enabled"`
	Frequency   string                                   `json:"frequency,omitempty"`
	Keep        int64                                    `json:"keep,omitempty"`
	Encryption  string                                   `json:"encryption,omitempty"`
	ConnectorID string                                   `json:"connector_id,omitempty"`
	Sources     []EnvironmentBlueprintBackupPolicySource `json:"sources,omitempty"`
}

type EnvironmentBlueprintBackupPolicySource struct {
	ID       string                `json:"id"`
	Kind     core.BackupSourceKind `json:"kind"`
	TargetID string                `json:"target_id"`
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
	Projection        EnvironmentComposeProjection
	AttachPreparation BlueprintAttachTaskPreparation
	CreatedAt         time.Time
}

func CloneEnvironmentBlueprintBackupPolicy(source *EnvironmentBlueprintBackupPolicy) *EnvironmentBlueprintBackupPolicy {
	if source == nil {
		return nil
	}
	clone := *source
	clone.Sources = append([]EnvironmentBlueprintBackupPolicySource(nil), source.Sources...)
	return &clone
}

func validateEnvironmentBlueprintBackupPolicy(
	environmentID string,
	policy *EnvironmentBlueprintBackupPolicy,
) error {
	if policy == nil {
		return nil
	}
	selections := make([]BackupPolicySourceSelection, len(policy.Sources))
	seenIDs := make(map[string]struct{}, len(policy.Sources))
	for index, source := range policy.Sources {
		if ids.Validate(ids.KindBackupSource, source.ID) != nil {
			return errs.New(errs.KindValidationFailed, "Blueprint Backup source identity is invalid")
		}
		if _, duplicate := seenIDs[source.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Blueprint Backup source identity is duplicated")
		}
		seenIDs[source.ID] = struct{}{}
		selections[index] = BackupPolicySourceSelection{Kind: source.Kind, TargetID: source.TargetID}
	}
	return validateBackupPolicyReplacementInput(context.Background(), BackupPolicyReplacementInput{
		EnvironmentID: environmentID, Enabled: policy.Enabled, Frequency: policy.Frequency,
		Keep: policy.Keep, Encryption: policy.Encryption, ConnectorID: policy.ConnectorID,
		Sources: selections,
	})
}

func PrepareEnvironmentBlueprintAttachTask(
	taskID string,
	environmentID string,
	inputs []EnvironmentBlueprintAttachCandidateInput,
	ownsEnvironmentFence bool,
	createdAt time.Time,
) (BlueprintAttachTaskPreparation, error) {
	if len(inputs) > MaximumEnvironmentBlueprintAttachCandidates {
		return BlueprintAttachTaskPreparation{}, errs.New(
			errs.KindValidationFailed, "Blueprint may introduce at most two Attaches",
		)
	}
	if len(inputs) == 0 {
		return BlueprintAttachTaskPreparation{}, nil
	}
	preparation := BlueprintAttachTaskPreparation{
		Intent: BlueprintAttachTaskIntent{
			TaskID: taskID, EnvironmentID: environmentID, Status: TaskStatusPending,
			OwnsEnvironmentFence: ownsEnvironmentFence, CreatedAt: createdAt.UTC(),
		},
		candidates: cloneEnvironmentBlueprintAttachCandidateInputs(inputs),
	}
	sort.Slice(preparation.candidates, func(left, right int) bool {
		return preparation.candidates[left].Record.ID < preparation.candidates[right].Record.ID
	})
	preparation.Intent.Candidates = make([]attachrecord.Record, 0, len(preparation.candidates))
	for _, input := range preparation.candidates {
		preparation.Intent.Candidates = append(preparation.Intent.Candidates, cloneAttachRecord(input.Record))
	}
	if err := validateBlueprintAttachTaskPreparation(preparation); err != nil {
		clearBlueprintAttachTaskPreparation(&preparation)
		return BlueprintAttachTaskPreparation{}, err
	}
	return preparation, nil
}

func blueprintAttachTaskIntentKey(taskID string) string {
	return blueprintAttachTaskIntentPrefix + taskID
}

func (repository *AttachRepository) GetBlueprintAttachTaskIntent(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[BlueprintAttachTaskIntent], bool, error) {
	if err := validateContext(ctx); err != nil {
		return etcdstore.Versioned[BlueprintAttachTaskIntent]{}, false, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[BlueprintAttachTaskIntent]{}, false, errs.New(
			errs.KindValidationFailed,
			"Blueprint Attach Task id is invalid",
		)
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{blueprintAttachTaskIntentKey(taskID)}})
	if err != nil {
		return etcdstore.Versioned[BlueprintAttachTaskIntent]{}, false, err
	}
	if result == nil || len(result.Values) != 1 {
		return etcdstore.Versioned[BlueprintAttachTaskIntent]{}, false, errs.New(
			errs.KindInternal,
			"Blueprint Attach Task intent read is incomplete",
		)
	}
	if result.Values[0] == nil {
		return etcdstore.Versioned[BlueprintAttachTaskIntent]{ReadRevision: result.ReadRevision}, false, nil
	}
	intent, err := decodeBlueprintAttachTaskIntent(result.Values[0].Value)
	if err != nil {
		return etcdstore.Versioned[BlueprintAttachTaskIntent]{}, false, err
	}
	return etcdstore.Versioned[BlueprintAttachTaskIntent]{
		Record: intent, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func encodeBlueprintAttachTaskIntent(intent BlueprintAttachTaskIntent) ([]byte, error) {
	if err := validateBlueprintAttachTaskIntent(intent); err != nil {
		return nil, err
	}
	return recordcodec.Encode("blueprint_attach_task_intent", intent)
}

func decodeBlueprintAttachTaskIntent(value []byte) (BlueprintAttachTaskIntent, error) {
	intent, err := recordcodec.Decode[BlueprintAttachTaskIntent](value, "blueprint_attach_task_intent")
	if err != nil {
		return BlueprintAttachTaskIntent{}, err
	}
	if err := validateBlueprintAttachTaskIntent(intent); err != nil {
		return BlueprintAttachTaskIntent{}, errs.New(errs.KindInternal, "Blueprint Attach Task intent is corrupt")
	}
	return intent, nil
}

func terminalBlueprintAttachTaskIntent(
	intent BlueprintAttachTaskIntent,
	status TaskStatus,
	terminalAt time.Time,
) (BlueprintAttachTaskIntent, error) {
	if intent.Status != TaskStatusPending || !isTerminalTaskStatus(status) || terminalAt.IsZero() {
		return BlueprintAttachTaskIntent{}, errs.New(errs.KindStateConflict, "Blueprint Attach intent is not pending")
	}
	terminal := intent
	terminal.Candidates = make([]attachrecord.Record, len(intent.Candidates))
	for index, record := range intent.Candidates {
		terminal.Candidates[index] = cloneAttachRecord(record)
	}
	terminal.Status = status
	terminal.TerminalAt = timePointer(terminalAt.UTC())
	if err := validateBlueprintAttachTaskIntent(terminal); err != nil {
		return BlueprintAttachTaskIntent{}, err
	}
	return terminal, nil
}

func validateBlueprintAttachTaskPreparation(preparation BlueprintAttachTaskPreparation) error {
	if err := validateBlueprintAttachTaskIntent(preparation.Intent); err != nil {
		return err
	}
	if len(preparation.candidates) != len(preparation.Intent.Candidates) {
		return errs.New(errs.KindValidationFailed, "Blueprint Attach preparation is incomplete")
	}
	byID := make(map[string]EnvironmentBlueprintAttachCandidateInput, len(preparation.candidates))
	byName := make(map[string]string, len(preparation.candidates))
	for index, input := range preparation.candidates {
		if !sameBlueprintAttachCandidateRecord(input.Record, preparation.Intent.Candidates[index]) {
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
				!sameBlueprintAttachCandidateRecord(existing.Record, retained.Record) {
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

func sameBlueprintAttachCandidateRecord(left, right attachrecord.Record) bool {
	return left.ID == right.ID && left.EnvironmentID == right.EnvironmentID && left.Name == right.Name &&
		left.BackingProjectID == right.BackingProjectID && left.BackingEnvironmentID == right.BackingEnvironmentID &&
		left.BackingServiceID == right.BackingServiceID && left.BackingNetworkID == right.BackingNetworkID &&
		left.ServiceID == right.ServiceID && left.CredentialAttachID == right.CredentialAttachID &&
		sameBlueprintAttachStrings(left.GrantAttachIDs, right.GrantAttachIDs) &&
		left.HookBundle == right.HookBundle && sameBlueprintAttachFactSets(left.FactSets, right.FactSets) &&
		left.Status == right.Status && left.Operation == right.Operation && left.TaskID == right.TaskID &&
		left.CreatedAt == right.CreatedAt
}

func sameBlueprintAttachStrings(left, right []string) bool {
	return (left == nil) == (right == nil) && slices.Equal(left, right)
}

func sameBlueprintAttachFactSets(left, right []attachrecord.FactSetMetadata) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].GrantAttachID != right[index].GrantAttachID ||
			(left[index].Facts == nil) != (right[index].Facts == nil) ||
			!slices.Equal(left[index].Facts, right[index].Facts) {
			return false
		}
	}
	return true
}

func validateBlueprintAttachTaskIntent(intent BlueprintAttachTaskIntent) error {
	if ids.Validate(ids.KindTask, intent.TaskID) != nil ||
		ids.Validate(ids.KindEnvironment, intent.EnvironmentID) != nil ||
		intent.CreatedAt.IsZero() ||
		len(intent.Candidates) == 0 {
		return errs.New(errs.KindValidationFailed, "Blueprint Attach Task intent is invalid")
	}
	if intent.Status == TaskStatusPending {
		if intent.TerminalAt != nil {
			return errs.New(errs.KindValidationFailed, "Pending Blueprint Attach intent has a terminal timestamp")
		}
	} else if !isTerminalTaskStatus(intent.Status) || intent.TerminalAt == nil ||
		intent.TerminalAt.Before(intent.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Terminal Blueprint Attach intent is invalid")
	}
	previousID := ""
	for _, record := range intent.Candidates {
		if attachrecord.ValidateAttachRecord(record) != nil || record.EnvironmentID != intent.EnvironmentID ||
			record.TaskID != intent.TaskID || record.Status != core.AttachPending ||
			record.Operation != attachrecord.AttachOperationProvision || record.ID <= previousID {
			return errs.New(errs.KindValidationFailed, "Blueprint Attach Task candidate is invalid or unsorted")
		}
		previousID = record.ID
	}
	return nil
}

func blueprintAttachTaskPreparationIsZero(preparation BlueprintAttachTaskPreparation) bool {
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
		cloned[index].Record = cloneAttachRecord(input.Record)
		if input.Facts != nil {
			facts := *input.Facts
			facts.Ciphertext = append([]byte(nil), input.Facts.Ciphertext...)
			cloned[index].Facts = &facts
		}
		if input.RetainedCredentialOwner != nil {
			owner := *input.RetainedCredentialOwner
			owner.Record = cloneAttachRecord(owner.Record)
			cloned[index].RetainedCredentialOwner = &owner
		}
		cloned[index].RetainedGrantTargets = append(
			[]etcdstore.Versioned[attachrecord.Record](nil), input.RetainedGrantTargets...,
		)
		for retainedIndex := range cloned[index].RetainedGrantTargets {
			cloned[index].RetainedGrantTargets[retainedIndex].Record = cloneAttachRecord(
				cloned[index].RetainedGrantTargets[retainedIndex].Record,
			)
		}
	}
	return cloned
}

func cloneAttachRecord(record attachrecord.Record) attachrecord.Record {
	record.GrantAttachIDs = append([]string(nil), record.GrantAttachIDs...)
	record.FactSets = attachrecord.CloneAttachFactSets(record.FactSets)
	return record
}

func clearBlueprintAttachTaskPreparation(preparation *BlueprintAttachTaskPreparation) {
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

// ClearBlueprintAttachTaskPreparation releases every encrypted fact copy held
// by application-side publication input after the atomic attempt returns.
func ClearBlueprintAttachTaskPreparation(preparation *BlueprintAttachTaskPreparation) {
	clearBlueprintAttachTaskPreparation(preparation)
}

func validateBlueprintAttachTaskOwner(task TaskRecord, intent BlueprintAttachTaskIntent) error {
	if task.ID != intent.TaskID || task.Target != intent.EnvironmentID {
		return errs.New(errs.KindStateConflict, "Blueprint Attach intent has the wrong Task owner")
	}
	return nil
}
