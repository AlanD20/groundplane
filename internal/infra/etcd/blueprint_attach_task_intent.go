package etcd

import (
	"context"
	"reflect"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const blueprintAttachTaskIntentPrefix = "/v1/records/blueprint-attach-task-intents/"

// EnvironmentBlueprintAttachCandidateInput is one fully resolved Attach that
// will be published by the same transaction as its owning Blueprint Task.
// Versioned backing records are private compare evidence, not desired state.
type EnvironmentBlueprintAttachCandidateInput struct {
	Record             AttachRecord
	Facts              *AttachEncryptedFacts
	BackingProject     Versioned[ProjectRecord]
	BackingEnvironment Versioned[EnvironmentRecord]
	BackingService     Versioned[ServiceRecord]
}

// BlueprintAttachTaskIntent is the non-secret, task-owned lifecycle manifest
// for every Attach introduced by one Blueprint application.
type BlueprintAttachTaskIntent struct {
	TaskID               string         `json:"task_id"`
	EnvironmentID        string         `json:"environment_id"`
	Status               TaskStatus     `json:"status"`
	OwnsEnvironmentFence bool           `json:"owns_environment_fence"`
	Candidates           []AttachRecord `json:"candidates"`
	CreatedAt            time.Time      `json:"created_at"`
	TerminalAt           *time.Time     `json:"terminal_at,omitempty"`
}

// BlueprintAttachTaskPreparation is immutable publication input. Ciphertext
// remains outside the durable intent so retries reuse the original fact set.
type BlueprintAttachTaskPreparation struct {
	Intent     BlueprintAttachTaskIntent
	candidates []EnvironmentBlueprintAttachCandidateInput
}

func PrepareEnvironmentBlueprintAttachTask(
	taskID string,
	environmentID string,
	inputs []EnvironmentBlueprintAttachCandidateInput,
	ownsEnvironmentFence bool,
	createdAt time.Time,
) (BlueprintAttachTaskPreparation, error) {
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
	preparation.Intent.Candidates = make([]AttachRecord, 0, len(preparation.candidates))
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
) (Versioned[BlueprintAttachTaskIntent], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[BlueprintAttachTaskIntent]{}, false, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return Versioned[BlueprintAttachTaskIntent]{}, false, errs.New(
			errs.KindValidationFailed,
			"Blueprint Attach Task id is invalid",
		)
	}
	result, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{blueprintAttachTaskIntentKey(taskID)}})
	if err != nil {
		return Versioned[BlueprintAttachTaskIntent]{}, false, err
	}
	if result == nil || len(result.Values) != 1 {
		return Versioned[BlueprintAttachTaskIntent]{}, false, errs.New(
			errs.KindInternal,
			"Blueprint Attach Task intent read is incomplete",
		)
	}
	if result.Values[0] == nil {
		return Versioned[BlueprintAttachTaskIntent]{ReadRevision: result.ReadRevision}, false, nil
	}
	intent, err := decodeBlueprintAttachTaskIntent(result.Values[0].Value)
	if err != nil {
		return Versioned[BlueprintAttachTaskIntent]{}, false, err
	}
	return Versioned[BlueprintAttachTaskIntent]{
		Record: intent, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func encodeBlueprintAttachTaskIntent(intent BlueprintAttachTaskIntent) ([]byte, error) {
	if err := validateBlueprintAttachTaskIntent(intent); err != nil {
		return nil, err
	}
	return encodeEnvelope("blueprint_attach_task_intent", intent)
}

func decodeBlueprintAttachTaskIntent(value []byte) (BlueprintAttachTaskIntent, error) {
	intent, err := decodeEnvelope[BlueprintAttachTaskIntent](value, "blueprint_attach_task_intent")
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
	terminal.Candidates = make([]AttachRecord, len(intent.Candidates))
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
		if !reflect.DeepEqual(input.Record, preparation.Intent.Candidates[index]) {
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
			if (input.Facts == nil) != (len(input.Record.FactSets) == 0) {
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
	for _, input := range preparation.candidates {
		record := input.Record
		owner, exists := byID[record.CredentialAttachID]
		if !exists || !owner.Record.OwnsCredential() || owner.Record.BackingServiceID != record.BackingServiceID {
			return errs.New(errs.KindValidationFailed, "Blueprint Attach credential owner is outside the candidate set")
		}
		for _, grantID := range record.GrantAttachIDs {
			grant, found := byID[grantID]
			if !found || grant.Record.BackingServiceID != record.BackingServiceID {
				return errs.New(errs.KindValidationFailed, "Blueprint Attach grant is outside the candidate set")
			}
		}
	}
	return nil
}

func validateBlueprintAttachTaskIntent(intent BlueprintAttachTaskIntent) error {
	if ids.Validate(ids.KindTask, intent.TaskID) != nil || ids.Validate(ids.KindEnvironment, intent.EnvironmentID) != nil ||
		intent.CreatedAt.IsZero() || len(intent.Candidates) == 0 {
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
		if validateAttachRecord(record) != nil || record.EnvironmentID != intent.EnvironmentID ||
			record.TaskID != intent.TaskID || record.Status != core.AttachPending ||
			record.Operation != AttachOperationProvision || record.ID <= previousID {
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
	}
	return cloned
}

func cloneAttachRecord(record AttachRecord) AttachRecord {
	record.GrantAttachIDs = append([]string(nil), record.GrantAttachIDs...)
	record.FactSets = cloneAttachFactSets(record.FactSets)
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
