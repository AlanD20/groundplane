package app

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backingendpoint"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	controllerpkg "github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type backingHookTaskInputRepository interface {
	GetBackingHookTaskInputs(context.Context, etcd.TaskRecord) (etcd.BackingHookEncryptedInputs, error)
}

type backingHookSecretRepository interface {
	ResolveSecret(context.Context, string, string) (etcd.Versioned[etcd.SecretRecord], error)
	GetSecretValue(context.Context, etcd.Versioned[etcd.SecretRecord]) (etcd.SecretEncryptedValue, error)
}

func (service *AttachFactService) EnableBackingHookInputs(repository backingHookSecretRepository) error {
	if service == nil || repository == nil || service.secrets != nil {
		return errs.New(errs.KindInternal, "Backing hook Secret resolver is invalid")
	}
	service.secrets = repository
	return nil
}

// SealCustomHookBundle creates the long-lived generated Attach values and the
// operation-owned capture for literals and exact Secret revisions.
func (service *AttachFactService) SealCustomHookBundle(
	ctx context.Context,
	attachID string,
	projectID string,
	operationID string,
	configuration backinghook.Configuration,
) ([]etcd.AttachFactSetMetadata, *etcd.AttachEncryptedFacts, *etcd.BackingHookEncryptedInputs, error) {
	if ctx == nil || ids.Validate(ids.KindAttach, attachID) != nil ||
		ids.Validate(ids.KindProject, projectID) != nil || ids.Validate(ids.KindOperation, operationID) != nil ||
		service.secrets == nil || service.random == nil {
		return nil, nil, nil, errs.New(errs.KindInternal, "Backing hook input resolver is not configured")
	}
	if err := backinghook.ValidateConfiguration(configuration); err != nil {
		return nil, nil, nil, err
	}
	bundle := attachFactBundle{Version: 4, AttachID: attachID}
	defer bundle.clear()
	for _, definition := range configuration.Inputs {
		if definition.Generate != backinghook.GeneratePassword {
			continue
		}
		value, err := generateAttachPassword(service.random)
		if err != nil {
			return nil, nil, nil, err
		}
		bundle.HookInputs = append(bundle.HookInputs, attachFactValue{Key: definition.Key, Value: value})
	}
	slices.SortFunc(bundle.HookInputs, func(left, right attachFactValue) int {
		return strings.Compare(left.Key, right.Key)
	})
	metadata := []etcd.AttachFactSetMetadata(nil)
	if len(configuration.Facts) != 0 {
		facts := append([]backinghook.FactDefinition(nil), configuration.Facts...)
		slices.SortFunc(facts, func(left, right backinghook.FactDefinition) int {
			return strings.Compare(left.Key, right.Key)
		})
		definitions := make([]etcd.AttachFactDefinition, len(facts))
		for index, fact := range facts {
			definitions[index] = etcd.AttachFactDefinition{Key: fact.Key, Secret: fact.Secret}
		}
		metadata = []etcd.AttachFactSetMetadata{{Facts: definitions}}
		bundle.Sets = []attachFactValueSet{{Facts: []attachFactValue{}}}
	}
	encrypted, err := service.sealHookBundle(ctx, attachID, bundle)
	if err != nil {
		return nil, nil, nil, err
	}
	hookInputs, err := service.SealBackingHookTaskInputs(ctx, operationID, projectID, configuration)
	if err != nil {
		clearAttachBytes(encrypted.Ciphertext)
		return nil, nil, nil, err
	}
	return metadata, &encrypted, hookInputs, nil
}

func (service *AttachFactService) SealBackingHookTaskInputs(
	ctx context.Context,
	operationID string,
	projectID string,
	configuration backinghook.Configuration,
) (*etcd.BackingHookEncryptedInputs, error) {
	if ctx == nil || ids.Validate(ids.KindOperation, operationID) != nil ||
		ids.Validate(ids.KindProject, projectID) != nil || service.secrets == nil {
		return nil, errs.New(errs.KindInternal, "Backing hook Task input resolver is not configured")
	}
	if err := backinghook.ValidateConfiguration(configuration); err != nil {
		return nil, err
	}
	bundle := backingHookTaskInputBundle{Version: 1, OperationID: operationID}
	defer bundle.clear()
	for _, definition := range configuration.Inputs {
		if definition.Generate == backinghook.GeneratePassword {
			continue
		}
		value, source, err := service.resolveBackingHookTaskInput(ctx, operationID, projectID, definition)
		if err != nil {
			return nil, err
		}
		bundle.Values = append(bundle.Values, attachFactValue{Key: definition.Key, Value: value})
		if source != nil {
			bundle.SecretSources = append(bundle.SecretSources, *source)
		}
	}
	if len(bundle.Values) == 0 {
		return nil, nil
	}
	slices.SortFunc(bundle.Values, func(left, right attachFactValue) int {
		return strings.Compare(left.Key, right.Key)
	})
	slices.SortFunc(bundle.SecretSources, func(left, right tasksecretpinrecord.Record) int {
		return strings.Compare(left.SecretID, right.SecretID)
	})
	payload, err := json.Marshal(bundle)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clearAttachBytes(payload)
	envelope, err := service.protector.Seal(ctx, payload)
	if err != nil {
		return nil, err
	}
	ciphertext := envelope.Ciphertext()
	defer clearAttachBytes(ciphertext)
	metadata := envelope.Metadata()
	sealed, err := etcd.NewBackingHookEncryptedInputs(
		operationID, uint8(metadata.Version), string(metadata.Cipher),
		string(metadata.Digest.Algorithm), ciphertext, bundle.SecretSources,
	)
	if err != nil {
		return nil, err
	}
	return &sealed, nil
}

func (service *AttachFactService) resolveBackingHookTaskInput(
	ctx context.Context,
	operationID string,
	projectID string,
	definition backinghook.InputDefinition,
) ([]byte, *tasksecretpinrecord.Record, error) {
	if definition.Value != nil {
		return []byte(*definition.Value), nil, nil
	}
	if definition.SecretRef == "" {
		return nil, nil, errs.New(errs.KindValidationFailed, "Backing hook Task input source is invalid")
	}
	current, err := service.secrets.ResolveSecret(ctx, projectID, definition.SecretRef)
	if err != nil {
		return nil, nil, err
	}
	stored, err := service.secrets.GetSecretValue(ctx, current)
	if err != nil {
		return nil, nil, err
	}
	defer clearAttachBytes(stored.Ciphertext)
	envelope, err := secretvalue.Restore(secretvalue.Metadata{
		Version: secretvalue.EnvelopeVersion(stored.EnvelopeVersion),
		Cipher:  secretvalue.CipherSuite(stored.Cipher),
		Digest: secretvalue.Digest{
			Algorithm: secretvalue.DigestAlgorithm(stored.DigestAlgorithm),
			Value:     stored.CiphertextSHA256,
		},
	}, stored.Ciphertext)
	if err != nil {
		return nil, nil, err
	}
	var value []byte
	err = service.protector.Open(ctx, envelope, func(plaintext []byte) error {
		value = append([]byte(nil), plaintext...)
		return nil
	})
	if err != nil {
		clearAttachBytes(value)
		return nil, nil, err
	}
	pin := &tasksecretpinrecord.Record{
		OperationID: operationID, SecretID: current.Record.Secret.ID,
		MetadataRevision: current.Revision, CiphertextSHA256: stored.CiphertextSHA256,
	}
	return value, pin, nil
}

func (service *AttachFactService) ResolveHookInput(
	ctx context.Context,
	current etcd.Versioned[etcd.AttachRecord],
	task etcd.TaskRecord,
	hookContext backinghook.Context,
	consume controllerpkg.BackingHookInputConsumer,
) error {
	if ctx == nil || consume == nil || ids.Validate(ids.KindTask, task.ID) != nil ||
		current.Record.TaskID != task.ID || !current.Record.OwnsCredential() || !current.Record.HookBundle ||
		hookContext.BackingServiceID != current.Record.BackingServiceID ||
		hookContext.AttachID != current.Record.ID || hookContext.EnvironmentID != current.Record.EnvironmentID ||
		hookContext.ServiceID != current.Record.ServiceID {
		return errs.New(errs.KindValidationFailed, "Backing hook Task input request is invalid")
	}
	allowed := hookContext.Event == backinghook.Attach && current.Record.Operation == etcd.AttachOperationProvision &&
		(current.Record.Status == core.AttachPending || current.Record.Status == core.AttachProvisioning)
	allowed = allowed || hookContext.Event == backinghook.Detach &&
		current.Record.Operation == etcd.AttachOperationDetach && current.Record.Status == core.AttachDetaching
	if !allowed {
		return errs.New(errs.KindStateConflict, "Backing hook input is unavailable in the current lifecycle")
	}
	return service.openBundle(ctx, current, func(bundle *attachFactBundle) error {
		return service.consumeHookInput(ctx, current.Record, task, nil, hookContext, bundle, consume)
	})
}

func (service *AttachFactService) ResolveDraftHookInput(
	ctx context.Context,
	current etcd.Versioned[etcd.AttachRecord],
	stored *etcd.AttachEncryptedFacts,
	task etcd.TaskRecord,
	hookInputs *etcd.BackingHookEncryptedInputs,
	hookContext backinghook.Context,
	consume controllerpkg.BackingHookInputConsumer,
) error {
	if current.Record.TaskID != task.ID || stored != nil && stored.AttachID != current.Record.ID {
		return errs.New(errs.KindValidationFailed, "Backing hook draft input request is invalid")
	}
	consumeBundle := func(bundle *attachFactBundle) error {
		return service.consumeHookInput(ctx, current.Record, task, hookInputs, hookContext, bundle, consume)
	}
	if stored != nil {
		return service.openStoredBundle(ctx, current.Record, *stored, consumeBundle)
	}
	return service.openBundle(ctx, current, consumeBundle)
}

func (service *AttachFactService) ResolveLifecycleHookInput(
	ctx context.Context,
	task etcd.TaskRecord,
	serviceID string,
	event backinghook.Event,
	consume controllerpkg.BackingHookInputConsumer,
) error {
	return service.resolveLifecycleHookInput(ctx, task, nil, serviceID, event, consume)
}

func (service *AttachFactService) ResolveDraftLifecycleHookInput(
	ctx context.Context,
	task etcd.TaskRecord,
	stored *etcd.BackingHookEncryptedInputs,
	serviceID string,
	event backinghook.Event,
	consume controllerpkg.BackingHookInputConsumer,
) error {
	return service.resolveLifecycleHookInput(ctx, task, stored, serviceID, event, consume)
}

func (service *AttachFactService) resolveLifecycleHookInput(
	ctx context.Context,
	task etcd.TaskRecord,
	draft *etcd.BackingHookEncryptedInputs,
	serviceID string,
	event backinghook.Event,
	consume controllerpkg.BackingHookInputConsumer,
) error {
	creationServiceID := task.Params[etcd.TaskBackingServiceCreationParam]
	ownsService := task.Target == serviceID ||
		event == backinghook.AfterStart && creationServiceID == serviceID
	if ctx == nil || consume == nil || !ownsService ||
		ids.Validate(ids.KindService, serviceID) != nil ||
		(event != backinghook.BeforeStop && event != backinghook.AfterStart) {
		return errs.New(errs.KindValidationFailed, "Backing lifecycle hook input request is invalid")
	}
	input := backinghook.Input{Context: backinghook.Context{Event: event, BackingServiceID: serviceID}}
	input.Values = append(input.Values, backinghook.Value{
		Key: "HOST", Value: []byte(backingendpoint.New(serviceID)),
	})
	if err := service.appendBackingHookTaskInputs(ctx, task, draft, &input); err != nil {
		clearBackingHookInput(&input)
		return err
	}
	slices.SortFunc(input.Values, func(left, right backinghook.Value) int {
		return strings.Compare(left.Key, right.Key)
	})
	defer clearBackingHookInput(&input)
	return consume(input)
}

func (service *AttachFactService) consumeHookInput(
	ctx context.Context,
	record etcd.AttachRecord,
	task etcd.TaskRecord,
	draft *etcd.BackingHookEncryptedInputs,
	hookContext backinghook.Context,
	bundle *attachFactBundle,
	consume controllerpkg.BackingHookInputConsumer,
) error {
	input := backinghook.Input{Context: hookContext}
	input.Values = append(input.Values, backinghook.Value{
		Key: "HOST", Value: []byte(backingendpoint.New(record.BackingServiceID)),
	})
	for _, value := range bundle.HookInputs {
		input.Values = append(input.Values, backinghook.Value{Key: value.Key, Value: append([]byte(nil), value.Value...)})
	}
	if err := service.appendBackingHookTaskInputs(ctx, task, draft, &input); err != nil {
		clearBackingHookInput(&input)
		return err
	}
	slices.SortFunc(input.Values, func(left, right backinghook.Value) int {
		return strings.Compare(left.Key, right.Key)
	})
	if hookContext.Event == backinghook.Detach && len(bundle.Sets) != 0 {
		for _, value := range bundle.Sets[0].Facts {
			input.Facts = append(input.Facts, backinghook.Value{Key: value.Key, Value: append([]byte(nil), value.Value...)})
		}
	}
	defer clearBackingHookInput(&input)
	return consume(input)
}

func (service *AttachFactService) appendBackingHookTaskInputs(
	ctx context.Context,
	task etcd.TaskRecord,
	draft *etcd.BackingHookEncryptedInputs,
	input *backinghook.Input,
) error {
	if task.Configuration == nil || task.Configuration.BackingHookInputs == nil {
		return nil
	}
	appendValues := func(bundle *backingHookTaskInputBundle) error {
		for _, value := range bundle.Values {
			input.Values = append(input.Values, backinghook.Value{
				Key: value.Key, Value: append([]byte(nil), value.Value...),
			})
		}
		return nil
	}
	if draft != nil {
		return service.openBackingHookTaskInputs(ctx, task, *draft, appendValues)
	}
	repository, ok := service.repository.(backingHookTaskInputRepository)
	if !ok {
		return errs.New(errs.KindInternal, "Backing hook Task input repository is not configured")
	}
	stored, err := repository.GetBackingHookTaskInputs(ctx, task)
	if err != nil {
		return err
	}
	defer clearAttachBytes(stored.Ciphertext)
	return service.openBackingHookTaskInputs(ctx, task, stored, appendValues)
}

func (service *AttachFactService) SealHookResult(
	ctx context.Context,
	current etcd.Versioned[etcd.AttachRecord],
	schema []backinghook.FactDefinition,
	output backinghook.Output,
) (etcd.AttachEncryptedFacts, error) {
	if ctx == nil || current.Revision <= 0 || !current.Record.HookBundle ||
		current.Record.Operation != etcd.AttachOperationProvision ||
		(current.Record.Status != core.AttachPending && current.Record.Status != core.AttachProvisioning) {
		return etcd.AttachEncryptedFacts{}, errs.New(errs.KindStateConflict, "Backing hook result owner is unavailable")
	}
	if err := backinghook.ValidateOutput(schema, output); err != nil {
		return etcd.AttachEncryptedFacts{}, err
	}
	var sealed etcd.AttachEncryptedFacts
	err := service.openBundle(ctx, current, func(bundle *attachFactBundle) error {
		if len(schema) == 0 {
			if len(bundle.Sets) != 0 || len(current.Record.FactSets) != 0 {
				return errs.New(errs.KindInternal, "Backing hook empty fact metadata is corrupt")
			}
			var sealErr error
			sealed, sealErr = service.sealHookBundle(ctx, current.Record.ID, *bundle)
			return sealErr
		}
		if len(bundle.Sets) != 1 || bundle.Sets[0].GrantAttachID != "" || len(current.Record.FactSets) != 1 ||
			len(current.Record.FactSets[0].Facts) != len(schema) {
			return errs.New(errs.KindInternal, "Backing hook fact metadata is corrupt")
		}
		values := make([]attachFactValue, len(output.Facts))
		for index, fact := range output.Facts {
			definition := current.Record.FactSets[0].Facts[index]
			if definition.Key != schema[index].Key || definition.Secret != schema[index].Secret ||
				fact.Key != schema[index].Key || fact.Secret != schema[index].Secret {
				return errs.New(errs.KindStateConflict, "Backing hook fact schema changed")
			}
			values[index] = attachFactValue{Key: fact.Key, Value: append([]byte(nil), fact.Value...)}
		}
		for index := range bundle.Sets[0].Facts {
			clearAttachBytes(bundle.Sets[0].Facts[index].Value)
		}
		bundle.Sets[0].Facts = values
		var sealErr error
		sealed, sealErr = service.sealHookBundle(ctx, current.Record.ID, *bundle)
		return sealErr
	})
	return sealed, err
}

func (service *AttachFactService) sealHookBundle(
	ctx context.Context,
	attachID string,
	bundle attachFactBundle,
) (etcd.AttachEncryptedFacts, error) {
	payload, err := json.Marshal(bundle)
	if err != nil {
		return etcd.AttachEncryptedFacts{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clearAttachBytes(payload)
	envelope, err := service.protector.Seal(ctx, payload)
	if err != nil {
		return etcd.AttachEncryptedFacts{}, err
	}
	ciphertext := envelope.Ciphertext()
	defer clearAttachBytes(ciphertext)
	metadata := envelope.Metadata()
	return etcd.NewAttachEncryptedFacts(
		attachID, uint8(metadata.Version), string(metadata.Cipher),
		string(metadata.Digest.Algorithm), ciphertext,
	)
}

func (service *AttachFactService) openBackingHookTaskInputs(
	ctx context.Context,
	task etcd.TaskRecord,
	stored etcd.BackingHookEncryptedInputs,
	consume func(*backingHookTaskInputBundle) error,
) error {
	if task.Configuration == nil || task.Configuration.BackingHookInputs == nil ||
		stored.OperationID != task.OperationID ||
		stored.CiphertextSHA256 != task.Configuration.BackingHookInputs.CiphertextSHA256 {
		return errs.New(errs.KindStateConflict, "Backing hook Task input authority changed")
	}
	envelope, err := secretvalue.Restore(secretvalue.Metadata{
		Version: secretvalue.EnvelopeVersion(stored.EnvelopeVersion),
		Cipher:  secretvalue.CipherSuite(stored.Cipher),
		Digest: secretvalue.Digest{
			Algorithm: secretvalue.DigestAlgorithm(stored.DigestAlgorithm),
			Value:     stored.CiphertextSHA256,
		},
	}, stored.Ciphertext)
	if err != nil {
		return err
	}
	return service.protector.Open(ctx, envelope, func(plaintext []byte) error {
		var bundle backingHookTaskInputBundle
		if decodeErr := json.Unmarshal(plaintext, &bundle); decodeErr != nil {
			return errs.New(errs.KindInternal, "Backing hook Task input plaintext is corrupt")
		}
		defer bundle.clear()
		if bundle.Version != 1 || bundle.OperationID != task.OperationID ||
			!slices.Equal(bundle.SecretSources, task.Configuration.BackingHookInputs.SecretSources) {
			return errs.New(errs.KindInternal, "Backing hook Task input plaintext does not match its authority")
		}
		previous := ""
		for _, value := range bundle.Values {
			if !backinghook.ValidKey(value.Key) || value.Key <= previous {
				return errs.New(errs.KindInternal, "Backing hook Task input plaintext is invalid")
			}
			previous = value.Key
		}
		return consume(&bundle)
	})
}

func clearBackingHookInput(input *backinghook.Input) {
	if input == nil {
		return
	}
	for index := range input.Values {
		clearAttachBytes(input.Values[index].Value)
	}
	for index := range input.Facts {
		clearAttachBytes(input.Facts[index].Value)
	}
	input.Values = nil
	input.Facts = nil
}

type backingHookTaskInputBundle struct {
	Version       uint8                        `json:"version"`
	OperationID   string                       `json:"operation_id"`
	Values        []attachFactValue            `json:"values"`
	SecretSources []tasksecretpinrecord.Record `json:"secret_sources,omitempty"`
}

func (bundle *backingHookTaskInputBundle) clear() {
	if bundle == nil {
		return
	}
	for index := range bundle.Values {
		clearAttachBytes(bundle.Values[index].Value)
		bundle.Values[index].Value = nil
	}
	bundle.Values = nil
	bundle.SecretSources = nil
}
