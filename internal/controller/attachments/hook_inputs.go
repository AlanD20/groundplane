package attachments

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type backingHookSecretRepository interface {
	ResolveSecretAtRevision(context.Context, string, string, int64) (etcdstore.Versioned[secretrecord.Record], error)
	GetSecretValue(context.Context, etcdstore.Versioned[secretrecord.Record]) (secretrecord.EncryptedValue, error)
}

func (service *FactService) EnableBackingHookInputs(repository backingHookSecretRepository) error {
	if service == nil || repository == nil || service.secrets != nil {
		return errs.New(errs.KindInternal, "Backing hook Secret resolver is invalid")
	}
	service.secrets = repository
	return nil
}

// SealCustomHookBundle creates the long-lived generated Attach values and the
// operation-owned capture for literals and exact Secret revisions.
func (service *FactService) SealCustomHookBundle(
	ctx context.Context,
	attachID string,
	projectID string,
	operationID string,
	configuration backinghook.Configuration,
) ([]attachrecord.FactSetMetadata, *attachrecord.EncryptedFacts, *taskconfiguration.BackingHookEncryptedInputs, error) {
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
		value, err := GeneratePassword(service.random)
		if err != nil {
			return nil, nil, nil, err
		}
		bundle.HookInputs = append(bundle.HookInputs, attachFactValue{Key: definition.Key, Value: value})
	}
	slices.SortFunc(bundle.HookInputs, func(left, right attachFactValue) int {
		return strings.Compare(left.Key, right.Key)
	})
	metadata := []attachrecord.FactSetMetadata(nil)
	if len(configuration.Facts) != 0 {
		facts := append([]backinghook.FactDefinition(nil), configuration.Facts...)
		slices.SortFunc(facts, func(left, right backinghook.FactDefinition) int {
			return strings.Compare(left.Key, right.Key)
		})
		definitions := make([]attachrecord.FactDefinition, len(facts))
		for index, fact := range facts {
			definitions[index] = attachrecord.FactDefinition{Key: fact.Key, Secret: fact.Secret}
		}
		metadata = []attachrecord.FactSetMetadata{{Facts: definitions}}
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

func (service *FactService) SealBackingHookTaskInputs(
	ctx context.Context,
	operationID string,
	projectID string,
	configuration backinghook.Configuration,
) (*taskconfiguration.BackingHookEncryptedInputs, error) {
	return service.sealBackingHookInputs(ctx, operationID, projectID, configuration, 0)
}

func (service *FactService) SealBackingHookInputsAtRevision(ctx context.Context, operationID, projectID string,
	configuration backinghook.Configuration, revision int64,
) (*taskconfiguration.BackingHookEncryptedInputs, error) {
	if revision <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "captured hook inputs require a snapshot revision")
	}
	return service.sealBackingHookInputs(ctx, operationID, projectID, configuration, revision)
}

func (service *FactService) sealBackingHookInputs(ctx context.Context, operationID, projectID string,
	configuration backinghook.Configuration, revision int64,
) (*taskconfiguration.BackingHookEncryptedInputs, error) {
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
		value, source, err := service.resolveBackingHookTaskInput(ctx, operationID, projectID, definition, revision)
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
	bundle.SecretSources = slices.Compact(bundle.SecretSources)
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
	sealed, err := taskconfiguration.NewBackingHookEncryptedInputs(
		operationID, uint8(metadata.Version), string(metadata.Cipher),
		string(metadata.Digest.Algorithm), ciphertext, bundle.SecretSources,
	)
	if err != nil {
		return nil, err
	}
	return &sealed, nil
}

func (service *FactService) resolveBackingHookTaskInput(
	ctx context.Context,
	operationID string,
	projectID string,
	definition backinghook.InputDefinition,
	revision int64,
) ([]byte, *tasksecretpinrecord.Record, error) {
	if definition.Value != nil {
		return []byte(*definition.Value), nil, nil
	}
	if definition.SecretRef == "" {
		return nil, nil, errs.New(errs.KindValidationFailed, "Backing hook Task input source is invalid")
	}
	current, err := service.secrets.ResolveSecretAtRevision(ctx, projectID, definition.SecretRef, revision)
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

func (service *FactService) SealHookResult(
	ctx context.Context,
	current etcdstore.Versioned[attachrecord.Record],
	schema []backinghook.FactDefinition,
	output backinghook.Output,
) (attachrecord.EncryptedFacts, error) {
	if ctx == nil || current.Revision <= 0 || !current.Record.HookBundle ||
		current.Record.Operation != attachrecord.AttachOperationProvision ||
		(current.Record.Status != core.AttachPending && current.Record.Status != core.AttachProvisioning) {
		return attachrecord.EncryptedFacts{}, errs.New(
			errs.KindStateConflict,
			"Backing hook result owner is unavailable",
		)
	}
	if err := backinghook.ValidateOutput(schema, output); err != nil {
		return attachrecord.EncryptedFacts{}, err
	}
	var sealed attachrecord.EncryptedFacts
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

func (service *FactService) sealHookBundle(
	ctx context.Context,
	attachID string,
	bundle attachFactBundle,
) (attachrecord.EncryptedFacts, error) {
	payload, err := json.Marshal(bundle)
	if err != nil {
		return attachrecord.EncryptedFacts{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clearAttachBytes(payload)
	envelope, err := service.protector.Seal(ctx, payload)
	if err != nil {
		return attachrecord.EncryptedFacts{}, err
	}
	ciphertext := envelope.Ciphertext()
	defer clearAttachBytes(ciphertext)
	metadata := envelope.Metadata()
	return attachrecord.NewAttachEncryptedFacts(
		attachID, uint8(metadata.Version), string(metadata.Cipher),
		string(metadata.Digest.Algorithm), ciphertext,
	)
}

func (service *FactService) openCapturedBackingHookInputs(ctx context.Context, operationID string,
	expected taskconfiguration.TaskBackingHookInputSet, stored taskconfiguration.BackingHookEncryptedInputs,
	consume func(*backingHookTaskInputBundle) error,
) error {
	if ctx == nil || consume == nil || ids.Validate(ids.KindOperation, operationID) != nil ||
		stored.OperationID != operationID || stored.CiphertextSHA256 != expected.CiphertextSHA256 ||
		taskconfiguration.ValidateBackingHookEncryptedInputs(stored) != nil {
		return errs.New(errs.KindStateConflict, "captured hook inputs differ from their operation authority")
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
		if bundle.Version != 1 || bundle.OperationID != operationID || !slices.Equal(bundle.SecretSources, expected.SecretSources) {
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
