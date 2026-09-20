package backingservices

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (service *CreationService) backingCreationEntries(
	ctx context.Context,
	projectID string,
	environmentID string,
	spec adapters.CreationSpec,
	allocator *desiredrevision.BlueprintIdentityAllocator,
	createdAt time.Time,
) ([]etcd.EntryRecord, []etcd.EntryValueGeneration, []secretrecord.Record, []secretrecord.EncryptedValue, map[string]string, error) {
	bootstrapValues := make(map[string][]byte)
	bootstrapSecrets := make(map[string]string)
	var secrets []secretrecord.Record
	var secretValues []secretrecord.EncryptedValue
	for _, declaration := range spec.Environment {
		if declaration.BootstrapKey == "" {
			continue
		}
		if _, exists := bootstrapValues[declaration.BootstrapKey]; exists {
			continue
		}
		value, err := randomBackingCredential()
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		bootstrapValues[declaration.BootstrapKey] = value
		secretID := allocator.Named(ids.KindSecret, "bootstrap-secret-"+declaration.BootstrapKey)
		secret, err := secretrecord.NewProjectRecord(
			secretID,
			projectID,
			declaration.Name,
			core.SecretKindEnvVar,
			"",
			createdAt,
		)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		encrypted, err := sealBackingSecret(ctx, service.protector, secretID, value)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		bootstrapSecrets[declaration.BootstrapKey] = secretID
		secrets = append(secrets, secret)
		secretValues = append(secretValues, encrypted)
	}
	defer func() {
		for key, value := range bootstrapValues {
			clear(value)
			delete(bootstrapValues, key)
		}
	}()
	entries := make([]etcd.EntryRecord, 0, len(spec.Environment))
	generations := make([]etcd.EntryValueGeneration, 0, len(spec.Environment))
	resolved := make(map[string]string, len(spec.Environment))
	for _, declaration := range spec.Environment {
		entryID := allocator.Named(ids.KindEnvEntry, "bootstrap-entry-"+declaration.Name)
		generationID := allocator.Named(ids.KindConfig, "bootstrap-entry-generation-"+declaration.Name)
		entry := core.EnvEntry{
			ID: entryID, Kind: core.EntryKindEnv, Key: declaration.Name,
			Exposure: []string{"all"}, Secret: declaration.Secret,
		}
		var value []byte
		if declaration.BootstrapKey == "" {
			entry.Source = core.EntrySource{Kind: core.SourceLiteral, Literal: declaration.Literal}
			value = []byte(declaration.Literal)
		} else {
			entry.Source = core.EntrySource{
				Kind: core.SourceSecretRef, SecretRef: bootstrapSecrets[declaration.BootstrapKey],
			}
			value = bootstrapValues[declaration.BootstrapKey]
		}
		record, err := etcd.NewEntryRecord(environmentID, entry, generationID)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		var generation etcd.EntryValueGeneration
		if declaration.Secret {
			encrypted, err := sealBackingEntry(
				ctx,
				service.protector,
				environmentID,
				entryID,
				generationID,
				value,
				createdAt,
			)
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			generation.Secret = &encrypted
		} else {
			digest := sha256.Sum256(value)
			generation.Plain = &etcd.PlainEntryValueGeneration{
				EnvironmentID: environmentID, EntryID: entryID, GenerationID: generationID,
				Content: append([]byte(nil), value...), PlaintextSHA256: hex.EncodeToString(digest[:]), CreatedAt: createdAt,
			}
		}
		entries = append(entries, record)
		generations = append(generations, generation)
		resolved[entryID] = string(value)
	}
	order := make([]int, len(entries))
	for index := range order {
		order[index] = index
	}
	sort.Slice(order, func(left, right int) bool {
		return entries[order[left]].Entry.ID < entries[order[right]].Entry.ID
	})
	sortedEntries := make([]etcd.EntryRecord, len(entries))
	sortedGenerations := make([]etcd.EntryValueGeneration, len(generations))
	for index, source := range order {
		sortedEntries[index] = entries[source]
		sortedGenerations[index] = generations[source]
	}
	return sortedEntries, sortedGenerations, secrets, secretValues, resolved, nil
}

func randomBackingCredential() ([]byte, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	encoded := make([]byte, base64.RawURLEncoding.EncodedLen(len(raw)))
	base64.RawURLEncoding.Encode(encoded, raw)
	clear(raw)
	return encoded, nil
}

func sealBackingSecret(
	ctx context.Context,
	protector *secretvalue.Protector,
	secretID string,
	value []byte,
) (secretrecord.EncryptedValue, error) {
	envelope, err := protector.Seal(ctx, value)
	if err != nil {
		return secretrecord.EncryptedValue{}, err
	}
	defer envelope.Clear()
	metadata := envelope.Metadata()
	return secretrecord.EncryptedValue{
		SecretID: secretID, EnvelopeVersion: uint8(metadata.Version), Cipher: string(metadata.Cipher),
		DigestAlgorithm: string(metadata.Digest.Algorithm), CiphertextSHA256: metadata.Digest.Value,
		Ciphertext: envelope.Ciphertext(),
	}, nil
}

func sealBackingEntry(
	ctx context.Context,
	protector *secretvalue.Protector,
	environmentID, entryID, generationID string,
	value []byte,
	createdAt time.Time,
) (etcd.SecretEntryValueGeneration, error) {
	envelope, err := protector.Seal(ctx, value)
	if err != nil {
		return etcd.SecretEntryValueGeneration{}, err
	}
	defer envelope.Clear()
	metadata := envelope.Metadata()
	return etcd.SecretEntryValueGeneration{
		EnvironmentID: environmentID, EntryID: entryID, GenerationID: generationID,
		EnvelopeVersion: uint8(metadata.Version), Cipher: string(metadata.Cipher),
		DigestAlgorithm: string(metadata.Digest.Algorithm), CiphertextSHA256: metadata.Digest.Value,
		Ciphertext: envelope.Ciphertext(), CreatedAt: createdAt,
	}, nil
}

func backingEnvironmentMaterialization(
	environmentID string,
	artifactID string,
	entries []etcd.EntryRecord,
	resolved map[string]string,
	allocator *desiredrevision.BlueprintIdentityAllocator,
) (etcd.TaskMaterializationRecord, *agentpb.ExecutionStep, error) {
	desired := make([]core.EnvEntry, len(entries))
	references := make([]etcd.TaskGeneratedEnvironmentEntryReference, len(entries))
	for index, entry := range entries {
		desired[index] = entry.Entry
		storage := etcd.TaskEntryValueStoragePlain
		if entry.Entry.Secret {
			storage = etcd.TaskEntryValueStorageSecret
		}
		references[index] = etcd.TaskGeneratedEnvironmentEntryReference{
			Name: entry.Entry.Key,
			Value: etcd.TaskEntryValueReference{
				EntryID:           entry.Entry.ID,
				ValueGenerationID: entry.CurrentValueGenerationID,
				Storage:           storage,
			},
		}
	}
	sort.Slice(references, func(left, right int) bool { return references[left].Name < references[right].Name })
	content, err := controller.RenderEnvFile(desired, resolved)
	if err != nil {
		return etcd.TaskMaterializationRecord{}, nil, err
	}
	defer clear(content)
	digest := sha256.Sum256(content)
	destination, err := entrymaterialization.GeneratedEnvDestination(environmentID, "")
	if err != nil {
		return etcd.TaskMaterializationRecord{}, nil, err
	}
	record := etcd.TaskMaterializationRecord{
		StepID:            allocator.Named(ids.KindStep, "bootstrap-environment"),
		MaterializationID: allocator.Named(ids.KindConfig, "bootstrap-environment-materialization"),
		EnvironmentID:     environmentID, Destination: destination,
		OutputKind: etcd.TaskMaterializationOutputGeneratedEnvironment,
		Mode: uint32(
			entrymaterialization.ModePrivate,
		), Length: uint64(len(content)), SHA256: hex.EncodeToString(digest[:]),
		Source: etcd.TaskMaterializationSource{
			Kind: etcd.TaskMaterializationSourceGeneratedEnvironment,
			GeneratedEnvironment: &etcd.TaskGeneratedEnvironmentValueReference{
				FormatVersion: 1, Values: references,
			},
		},
	}
	step, err := controller.BuildTaskMaterializationStep(record, artifactID, uint32(desiredrevision.TaskTimeoutSeconds))
	if err != nil {
		return etcd.TaskMaterializationRecord{}, nil, err
	}
	return record, step, nil
}
