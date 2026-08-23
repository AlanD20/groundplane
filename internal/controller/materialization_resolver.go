package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"io"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type materializationBlueprintReader interface {
	GetEnvironmentBlueprintRevision(
		context.Context,
		string,
		string,
	) (etcd.Versioned[etcd.EnvironmentBlueprintRevision], bool, error)
}

type materializationEntryValueReader interface {
	GetPlain(context.Context, string, string) (etcd.PlainEntryValueGeneration, bool, error)
	GetSecret(context.Context, string, string) (etcd.SecretEntryValueGeneration, bool, error)
}

type materializationComponentFileReader interface {
	ResolveComponentFile(
		context.Context,
		string,
		etcd.TaskComponentFileValueReference,
	) ([]byte, error)
}

// TaskMaterializationResolver resolves only the immutable Controller source
// named by one durable Task reference and returns one clearing byte stream.
type TaskMaterializationResolver struct {
	blueprints materializationBlueprintReader
	values     materializationEntryValueReader
	components materializationComponentFileReader
	protector  *secretvalue.Protector
}

func NewTaskMaterializationResolver(
	blueprints materializationBlueprintReader,
	values materializationEntryValueReader,
	components materializationComponentFileReader,
	protector *secretvalue.Protector,
) (*TaskMaterializationResolver, error) {
	if blueprints == nil || values == nil || components == nil || protector == nil {
		return nil, errs.New(errs.KindInternal, "materialization value resolver dependencies are required")
	}
	return &TaskMaterializationResolver{
		blueprints: blueprints, values: values, components: components, protector: protector,
	}, nil
}

// ResolveTaskMaterializationSource resolves one closed immutable source for
// Controller-side plan construction. The caller owns and must clear the bytes.
func (resolver *TaskMaterializationResolver) ResolveTaskMaterializationSource(
	ctx context.Context,
	environmentID string,
	source etcd.TaskMaterializationSource,
) ([]byte, error) {
	if ctx == nil || resolver == nil || resolver.blueprints == nil || resolver.values == nil ||
		resolver.components == nil || resolver.protector == nil {
		return nil, errs.New(errs.KindInternal, "materialization value resolver is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return resolver.resolveSource(ctx, environmentID, source)
}

func (resolver *TaskMaterializationResolver) ResolveMaterialization(
	ctx context.Context,
	task etcd.TaskRecord,
	plan *agentpb.ExecutionPlan,
	step *agentpb.ExecutionStep,
) (io.ReadCloser, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "materialization resolution context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if resolver == nil || resolver.blueprints == nil || resolver.values == nil || resolver.components == nil ||
		resolver.protector == nil {
		return nil, errs.New(errs.KindInternal, "materialization value resolver is not configured")
	}
	materialization := step.GetMaterializeFile()
	if plan == nil || step == nil || materialization == nil || task.PlanID != plan.PlanId ||
		task.RenderGeneration != int32(plan.RenderGeneration) ||
		task.PlanHash != hex.EncodeToString(plan.PlanHash) {
		return nil, errs.New(errs.KindInternal, "materialization Task and plan identity are inconsistent")
	}
	reference, err := taskMaterializationReference(task, step.StepId)
	if err != nil {
		return nil, err
	}
	if reference.EnvironmentID != materialization.EnvironmentId {
		return nil, errs.New(errs.KindInternal, "materialization source Environment is inconsistent")
	}
	if !taskMaterializationMetadataMatches(reference, materialization) {
		return nil, errs.New(errs.KindInternal, "materialization Task and plan metadata are inconsistent")
	}
	content, err := resolver.resolveSource(ctx, reference.EnvironmentID, reference.Source)
	if err != nil {
		clear(content)
		return nil, err
	}
	if uint64(len(content)) != materialization.Length {
		clear(content)
		return nil, errs.New(errs.KindInternal, "materialization source length changed")
	}
	digest := sha256.Sum256(content)
	if len(materialization.Sha256) != sha256.Size ||
		subtle.ConstantTimeCompare(digest[:], materialization.Sha256) != 1 {
		clear(content)
		return nil, errs.New(errs.KindInternal, "materialization source digest changed")
	}
	return entrymaterialization.OwnBytes(content), nil
}

func taskMaterializationReference(
	task etcd.TaskRecord,
	stepID string,
) (etcd.TaskMaterializationRecord, error) {
	var selected *etcd.TaskMaterializationRecord
	for index := range task.Materializations {
		if task.Materializations[index].StepID != stepID {
			continue
		}
		if selected != nil {
			return etcd.TaskMaterializationRecord{}, errs.New(
				errs.KindInternal,
				"materialization Task contains duplicate step references",
			)
		}
		selected = &task.Materializations[index]
	}
	if selected == nil {
		return etcd.TaskMaterializationRecord{}, errs.New(
			errs.KindInternal,
			"materialization Task is missing its source reference",
		)
	}
	return *selected, nil
}

func (resolver *TaskMaterializationResolver) resolveSource(
	ctx context.Context,
	environmentID string,
	source etcd.TaskMaterializationSource,
) ([]byte, error) {
	switch source.Kind {
	case etcd.TaskMaterializationSourceBlueprintFile:
		if source.BlueprintFile == nil || source.ComponentFile != nil || source.EntryValue != nil ||
			source.GeneratedEnvironment != nil {
			return nil, corruptMaterializationSource()
		}
		return resolver.resolveBlueprintFile(ctx, environmentID, *source.BlueprintFile)
	case etcd.TaskMaterializationSourceComponentFile:
		if source.ComponentFile == nil || source.BlueprintFile != nil || source.EntryValue != nil ||
			source.GeneratedEnvironment != nil {
			return nil, corruptMaterializationSource()
		}
		return resolver.components.ResolveComponentFile(ctx, environmentID, *source.ComponentFile)
	case etcd.TaskMaterializationSourceEntryValue:
		if source.EntryValue == nil || source.BlueprintFile != nil || source.ComponentFile != nil ||
			source.GeneratedEnvironment != nil {
			return nil, corruptMaterializationSource()
		}
		return resolver.resolveEntryValue(ctx, environmentID, *source.EntryValue)
	case etcd.TaskMaterializationSourceGeneratedEnvironment:
		if source.GeneratedEnvironment == nil || source.BlueprintFile != nil || source.ComponentFile != nil ||
			source.EntryValue != nil {
			return nil, corruptMaterializationSource()
		}
		return resolver.resolveGeneratedEnvironment(ctx, environmentID, *source.GeneratedEnvironment)
	case etcd.TaskMaterializationSourceRemoval:
		if source.BlueprintFile != nil || source.ComponentFile != nil || source.EntryValue != nil ||
			source.GeneratedEnvironment != nil {
			return nil, corruptMaterializationSource()
		}
		return []byte{}, nil
	default:
		return nil, corruptMaterializationSource()
	}
}

func (resolver *TaskMaterializationResolver) resolveBlueprintFile(
	ctx context.Context,
	environmentID string,
	reference etcd.TaskBlueprintFileValueReference,
) ([]byte, error) {
	revision, found, err := resolver.blueprints.GetEnvironmentBlueprintRevision(
		ctx,
		environmentID,
		reference.RevisionID,
	)
	if err != nil {
		return nil, err
	}
	for index := range revision.Record.Files {
		defer clear(revision.Record.Files[index].Content)
	}
	if !found || revision.Record.EnvironmentID != environmentID ||
		revision.Record.RevisionID != reference.RevisionID {
		return nil, corruptMaterializationSource()
	}
	for _, file := range revision.Record.Files {
		if file.Path == reference.Path {
			return append([]byte(nil), file.Content...), nil
		}
	}
	return nil, corruptMaterializationSource()
}

func (resolver *TaskMaterializationResolver) resolveEntryValue(
	ctx context.Context,
	environmentID string,
	reference etcd.TaskEntryValueReference,
) ([]byte, error) {
	switch reference.Storage {
	case etcd.TaskEntryValueStoragePlain:
		record, found, err := resolver.values.GetPlain(ctx, reference.EntryID, reference.ValueGenerationID)
		if err != nil {
			return nil, err
		}
		if !found || record.EnvironmentID != environmentID || record.EntryID != reference.EntryID ||
			record.GenerationID != reference.ValueGenerationID {
			clear(record.Content)
			return nil, corruptMaterializationSource()
		}
		return record.Content, nil
	case etcd.TaskEntryValueStorageSecret:
		record, found, err := resolver.values.GetSecret(ctx, reference.EntryID, reference.ValueGenerationID)
		if err != nil {
			return nil, err
		}
		if !found || record.EnvironmentID != environmentID || record.EntryID != reference.EntryID ||
			record.GenerationID != reference.ValueGenerationID {
			clear(record.Ciphertext)
			return nil, corruptMaterializationSource()
		}
		metadata := secretvalue.Metadata{
			Version: secretvalue.EnvelopeVersion(record.EnvelopeVersion),
			Cipher:  secretvalue.CipherSuite(record.Cipher),
			Digest: secretvalue.Digest{
				Algorithm: secretvalue.DigestAlgorithm(record.DigestAlgorithm),
				Value:     record.CiphertextSHA256,
			},
		}
		envelope, err := secretvalue.Restore(metadata, record.Ciphertext)
		clear(record.Ciphertext)
		if err != nil {
			return nil, err
		}
		var plaintext []byte
		err = resolver.protector.Open(ctx, envelope, func(value []byte) error {
			plaintext = append([]byte(nil), value...)
			return nil
		})
		if err != nil {
			clear(plaintext)
			return nil, err
		}
		return plaintext, nil
	default:
		return nil, corruptMaterializationSource()
	}
}

func (resolver *TaskMaterializationResolver) resolveGeneratedEnvironment(
	ctx context.Context,
	environmentID string,
	reference etcd.TaskGeneratedEnvironmentValueReference,
) ([]byte, error) {
	if reference.FormatVersion != 1 {
		return nil, corruptMaterializationSource()
	}
	output := make([]byte, 0)
	previousName := ""
	for _, entry := range reference.Values {
		if entry.Name <= previousName {
			clear(output)
			return nil, corruptMaterializationSource()
		}
		value, err := resolver.resolveEntryValue(ctx, environmentID, entry.Value)
		if err != nil {
			clear(value)
			clear(output)
			return nil, err
		}
		if !utf8.Valid(value) || bytes.IndexByte(value, 0) >= 0 {
			clear(value)
			clear(output)
			return nil, errs.New(errs.KindInternal, "generated Environment value is not valid text")
		}
		output = append(output, entry.Name...)
		output = append(output, '=', '"')
		output = appendComposeDotEnvBytes(output, value)
		output = append(output, '"', '\n')
		clear(value)
		if uint64(len(output)) > entrymaterialization.MaximumContentBytes {
			clear(output)
			return nil, errs.New(errs.KindInternal, "generated Environment output exceeds its limit")
		}
		previousName = entry.Name
	}
	return output, nil
}

func appendComposeDotEnvBytes(output []byte, value []byte) []byte {
	for _, character := range value {
		switch character {
		case '\\':
			output = append(output, '\\', '\\')
		case '"':
			output = append(output, '\\', '"')
		case '$':
			output = append(output, '$', '$')
		case '\n':
			output = append(output, '\\', 'n')
		case '\r':
			output = append(output, '\\', 'r')
		case '\t':
			output = append(output, '\\', 't')
		case '\a':
			output = append(output, '\\', 'a')
		case '\b':
			output = append(output, '\\', 'b')
		case '\f':
			output = append(output, '\\', 'f')
		case '\v':
			output = append(output, '\\', 'v')
		default:
			output = append(output, character)
		}
	}
	return output
}

func corruptMaterializationSource() error {
	return errs.New(errs.KindInternal, "durable materialization source is corrupt")
}
