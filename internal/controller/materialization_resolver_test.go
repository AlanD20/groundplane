package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	resolverEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	resolverPlanID        = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	resolverStepID        = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	resolverPlainEntryID  = "ev_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	resolverSecretEntryID = "ev_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	resolverPlainValueID  = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	resolverSecretValueID = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
)

type resolverBlueprintReader struct {
	revision etcd.EnvironmentBlueprintRevision
}

func (reader *resolverBlueprintReader) GetEnvironmentBlueprintRevision(
	_ context.Context,
	_ string,
	_ string,
) (etcd.Versioned[etcd.EnvironmentBlueprintRevision], bool, error) {
	revision := reader.revision
	revision.Files = append([]etcd.EnvironmentBlueprintFile(nil), reader.revision.Files...)
	for index := range revision.Files {
		revision.Files[index].Content = append([]byte(nil), revision.Files[index].Content...)
	}
	return etcd.Versioned[etcd.EnvironmentBlueprintRevision]{Record: revision}, true, nil
}

type resolverEntryValueReader struct {
	plain  etcd.PlainEntryValueGeneration
	secret etcd.SecretEntryValueGeneration
}

type resolverComponentFileReader struct {
	content []byte
}

type resolverSecretValueReader struct{}

func (resolverSecretValueReader) GetSecret(context.Context, string) (etcd.Versioned[etcd.SecretRecord], error) {
	return etcd.Versioned[etcd.SecretRecord]{}, nil
}

func (resolverSecretValueReader) GetSecretValue(context.Context, etcd.Versioned[etcd.SecretRecord]) (etcd.SecretEncryptedValue, error) {
	return etcd.SecretEncryptedValue{}, nil
}

func (reader *resolverComponentFileReader) ResolveComponentFile(
	_ context.Context,
	_ string,
	_ etcd.TaskComponentFileValueReference,
) ([]byte, error) {
	return append([]byte(nil), reader.content...), nil
}

func (reader *resolverEntryValueReader) GetPlain(
	_ context.Context,
	_ string,
	_ string,
) (etcd.PlainEntryValueGeneration, bool, error) {
	record := reader.plain
	record.Content = append([]byte(nil), record.Content...)
	return record, true, nil
}

func (reader *resolverEntryValueReader) GetSecret(
	_ context.Context,
	_ string,
	_ string,
) (etcd.SecretEntryValueGeneration, bool, error) {
	record := reader.secret
	record.Ciphertext = append([]byte(nil), record.Ciphertext...)
	return record, true, nil
}

type resolverCipher struct {
	plaintext []byte
}

func (cipher *resolverCipher) Seal(_ context.Context, value []byte) ([]byte, error) {
	return append([]byte(nil), value...), nil
}

func (cipher *resolverCipher) Open(_ context.Context, _ []byte) ([]byte, error) {
	return append([]byte(nil), cipher.plaintext...), nil
}

// Rationale: generated env resolution must combine plain and decrypted Entry
// generations without string conversion and preserve canonical Compose escapes.
func TestTaskMaterializationResolverRendersGeneratedEnvironmentBytes(t *testing.T) {
	expected := []byte("APP_ENV=\"staging\"\nTOKEN=\"s$$ecret\\nline\"\n")
	resolver := testTaskMaterializationResolver(t, []byte("s$ecret\nline"))
	source := etcd.TaskMaterializationSource{
		Kind: etcd.TaskMaterializationSourceGeneratedEnvironment,
		GeneratedEnvironment: &etcd.TaskGeneratedEnvironmentValueReference{
			FormatVersion: 1,
			Values: []etcd.TaskGeneratedEnvironmentEntryReference{
				{Name: "APP_ENV", Value: etcd.TaskEntryValueReference{
					EntryID: resolverPlainEntryID, ValueGenerationID: resolverPlainValueID,
					Storage: etcd.TaskEntryValueStoragePlain,
				}},
				{Name: "TOKEN", Value: etcd.TaskEntryValueReference{
					EntryID: resolverSecretEntryID, ValueGenerationID: resolverSecretValueID,
					Storage: etcd.TaskEntryValueStorageSecret,
				}},
			},
		},
	}
	actual := resolveMaterializationForTest(t, resolver, source, expected)
	if !bytes.Equal(actual, expected) {
		t.Fatalf("generated Environment = %q, want %q", actual, expected)
	}
}

// Rationale: a removal step must resolve to owned zero bytes and never consult
// a value repository or decrypt unrelated Entry content.
func TestTaskMaterializationResolverResolvesRemovalToEmptySource(t *testing.T) {
	t.Parallel()
	resolver := testTaskMaterializationResolver(t, []byte("unused-secret"))
	content, err := resolver.resolveSource(
		context.Background(),
		resolverEnvironmentID,
		etcd.TaskMaterializationSource{Kind: etcd.TaskMaterializationSourceRemoval},
	)
	if err != nil || len(content) != 0 {
		t.Fatalf("resolveSource(removal) = %q/%v", content, err)
	}
}

// Rationale: immutable Blueprint file references must resolve one exact
// revision path and still pass the authenticated output digest.
func TestTaskMaterializationResolverReadsPinnedBlueprintFile(t *testing.T) {
	expected := []byte("server:\n  port: 8080\n")
	resolver := testTaskMaterializationResolver(t, nil)
	resolver.blueprints.(*resolverBlueprintReader).revision = etcd.EnvironmentBlueprintRevision{
		EnvironmentID: resolverEnvironmentID,
		RevisionID:    "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Files:         []etcd.EnvironmentBlueprintFile{{Path: "config/app.yaml", Content: expected}},
	}
	source := etcd.TaskMaterializationSource{
		Kind: etcd.TaskMaterializationSourceBlueprintFile,
		BlueprintFile: &etcd.TaskBlueprintFileValueReference{
			RevisionID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", Path: "config/app.yaml",
		},
	}
	actual := resolveMaterializationForTest(t, resolver, source, expected)
	if !bytes.Equal(actual, expected) {
		t.Fatalf("Blueprint file = %q, want %q", actual, expected)
	}
}

// Rationale: generated Component files must be regenerated by the immutable
// plan reader and still satisfy the exact durable metadata digest.
func TestTaskMaterializationResolverReadsPinnedComponentFile(t *testing.T) {
	expected := []byte("app.example.com { reverse_proxy api:8080 }\n")
	resolver := testTaskMaterializationResolver(t, nil)
	resolver.components.(*resolverComponentFileReader).content = expected
	source := etcd.TaskMaterializationSource{
		Kind: etcd.TaskMaterializationSourceComponentFile,
		ComponentFile: &etcd.TaskComponentFileValueReference{
			RevisionID:  "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			ComponentID: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			Path:        "components/caddy/Caddyfile",
		},
	}
	actual := resolveMaterializationForTest(t, resolver, source, expected)
	if !bytes.Equal(actual, expected) {
		t.Fatalf("Component file = %q, want %q", actual, expected)
	}
}

func testTaskMaterializationResolver(t *testing.T, secret []byte) *TaskMaterializationResolver {
	t.Helper()
	ciphertext := []byte("age-ciphertext")
	ciphertextDigest := sha256.Sum256(ciphertext)
	protector, err := secretvalue.NewProtector(&resolverCipher{}, &resolverCipher{plaintext: secret})
	if err != nil {
		t.Fatalf("secretvalue.NewProtector() error = %v", err)
	}
	resolver, err := NewTaskMaterializationResolver(
		&resolverBlueprintReader{},
		&resolverEntryValueReader{
			plain: etcd.PlainEntryValueGeneration{
				EnvironmentID: resolverEnvironmentID, EntryID: resolverPlainEntryID,
				GenerationID: resolverPlainValueID, Content: []byte("staging"),
			},
			secret: etcd.SecretEntryValueGeneration{
				EnvironmentID: resolverEnvironmentID, EntryID: resolverSecretEntryID,
				GenerationID: resolverSecretValueID, EnvelopeVersion: 1,
				Cipher: "age-x25519", DigestAlgorithm: "sha256",
				CiphertextSHA256: hex.EncodeToString(ciphertextDigest[:]), Ciphertext: ciphertext,
			},
		},
		resolverSecretValueReader{},
		&resolverComponentFileReader{},
		protector,
	)
	if err != nil {
		t.Fatalf("NewTaskMaterializationResolver() error = %v", err)
	}
	return resolver
}

func resolveMaterializationForTest(
	t *testing.T,
	resolver *TaskMaterializationResolver,
	sourceReference etcd.TaskMaterializationSource,
	expected []byte,
) []byte {
	t.Helper()
	planHash := bytes.Repeat([]byte{0x5a}, sha256.Size)
	digest := sha256.Sum256(expected)
	reference := etcd.TaskMaterializationRecord{
		StepID: resolverStepID, MaterializationID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAX",
		EnvironmentID: resolverEnvironmentID, Destination: "config/materialized",
		OutputKind: etcd.TaskMaterializationOutputPlainFile, Mode: 0o444,
		Length: uint64(len(expected)), SHA256: hex.EncodeToString(digest[:]), Source: sourceReference,
	}
	if sourceReference.Kind == etcd.TaskMaterializationSourceGeneratedEnvironment {
		reference.Destination = "secrets/.env." + resolverEnvironmentID + ".app"
		reference.ServiceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		reference.ServiceName = "app"
		reference.OutputKind = etcd.TaskMaterializationOutputGeneratedEnvironment
		reference.Mode = 0o600
	}
	step, err := BuildTaskMaterializationStep(reference, "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAY", 120)
	if err != nil {
		t.Fatalf("BuildTaskMaterializationStep() error = %v", err)
	}
	plan := &agentpb.ExecutionPlan{
		PlanId: resolverPlanID, PlanHash: planHash, RenderGeneration: 9,
		Steps: []*agentpb.ExecutionStep{step},
	}
	task := etcd.TaskRecord{
		PlanID: resolverPlanID, PlanHash: hex.EncodeToString(planHash), RenderGeneration: 9,
		Materializations: []etcd.TaskMaterializationRecord{reference},
	}
	stream, err := resolver.ResolveMaterialization(context.Background(), task, plan, step)
	if err != nil {
		t.Fatalf("ResolveMaterialization() error = %v", err)
	}
	content, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("io.ReadAll() error = %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	return content
}
