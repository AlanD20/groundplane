package taskmaterialization

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"

	testtaskmaterialization "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testentryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
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
	revision testenvironmentprojection.EnvironmentComposeProjection
}

func (reader *resolverBlueprintReader) GetEnvironmentComposeProjectionRevision(
	_ context.Context,
	_ string,
	_ string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	revision := reader.revision
	revision.RuntimeFiles = append([]core.BlueprintFile(nil), reader.revision.RuntimeFiles...)
	for index := range revision.RuntimeFiles {
		revision.RuntimeFiles[index].Content = append([]byte(nil), revision.RuntimeFiles[index].Content...)
	}
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{Record: revision}, true, nil
}

type resolverEntryValueReader struct {
	plain  testentryvalues.PlainGeneration
	secret testentryvalues.SecretGeneration
}

type resolverComponentFileReader struct {
	content []byte
}

type resolverRetainedContent struct {
	content    []byte
	loaded     testtaskmaterialization.Record
	generation uint64
}

func (repository *resolverRetainedContent) Stage(
	_ context.Context,
	record testtaskmaterialization.Record,
	generation uint64,
	content []byte,
) error {
	repository.loaded = record
	repository.generation = generation
	repository.content = append([]byte(nil), content...)
	return nil
}

func (repository *resolverRetainedContent) Load(
	_ context.Context,
	record testtaskmaterialization.Record,
	_ uint64,
) ([]byte, error) {
	repository.loaded = record
	return append([]byte(nil), repository.content...), nil
}

type resolverSecretValueReader struct{}

func (resolverSecretValueReader) GetSecret(
	context.Context,
	string,
) (testkeyvalue.Versioned[testsecrets.Record], error) {
	return testkeyvalue.Versioned[testsecrets.Record]{}, nil
}

func (resolverSecretValueReader) GetSecretValue(
	context.Context, testkeyvalue.Versioned[testsecrets.Record],
) (testsecrets.EncryptedValue, error) {
	return testsecrets.EncryptedValue{}, nil
}

func (reader *resolverComponentFileReader) ResolveComponentFile(
	_ context.Context,
	_ string,
	_ testtaskmaterialization.ComponentFileValueReference,
) ([]byte, error) {
	return append([]byte(nil), reader.content...), nil
}

func (reader *resolverEntryValueReader) GetPlain(
	_ context.Context,
	_ string,
	_ string,
) (testentryvalues.PlainGeneration, bool, error) {
	record := reader.plain
	record.Content = append([]byte(nil), record.Content...)
	return record, true, nil
}

func (reader *resolverEntryValueReader) GetSecret(
	_ context.Context,
	_ string,
	_ string,
) (testentryvalues.SecretGeneration, bool, error) {
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
	source := testtaskmaterialization.Source{
		Kind: testtaskmaterialization.SourceGeneratedEnvironment,
		GeneratedEnvironment: &testtaskmaterialization.GeneratedEnvironmentValueReference{
			FormatVersion: 1,
			Values: []testtaskmaterialization.GeneratedEnvironmentEntryReference{
				{Name: "APP_ENV", Value: testtaskmaterialization.EntryValueReference{
					EntryID: resolverPlainEntryID, ValueGenerationID: resolverPlainValueID,
					Storage: testtaskmaterialization.EntryValueStoragePlain,
				}},
				{Name: "TOKEN", Value: testtaskmaterialization.EntryValueReference{
					EntryID: resolverSecretEntryID, ValueGenerationID: resolverSecretValueID,
					Storage: testtaskmaterialization.EntryValueStorageSecret,
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
		resolverEnvironmentID, testtaskmaterialization.Source{Kind: testtaskmaterialization.SourceRemoval},
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
	resolver.blueprints.(*resolverBlueprintReader).revision = testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: resolverEnvironmentID,
		RevisionID:    "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		RuntimeFiles:  []core.BlueprintFile{{Path: "config/app.yaml", Content: expected}},
	}
	source := testtaskmaterialization.Source{
		Kind: testtaskmaterialization.SourceBlueprintFile,
		BlueprintFile: &testtaskmaterialization.BlueprintFileValueReference{
			RevisionID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", Path: "config/app.yaml",
		},
	}
	actual := resolveMaterializationForTest(t, resolver, source, expected)
	if !bytes.Equal(actual, expected) {
		t.Fatalf("Blueprint file = %q, want %q", actual, expected)
	}
}

// Rationale: ADR0079 recovery must return the originally retained Component
// bytes even after the producing Task disappears or the renderer changes.
func TestTaskMaterializationResolverReadsPinnedComponentFile(t *testing.T) {
	expected := []byte("app.example.com { reverse_proxy api:8080 }\n")
	resolver := testTaskMaterializationResolver(t, nil)
	resolver.components.(*resolverComponentFileReader).content = []byte("new renderer output must not be used")
	retained := &resolverRetainedContent{content: expected}
	if err := resolver.EnableComponentMaterializationContent(retained); err != nil {
		t.Fatalf("EnableComponentMaterializationContent() error = %v", err)
	}
	source := testtaskmaterialization.Source{
		Kind: testtaskmaterialization.SourceComponentFile,
		ComponentFile: &testtaskmaterialization.ComponentFileValueReference{
			RevisionID:  "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			ComponentID: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			Path:        "components/caddy/Caddyfile",
		},
	}
	actual := resolveMaterializationForTest(t, resolver, source, expected)
	if !bytes.Equal(actual, expected) {
		t.Fatalf("Component file = %q, want %q", actual, expected)
	}
	if retained.loaded.Source.ComponentFile == nil ||
		retained.loaded.Source.ComponentFile.ComponentID != source.ComponentFile.ComponentID {
		t.Fatalf("loaded retained identity = %#v", retained.loaded)
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
			plain: testentryvalues.PlainGeneration{
				EnvironmentID: resolverEnvironmentID, EntryID: resolverPlainEntryID,
				GenerationID: resolverPlainValueID, Content: []byte("staging"),
			},
			secret: testentryvalues.SecretGeneration{
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
	sourceReference testtaskmaterialization.Source,
	expected []byte,
) []byte {
	t.Helper()
	planHash := bytes.Repeat([]byte{0x5a}, sha256.Size)
	digest := sha256.Sum256(expected)
	reference := testtaskmaterialization.Record{
		StepID: resolverStepID, MaterializationID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAX",
		EnvironmentID: resolverEnvironmentID, Destination: "config/materialized",
		OutputKind: testtaskmaterialization.OutputPlainFile, Mode: 0o444,
		Length: uint64(len(expected)), SHA256: hex.EncodeToString(digest[:]), Source: sourceReference,
	}
	if sourceReference.Kind == testtaskmaterialization.SourceGeneratedEnvironment {
		reference.Destination = "secrets/.env." + resolverEnvironmentID + ".app"
		reference.ServiceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		reference.ServiceName = "app"
		reference.OutputKind = testtaskmaterialization.OutputGeneratedEnvironment
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
		Materializations: []testtaskmaterialization.Record{reference},
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
