package blueprint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskmaterialization "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	testcomponentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
)

type environmentBlueprintMaterializationResolverFake struct {
	content            []byte
	source             testtaskmaterialization.Source
	retained           []byte
	retainedGeneration uint64
	retainedRecord     testtaskmaterialization.Record
}

func (resolver *environmentBlueprintMaterializationResolverFake) PinSecretValue(
	_ context.Context,
	_ string,
	secretID string,
) (testtaskmaterialization.SecretValueReference, error) {
	digest := sha256.Sum256(resolver.content)
	return testtaskmaterialization.SecretValueReference{
		SecretID: secretID, Revision: 1, CiphertextSHA256: hex.EncodeToString(digest[:]),
	}, nil
}

func (resolver *environmentBlueprintMaterializationResolverFake) ResolveTaskMaterializationSource(
	_ context.Context,
	_ string,
	source testtaskmaterialization.Source,
) ([]byte, error) {
	resolver.source = source
	return append([]byte(nil), resolver.content...), nil
}

func (resolver *environmentBlueprintMaterializationResolverFake) RetainComponentFile(
	_ context.Context,
	record testtaskmaterialization.Record,
	generation uint64,
	content []byte,
) error {
	resolver.retainedRecord = record
	resolver.retainedGeneration = generation
	resolver.retained = append([]byte(nil), content...)
	return nil
}

// Rationale: Component output bytes must produce authenticated Task metadata
// while secret token bytes remain transient and absent from durable JSON.
func TestEnvironmentComponentMaterializationsBuildMetadataOnlyTaskInputs(t *testing.T) {
	const (
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		projectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		revisionID    = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		artifactID    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		caddyID       = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		tunnelID      = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		serviceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		secretID      = "sec_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	secretContent := []byte("TUNNEL_TOKEN=\"secret-token-value\"\n")
	resolver := &environmentBlueprintMaterializationResolverFake{content: secretContent}
	service := &Service{materials: resolver}
	projection := testcomposerender.EnvironmentComponentComposeProjection{
		PlainFiles: []testcomponentrender.GeneratedEnvironmentFile{{
			ComponentID: caddyID, Path: "components/caddy/Caddyfile",
			Content: []byte("app.example.com { reverse_proxy api:8080 }\n"),
		}},
		EnvironmentFiles: []testcomposerender.EnvironmentComponentEnvironmentFile{{
			ComponentID: tunnelID, ServiceID: serviceID, ServiceName: "cloudflare-tunnel",
			Destination: "secrets/.env." + environmentID + ".cloudflare-tunnel",
			Values:      []componentsdk.ManagedSecretEnvironment{{Name: "TUNNEL_TOKEN", SecretID: secretID}},
		}},
	}
	references, steps, err := service.environmentComponentMaterializations(
		context.Background(), environmentID, projectID, revisionID, artifactID, 7,
		func(kind ids.Kind, _ string) string { return ids.New(kind) },
		nil, projection, nil, nil,
	)
	if err != nil {
		t.Fatalf("environmentComponentMaterializations() error = %v", err)
	}
	if len(references) != 2 || len(steps) != 2 || references[0].StepID >= references[1].StepID {
		t.Fatalf("materialization references/steps = %#v / %#v", references, steps)
	}
	byKind := make(map[testtaskmaterialization.OutputKind]testtaskmaterialization.Record, len(references))
	for _, reference := range references {
		byKind[reference.OutputKind] = reference
	}
	plain := byKind[testtaskmaterialization.OutputPlainFile]
	plainDigest := sha256.Sum256(projection.PlainFiles[0].Content)
	if plain.Source.ComponentFile == nil || plain.Source.ComponentFile.ComponentID != caddyID ||
		plain.Source.ComponentFile.RevisionID != revisionID || plain.Mode != 0o444 ||
		plain.SHA256 != hex.EncodeToString(plainDigest[:]) {
		t.Fatalf("plain Component materialization = %#v", plain)
	}
	if resolver.retainedGeneration != 7 || resolver.retainedRecord.MaterializationID != plain.MaterializationID ||
		!bytes.Equal(resolver.retained, projection.PlainFiles[0].Content) {
		t.Fatalf(
			"retained Component materialization = %#v/%d/%q",
			resolver.retainedRecord,
			resolver.retainedGeneration,
			resolver.retained,
		)
	}
	generated := byKind[testtaskmaterialization.OutputGeneratedEnvironment]
	secretDigest := sha256.Sum256(secretContent)
	if generated.Source.GeneratedEnvironment == nil ||
		len(generated.Source.GeneratedEnvironment.Values) != 1 ||
		generated.Source.GeneratedEnvironment.Values[0].Secret == nil ||
		generated.Source.GeneratedEnvironment.Values[0].Secret.SecretID != secretID ||
		generated.Mode != 0o600 || generated.Length != uint64(len(secretContent)) ||
		generated.SHA256 != hex.EncodeToString(secretDigest[:]) {
		t.Fatalf("generated Environment materialization = %#v", generated)
	}
	encoded, err := json.Marshal(references)
	if err != nil {
		t.Fatalf("json.Marshal(references) error = %v", err)
	}
	if bytes.Contains(encoded, []byte("secret-token-value")) {
		t.Fatal("durable materialization references contain secret bytes")
	}
	if resolver.source.GeneratedEnvironment == nil ||
		resolver.source.GeneratedEnvironment.Values[0].Secret == nil ||
		resolver.source.GeneratedEnvironment.Values[0].Secret.SecretID != secretID {
		t.Fatalf("resolved generated Environment source = %#v", resolver.source)
	}
}

// Rationale: a declared read-only Blueprint bind must become a durable,
// metadata-only Agent materialization before the Compose execution step.
func TestEnvironmentComponentMaterializationsBuildBlueprintFileInput(t *testing.T) {
	const (
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		projectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		revisionID    = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		artifactID    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	content := []byte("example.test { reverse_proxy app:8080 }\n")
	service := &Service{}
	references, steps, err := service.environmentComponentMaterializations(
		context.Background(),
		environmentID,
		projectID,
		revisionID,
		artifactID,
		1,
		func(kind ids.Kind, _ string) string { return ids.New(kind) },
		[]core.BlueprintFile{
			{Path: "config/Caddyfile", Content: content},
		},
		testcomposerender.EnvironmentComponentComposeProjection{},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("environmentComponentMaterializations() error = %v", err)
	}
	if len(references) != 1 || len(steps) != 1 {
		t.Fatalf("materialization references/steps = %#v / %#v", references, steps)
	}
	reference := references[0]
	digest := sha256.Sum256(content)
	if reference.Destination != "config/Caddyfile" || reference.Mode != 0o444 ||
		reference.SHA256 != hex.EncodeToString(digest[:]) ||
		reference.Source.Kind != testtaskmaterialization.SourceBlueprintFile ||
		reference.Source.BlueprintFile == nil ||
		reference.Source.BlueprintFile.RevisionID != revisionID ||
		reference.Source.BlueprintFile.Path != "config/Caddyfile" {
		t.Fatalf("Blueprint materialization = %#v", reference)
	}
}
