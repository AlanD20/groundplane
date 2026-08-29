package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type environmentBlueprintMaterializationResolverFake struct {
	content []byte
	source  etcd.TaskMaterializationSource
}

func (resolver *environmentBlueprintMaterializationResolverFake) PinSecretValue(
	_ context.Context,
	_ string,
	secretID string,
) (etcd.TaskSecretValueReference, error) {
	digest := sha256.Sum256(resolver.content)
	return etcd.TaskSecretValueReference{
		SecretID: secretID, Revision: 1, CiphertextSHA256: hex.EncodeToString(digest[:]),
	}, nil
}

func (resolver *environmentBlueprintMaterializationResolverFake) ResolveTaskMaterializationSource(
	_ context.Context,
	_ string,
	source etcd.TaskMaterializationSource,
) ([]byte, error) {
	resolver.source = source
	return append([]byte(nil), resolver.content...), nil
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
	service := &environmentBlueprintService{materials: resolver}
	projection := controller.EnvironmentComponentComposeProjection{
		PlainFiles: []controller.GeneratedEnvironmentFile{{
			ComponentID: caddyID, Path: "components/caddy/Caddyfile",
			Content: []byte("app.example.com { reverse_proxy api:8080 }\n"),
		}},
		EnvironmentFiles: []controller.EnvironmentComponentEnvironmentFile{{
			ComponentID: tunnelID, ServiceID: serviceID, ServiceName: "cloudflare-tunnel",
			Destination: "secrets/.env." + environmentID + ".cloudflare-tunnel",
			Values:      []componentsdk.ManagedSecretEnvironment{{Name: "TUNNEL_TOKEN", SecretID: secretID}},
		}},
	}
	references, steps, err := service.environmentComponentMaterializations(
		context.Background(), environmentID, projectID, revisionID, artifactID,
		func(kind ids.Kind, _ string) string { return ids.New(kind) },
		nil, projection, nil, nil,
	)
	if err != nil {
		t.Fatalf("environmentComponentMaterializations() error = %v", err)
	}
	if len(references) != 2 || len(steps) != 2 || references[0].StepID >= references[1].StepID {
		t.Fatalf("materialization references/steps = %#v / %#v", references, steps)
	}
	byKind := make(map[etcd.TaskMaterializationOutputKind]etcd.TaskMaterializationRecord, len(references))
	for _, reference := range references {
		byKind[reference.OutputKind] = reference
	}
	plain := byKind[etcd.TaskMaterializationOutputPlainFile]
	plainDigest := sha256.Sum256(projection.PlainFiles[0].Content)
	if plain.Source.ComponentFile == nil || plain.Source.ComponentFile.ComponentID != caddyID ||
		plain.Source.ComponentFile.RevisionID != revisionID || plain.Mode != 0o444 ||
		plain.SHA256 != hex.EncodeToString(plainDigest[:]) {
		t.Fatalf("plain Component materialization = %#v", plain)
	}
	generated := byKind[etcd.TaskMaterializationOutputGeneratedEnvironment]
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
	service := &environmentBlueprintService{}
	references, steps, err := service.environmentComponentMaterializations(
		context.Background(), environmentID, projectID, revisionID, artifactID,
		func(kind ids.Kind, _ string) string { return ids.New(kind) },
		[]core.BlueprintFile{{Path: "config/Caddyfile", Content: content}},
		controller.EnvironmentComponentComposeProjection{}, nil, nil,
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
		reference.Source.Kind != etcd.TaskMaterializationSourceBlueprintFile ||
		reference.Source.BlueprintFile == nil ||
		reference.Source.BlueprintFile.RevisionID != revisionID ||
		reference.Source.BlueprintFile.Path != "config/Caddyfile" {
		t.Fatalf("Blueprint materialization = %#v", reference)
	}
}
