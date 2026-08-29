package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestEnvironmentBlueprintGDR1RecordsAreDeterministicBinary(t *testing.T) {
	t.Parallel()
	descriptor := validEnvironmentBlueprintStageDescriptorForTest(t)
	encoded, err := encodeEnvironmentBlueprintStageDescriptor(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded[:4]) != "GDR1" || encoded[4] != byte(environmentBlueprintDescriptorRecord) ||
		strings.Contains(string(encoded), `"environment_id"`) {
		t.Fatalf("descriptor is not strict binary GDR1: %x", encoded)
	}
	second, err := encodeEnvironmentBlueprintStageDescriptor(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != string(second) {
		t.Fatal("descriptor encoding is not deterministic")
	}
	decoded, err := decodeEnvironmentBlueprintStageDescriptor(encoded)
	if err != nil || !sameEnvironmentBlueprintStageClaim(decoded.Claim, descriptor.Claim) ||
		!sameEnvironmentBlueprintStageStreams(decoded, descriptor) || decoded.UpdatedAt != descriptor.UpdatedAt {
		t.Fatalf("descriptor round trip = %#v, %v", decoded, err)
	}
}

func TestEnvironmentBlueprintAuditStreamIsDeterministicBinary(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	revision := EnvironmentBlueprintRevision{
		EnvironmentID:  ids.NewAt(ids.KindEnvironment, now, 1),
		RevisionID:     ids.NewAt(ids.KindTask, now, 2),
		RootPath:       "blueprint.yaml",
		ComposeSources: []string{"blueprint.yaml"},
		Interpolation:  map[string]string{"ZED": "last", "ALPHA": "first"},
		Files: []EnvironmentBlueprintFile{{
			Path: "blueprint.yaml", Content: []byte("services: {}\n"),
		}},
		CreatedAt: now,
	}
	encoded, err := encodeEnvironmentBlueprintAuditStream(revision)
	if err != nil {
		t.Fatal(err)
	}
	second, err := encodeEnvironmentBlueprintAuditStream(revision)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded[:4]) != "GPAU" || strings.Contains(string(encoded), `"environment_id"`) ||
		!reflect.DeepEqual(encoded, second) {
		t.Fatalf("audit stream is not deterministic binary: %x", encoded)
	}
	decoded, err := decodeEnvironmentBlueprintAuditStream(encoded)
	if err != nil || !reflect.DeepEqual(decoded, revision) {
		t.Fatalf("audit stream round trip = %#v, %v", decoded, err)
	}
}

func TestEnvironmentBlueprintChunkUsesExactNinetySevenByteFixedCost(t *testing.T) {
	t.Parallel()
	data := make([]byte, EnvironmentBlueprintChunkBytes)
	for index := range data {
		data[index] = byte(index)
	}
	encoded, err := encodeEnvironmentBlueprintChunk(EnvironmentBlueprintChunk{
		Family: EnvironmentBlueprintChunkProjection, Sequence: 2,
		LogicalOffset: 2 * EnvironmentBlueprintChunkBytes,
		LogicalLength: uint32(len(data)), Digest: sha256.Sum256(data), Data: data,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != 97+EnvironmentBlueprintChunkBytes {
		t.Fatalf("chunk bytes = %d, want %d", len(encoded), 97+EnvironmentBlueprintChunkBytes)
	}
	decoded, err := decodeEnvironmentBlueprintChunk(encoded)
	if err != nil || decoded.Sequence != 2 || string(decoded.Data) != string(data) {
		t.Fatalf("chunk round trip = %#v, %v", decoded, err)
	}
}

func TestEnvironmentBlueprintStageTransactionBudgetIsExact(t *testing.T) {
	t.Parallel()
	conditions := make([]Condition, 13)
	mutations := make([]Mutation, 13)
	for index := range conditions {
		conditions[index] = Condition{Key: strings.Repeat(string(rune('a'+index)), EnvironmentBlueprintKeyMaxBytes)}
	}
	for index := range mutations {
		valueBytes := 97 + EnvironmentBlueprintChunkBytes
		if index == len(mutations)-1 {
			valueBytes = environmentBlueprintDescriptorMaxBytes
		}
		mutations[index] = Mutation{
			Type:  MutationPut,
			Key:   strings.Repeat(string(rune('n'+index)), EnvironmentBlueprintKeyMaxBytes),
			Value: make([]byte, valueBytes),
		}
	}
	if err := validateBlueprintTransaction(
		newMemoryHierarchyStore(), conditions, mutations, 26, EnvironmentBlueprintStageTransactionBytes,
	); err != nil {
		t.Fatalf("validateBlueprintTransaction(exact) error = %v", err)
	}
	mutations[len(mutations)-1].Value = append(mutations[len(mutations)-1].Value, 0)
	if err := validateBlueprintTransaction(
		newMemoryHierarchyStore(), conditions, mutations, 26, EnvironmentBlueprintStageTransactionBytes,
	); !isKind(err, errs.KindInternal) {
		t.Fatalf("validateBlueprintTransaction(above) error = %v", err)
	}
}

func TestEnvironmentVolumeMutationAuditIsTypedAndDeterministic(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	precondition := sha256.Sum256([]byte("fixed impact revision"))
	audit := EnvironmentDesiredMutationAudit{Volume: &EnvironmentVolumeMutationAudit{
		Action:   EnvironmentVolumeMutationRemove,
		VolumeID: ids.NewAt(ids.KindVolume, now, 1), Slug: "application-data", Key: "app_data",
		KeySupplied: true, PreconditionDigest: precondition,
	}}
	encoded, err := encodeEnvironmentDesiredMutationAudit(audit)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded[:4]) != "GPMU" || strings.Contains(string(encoded), `"slug"`) {
		t.Fatalf("mutation audit framing = %x", encoded)
	}
	decoded, err := decodeEnvironmentDesiredMutationAudit(encoded)
	if err != nil || decoded.Volume == nil || *decoded.Volume != *audit.Volume {
		t.Fatalf("mutation audit round trip = %#v, %v", decoded, err)
	}
}

func TestEnvironmentServiceMutationAuditIsTypedAndDeterministic(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	audit := EnvironmentDesiredMutationAudit{Service: &EnvironmentServiceMutationAudit{
		Action:         EnvironmentServiceMutationRemove,
		BaseRevisionID: ids.NewAt(ids.KindTask, now, 1),
		ServiceID:      ids.NewAt(ids.KindService, now, 2),
	}}
	encoded, err := encodeEnvironmentDesiredMutationAudit(audit)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeEnvironmentDesiredMutationAudit(encoded)
	if err != nil || decoded.Service == nil || *decoded.Service != *audit.Service {
		t.Fatalf("mutation audit round trip = %#v, %v", decoded, err)
	}
}

func TestEnvironmentServiceMutationAuditAllowsInitialCreateWithoutBaseRevision(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 29, 8, 0, 0, 0, time.UTC)
	audit := EnvironmentDesiredMutationAudit{Service: &EnvironmentServiceMutationAudit{
		Action:    EnvironmentServiceMutationCreate,
		ServiceID: ids.NewAt(ids.KindService, now, 1),
		Request: &EnvironmentServiceMutationRequest{
			EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 2),
			Name:          "web", Image: "nginx:1.27-alpine", Strategy: core.StrategyRecreate,
			OnFailure: core.OnFailureSwitchBack, Restart: "unless-stopped", Replicas: 1,
		},
	}}
	encoded, err := encodeEnvironmentDesiredMutationAudit(audit)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeEnvironmentDesiredMutationAudit(encoded)
	if err != nil || decoded.Service == nil || !reflect.DeepEqual(decoded.Service, audit.Service) {
		t.Fatalf("initial Service mutation audit round trip = %#v, %v", decoded, err)
	}
	audit.Service.Action = EnvironmentServiceMutationEdit
	if _, err := encodeEnvironmentDesiredMutationAudit(audit); !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("Service edit without base revision error = %v", err)
	}
}

func TestEnvironmentEntryMutationAuditIsTypedRedactedAndDeterministic(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	entryID := ids.NewAt(ids.KindEnvEntry, now, 2)
	record, err := NewEntryRecord(environmentID, core.EnvEntry{
		ID: entryID, Kind: core.EntryKindEnv, Key: "APP_MODE",
		Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"all"},
	}, ids.NewAt(ids.KindConfig, now, 3))
	if err != nil {
		t.Fatal(err)
	}
	audit := EnvironmentDesiredMutationAudit{Entry: &EnvironmentEntryMutationAudit{
		Action: EnvironmentEntryMutationCreate, BaseRevisionID: ids.NewAt(ids.KindTask, now, 4),
		EntryID: entryID, Record: &record,
	}}
	encoded, err := encodeEnvironmentDesiredMutationAudit(audit)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeEnvironmentDesiredMutationAudit(encoded)
	if err != nil || decoded.Entry == nil || !reflect.DeepEqual(decoded.Entry, audit.Entry) {
		t.Fatalf("Entry mutation audit round trip = %#v, %v", decoded, err)
	}
	record.Entry.Source.Literal = "must-not-persist"
	if _, err := encodeEnvironmentDesiredMutationAudit(EnvironmentDesiredMutationAudit{Entry: &EnvironmentEntryMutationAudit{
		Action: EnvironmentEntryMutationEdit, BaseRevisionID: audit.Entry.BaseRevisionID,
		EntryID: entryID, Record: &record,
	}}); !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("Entry mutation literal audit error = %v", err)
	}
}

func TestEnvironmentBlueprintLocatorUsesScopedRawDigestSegments(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	locator := IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment,
		ScopeID:   ids.NewAt(ids.KindEnvironment, now, 1),
		Method:    "PUT", Route: "/environments/{id}/blueprint", Key: "01K39Y7A9NFPN2Q7B3DJQ0H4AB",
	}
	key, digest, err := environmentBlueprintLocatorKey(locator)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(key, environmentBlueprintLocatorPrefix+"~") ||
		strings.Contains(key, hex.EncodeToString(digest[:])) || strings.Contains(key, locator.Key) {
		t.Fatalf("locator key leaks raw or hexadecimal identity: %q", key)
	}
}

func stageEnvironmentBlueprintForPublicationTest(
	t *testing.T,
	repository *HierarchyRepository,
	expectedHeadRevision int64,
	revision EnvironmentBlueprintRevision,
	projection EnvironmentComposeProjection,
	marker IdempotencyMarker,
) EnvironmentBlueprintStageClaim {
	t.Helper()
	dependencyDigest, err := EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		t.Fatal(err)
	}
	claim := EnvironmentBlueprintStageClaim{
		DescriptorID:  strings.TrimPrefix(marker.TaskID, "task_"),
		EnvironmentID: revision.EnvironmentID, RevisionID: revision.RevisionID,
		TaskID: marker.TaskID, Locator: marker.Locator, Intent: marker.Intent,
		BaselineHeadRevision: expectedHeadRevision, SourceKind: EnvironmentBlueprintSourceApply,
		RenderGeneration: projection.RenderGeneration, ProjectionSchema: 1,
		CreatedAt: revision.CreatedAt,
	}
	streams, err := buildEnvironmentBlueprintStreams(EnvironmentBlueprintStageRequest{
		Claim: claim, Blueprint: &revision, Projection: projection, DependencyDigest: dependencyDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(streams.Audit)
	defer clear(streams.Projection)
	descriptor := streams.Descriptor
	descriptor.State = EnvironmentBlueprintStageSealed
	descriptor.NextAuditChunk = descriptor.AuditChunks
	descriptor.NextProjectionChunk = descriptor.ProjectionChunks
	descriptorValue, err := encodeEnvironmentBlueprintStageDescriptor(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(descriptorValue)
	intentDigest, err := protectedBlueprintIntentDigest(claim.Intent)
	if err != nil {
		t.Fatal(err)
	}
	locatorValue, err := encodeEnvironmentBlueprintStageLocator(claim.DescriptorID, intentDigest)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(locatorValue)
	rootValue, err := encodeEnvironmentBlueprintSeal(environmentBlueprintSealFromDescriptor(descriptor))
	if err != nil {
		t.Fatal(err)
	}
	defer clear(rootValue)
	mutations := []Mutation{
		{Type: MutationPut, Key: environmentBlueprintDescriptorKeyByID(claim.DescriptorID), Value: descriptorValue},
		{Type: MutationPut, Key: environmentBlueprintRootKey(claim.EnvironmentID, claim.RevisionID), Value: rootValue},
	}
	locatorKey, _, err := environmentBlueprintLocatorKey(claim.Locator)
	if err != nil {
		t.Fatal(err)
	}
	mutations = append(mutations, Mutation{Type: MutationPut, Key: locatorKey, Value: locatorValue})
	for _, family := range []struct {
		id    uint8
		value []byte
	}{
		{id: EnvironmentBlueprintChunkAudit, value: streams.Audit},
		{id: EnvironmentBlueprintChunkProjection, value: streams.Projection},
	} {
		for index := uint32(0); index < chunkCount32(len(family.value)); index++ {
			from := int(index) * EnvironmentBlueprintChunkBytes
			to := from + EnvironmentBlueprintChunkBytes
			if to > len(family.value) {
				to = len(family.value)
			}
			data := family.value[from:to]
			chunkValue, encodeErr := encodeEnvironmentBlueprintChunk(EnvironmentBlueprintChunk{
				Family: family.id, Sequence: index,
				LogicalOffset: uint64(from), LogicalLength: uint32(len(data)),
				Digest: sha256.Sum256(data), Data: data,
			})
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			mutations = append(mutations, Mutation{Type: MutationPut,
				Key:   environmentBlueprintChunkKeyFor(claim.EnvironmentID, claim.RevisionID, family.id, index),
				Value: chunkValue})
		}
	}
	defer clearMutationValues(mutations)
	result, err := repository.store.Transact(context.Background(), nil, mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("persist sealed desired revision = %#v, %v", result, err)
	}
	return claim
}

func validEnvironmentBlueprintStageDescriptorForTest(t *testing.T) EnvironmentBlueprintStageDescriptor {
	t.Helper()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	revisionID := ids.NewAt(ids.KindTask, now, 2)
	audit := sha256.Sum256([]byte("audit"))
	projection := sha256.Sum256([]byte("projection"))
	dependency := sha256.Sum256([]byte("dependency"))
	return EnvironmentBlueprintStageDescriptor{
		Claim: EnvironmentBlueprintStageClaim{
			DescriptorID:  strings.TrimPrefix(ids.NewAt(ids.KindTask, now, 99), "task_"),
			EnvironmentID: environmentID, RevisionID: revisionID,
			TaskID: ids.NewAt(ids.KindTask, now, 3),
			Locator: IdempotencyLocator{
				ScopeKind: IdempotencyScopeEnvironment, ScopeID: environmentID,
				Method: "PUT", Route: "/environments/{id}/blueprint", Key: "01K39Y7A9NFPN2Q7B3DJQ0H4AB",
			},
			Intent:     validEnvironmentBlueprintProtectedIntentForTest("protected intent"),
			SourceKind: EnvironmentBlueprintSourceApply, RenderGeneration: 1,
			ProjectionSchema: 1, CreatedAt: now,
		},
		State: EnvironmentBlueprintStageOpen, Bound: true,
		AuditChunks: 1, AuditBytes: 5, AuditSHA256: audit,
		ProjectionChunks: 1, ProjectionBytes: 10, ProjectionSHA256: projection,
		ProjectionResources: 1, DependencyDigest: dependency, UpdatedAt: now,
	}
}

func validEnvironmentBlueprintProtectedIntentForTest(value string) ProtectedIntentRecord {
	ciphertext := []byte(value)
	digest := sha256.Sum256(ciphertext)
	return ProtectedIntentRecord{
		EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
	}
}
