package etcd

import (
	sha256 "crypto/sha256"
	hex "encoding/hex"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	fixtureowner "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	strings "strings"
	testing "testing"
	time "time"
)

func validEnvironmentBlueprintStageDescriptorForTest(t *testing.T) fixtureowner.EnvironmentBlueprintStageDescriptor {
	t.Helper()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	revisionID := ids.NewAt(ids.KindTask, now, 2)
	audit := sha256.Sum256([]byte("audit"))
	projection := sha256.Sum256([]byte("projection"))
	dependency := sha256.Sum256([]byte("dependency"))
	return fixtureowner.EnvironmentBlueprintStageDescriptor{
		Claim: fixtureowner.EnvironmentBlueprintStageClaim{
			DescriptorID:  strings.TrimPrefix(ids.NewAt(ids.KindTask, now, 99), "task_"),
			EnvironmentID: environmentID, RevisionID: revisionID,
			TaskID: ids.NewAt(ids.KindTask, now, 3),
			Locator: testidempotency.IdempotencyLocator{
				ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: environmentID,
				Method: "PUT", Route: "/environments/{id}/blueprint", Key: "01K39Y7A9NFPN2Q7B3DJQ0H4AB",
			},
			Intent:     validEnvironmentBlueprintProtectedIntentForTest("protected intent"),
			SourceKind: fixtureowner.EnvironmentBlueprintSourceApply, RenderGeneration: 1,
			ProjectionSchema: 1, CreatedAt: now,
		},
		State: fixtureowner.EnvironmentBlueprintStageOpen, Bound: true,
		AuditChunks: 1, AuditBytes: 5, AuditSHA256: audit,
		ProjectionChunks: 1, ProjectionBytes: 10, ProjectionSHA256: projection,
		DependencyDigest: dependency, UpdatedAt: now,
	}
}

func validEnvironmentBlueprintProtectedIntentForTest(value string) testidempotency.ProtectedIntentRecord {
	ciphertext := []byte(value)
	digest := sha256.Sum256(ciphertext)
	return testidempotency.ProtectedIntentRecord{
		EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
	}
}
