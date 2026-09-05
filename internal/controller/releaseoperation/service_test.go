package releaseoperation

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestReleaseImageWithTagPreservesFirstDeployDigest(t *testing.T) {
	// Rationale: the first Release has no current tag, but a bare digest is
	// already immutable and can carry collision-free derived tag metadata.
	t.Parallel()
	digest := strings.Repeat("a", 64)
	image := "registry.example.invalid/app@sha256:" + digest

	gotImage, gotTag, gotDigest, err := releaseImageWithTag(image, "", "")
	if err != nil {
		t.Fatalf("releaseImageWithTag() error = %v", err)
	}
	if gotImage != image || gotTag != "sha-"+digest || gotDigest != digest {
		t.Fatalf("release image = %q/%q/%q", gotImage, gotTag, gotDigest)
	}
}

func TestReleaseImageWithTagPreservesDigestPinnedRedeploy(t *testing.T) {
	// Rationale: hook snapshots require the exact immutable candidate image;
	// retaining the current operator tag must not turn it into a mutable tag.
	t.Parallel()
	image := "registry.example.invalid/app@sha256:" + strings.Repeat("a", 64)

	gotImage, gotTag, gotDigest, err := releaseImageWithTag(image, "", "dev")
	if err != nil {
		t.Fatalf("releaseImageWithTag() error = %v", err)
	}
	if gotImage != image || gotTag != "dev" || gotDigest != strings.Repeat("a", 64) {
		t.Fatalf("release image = %q/%q/%q", gotImage, gotTag, gotDigest)
	}
}

func TestValidateReleaseDependencyOrderRejectsConsumerBeforePrerequisite(t *testing.T) {
	candidate := func(name string) releaseCandidateInput {
		return releaseCandidateInput{planning: etcd.ReleasePlanningService{
			Service: etcd.Versioned[etcd.ServiceRecord]{Record: etcd.ServiceRecord{
				Desired: core.Service{Name: name},
			}},
		}}
	}
	plan := core.ServiceDependencyPhasePlan{
		Phase:           core.ServiceLifecycleDeploy,
		OrderedServices: []string{"database", "worker"},
		Edges: []core.ServiceDependencyEdge{{
			Service: "worker", Dependency: "database", Condition: core.ServiceDependencyHealthy,
		}},
	}

	if err := validateReleaseDependencyOrder(
		[]releaseCandidateInput{candidate("database"), candidate("worker")},
		plan,
	); err != nil {
		t.Fatalf("prerequisite-first order rejected: %v", err)
	}
	err := validateReleaseDependencyOrder(
		[]releaseCandidateInput{candidate("worker"), candidate("database")},
		plan,
	)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
		t.Fatalf("consumer-first order error = %v, want validation_failed", err)
	}
}

func TestReleaseGroupRollbackCanonicalRequestIncludesSelection(t *testing.T) {
	// Rationale: one idempotency key must replay only the same rollback tag;
	// omission and an explicit group-wide member filter are different intents.
	t.Parallel()
	ctx := context.Background()
	cipher := releaseOperationTestCipher{}
	protector, err := secretvalue.NewProtector(cipher, cipher)
	if err != nil {
		t.Fatalf("NewProtector() error = %v", err)
	}
	coordinator, err := idempotentintent.NewCoordinator(protector)
	if err != nil {
		t.Fatalf("NewCoordinator() error = %v", err)
	}
	protected := func(input domain.GroupRollbackInput) idempotentintent.ProtectedEvidence {
		t.Helper()
		version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
			Method: http.MethodPost, Route: releaseGroupRollbackRoute,
			Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: "env_01J00000000000000000000000"},
			Path:  []idempotentintent.PathBinding{{Name: "id", Value: "rg_01J00000000000000000000000"}},
			Query: idempotentintent.Object(), Body: groupRollbackRequestBody(input),
		})
		if err != nil {
			t.Fatalf("Canonicalize(%#v) error = %v", input, err)
		}
		evidence, err := coordinator.ProtectIntent(ctx, version, digest)
		if err != nil {
			t.Fatalf("ProtectIntent(%#v) error = %v", input, err)
		}
		return evidence
	}
	tag, revision := "release-2026-09-04", int64(42)
	omitted := protected(domain.GroupRollbackInput{})
	explicit := protected(domain.GroupRollbackInput{Tag: &tag, PreviewRevision: &revision})
	omittedRecord, err := omitted.DurableRecord()
	if err != nil {
		t.Fatalf("DurableRecord() error = %v", err)
	}
	matched, err := coordinator.MatchesDurable(ctx, explicit, omittedRecord)
	if err != nil || matched {
		t.Fatalf("explicit tag matched omitted request: matched=%t error=%v", matched, err)
	}
}

type releaseOperationTestCipher struct{}

func (releaseOperationTestCipher) Seal(_ context.Context, plaintext []byte) ([]byte, error) {
	return append([]byte("sealed:"), plaintext...), nil
}

func (releaseOperationTestCipher) Open(_ context.Context, ciphertext []byte) ([]byte, error) {
	return append([]byte(nil), ciphertext[len("sealed:"):]...), nil
}
