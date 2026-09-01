package app

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
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
