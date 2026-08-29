package app

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestReleaseImageWithTagRejectsDigestOnlyImageWithoutCurrentTag(t *testing.T) {
	t.Parallel()

	_, _, _, err := releaseImageWithTag(
		"registry.example.invalid/app@sha256:"+strings.Repeat("a", 64),
		"",
	)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
		t.Fatalf("releaseImageWithTag() error = %v, want validation_failed", err)
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
