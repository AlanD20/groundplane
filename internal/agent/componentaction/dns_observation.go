package componentaction

import (
	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"

	"github.com/AlanD20/groundplane/internal/infra/docker/dnsresolverobserver"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func dnsResolverObservationRequest(
	assignment taskassignment.Assignment,
	action *agentpb.ComponentApply,
	recipe registeredcatalog.DNSResolverObservationRecipe,
) (dnsresolverobserver.Request, error) {
	artifact := componentObservationComposeArtifact(assignment.Plan)
	if artifact == nil {
		return dnsresolverobserver.Request{}, errs.New(
			errs.KindValidationFailed,
			"agent: DNS resolver observation artifact is invalid",
		)
	}
	return dnsresolverobserver.CapturedRequest(artifact, action, recipe)
}

func componentObservationComposeArtifact(plan *agentpb.ExecutionPlan) *agentpb.ComposeArtifact {
	if plan == nil {
		return nil
	}
	if len(plan.GetArtifacts()) == 1 {
		return plan.GetArtifacts()[0]
	}
	if len(plan.GetArtifacts()) != 2 {
		return nil
	}
	for _, step := range plan.GetSteps() {
		if apply := step.GetComposeApply(); apply != nil {
			for _, artifact := range plan.GetArtifacts() {
				if artifact.GetArtifactId() == apply.GetArtifactId() {
					return artifact
				}
			}
		}
	}
	return nil
}
