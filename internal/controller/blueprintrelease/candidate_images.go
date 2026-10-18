package blueprintrelease

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func bindBlueprintCandidateServiceImages(
	artifact *agentpb.ComposeArtifact,
	members []releaserender.ReleaseTaskRenderMember,
) error {
	const op = "bind blueprint candidate service images"
	if artifact == nil {
		return errs.New(errs.KindInternal, op+": compose artifact is required")
	}
	byService := make(map[string]releaserender.ReleaseTaskRenderMember, len(members))
	candidateRoles := make(map[string]agentpb.ComposeServiceRole, len(members))
	for _, member := range members {
		if member.Render.ServiceID == "" || member.Intent.ID == "" ||
			member.Render.CandidateWorkload.LocalImageID == "" {
			return errs.New(errs.KindInternal, op+": release member is incomplete")
		}
		if _, exists := byService[member.Render.ServiceID]; exists {
			return errs.New(errs.KindInternal, op+": release service is duplicated")
		}
		byService[member.Render.ServiceID] = member
		candidateRole := agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT
		if member.Render.Strategy == domain.StrategyRecreate {
			candidateRole = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON
		} else if member.Render.Strategy == domain.StrategyRolling {
			candidateRole = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA
		}
		candidateRoles[member.Render.ServiceID] = candidateRole
	}
	seen := make(map[string]int, len(byService))
	for _, service := range artifact.GetServices() {
		if service == nil {
			return errs.New(errs.KindInternal, op+": compose service is required")
		}
		member, selected := byService[service.GetServiceId()]
		if !selected {
			continue
		}
		if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		if service.GetRole() != candidateRoles[service.GetServiceId()] {
			return errs.New(errs.KindInternal, op+": compose service release ownership does not match")
		}
		releaseID := ""
		for _, label := range service.GetExpectedLabels() {
			if label.GetKey() == composerender.ComposeLabelReleaseID {
				releaseID = label.GetValue()
				break
			}
		}
		if releaseID == "" {
			continue
		}
		if releaseID != member.Intent.ID {
			return errs.New(errs.KindInternal, op+": compose service release ownership does not match")
		}
		if seen[service.GetServiceId()] > 0 && member.Render.Strategy != domain.StrategyRolling {
			return errs.New(errs.KindInternal, op+": candidate compose service is duplicated")
		}
		if member.Render.Strategy == domain.StrategyRolling {
			if member.Render.Rolling == nil || seen[service.GetServiceId()] >= len(member.Render.Rolling.Candidate) {
				return errs.New(errs.KindInternal, op+": unexpected rolling candidate replica")
			}
			if _, err := executionplan.RollingReplicaService(artifact, service.GetServiceId(), member.Render.Rolling.Candidate[seen[service.GetServiceId()]]); err != nil {
				return err
			}
		}
		service.ImageReference = member.Render.CandidateWorkload.LocalImageID
		seen[service.GetServiceId()]++
	}
	if len(seen) != len(byService) {
		return errs.New(errs.KindInternal, op+": selected candidate compose service is missing")
	}
	for serviceID, member := range byService {
		if member.Render.Rolling != nil && seen[serviceID] != len(member.Render.Rolling.Candidate) {
			return errs.New(errs.KindInternal, op+": candidate rolling replica set is incomplete")
		}
	}
	return nil
}
