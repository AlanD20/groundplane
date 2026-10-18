package releaseoperation

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/distribution/reference"
)

func validateReleaseDependencyOrder(candidates []releaseCandidateInput, plan core.ServiceDependencyPhasePlan) error {
	positions := make(map[string]int, len(candidates))
	for index, candidate := range candidates {
		positions[candidate.planning.Service.Record.Desired.Name] = index
	}
	for _, edge := range plan.Edges {
		consumer, selectedConsumer := positions[edge.Service]
		dependency, selectedDependency := positions[edge.Dependency]
		if selectedConsumer && selectedDependency && dependency >= consumer {
			return errs.Newf(
				errs.KindValidationFailed,
				"release group order places Service %s before prerequisite %s",
				edge.Service,
				edge.Dependency,
			)
		}
	}
	return nil
}

func releasePriorWorkload(serving domain.Intent, hasServing bool) *domain.WorkloadSeal {
	if !hasServing {
		return nil
	}
	seal := serving.CandidateWorkload
	return &seal
}

func releasePriorID(serving domain.Intent, hasServing bool) string {
	if hasServing {
		return serving.ID
	}
	return ""
}

func releasePriorStrategy(serving domain.Intent, hasServing bool) domain.Strategy {
	if hasServing {
		return serving.Strategy
	}
	return domain.StrategyRecreate
}

func releaseAcceptedResponse(
	groupID string,
	taskID string,
	operationID string,
	members []domain.GroupMember,
) (idempotencyrecord.IdempotencyResponse, []byte, error) {
	var value any
	if groupID == "" {
		value = apiTypes.ReleaseTaskAccepted{TaskID: taskID, OperationID: operationID, ReleaseID: members[0].ReleaseID}
	} else {
		releases := make([]apiTypes.ReleaseGroupMemberRelease, len(members))
		for index, member := range members {
			releases[index] = apiTypes.ReleaseGroupMemberRelease{ServiceID: member.ServiceID, ReleaseID: member.ReleaseID}
		}
		value = apiTypes.ReleaseGroupTaskAccepted{
			TaskID: taskID, ReleaseGroupOperationID: operationID, Releases: releases,
		}
	}
	body, err := json.Marshal(value)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, nil, errs.Wrap(errs.KindInternal, err)
	}
	return idempotencyrecord.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        body,
	}, body, nil
}

func releaseOperationResolution(
	resolution requestidempotency.Resolution,
	applied idempotencyrecord.IdempotencyResponse,
) (idempotencyrecord.IdempotencyResponse, error) {
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return cloneIdempotencyResponse(applied), nil
	case requestidempotency.ResolutionReplay:
		return cloneIdempotencyResponse(resolution.Response), nil
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"release idempotency resolution is invalid",
		)
	}
}

func releaseImageWithTag(value string, requested string, current string) (string, string, string, error) {
	named, err := reference.ParseNormalizedNamed(value)
	if err != nil {
		return "", "", "", errs.New(errs.KindValidationFailed, "service image reference is invalid")
	}
	digest := ""
	if digested, ok := named.(reference.Digested); ok {
		digest = digested.Digest().Encoded()
	}
	tag := requested
	preserveDigest := digest != "" && requested == ""
	if tag == "" && current != "" {
		tag = current
	}
	if tag == "" {
		tagged, ok := named.(reference.NamedTagged)
		if ok {
			tag = tagged.Tag()
		} else if digest != "" {
			tag = "sha-" + digest
			preserveDigest = true
		} else {
			return "", "", "", errs.New(
				errs.KindValidationFailed,
				"release tag is required when the Service image has no tag",
			)
		}
	}
	if strings.ContainsAny(tag, "@/\\") || strings.TrimSpace(tag) != tag || tag == "" {
		return "", "", "", errs.New(errs.KindValidationFailed, "release image tag is invalid")
	}
	if preserveDigest {
		return reference.FamiliarString(named), tag, digest, nil
	}
	tagged, err := reference.WithTag(reference.TrimNamed(named), tag)
	if err != nil {
		return "", "", "", errs.New(errs.KindValidationFailed, "release image tag is invalid")
	}
	return reference.FamiliarString(tagged), tag, digest, nil
}

func releaseFailurePolicy(requested domain.OnFailure, declared core.OnFailure) (domain.OnFailure, error) {
	selected := string(requested)
	if selected == "" {
		selected = string(declared.WithDefault())
	}
	switch selected {
	case string(core.OnFailureSwitchBack):
		return domain.OnFailureSwitchBack, nil
	case string(core.OnFailureLeaveActive):
		return domain.OnFailureLeaveActive, nil
	default:
		return "", errs.New(errs.KindValidationFailed, "release failure policy is invalid")
	}
}

func cloneIdempotencyResponse(response idempotencyrecord.IdempotencyResponse) idempotencyrecord.IdempotencyResponse {
	response.Body = append([]byte(nil), response.Body...)
	return response
}

func releaseGroupUnknownOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
