package executionplan

import (
	"crypto/sha256"
	"crypto/subtle"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"strconv"
	"strings"
	"unicode/utf8"
)

func validateArtifact(plan *agentpb.ExecutionPlan, artifact *agentpb.ComposeArtifact) error {
	if artifact == nil {
		return errs.New(errs.KindValidationFailed, "execution plan contains an empty artifact")
	}
	if err := validateID(ids.KindConfig, artifact.ArtifactId); err != nil {
		return err
	}
	switch artifact.OwnerKind {
	case agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT:
		if err := validateID(ids.KindEnvironment, artifact.OwnerId); err != nil {
			return err
		}
		if artifact.ProjectName != "gp-"+strings.ToLower(artifact.OwnerId) {
			return errs.New(errs.KindValidationFailed, "environment Compose project name is not id-derived")
		}
		if err := validateVolumeDirectory(artifact.AuthorizedVolumeDir, artifact.OwnerId); err != nil {
			return err
		}
	case agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM:
		if artifact.OwnerId != "" || artifact.ProjectName != "groundplane-infra" || artifact.AuthorizedVolumeDir != "" {
			return errs.New(errs.KindValidationFailed, "platform Compose artifact identity is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "Compose artifact owner kind is unsupported")
	}
	if len(artifact.CanonicalYaml) == 0 || len(artifact.CanonicalYaml) > MaximumArtifactYAMLBytes ||
		!utf8.Valid(artifact.CanonicalYaml) {
		return errs.New(errs.KindValidationFailed, "Compose artifact YAML is empty, oversized, or invalid UTF-8")
	}
	if len(artifact.YamlSha256) != sha256.Size {
		return errs.New(errs.KindValidationFailed, "Compose artifact digest must contain 32 bytes")
	}
	digest := sha256.Sum256(artifact.CanonicalYaml)
	if subtle.ConstantTimeCompare(artifact.YamlSha256, digest[:]) != 1 {
		return errs.New(errs.KindValidationFailed, "Compose artifact digest does not match its YAML")
	}
	if interpolation := strings.Index(string(artifact.CanonicalYaml), "${"); interpolation >= 0 {
		name := string(artifact.CanonicalYaml[interpolation+2:])
		if end := strings.IndexAny(name, ":-?}"); end >= 0 {
			name = name[:end]
		}
		return errs.Newf(
			errs.KindValidationFailed,
			"Compose artifact contains unresolved interpolation for variable %q",
			name,
		)
	}
	if err := validateServices(plan, artifact); err != nil {
		return err
	}
	if err := validateNetworks(plan, artifact); err != nil {
		return err
	}
	return validateVolumes(plan, artifact)
}

func validateNetworks(plan *agentpb.ExecutionPlan, artifact *agentpb.ComposeArtifact) error {
	previous := ""
	for _, network := range artifact.Networks {
		if network == nil {
			return errs.New(errs.KindValidationFailed, "Compose artifact network id is invalid")
		}
		dockerName, err := networkname.New(network.NetworkId)
		if err != nil {
			return errs.New(errs.KindValidationFailed, "Compose artifact network id is invalid")
		}
		if previous >= network.NetworkId {
			return errs.New(errs.KindValidationFailed, "Compose artifact networks must be uniquely sorted by id")
		}
		previous = network.NetworkId
		if err := validateComposeName(network.ComposeName); err != nil {
			return err
		}
		if err := validateComposeName(network.DockerName); err != nil {
			return err
		}
		if network.DockerName != dockerName {
			return errs.New(errs.KindValidationFailed, "Compose artifact physical network name is invalid")
		}
		if err := validateLabels(plan, artifact, "network", network.NetworkId, network.ExpectedLabels); err != nil {
			return err
		}
	}
	return nil
}

func validateLabels(
	plan *agentpb.ExecutionPlan,
	artifact *agentpb.ComposeArtifact,
	resourceKind string,
	resourceID string,
	pairs []*agentpb.LabelPair,
) error {
	values := make(map[string]string, len(pairs))
	previous := ""
	for _, pair := range pairs {
		if pair == nil {
			return errs.New(errs.KindValidationFailed, "expected ownership labels are invalid or unsorted")
		}
		if pair.Key <= previous || !validLabelKey(pair.Key) || !utf8.ValidString(pair.Value) {
			return errs.Newf(
				errs.KindValidationFailed,
				"expected ownership label %q after %q is invalid or unsorted",
				pair.Key,
				previous,
			)
		}
		previous = pair.Key
		values[pair.Key] = pair.Value
	}
	if values[labelManaged] != "true" || values[labelKind] != resourceKind {
		return errs.New(errs.KindValidationFailed, "expected ownership labels do not identify the resource")
	}
	if resourceKind == "service" {
		labelPlan := values[labelPlanID]
		labelGeneration := values[labelRenderGen]
		if plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_REMOVE {
			generation, err := strconv.ParseUint(labelGeneration, 10, 64)
			if validateID(ids.KindPlan, labelPlan) != nil || err != nil || generation == 0 ||
				strconv.FormatUint(generation, 10) != labelGeneration {
				return errs.New(errs.KindValidationFailed, "expected service labels do not identify a sealed plan")
			}
		} else if !validPriorComponentOwnership(
			plan,
			artifact,
			resourceID,
			values,
			labelPlan,
			labelGeneration,
		) {
			retained, reason := validRetainedOwnership(plan, artifact, resourceID, values)
			if !retained && (labelPlan != plan.PlanId || labelGeneration != strconv.FormatUint(plan.RenderGeneration, 10)) {
				return errs.Newf(errs.KindValidationFailed,
					"expected service labels do not identify the current plan (retained ownership: %s)", reason)
			}
		}
	} else {
		_, hasPlanID := values[labelPlanID]
		_, hasRenderGeneration := values[labelRenderGen]
		if hasPlanID || hasRenderGeneration {
			return errs.New(errs.KindValidationFailed, "persistent resource labels contain volatile execution identity")
		}
	}
	if resourceKind == "service" && values[labelServiceID] != resourceID {
		return errs.New(errs.KindValidationFailed, "expected service labels do not identify the service")
	}
	if artifact.OwnerKind == agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT &&
		values[labelEnvironmentID] != artifact.OwnerId {
		return errs.New(errs.KindValidationFailed, "expected ownership labels do not identify the environment")
	}
	for key, value := range values {
		var kind ids.Kind
		switch key {
		case labelTenantID:
			kind = ids.KindTenant
		case labelProjectID:
			kind = ids.KindProject
		case labelEnvironmentID:
			kind = ids.KindEnvironment
		case labelComponentID:
			kind = ids.KindComponent
		case labelServiceID:
			if validateID(ids.KindService, value) == nil || validateID(ids.KindComponent, value) == nil {
				continue
			}
			return errs.New(errs.KindValidationFailed, "expected service label is invalid")
		case labelReleaseID:
			kind = ids.KindDeployment
		case labelSlot:
			if value == "blue" || value == "green" {
				continue
			}
			return errs.New(errs.KindValidationFailed, "expected slot label is invalid")
		case labelRuntimeRole:
			if value == "slot" || value == "proxy" || value == "singleton" {
				continue
			}
			return errs.New(errs.KindValidationFailed, "expected runtime role label is invalid")
		default:
			continue
		}
		if err := validateID(kind, value); err != nil {
			return err
		}
	}
	return nil
}
