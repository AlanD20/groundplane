// Package edgechecks owns captured Tunnel/public Route diagnostics and exact
// helper cleanup authority. Provider tokens and response bodies are not stored.
package edgechecks

import (
	"slices"
	"strings"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type ZoneSource struct {
	ID       string `json:"id"`
	Revision int64  `json:"revision"`
}

// Desired and applied revisions are distinct: a check against the serving
// runtime never validates newer settings that have not been deployed.
type Source struct {
	Request                  contract.Request `json:"request"`
	EnvironmentRevision      int64            `json:"environment_revision"`
	BlueprintRevision        int64            `json:"blueprint_revision"`
	OriginRuntimeRevision    int64            `json:"origin_runtime_revision"`
	OriginIntentRevision     int64            `json:"origin_intent_revision"`
	OriginRuntimeSHA256      string           `json:"origin_runtime_sha256"`
	RouteRevision            int64            `json:"route_revision"`
	RouteObservationRevision int64            `json:"route_observation_revision"`
	ComponentRevision        int64            `json:"component_revision"`
	ComponentAppliedRevision int64            `json:"component_applied_revision"`
	RouterComponentID        string           `json:"router_component_id"`
	RouterRevision           int64            `json:"router_revision"`
	RouterAppliedRevision    int64            `json:"router_applied_revision"`
	TokenSecretID            string           `json:"token_secret_id"`
	TokenSecretRevision      int64            `json:"token_secret_revision"`
	TokenValueRevision       int64            `json:"token_value_revision"`
	AgentID                  string           `json:"agent_id"`
	AgentRevision            int64            `json:"agent_revision"`
	AgentSingletonRevision   int64            `json:"agent_singleton_revision"`
	AgentGeneration          uint64           `json:"agent_generation"`
	HelperImage              string           `json:"helper_image"`
	HelperImageID            string           `json:"helper_image_id"`
	ConnectorContainerID     string           `json:"connector_container_id"`
	Zones                    []ZoneSource     `json:"zones"`
	Runtime                  []byte           `json:"runtime"`
	Action                   []byte           `json:"action"`
}

func (source Source) CapturedRuntime() (*agentpb.ComposeArtifact, *agentpb.ComponentApply, error) {
	if len(source.Runtime) == 0 || len(source.Runtime) > 512<<10 || len(source.Action) == 0 ||
		len(source.Action) > 4096 {
		return nil, nil, invalidRecord()
	}
	artifact, action := &agentpb.ComposeArtifact{}, &agentpb.ComponentApply{}
	if proto.Unmarshal(source.Runtime, artifact) != nil || proto.Unmarshal(source.Action, action) != nil ||
		contract.ValidateCapturedRuntime(source.Request, artifact, action) != nil {
		return nil, nil, invalidRecord()
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
	if err != nil || !slices.Equal(canonical, source.Runtime) {
		return nil, nil, invalidRecord()
	}
	canonical, err = proto.MarshalOptions{Deterministic: true}.Marshal(action)
	if err != nil || !slices.Equal(canonical, source.Action) {
		return nil, nil, invalidRecord()
	}
	return artifact, action, nil
}

func (source Source) Validate() error {
	if source.Request.Validate() != nil || source.EnvironmentRevision <= 0 || source.BlueprintRevision <= 0 ||
		!source.validOriginSource() ||
		source.ComponentRevision <= 0 ||
		source.ComponentAppliedRevision <= 0 ||
		source.TokenSecretRevision <= 0 ||
		source.TokenValueRevision <= 0 ||
		source.AgentRevision <= 0 ||
		source.AgentSingletonRevision <= 0 ||
		source.AgentGeneration == 0 ||
		ids.Validate(ids.KindSecret, source.TokenSecretID) != nil ||
		ids.Validate(ids.KindAgent, source.AgentID) != nil ||
		!imageref.IsDigestPinned(source.HelperImage) ||
		!strings.HasPrefix(source.HelperImageID, "sha256:") ||
		!recordcodec.ValidSHA256(strings.TrimPrefix(source.HelperImageID, "sha256:")) ||
		!recordcodec.ValidSHA256(source.ConnectorContainerID) ||
		len(source.Zones) == 0 ||
		len(source.Zones) > 64 {
		return invalidRecord()
	}
	seen := make(map[string]bool, len(source.Zones))
	for _, zone := range source.Zones {
		if ids.Validate(ids.KindNetwork, zone.ID) != nil || zone.Revision <= 0 || seen[zone.ID] {
			return invalidRecord()
		}
		seen[zone.ID] = true
	}
	_, _, err := source.CapturedRuntime()
	return err
}

func (source Source) validOriginSource() bool {
	if source.Request.RouteID != "" {
		return source.RouteRevision == source.BlueprintRevision && source.RouteObservationRevision > 0 &&
			source.RouterRevision > 0 && source.RouterAppliedRevision > 0 &&
			ids.Validate(
				ids.KindComponent,
				source.RouterComponentID,
			) == nil && source.RouterComponentID != source.Request.ComponentID &&
			source.OriginRuntimeRevision == 0 && source.OriginIntentRevision == 0 && source.OriginRuntimeSHA256 == ""
	}
	return source.RouteRevision == 0 && source.RouteObservationRevision == 0 && source.RouterRevision == 0 &&
		source.RouterAppliedRevision == 0 && source.RouterComponentID == "" && source.OriginRuntimeRevision > 0 &&
		source.OriginIntentRevision >= 0 && recordcodec.ValidSHA256(source.OriginRuntimeSHA256)
}

func cloneSource(source Source) Source {
	source.Runtime, source.Action, source.Zones = slices.Clone(
		source.Runtime,
	), slices.Clone(
		source.Action,
	), slices.Clone(
		source.Zones,
	)
	return source
}

func invalidRecord() error {
	return errs.New(errs.KindValidationFailed, "edge diagnostic record is invalid")
}
