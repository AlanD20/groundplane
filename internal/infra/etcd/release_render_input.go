package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const TaskReleasePublicationParam = "release_publication_id"

// ReleaseRenderInput is the immutable, revision-owned artifact input for one
// candidate. It is staged before publication and never re-reads mutable labels,
// Blueprint projection, image identity, or selected slot at dispatch time.
type ReleaseRenderInput struct {
	ReleaseID            string                             `json:"release_id"`
	PlanID               string                             `json:"plan_id"`
	ArtifactID           string                             `json:"artifact_id"`
	PriorArtifactID      string                             `json:"prior_artifact_id,omitempty"`
	PriorRuntime         *ReleaseNativePredecessorAuthority `json:"prior_runtime,omitempty"`
	ServiceID            string                             `json:"service_id"`
	ServiceName          string                             `json:"service_name"`
	CandidateWorkload    domain.WorkloadSeal                `json:"candidate_workload"`
	PriorWorkload        *domain.WorkloadSeal               `json:"prior_workload,omitempty"`
	Strategy             domain.Strategy                    `json:"strategy"`
	PriorStrategy        domain.Strategy                    `json:"prior_strategy"`
	Slot                 domain.Slot                        `json:"slot,omitempty"`
	PriorSlot            domain.Slot                        `json:"prior_slot"`
	CandidateTarget      domain.WorkloadTarget              `json:"candidate_target"`
	PriorTarget          domain.WorkloadTarget              `json:"prior_target"`
	ProxyGeneration      uint64                             `json:"proxy_generation"`
	PriorProxyGeneration uint64                             `json:"prior_proxy_generation"`
	ProxyPorts           []uint16                           `json:"proxy_ports"`
	ProxyConfigDigest    string                             `json:"proxy_config_digest"`
	PriorProxyDigest     string                             `json:"prior_proxy_digest"`
	ProxyImage           *domain.ProxyImage                 `json:"proxy_image,omitempty"`
	core.ServiceDependencyPlans
	TenantID            string                                        `json:"tenant_id"`
	TenantSlug          string                                        `json:"tenant_slug"`
	ProjectID           string                                        `json:"project_id"`
	ProjectSlug         string                                        `json:"project_slug"`
	EnvironmentID       string                                        `json:"environment_id"`
	EnvironmentName     string                                        `json:"environment_name"`
	AuthorizedVolumeDir string                                        `json:"authorized_volume_dir"`
	Projection          projectionrecord.EnvironmentComposeProjection `json:"projection"`
	Hooks               []ReleaseHookRenderInput                      `json:"hooks,omitempty"`
}

type ReleaseTaskRenderInput struct {
	NativePredecessors []BlueprintNativePredecessor
	PublicationID      string
	Operation          ReleaseOperationHead
	Members            []ReleaseTaskRenderMember
}

type ReleaseTaskRenderMember struct {
	Intent domain.Intent
	Render ReleaseRenderInput
}

func EncodeReleaseRenderInput(input ReleaseRenderInput) (json.RawMessage, error) {
	input = cloneReleaseRenderInput(input)
	if err := validateReleaseRenderInput(input); err != nil {
		return nil, err
	}
	value, err := json.Marshal(input)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(value) > maximumReleaseRenderInputBytes {
		clear(value)
		return nil, errs.New(errs.KindReleasePlanTooLarge, "release render input exceeds the durable size limit")
	}
	return value, nil
}

func decodeReleaseRenderInput(value []byte) (ReleaseRenderInput, error) {
	if recordcodec.RejectDuplicateFields(value) != nil {
		return ReleaseRenderInput{}, corruptReleaseRecord()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var input ReleaseRenderInput
	if err := decoder.Decode(&input); err != nil || recordcodec.RequireEOF(decoder) != nil ||
		validateReleaseRenderInput(input) != nil {
		return ReleaseRenderInput{}, corruptReleaseRecord()
	}
	return cloneReleaseRenderInput(input), nil
}

func validateReleaseRenderInput(input ReleaseRenderInput) error {
	if err := validateOrdinaryPriorRuntime(input); err != nil {
		return err
	}
	if (len(input.ProxyPorts) != 0) != (input.ProxyImage != nil) ||
		input.ProxyImage != nil && input.ProxyImage.Validate() != nil {
		return errs.New(errs.KindValidationFailed, "release proxy image authority is invalid")
	}
	if ids.Validate(ids.KindDeployment, input.ReleaseID) != nil || ids.Validate(ids.KindPlan, input.PlanID) != nil ||
		ids.Validate(
			ids.KindConfig,
			input.ArtifactID,
		) != nil || ids.Validate(ids.KindService, input.ServiceID) != nil ||
		ids.Validate(ids.KindTenant, input.TenantID) != nil || ids.Validate(ids.KindProject, input.ProjectID) != nil ||
		ids.Validate(
			ids.KindEnvironment,
			input.EnvironmentID,
		) != nil || input.ServiceName == "" || domain.ValidateWorkloadSeal(input.CandidateWorkload) != nil ||
		input.TenantSlug == "" || input.ProjectSlug == "" || input.EnvironmentName == "" || input.AuthorizedVolumeDir == "" {
		return errs.New(errs.KindValidationFailed, "release render input identity is invalid")
	}
	candidateTarget, candidateErr := domain.TargetFor(input.Strategy, input.Slot)
	priorTarget, priorErr := domain.TargetFor(input.PriorStrategy, input.PriorSlot)
	if candidateErr != nil || priorErr != nil || input.CandidateTarget != candidateTarget ||
		input.PriorTarget != priorTarget {
		return errs.New(errs.KindValidationFailed, "release render workload topology is invalid")
	}
	if input.Strategy == domain.StrategyBlueGreen {
		if input.CandidateWorkload.ReplicaCount != 1 ||
			input.PriorWorkload != nil && input.PriorWorkload.ReplicaCount != 1 {
			return errs.New(errs.KindValidationFailed, "blue-green transition requires singleton workloads")
		}
		if input.Slot != domain.SlotBlue && input.Slot != domain.SlotGreen {
			return errs.New(errs.KindValidationFailed, "release render input slot is invalid")
		}
		hasPrior := input.PriorArtifactID != "" || input.PriorWorkload != nil
		if input.PriorTarget.Validate() != nil || input.ProxyGeneration == 0 || len(input.ProxyPorts) == 0 ||
			input.ProxyConfigDigest == "" ||
			hasPrior && (input.PriorProxyGeneration == 0 || input.PriorProxyDigest == "" ||
				ids.Validate(ids.KindConfig, input.PriorArtifactID) != nil || input.PriorWorkload == nil) ||
			!hasPrior && (input.PriorProxyGeneration != 0 || input.PriorProxyDigest != "") {
			return errs.New(errs.KindValidationFailed, "blue-green release render proxy authority is invalid")
		}
	} else if input.Strategy != domain.StrategyRecreate || input.Slot != "" {
		return errs.New(errs.KindValidationFailed, "release render input strategy is invalid")
	} else if len(input.ProxyPorts) != 0 {
		hasPrior := input.PriorArtifactID != "" || input.PriorWorkload != nil
		if input.CandidateTarget != domain.WorkloadSingleton || input.ProxyGeneration == 0 || input.ProxyConfigDigest == "" ||
			hasPrior && (input.PriorProxyGeneration == 0 || input.PriorProxyDigest == "" ||
				ids.Validate(ids.KindConfig, input.PriorArtifactID) != nil || input.PriorWorkload == nil) ||
			!hasPrior && (input.PriorProxyGeneration != 0 || input.PriorProxyDigest != "") {
			return errs.New(errs.KindValidationFailed, "addressable recreate render authority is invalid")
		}
	} else if (input.PriorArtifactID != "" && ids.Validate(ids.KindConfig, input.PriorArtifactID) != nil) ||
		(input.PriorArtifactID == "") != (input.PriorWorkload == nil) ||
		input.CandidateTarget != domain.WorkloadSingleton || input.PriorTarget != domain.WorkloadSingleton ||
		input.ProxyGeneration != 0 || input.PriorProxyGeneration != 0 ||
		input.ProxyConfigDigest != "" || input.PriorProxyDigest != "" {
		return errs.New(errs.KindValidationFailed, "portless recreate render authority is invalid")
	}
	if (input.PriorArtifactID != "") != (input.PriorWorkload != nil) {
		return errs.New(errs.KindValidationFailed, "release prior topology artifact authority is invalid")
	}
	if input.PriorWorkload != nil && (domain.ValidateWorkloadSeal(*input.PriorWorkload) != nil ||
		input.PriorStrategy == domain.StrategyBlueGreen && (input.PriorWorkload.ReplicaCount != 1 || input.CandidateWorkload.ReplicaCount != 1)) {
		return errs.New(errs.KindValidationFailed, "release prior workload authority is invalid")
	}
	serviceNames := make([]string, 0, len(input.Projection.DesiredServices)+1)
	selected := false
	desiredSelected := false
	for _, service := range input.Projection.DesiredServices {
		serviceNames = append(serviceNames, service.Desired.Name)
		if service.Desired.ID == input.ServiceID {
			desiredSelected = true
			selected = service.Desired.Name == input.ServiceName
		}
	}
	if !desiredSelected && releaseRenderTargetsGeneratedService(input.Projection.Components, input.ServiceID) {
		serviceNames = append(serviceNames, input.ServiceName)
		selected = true
	}
	if err := input.ServiceDependencyPlans.Validate(serviceNames); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	if !input.ServiceDependencyPlans.Equal(input.Projection.ServiceDependencyPlans) {
		return errs.New(errs.KindValidationFailed, "release render dependency plans do not match the frozen projection")
	}
	if len(input.ProxyPorts) != 0 {
		if _, err := domain.RenderProxyConfig(input.ServiceName, input.ReleaseID, input.CandidateTarget, input.ProxyGeneration, input.ProxyPorts); err != nil {
			return err
		}
	}
	if projectionrecord.ValidateEnvironmentComposeProjection(input.Projection) != nil ||
		input.Projection.EnvironmentID != input.EnvironmentID {
		return errs.New(errs.KindValidationFailed, "release render projection is invalid")
	}
	if !selected {
		return errs.New(errs.KindValidationFailed, "release render projection does not contain the selected service")
	}
	if err := validateReleaseHookRenderInputs(input.Hooks, input.ServiceID); err != nil {
		return err
	}
	return nil
}

func releaseRenderTargetsGeneratedService(components []componentrecord.Record, serviceID string) bool {
	for _, component := range components {
		for _, generatedServiceID := range component.Runtime.GeneratedServices {
			if generatedServiceID == serviceID {
				return true
			}
		}
	}
	return false
}

func cloneReleaseRenderInput(input ReleaseRenderInput) ReleaseRenderInput {
	if input.PriorRuntime != nil {
		prior := *input.PriorRuntime
		prior.CurrentArtifact = slices.Clone(prior.CurrentArtifact)
		prior.RetainedPriorArtifact = slices.Clone(prior.RetainedPriorArtifact)
		input.PriorRuntime = &prior
	}
	if input.ProxyImage != nil {
		image := *input.ProxyImage
		input.ProxyImage = &image
	}
	if input.PriorWorkload != nil {
		prior := *input.PriorWorkload
		input.PriorWorkload = &prior
	}
	input.Projection = projectionrecord.CloneEnvironmentComposeProjection(input.Projection)
	input.ProxyPorts = slices.Clone(input.ProxyPorts)
	input.ServiceDependencyPlans = input.ServiceDependencyPlans.Clone()
	input.Hooks = cloneReleaseHookRenderInputs(input.Hooks)
	return input
}

func (ledger *ReleaseLedger) GetTaskRenderInput(
	ctx context.Context,
	task TaskRecord,
) (ReleaseTaskRenderInput, error) {
	publicationID := task.Params[TaskReleasePublicationParam]
	if ctx == nil || ledger == nil || validatePublicationID(publicationID) != nil ||
		(task.Type != taskjournal.TaskDeploy && task.Type != taskjournal.TaskRollback) || ids.Validate(ids.KindPlan, task.PlanID) != nil {
		return ReleaseTaskRenderInput{}, errs.New(
			errs.KindValidationFailed,
			"release Task render input request is invalid",
		)
	}
	keys := []string{
		releaseManifestStagingKey(
			publicationID,
		), releasePublicationKey(publicationID), releaseOperationKey(task.OperationID),
	}
	loaded, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return ReleaseTaskRenderInput{}, err
	}
	if loaded == nil || len(loaded.Values) != len(keys) || loaded.Values[0] == nil || loaded.Values[1] == nil ||
		loaded.Values[2] == nil {
		return ReleaseTaskRenderInput{}, corruptReleaseRecord()
	}
	manifest, err := decodeReleaseRecord[ReleaseStagedManifest](loaded.Values[0].Value, "release-staged-manifest")
	if err != nil || manifest.PublicationID != publicationID || manifest.OperationID != task.OperationID {
		return ReleaseTaskRenderInput{}, corruptReleaseRecord()
	}
	marker, err := decodeReleaseRecord[ReleasePublicationMarker](loaded.Values[1].Value, "release-publication")
	if err != nil || marker.PublicationID != publicationID || marker.OperationID != task.OperationID ||
		marker.ManifestDigest != manifest.Digest {
		return ReleaseTaskRenderInput{}, corruptReleaseRecord()
	}
	head, err := decodeReleaseRecord[ReleaseOperationHead](loaded.Values[2].Value, "release-operation")
	if err != nil || head.OperationID != task.OperationID || head.PublicationID != publicationID ||
		head.LatestTaskID != task.ID {
		return ReleaseTaskRenderInput{}, corruptReleaseRecord()
	}
	memberKeys := make([]string, 0, len(manifest.Members)*2)
	for _, member := range manifest.Members {
		memberKeys = append(memberKeys, releaseIntentStagingKey(publicationID, member.ReleaseID),
			releaseRenderInputStagingKey(publicationID, member.ReleaseID))
	}
	members, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: memberKeys, Revision: loaded.ReadRevision})
	if err != nil {
		return ReleaseTaskRenderInput{}, err
	}
	if members == nil || members.ReadRevision != loaded.ReadRevision || len(members.Values) != len(memberKeys) {
		return ReleaseTaskRenderInput{}, corruptReleaseRecord()
	}
	result := ReleaseTaskRenderInput{
		PublicationID: publicationID, Operation: cloneReleaseOperationHead(head),
		Members: make([]ReleaseTaskRenderMember, len(manifest.Members)),
	}
	for index, reference := range manifest.Members {
		intentValue := members.Values[index*2]
		renderValue := members.Values[index*2+1]
		if intentValue == nil || renderValue == nil {
			return ReleaseTaskRenderInput{}, corruptReleaseRecord()
		}
		intent, err := decodeReleaseRecord[domain.Intent](intentValue.Value, "release-intent")
		if err != nil || domain.ValidateIntent(intent) != nil || intent.ID != reference.ReleaseID ||
			intent.ServiceID != reference.ServiceID || intent.OperationID != task.OperationID ||
			(len(head.Attempts) == 0 || intent.OriginatingTaskID != head.Attempts[0].TaskID) || intent.RenderInputID == "" {
			return ReleaseTaskRenderInput{}, corruptReleaseRecord()
		}
		raw, err := decodeReleaseRecord[json.RawMessage](renderValue.Value, "release-render-input")
		if err != nil {
			return ReleaseTaskRenderInput{}, err
		}
		render, err := decodeReleaseRenderInput(raw)
		if err != nil || render.ReleaseID != intent.ID || render.PlanID != task.PlanID ||
			render.ArtifactID != intent.RenderInputID || render.ServiceID != intent.ServiceID ||
			render.CandidateWorkload != intent.CandidateWorkload || render.Strategy != intent.Strategy || render.Slot != intent.Slot {
			return ReleaseTaskRenderInput{}, corruptReleaseRecord()
		}
		digest, _ := domain.Digest(raw)
		if digest != intent.RenderInputDigest || digest != reference.RenderDigest {
			return ReleaseTaskRenderInput{}, corruptReleaseRecord()
		}
		result.Members[index] = ReleaseTaskRenderMember{Intent: intent, Render: render}
	}
	return result, nil
}

func cloneReleaseTaskRenderMembers(values []ReleaseTaskRenderMember) []ReleaseTaskRenderMember {
	result := slices.Clone(values)
	for index := range result {
		result[index].Render = cloneReleaseRenderInput(result[index].Render)
	}
	return result
}
