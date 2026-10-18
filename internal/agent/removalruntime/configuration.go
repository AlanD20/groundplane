package removalruntime

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/materialization"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ConfigurationExchange interface {
	RemovalConfiguration(context.Context, *agentpb.ServiceRemovalConfigurationRequest) (*agentpb.ServiceRemovalConfigurationManifest, error)
	RemovalConfigurationFile(context.Context, *agentpb.ServiceRemovalConfigurationFileRequest) (*agentpb.ServiceRemovalConfigurationFileDelivery, error)
}

type RecoveryConfiguration struct {
	runtime   *materialization.Runtime
	exchange  ConfigurationExchange
	ownership *agentownership.Lease
}

func NewRecoveryConfiguration(runtime *materialization.Runtime, exchange ConfigurationExchange, ownership *agentownership.Lease) (*RecoveryConfiguration, error) {
	if runtime == nil || exchange == nil || ownership == nil {
		return nil, errs.New(errs.KindInternal, "Removal configuration requires materialization, delivery and process ownership")
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	return &RecoveryConfiguration{runtime: runtime, exchange: exchange, ownership: ownership}, nil
}

func (configuration *RecoveryConfiguration) RestoreRemovalConfiguration(ctx context.Context, request *agentpb.ServiceRemovalRecoveryHelperRequest) (*agentpb.ServiceRemovalRecoveryHelperResponse, error) {
	input, err := executionplan.OpenRemovalRecoveryHelperRequest(request)
	if err != nil {
		return nil, err
	}
	if ctx == nil || input.Progress.Phase != removalexecution.RestoreConfiguration {
		return nil, invalidInput()
	}
	bounded, cancel := context.WithDeadline(ctx, input.Attempt.Deadline)
	defer cancel()
	if err := configuration.ownership.CheckRecovery(input.Capture.Source.Forward, input.Attempt); err != nil {
		return nil, err
	}
	manifest, err := configuration.exchange.RemovalConfiguration(bounded, &agentpb.ServiceRemovalConfigurationRequest{Attempt: request.Assignment.Attempt, StageId: input.Checkpoint.StageID})
	if err != nil {
		return nil, err
	}
	headers, err := executionplan.RemovalConfigurationHeaders(request, manifest)
	if err != nil {
		return nil, err
	}
	// Stable task/stage/file identities also cover earlier cleanup attempts. Join
	// any abandoned helper before inspection or a newly granted restoration.
	if err := configuration.runtime.StopRemovalRecoveryFiles(bounded, request, manifest); err != nil {
		return nil, err
	}
	satisfied := true
	for index, header := range headers {
		if err := configuration.ownership.CheckRecovery(input.Capture.Source.Forward, input.Attempt); err != nil {
			return nil, err
		}
		if !input.InspectOnly {
			if err := configuration.file(bounded, request, manifest, index, header, false); err != nil {
				return nil, err
			}
		}
		if err := configuration.file(bounded, request, manifest, index, header, true); err != nil {
			if !input.InspectOnly || !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
				return nil, err
			}
			satisfied = false
		}
	}
	if err := configuration.ownership.CheckRecovery(input.Capture.Source.Forward, input.Attempt); err != nil {
		return nil, err
	}
	response := &agentpb.ServiceRemovalRecoveryHelperResponse{StageId: input.Checkpoint.StageID,
		ObservedAt: timestamppb.New(time.Now().UTC()), Satisfied: satisfied, Inspection: input.InspectOnly}
	if satisfied {
		raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(manifest)
		if err != nil {
			return nil, err
		}
		digest := sha256.Sum256(raw)
		response.Evidence = &agentpb.WorkInputReference{InputId: input.Attempt.Input.ID, Sha256: digest[:]}
	}
	return response, nil
}

func (configuration *RecoveryConfiguration) file(ctx context.Context, request *agentpb.ServiceRemovalRecoveryHelperRequest,
	manifest *agentpb.ServiceRemovalConfigurationManifest, index int, header entrymaterialization.Header, verifyOnly bool,
) error {
	delivery, err := configuration.exchange.RemovalConfigurationFile(ctx, &agentpb.ServiceRemovalConfigurationFileRequest{
		Attempt: request.Assignment.Attempt, StageId: manifest.StageId, Destination: header.Destination()})
	defer removalexecution.ClearConfigurationDelivery(delivery)
	if err != nil {
		return err
	}
	if delivery == nil || executionplan.RejectUnknown(delivery) != nil || !proto.Equal(delivery.Attempt, manifest.Attempt) ||
		delivery.StageId != manifest.StageId || !proto.Equal(delivery.Configuration, manifest.Configuration) ||
		!proto.Equal(delivery.File, manifest.Files[index]) || uint64(len(delivery.Content)) != header.Length() {
		return invalidInput()
	}
	if sha256.Sum256(delivery.Content) != [32]byte(header.Digest()) {
		return invalidInput()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	content := delivery.Content
	delivery.Content = nil
	return configuration.runtime.RunRemovalRecoveryFile(ctx, request, manifest, header, entrymaterialization.OwnBytes(content), verifyOnly)
}
