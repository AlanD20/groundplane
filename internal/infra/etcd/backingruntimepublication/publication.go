package backingruntimepublication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingpostgresrelease"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Input is the exact assigned successful provisioning evidence extracted by
// the terminal transaction coordinator, before publishing applied state.
type Input struct {
	Source    projectionrecord.BackingRuntimeReceipt
	Result    taskjournal.TaskResultRecord
	CreatedAt time.Time
}

func IsManagedPostgres(projection projectionrecord.EnvironmentComposeProjection, serviceID string) bool {
	for _, service := range projection.DesiredServices {
		if service.Desired.ID == serviceID && service.Desired.Adapter == "postgres:16" &&
			service.BackingNetworkID != "" {
			return true
		}
	}
	return false
}

// Preserve retains authority only for byte-identical applied runtime. A changed
// artifact retires the receipt until its owning execution acknowledges it.
func Preserve(
	projection *projectionrecord.EnvironmentComposeProjection,
	previous *projectionrecord.EnvironmentComposeProjection,
) error {
	projection.BackingRuntime = nil
	if previous != nil && previous.BackingRuntime != nil &&
		bytes.Equal(previous.ComposeArtifact, projection.ComposeArtifact) {
		receipt := *previous.BackingRuntime
		projection.BackingRuntime = &receipt
	}
	return projectionrecord.ValidateBackingRuntimeReceipt(*projection)
}

type CatalogReader interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

func Prepare(ctx context.Context, storage CatalogReader, input Input,
	projection *projectionrecord.EnvironmentComposeProjection, revision int64,
) ([]etcdstore.Condition, error) {
	projection.BackingRuntime = nil
	serviceID := input.Source.ServiceID
	if input.Result.Kind != taskjournal.TaskResultCompose || input.Result.ExitCode != 0 ||
		input.Result.FailedStepID != "" || input.Result.ReconciliationRequired ||
		input.Result.ExecutionEpoch == 0 || input.Result.ExecutionEpoch != input.Source.ExecutionEpoch ||
		input.Source.RenderGeneration != projection.RenderGeneration {
		return nil, invalidBackingAcknowledgement()
	}
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(projection.ComposeArtifact, artifact) != nil || artifact.OwnerId != projection.EnvironmentID {
		return nil, invalidBackingAcknowledgement()
	}
	workload, err := backingruntimefact.Workload(
		artifact,
		serviceID,
		input.Source.PlanID,
		input.Source.RenderGeneration,
	)
	if err != nil {
		return nil, err
	}
	projectFound := false
	for _, project := range input.Result.Projects {
		if project.ProjectName == artifact.ProjectName {
			if projectFound || project.CollisionCount != 0 {
				return nil, invalidBackingAcknowledgement()
			}
			projectFound = true
		}
	}
	if !projectFound {
		return nil, invalidBackingAcknowledgement()
	}
	var observed *backingruntimefact.Observation
	for index := range input.Result.BackingObservations {
		candidate := &input.Result.BackingObservations[index]
		if candidate.ServiceID != serviceID {
			continue
		}
		if observed != nil || !backingruntimefact.MatchesObservation(*candidate, artifact, workload) ||
			candidate.ObservedAt.Before(input.CreatedAt) || candidate.ObservedAt.After(input.Source.AcknowledgedAt) {
			return nil, invalidBackingAcknowledgement()
		}
		observed = candidate
	}
	if observed == nil {
		return nil, invalidBackingAcknowledgement()
	}
	key := backingpostgresrelease.Key(projection.EnvironmentID, serviceID)
	read, err := storage.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, invalidBackingAcknowledgement()
	}
	defer etcdstore.ClearValues(read.Values)
	release, err := backingpostgresrelease.Decode(read.Values[0].Value)
	if err != nil || release.EnvironmentID != projection.EnvironmentID || release.ServiceID != serviceID ||
		release.Release.Image != workload.ImageReference || !release.Release.ContainsRuntimeImageID(observed.LocalImageID) {
		return nil, invalidBackingAcknowledgement()
	}
	artifactSHA, releaseSHA := sha256.Sum256(projection.ComposeArtifact), sha256.Sum256(read.Values[0].Value)
	source := input.Source
	source.ArtifactSHA256 = hex.EncodeToString(artifactSHA[:])
	source.LocalImageID = observed.LocalImageID
	source.ManagedReleaseSHA256 = hex.EncodeToString(releaseSHA[:])
	projection.BackingRuntime = &source
	if err := projectionrecord.ValidateBackingRuntimeReceipt(*projection); err != nil {
		return nil, err
	}
	return []etcdstore.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}, nil
}

func invalidBackingAcknowledgement() error {
	return errs.New(errs.KindStateConflict, "Backing provisioning lacks exact assigned runtime evidence")
}
