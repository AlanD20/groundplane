package blueprint

import (
	"context"
	"encoding/hex"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// prepareComponentWorkFiles retains source metadata separately from transport
// file descriptors. Neither the input nor its diagnostics contain Secret bytes.
// The configuration publisher must pin these exact sources before work is ready.
func (service *Service) prepareComponentWorkFiles(ctx context.Context, work workqueue.View,
	placement componentWorkPlacement, rendered componentWorkRender,
) ([]taskmaterialization.Record, []*agentpb.MaterializeFile, error) {
	if rendered.Artifact == nil {
		if placement.Candidate.Desired.Enabled {
			return nil, nil, errs.New(errs.KindStateConflict, "Enabled Component has no rendered workload")
		}
		return nil, nil, nil
	}
	inputID := ids.DeriveAt(ids.KindPlan, work.Item.Item.CreatedAt, work.Item.Item.ID, "component-input")
	allocate := func(kind ids.Kind, name string) string {
		return ids.DeriveAt(kind, work.Item.Item.CreatedAt, inputID, name)
	}
	records, err := service.environmentFileSources(ctx, work.Task.Task.Scope.EnvironmentID,
		placement.Selection.Source.project.Record.ID, work.Task.Task.ID,
		placement.Selection.Source.desired.Record.RenderGeneration, allocate, nil,
		rendered.Projection, nil, rendered.Entries)
	if err != nil {
		return nil, nil, err
	}
	files := make([]*agentpb.MaterializeFile, 0, len(records))
	for _, record := range records {
		var output agentpb.MaterializationOutputKind
		switch record.OutputKind {
		case taskmaterialization.OutputPlainFile:
			output = agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE
		case taskmaterialization.OutputGeneratedEnvironment:
			output = agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_GENERATED_ENV
		default:
			return nil, nil, errs.New(errs.KindStateConflict, "Component generated an unsupported file output")
		}
		digest, err := hex.DecodeString(record.SHA256)
		if err != nil {
			return nil, nil, errs.New(errs.KindStateConflict, "Component file digest is invalid")
		}
		files = append(files, &agentpb.MaterializeFile{
			ArtifactId: rendered.Artifact.ArtifactId, MaterializationId: record.MaterializationID,
			EnvironmentId: record.EnvironmentID, Destination: record.Destination,
			ServiceId: record.ServiceID, ServiceName: record.ServiceName, OutputKind: output,
			Uid: record.UID, Gid: record.GID, Mode: record.Mode, Length: record.Length, Sha256: digest,
		})
	}
	slices.SortFunc(files, func(left, right *agentpb.MaterializeFile) int {
		return strings.Compare(left.Destination, right.Destination)
	})
	return records, files, nil
}
