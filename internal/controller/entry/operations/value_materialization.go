package operations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/ids"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/entrygeneration"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/pkg/errs"
	"sort"
	"time"
)

func (service *entryDesiredMutationService) prepareEntryGeneration(
	ctx context.Context,
	projectID string,
	environmentID string,
	desired core.EnvEntry,
	record entryrecord.Record,
	createdAt time.Time,
) error {
	found, err := service.repository.BlueprintEntryValueGenerationExists(ctx, record)
	if err != nil {
		return err
	}
	if !found {
		desired.ID = record.Entry.ID
		generation, err := service.generator.Generate(ctx, projectID, environmentID, desired,
			record.CurrentValueGenerationID, createdAt)
		if err != nil {
			return err
		}
		createErr := service.repository.CreateBlueprintEntryValueGeneration(ctx, generation)
		entrygeneration.ClearEntryValueGeneration(&generation)
		if createErr != nil {
			return createErr
		}
	}
	return service.repository.BindBlueprintEntryEnvironment(ctx, environmentID, record.Entry.ID)
}

func (service *entryDesiredMutationService) entryMaterializations(
	ctx context.Context,
	environmentID string,
	allocator *controllerrevision.BlueprintIdentityAllocator,
	inputs []composerender.EnvironmentEntryMaterialization,
) ([]etcd.TaskMaterializationRecord, error) {
	sort.Slice(inputs, func(left, right int) bool { return inputs[left].Destination < inputs[right].Destination })
	records := make([]etcd.TaskMaterializationRecord, 0, len(inputs))
	previous := ""
	for _, input := range inputs {
		if input.Destination == previous {
			return nil, errs.New(errs.KindNameConflict, "Environment materialization destination is duplicated")
		}
		content, err := service.materials.ResolveTaskMaterializationSource(ctx, environmentID, input.Source)
		if err != nil {
			clear(content)
			return nil, err
		}
		digest := sha256.Sum256(content)
		record := etcd.TaskMaterializationRecord{
			StepID:            allocator.Named(ids.KindStep, "entry-materialization-step/"+input.Destination),
			MaterializationID: allocator.Named(ids.KindConfig, "entry-materialization/"+input.Destination),
			EnvironmentID:     environmentID, Destination: input.Destination,
			ServiceID: input.ServiceID, ServiceName: input.ServiceName,
			OutputKind: input.OutputKind, UID: input.UID, GID: input.GID, Mode: uint32(input.Mode),
			Length: uint64(len(content)), SHA256: hex.EncodeToString(digest[:]), Source: input.Source,
		}
		clear(content)
		records = append(records, record)
		previous = input.Destination
	}
	sort.Slice(records, func(left, right int) bool { return records[left].StepID < records[right].StepID })
	return records, nil
}
