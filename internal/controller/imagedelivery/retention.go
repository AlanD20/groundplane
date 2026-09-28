package imagedelivery

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type imageRetention struct {
	references map[string]string
	busy       string
	fetches    []retainedFetch
}

// Read all authority at one MVCC revision. Pending/failed host work is a
// conservative removal barrier: retry/recovery may still use its sealed input.
// Completed Tasks do not imply that their Release rollback material expired.
func (service *Service) retainedImages(ctx context.Context, operationID string) (imageRetention, error) {
	result := imageRetention{references: make(map[string]string)}
	var revision int64
	read := func(prefix string, visit func(keyvalue.KeyValue) error) error {
		start := ""
		for {
			page, err := service.store.Range(
				ctx,
				keyvalue.RangeRequest{Prefix: prefix, StartExclusive: start, Limit: 128, Revision: revision},
			)
			if err != nil {
				return err
			}
			if page == nil || page.ReadRevision <= 0 || page.More && len(page.Values) == 0 {
				return errs.New(errs.KindInternal, "image authority read is incomplete")
			}
			if revision == 0 {
				revision = page.ReadRevision
			}
			if revision != page.ReadRevision {
				return errs.New(errs.KindInternal, "image authority read changed revision")
			}
			for _, value := range page.Values {
				if err := visit(value); err != nil {
					return err
				}
			}
			if !page.More {
				return nil
			}
			start = page.Values[len(page.Values)-1].Key
		}
	}
	if err := read(taskjournal.TaskPrefix, func(value keyvalue.KeyValue) error {
		task, err := etcd.DecodeTaskRecord(value.Value)
		if err != nil {
			return err
		}
		if task.OperationID != operationID && task.Status != taskjournal.TaskStatusCompleted && task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceImage {
			// Terminal Runner creation requires a fresh token to retry. Protect
			// its exact image through the retained Runner below, not every image
			// through the old attempt. Containers remain independently protected.
			terminalRunnerCreate := task.Executor == taskjournal.TaskExecutorController &&
				task.Type == taskjournal.TaskCreate && task.Params[taskjournal.TaskResourceKindParam] == runners.TaskResourceRunner &&
				taskjournal.IsTerminalTaskStatus(task.Status)
			if !terminalRunnerCreate {
				result.busy = fmt.Sprintf("Task %s may still execute or recover; finish it or let its retention expire", task.ID)
			}
		}
		if task.Type == taskjournal.TaskFetch &&
			task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceImage {
			plan, err := imagefetch.Decode(task.Params[taskjournal.TaskImageFetchInputParam], task.PlanHash)
			if err != nil {
				return err
			}
			result.fetches = append(result.fetches, retainedFetch{plan: plan, taskID: task.ID, requestedAt: task.CreatedAt, status: task.Status})
		}
		return nil
	}); err != nil {
		return result, err
	}
	if err := read(runners.RunnerKey(""), func(value keyvalue.KeyValue) error {
		runner, err := runners.DecodeRunnerDesiredRecord(value.Value)
		if err != nil {
			return err
		}
		result.retain(runner.ImageRef, "Retained Runner "+runner.ID)
		return nil
	}); err != nil {
		return result, err
	}
	if err := read("/v1/staging/release-render-inputs/", func(value keyvalue.KeyValue) error {
		raw, err := releases.DecodeReleaseRecord[json.RawMessage](value.Value, "release-render-input")
		if err != nil {
			return err
		}
		input, err := releaserender.DecodeReleaseRenderInput(raw)
		if err != nil {
			return err
		}
		reason := "Retained Release " + input.ReleaseID
		result.retain(input.CandidateWorkload.LocalImageID, reason)
		if input.PriorWorkload != nil {
			result.retain(input.PriorWorkload.LocalImageID, reason)
		}
		if err := result.artifact(input.Projection.ComposeArtifact, reason); err != nil {
			return err
		}
		if input.PriorRuntime != nil {
			if err := result.artifact(input.PriorRuntime.CurrentArtifact, reason); err != nil {
				return err
			}
			if err := result.artifact(input.PriorRuntime.RetainedPriorArtifact, reason); err != nil {
				return err
			}
		}
		for _, hook := range input.Hooks {
			var snapshot agentpb.ResolvedRunnerSnapshot
			if err := proto.Unmarshal(hook.RunnerSnapshot, &snapshot); err != nil {
				return errs.Wrap(errs.KindInternal, err)
			}
			result.retain(snapshot.GetLocalImageId(), reason)
		}
		return nil
	}); err != nil {
		return result, err
	}
	if err := read(environmentprojection.EnvironmentComposeProjectionPrefix, func(value keyvalue.KeyValue) error {
		projection, err := environmentprojection.DecodeEnvironmentComposeProjectionStorage(value.Value)
		if err != nil {
			return err
		}
		return result.artifact(projection.ComposeArtifact, "Environment runtime "+projection.EnvironmentID)
	}); err != nil {
		return result, err
	}
	return result, nil
}

func (retention imageRetention) artifact(value []byte, reason string) error {
	if len(value) == 0 {
		return nil
	}
	var artifact agentpb.ComposeArtifact
	if err := proto.Unmarshal(value, &artifact); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	for _, service := range artifact.GetServices() {
		if reference := service.GetImageReference(); strings.TrimSpace(reference) != "" {
			retention.retain(reference, reason)
		}
	}
	return nil
}
