package handlers

import (
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func imageFetchTaskDetails(task etcd.TaskRecord) (*apiTypes.TaskImageFetch, error) {
	if task.Type != taskjournal.TaskFetch || task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceImage {
		return nil, nil
	}
	plan, err := imagefetch.Decode(task.Params[taskjournal.TaskImageFetchInputParam], task.PlanHash)
	if err != nil {
		return nil, err
	}
	platform := "linux/" + plan.Architecture
	if plan.Variant != "" {
		platform += "/" + plan.Variant
	}
	return &apiTypes.TaskImageFetch{Requested: plan.Requested, Image: plan.Reference(), Platform: platform, Progress: task.ImageFetchProgress}, nil
}
