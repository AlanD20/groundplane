package materialization

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/infra/docker/materializerrunner"
)

const (
	materializationTaskID        = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationStepID        = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationVolumeDir     = "/var/lib/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + materializationEnvironmentID
)

type decodingMaterializationHelper struct {
	volumeDir string
	header    entrymaterialization.Header
	content   []byte
	err       error
}

func (helper *decodingMaterializationHelper) StopCaptured(context.Context, materializerrunner.Ownership) error {
	return nil
}

func (helper *decodingMaterializationHelper) Run(
	ctx context.Context,
	request materializerrunner.Request,
) error {
	helper.volumeDir = request.VolumeDir
	header, err := entrymaterialization.Decode(
		ctx,
		request.Stream,
		entrymaterialization.Limits{
			MaxContentBytes:     entrymaterialization.MaximumContentBytes,
			MaxDestinationBytes: entrymaterialization.MaximumDestinationBytes,
		},
		func(_ context.Context, _ entrymaterialization.Header, source io.Reader) error {
			content, readErr := io.ReadAll(source)
			helper.content = append([]byte(nil), content...)
			return readErr
		},
	)
	helper.header = header
	if err != nil {
		return err
	}
	return helper.err
}
