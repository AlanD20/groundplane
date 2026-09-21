package taskplanning

import (
	context "context"

	testtaskmaterialization "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
)

type resolverRetainedContent struct {
	content    []byte
	loaded     testtaskmaterialization.Record
	generation uint64
}

func (repository *resolverRetainedContent) Stage(
	_ context.Context,
	record testtaskmaterialization.Record,
	generation uint64,
	content []byte,
) error {
	repository.loaded = record
	repository.generation = generation
	repository.content = append([]byte(nil), content...)
	return nil
}

func (repository *resolverRetainedContent) Load(
	_ context.Context,
	record testtaskmaterialization.Record,
	_ uint64,
) ([]byte, error) {
	repository.loaded = record
	return append([]byte(nil), repository.content...), nil
}
