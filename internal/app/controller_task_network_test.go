package app

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type fakeBackingZoneCascadeExecutor struct{}

func (*fakeBackingZoneCascadeExecutor) Execute(context.Context, etcd.TaskRecord) error { return nil }

func testBackingZoneCascade(t *testing.T) backingZoneCascadeExecutor {
	t.Helper()
	return &fakeBackingZoneCascadeExecutor{}
}
