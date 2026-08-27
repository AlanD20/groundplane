package agent

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/components/coredns"
)

type coreDNSExecutorFake struct {
	calls       []string
	validateErr error
}

func (fake *coreDNSExecutorFake) ValidateCorefile(context.Context, coredns.TaskPlan) error {
	fake.calls = append(fake.calls, "validate")
	return fake.validateErr
}

func (fake *coreDNSExecutorFake) ReloadCorefile(context.Context, coredns.TaskPlan) error {
	fake.calls = append(fake.calls, "reload")
	return nil
}

func (fake *coreDNSExecutorFake) ObserveCoreDNS(context.Context, coredns.TaskPlan) (bool, error) {
	fake.calls = append(fake.calls, "observe")
	return true, nil
}

func TestApplyCoreDNSValidatesBeforeReload(t *testing.T) {
	t.Parallel()
	fake := &coreDNSExecutorFake{validateErr: errors.New("invalid candidate")}
	corefile := []byte("candidate")
	_, err := ApplyCoreDNS(context.Background(), fake, coredns.TaskPlan{
		ComponentID: "cmp_coredns", ServiceID: "svc_coredns", Corefile: corefile, CorefileSHA256: sha256.Sum256(corefile),
		Steps: []coredns.TaskStep{coredns.TaskStepValidateConfig, coredns.TaskStepRender, coredns.TaskStepApply, coredns.TaskStepObserve},
	})
	if err == nil || len(fake.calls) != 1 || fake.calls[0] != "validate" {
		t.Fatalf("ApplyCoreDNS() err=%v calls=%v, want validation without reload", err, fake.calls)
	}
}
