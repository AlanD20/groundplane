package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestDispatcherRetryPreservesOperationContract(t *testing.T) {
	dispatcher := NewDispatcher()
	original, err := dispatcher.Dispatch(context.Background(), DispatchRequest{
		Type: TaskScript, Target: "svc_1", Params: map[string]string{"name": "migrate"},
		Steps: []adapters.Step{{Op: adapters.StepRunScript}}, Timeout: 2 * time.Minute,
		PlanHash: "plan-hash", IdempotencyKey: "operation-key",
	})
	if err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	original.Status = StatusFailed

	retry, err := dispatcher.Retry(context.Background(), original.ID)
	if err != nil {
		t.Fatalf("Retry() error = %v", err)
	}
	if retry.ID == original.ID || retry.RetryOf != original.ID {
		t.Fatalf("retry identity = %#v", retry)
	}
	if retry.OperationID != original.OperationID || retry.IdempotencyKey != original.IdempotencyKey || retry.PlanHash != original.PlanHash || retry.Target != original.Target || retry.Timeout != original.Timeout || !reflect.DeepEqual(retry.Params, original.Params) || !reflect.DeepEqual(retry.Steps, original.Steps) {
		t.Fatalf("retry did not preserve operation contract: original=%#v retry=%#v", original, retry)
	}
}

func TestDispatcherRetryRejectsInvalidSourceAndActiveDuplicate(t *testing.T) {
	dispatcher := NewDispatcher()
	original, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Type: TaskScript, Target: "svc_1"})
	if err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	if _, err := dispatcher.Retry(context.Background(), original.ID); !errors.Is(err, errs.New(errs.CodeTaskNotRetryable, "")) {
		t.Fatalf("Retry(pending) error = %v, want task.not_retryable", err)
	}
	original.Status = StatusAborted
	if _, err := dispatcher.Retry(context.Background(), original.ID); err != nil {
		t.Fatalf("Retry(aborted) error = %v", err)
	}
	if _, err := dispatcher.Retry(context.Background(), original.ID); !errors.Is(err, errs.New(errs.CodeTaskRetryInFlight, "")) {
		t.Fatalf("second Retry() error = %v, want task.retry_in_flight", err)
	}
}
