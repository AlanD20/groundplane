package s3compatible

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/s3connector"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type ConnectionCheckStage = s3connector.ConnectionCheckStage

type ConnectionCheckResult struct {
	Stages  []ConnectionCheckStage
	Cleanup s3connector.ConnectionCheckCleanup
}

// ConnectionCheckJournal is the durable publication seam, not a logging hook.
// Before CheckConnection the caller acquires exclusive ownership of a fresh
// operation and retains exact encrypted credentials. Write must fence that owner
// and persist pending state before returning. A replay with pending mutations
// calls CleanupConnectionCheck instead of starting the permission check again.
type ConnectionCheckJournal interface {
	WriteConnectionCheckCleanup(context.Context, s3connector.ConnectionCheckCleanup) error
	WriteConnectionCheckStage(context.Context, s3connector.ConnectionCheckStage) error
}

// CheckConnection checks real object and multipart permissions in the configured
// bucket/prefix. It never lists or modifies backup objects. Provider response text
// is not included in results. Save and this operation remain independent.
func (adapter *Adapter) CheckConnection(
	ctx context.Context,
	operationID string,
	journal ConnectionCheckJournal,
) (ConnectionCheckResult, error) {
	if ctx == nil || journal == nil || ids.Validate(ids.KindOperation, operationID) != nil {
		return ConnectionCheckResult{}, errs.New(errs.KindValidationFailed, "connector check authority is required")
	}
	ctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	state, err := s3connector.NewConnectionCheckCleanup(operationID, adapter.config.Prefix)
	if err != nil {
		return ConnectionCheckResult{}, err
	}
	result := ConnectionCheckResult{Cleanup: state}
	if err := journal.WriteConnectionCheckCleanup(ctx, state); err != nil {
		return result, err
	}
	body := s3connector.ConnectionCheckPayload(operationID)
	metadata := checkObjectMetadata(state)
	run := func(stageCtx context.Context, name s3connector.ConnectionCheckStageName, operation func() error) error {
		started := time.Now()
		err := operation()
		stage := ConnectionCheckStage{Name: name, Elapsed: time.Since(started), Succeeded: err == nil}
		if err != nil {
			stage.Failure = s3connector.CheckFailureForError(err)
		}
		result.Stages = append(result.Stages, stage)
		return errors.Join(err, journal.WriteConnectionCheckStage(stageCtx, stage))
	}
	err = run(ctx, s3connector.CheckWriteObject, func() error {
		state.ObjectPending = true
		if err := journal.WriteConnectionCheckCleanup(ctx, state); err != nil {
			return err
		}
		_, err := adapter.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(adapter.config.Bucket), Key: aws.String(state.ObjectKey),
			Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
			Metadata: metadata, IfNoneMatch: aws.String("*"),
		}, oneAttempt)
		return providerError(err)
	})
	if err == nil {
		err = run(ctx, s3connector.CheckReadObject, func() error {
			output, err := adapter.client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(adapter.config.Bucket), Key: aws.String(state.ObjectKey),
			}, oneAttempt)
			if err != nil {
				return providerError(err)
			}
			if output == nil || output.Body == nil {
				return rejectedError()
			}
			observed, readErr := io.ReadAll(io.LimitReader(output.Body, int64(len(body)+1)))
			closeErr := output.Body.Close()
			if readErr != nil || closeErr != nil {
				return providerError(errors.Join(readErr, closeErr))
			}
			if !bytes.Equal(body, observed) || !maps.Equal(metadata, output.Metadata) {
				return checkOwnershipConflict()
			}
			return nil
		})
	}
	if err == nil {
		err = run(ctx, s3connector.CheckMultipart, func() error {
			return adapter.checkMultipart(ctx, &state, journal, body, metadata)
		})
	}
	// Cleanup has its own bounded context even if the request was disconnected or
	// timed out. Pending authority is retained when cleanup or its publication fails.
	cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer stop()
	cleanupErr := run(cleanupCtx, s3connector.CheckCleanup, func() error {
		var cleanupErr error
		state, cleanupErr = adapter.CleanupConnectionCheck(cleanupCtx, state, journal)
		return cleanupErr
	})
	result.Cleanup = state
	return result, errors.Join(err, cleanupErr)
}

func checkObjectMetadata(state s3connector.ConnectionCheckCleanup) map[string]string {
	return map[string]string{"groundplane-check": state.OperationID, "payload-sha256": state.PayloadSHA256}
}

func checkOwnershipConflict() error {
	return errs.New(errs.KindStateConflict, "connector check object ownership changed")
}

func checkCleanupError(action string, err error) error {
	return fmt.Errorf("connector check %s: %w", action, err)
}
