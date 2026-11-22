package softwareactivation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/consolerelease"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
)

func TestStagingReceiptPresenceRetainsAcceptedEvidence(t *testing.T) {
	cases := []struct {
		name       string
		selection  preparation.Selection
		resultJSON string
	}{
		{name: "legacy missing Console field", selection: preparation.SelectionAgent,
			resultJSON: `{"agent":true,"settled":true}`},
		{name: "intermediate explicit false", selection: preparation.SelectionAgent,
			resultJSON: `{"agent":true,"console":false,"settled":true}`},
		{name: "Console staged", selection: preparation.SelectionConsole,
			resultJSON: `{"agent":false,"console":true,"settled":true}`},
	}
	for index, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			record, attempt, _ := stagingReceiptFixture(t, test.selection, int64(index+1))
			at := record.Record.CreatedAt.Add(2 * time.Minute).Format(time.RFC3339Nano)
			raw := []byte(fmt.Sprintf(
				`{"schema":1,"kind":"software_staging_result","data":{"task_id":"%s","work_item_id":"%s","attempt_id":"%s","input":{"id":"%s","sha256":"%s"},"result":%s,"at":"%s"}}`,
				attempt.TaskID,
				attempt.WorkItemID,
				attempt.ID,
				attempt.Input.ID,
				attempt.Input.SHA256,
				test.resultJSON,
				at,
			))
			repository, err := NewRepository(&stagingReceiptReadStore{key: StagingResultKey(attempt.ID), value: raw})
			if err != nil {
				t.Fatal(err)
			}
			saved, found, err := repository.StagingResult(t.Context(), attempt, 9)
			if err != nil || !found {
				t.Fatalf("read staging receipt: found=%v err=%v", found, err)
			}
			reencoded, err := encodeStagingReceipt(saved.Record)
			if err != nil || !bytes.Equal(reencoded, raw) {
				t.Fatalf("receipt presence changed across repository read: err=%v", err)
			}
			_, completion, err := PrepareStagingResult(record, saved, attempt)
			if err != nil {
				t.Fatal(err)
			}
			expected := sha256.Sum256(raw)
			if completion.Evidence.SHA256 != hex.EncodeToString(expected[:]) {
				t.Fatalf("completion evidence = %s, want stored receipt digest %x",
					completion.Evidence.SHA256, expected)
			}
		})
	}
}

func TestStagingReceiptPresenceCannotClaimForeignConsoleEffect(t *testing.T) {
	record, attempt, result := stagingReceiptFixture(t, preparation.SelectionAgent, 10)
	claimed := true
	result.Console = &claimed
	receipt := StagingReceipt{TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID,
		AttemptID: attempt.ID, Input: attempt.Input, Result: result,
		At: record.Record.CreatedAt.Add(2 * time.Minute)}
	raw, err := encodeStagingReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := NewRepository(&stagingReceiptReadStore{key: StagingResultKey(attempt.ID), value: raw})
	if err != nil {
		t.Fatal(err)
	}
	saved, found, err := repository.StagingResult(t.Context(), attempt, 9)
	if err != nil || !found {
		t.Fatalf("read staging receipt: found=%v err=%v", found, err)
	}
	if _, _, err := PrepareStagingResult(record, saved, attempt); err == nil {
		t.Fatal("foreign Console effect was accepted")
	}
}

func stagingReceiptFixture(
	t *testing.T,
	selection preparation.Selection,
	seed int64,
) (keyvalue.Versioned[Record], workexecution.Attempt, StagingResult) {
	t.Helper()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	digest := "sha256:" + strings.Repeat("a", 64)
	source := preparation.ResolvedSource{Selection: selection, Platform: preparation.HostPlatform()}
	prepared := preparation.Result{}
	staging := StagingResult{Settled: true}
	switch selection {
	case preparation.SelectionAgent:
		source.SourceKind, source.OriginalRef = preparation.SourceRef, "main"
		source.ResolvedSHA, source.CommitTime = strings.Repeat("b", 40), at
		artifact := preparation.Artifact{Reference: imagefetch.RegistryAuthority + "/groundplane-agent@" + digest,
			ManifestDigest: digest, ConfigDigest: digest, Platform: preparation.HostPlatform()}
		prepared = preparation.Result{Source: source, Agent: &preparation.AgentResult{Artifact: artifact}}
		staging.Agent = true
	case preparation.SelectionConsole:
		source.SourceKind, source.OriginalRef = preparation.Release, "console/v0.0.1"
		source.ConsoleRelease = &preparation.ResolvedRelease{OriginalRef: source.OriginalRef,
			ResolvedSHA: strings.Repeat("b", 40), CommitTime: at,
			Assets: []preparation.ReleaseAsset{{Name: "groundplane-console.tar.gz", Size: 1, SHA256: digest}}}
		prepared = preparation.Result{
			Source: source,
			Console: &preparation.ConsoleResult{Artifact: consolerelease.Artifact{
				Release: source.OriginalRef, BundleSHA256: digest, ManifestSHA256: digest, APISHA256: digest}},
		}
	default:
		t.Fatalf("unsupported fixture selection %q", selection)
	}
	preparationTask := ids.NewAt(ids.KindTask, at, seed)
	preparationOperation := ids.NewAt(ids.KindOperation, at, seed+20)
	preparedRecord := preparationrecord.Record{TaskID: preparationTask, OperationID: preparationOperation,
		InputSHA256: strings.Repeat("c", 64), Source: source,
		Progress: preparation.Progress{
			Phase:  preparation.PhaseVerified,
			Result: prepared,
		}, CreatedAt: at, UpdatedAt: at}
	if err := preparationrecord.ValidateRecord(preparedRecord); err != nil {
		t.Fatal(err)
	}

	agentID := ids.NewAt(ids.KindAgent, at, seed+30)
	input := Input{OperationID: ids.NewAt(ids.KindOperation, at, seed+40), Preparation: preparedRecord}
	if selection == preparation.SelectionAgent {
		input.Agent = &upgrade.AgentPredecessor{ID: agentID,
			Image: imagefetch.RegistryAuthority + "/groundplane-agent@" + digest, Generation: 1}
	}
	return stagingRecordFixture(t, at, seed, input, staging)
}

func stagingRecordFixture(
	t *testing.T,
	at time.Time,
	seed int64,
	input Input,
	result StagingResult,
) (keyvalue.Versioned[Record], workexecution.Attempt, StagingResult) {
	t.Helper()
	_, inputSHA256, err := EncodeInput(input)
	if err != nil {
		t.Fatal(err)
	}
	record := Record{TaskID: ids.NewAt(ids.KindTask, at, seed+50),
		WorkItemID: ids.NewAt(ids.KindWorkItem, at, seed+51), OperationID: input.OperationID,
		InputSHA256: inputSHA256, Input: input, Progress: Progress{Phase: PhaseAccepted}, CreatedAt: at, UpdatedAt: at}
	selection := input.Preparation.Source.Selection
	if selection == preparation.SelectionAgent {
		record.AgentWorkItemID = ids.NewAt(ids.KindWorkItem, at, seed+52)
	}
	if selection.IncludesConsole() {
		record.ConsoleWorkItemID = ids.NewAt(ids.KindWorkItem, at, seed+53)
	}
	if err := ValidateRecord(record); err != nil {
		t.Fatal(err)
	}
	reference, err := record.Reference()
	if err != nil {
		t.Fatal(err)
	}
	attempt := workexecution.Attempt{ID: ids.NewAt(ids.KindAttempt, at, seed+54), WorkItemID: record.WorkItemID,
		TaskID: record.TaskID, Number: 1, Executor: workexecution.Identity{Executor: workexecution.Controller, Generation: 1},
		Direction: workexecution.Forward, Input: reference, AssignedAt: at.Add(time.Minute), Deadline: at.Add(time.Hour)}
	if err := attempt.Validate(); err != nil {
		t.Fatal(err)
	}
	return keyvalue.Versioned[Record]{Record: record, Revision: 5, ReadRevision: 9}, attempt, result
}

type stagingReceiptReadStore struct {
	key   string
	value []byte
}

func (store *stagingReceiptReadStore) GetMany(
	_ context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	if len(request.Keys) != 1 || request.Keys[0] != store.key || request.Revision != 9 {
		return nil, errors.New("unexpected staging receipt read")
	}
	return &keyvalue.GetManyResult{Values: []*keyvalue.KeyValue{{Key: store.key,
		Value: bytes.Clone(store.value), ModRevision: 7}}, ReadRevision: 9, ResponseRevision: 9}, nil
}

func (*stagingReceiptReadStore) Transact(
	context.Context,
	[]keyvalue.Condition,
	[]keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	return keyvalue.TransactionResult{}, errors.New("unexpected staging receipt write")
}
