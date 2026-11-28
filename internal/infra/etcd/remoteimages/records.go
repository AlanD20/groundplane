// Package remoteimages owns sealed remote Fetch plans and verified image witnesses.
package remoteimages

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/remotefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Plan struct {
	TaskID   string                 `json:"task_id"`
	Executor workexecution.Identity `json:"executor"`
	Image    imagefetch.Plan        `json:"image"`
}

type Witness struct {
	HostID       string                 `json:"host_id"`
	Image        string                 `json:"image"`
	LocalImageID string                 `json:"local_image_id"`
	Executor     workexecution.Identity `json:"executor"`
}

func PlanKey(taskID string) string { return "/v1/remote-image-plans/" + taskID }

func WitnessKey(hostID, image string) string {
	digest := sha256.Sum256([]byte(image))
	return "/v1/remote-image-witnesses/" + hostID + "/" + hex.EncodeToString(digest[:])
}

func ReadPlan(ctx context.Context, reader hostbindings.Reader, taskID string,
	revision int64) (keyvalue.Versioned[Plan], error) {
	if ids.Validate(ids.KindTask, taskID) != nil {
		return keyvalue.Versioned[Plan]{}, corrupt()
	}
	value, err := read[Plan](ctx, reader, PlanKey(taskID), "remote-image-plan", revision)
	if err == nil && value.Revision != 0 && (value.Record.TaskID != taskID || !validPlan(value.Record)) {
		return keyvalue.Versioned[Plan]{}, corrupt()
	}
	return value, err
}

func PreparePlan(plan Plan) (workqueue.Change, error) {
	if !validPlan(plan) {
		return workqueue.Change{}, corrupt()
	}
	return put(PlanKey(plan.TaskID), "remote-image-plan", plan, 0)
}

func ReadWitness(ctx context.Context, reader hostbindings.Reader, hostID, image string,
	revision int64) (keyvalue.Versioned[Witness], error) {
	ref, err := remotefetch.Reference(image)
	if err != nil || ids.Validate(ids.KindHost, hostID) != nil {
		return keyvalue.Versioned[Witness]{}, corrupt()
	}
	value, err := read[Witness](ctx, reader, WitnessKey(hostID, ref), "remote-image-witness", revision)
	if err == nil && value.Revision != 0 &&
		(value.Record.HostID != hostID || value.Record.Image != ref || !validWitness(value.Record)) {
		return keyvalue.Versioned[Witness]{}, corrupt()
	}
	return value, err
}

func PrepareWitness(witness Witness, prior int64) (workqueue.Change, error) {
	if !validWitness(witness) || prior < 0 {
		return workqueue.Change{}, corrupt()
	}
	return put(WitnessKey(witness.HostID, witness.Image), "remote-image-witness", witness, prior)
}

func validPlan(plan Plan) bool {
	return ids.Validate(ids.KindTask, plan.TaskID) == nil && plan.Executor.Executor == workexecution.Agent &&
		plan.Executor.Validate() == nil && remotefetch.ValidatePlan(plan.Image) == nil
}

func validWitness(witness Witness) bool {
	ref, err := remotefetch.Reference(witness.Image)
	return err == nil && ref == witness.Image && workloadimage.LocalIDValid(witness.LocalImageID) &&
		witness.Executor.HostID == witness.HostID && witness.Executor.Executor == workexecution.Agent &&
		witness.Executor.Validate() == nil
}

func read[T any](ctx context.Context, reader hostbindings.Reader, key, kind string,
	revision int64) (keyvalue.Versioned[T], error) {
	result, err := reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[T]{}, err
	}
	if result == nil || result.ReadRevision <= 0 || len(result.Values) != 1 ||
		revision > 0 && result.ReadRevision != revision {
		return keyvalue.Versioned[T]{}, corrupt()
	}
	defer keyvalue.ClearValues(result.Values)
	value := result.Values[0]
	if value == nil {
		return keyvalue.Versioned[T]{ReadRevision: result.ReadRevision}, nil
	}
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > result.ReadRevision {
		return keyvalue.Versioned[T]{}, corrupt()
	}
	record, err := recordcodec.Decode[T](value.Value, kind)
	if err != nil {
		return keyvalue.Versioned[T]{}, err
	}
	return keyvalue.Versioned[T]{ReadRevision: result.ReadRevision, Revision: value.ModRevision, Record: record}, nil
}

func put(key, kind string, record any, revision int64) (workqueue.Change, error) {
	encoded, err := recordcodec.Encode(kind, record)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: key, ModRevision: revision}},
		Mutations:  []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: encoded}},
	}, nil
}

func corrupt() error { return errs.New(errs.KindInternal, "Remote Fetch evidence is inconsistent") }
