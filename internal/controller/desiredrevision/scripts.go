package desiredrevision

import (
	"crypto/sha256"
	"encoding/hex"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumBlueprintScripts = 64

// BlueprintScriptReconciliation is the complete next Script projection for an
// Environment. BodyGenerations contains the append-only records that must be
// created for new or changed bodies; unchanged bodies already have their
// immutable generation in durable state.
type BlueprintScriptReconciliation struct {
	Current         []scriptrecord.Record
	BodyGenerations []scriptrecord.BodyGenerationRecord
}

// ReconcileBlueprintScripts resolves authored service names to stable Service
// ids and produces durable Script records without mutating storage. A
// Blueprint-origin Script is matched only by its immutable reconciliation key;
// omitted records remain visible, including records edited through the human
// API. API-origin records are never adopted by a Blueprint key.
func ReconcileBlueprintScripts(
	environmentID string,
	authored map[string]core.ScriptSpec,
	services []etcd.ServiceRecord,
	previous []scriptrecord.Record,
	resources BlueprintScriptResources,
	allocate func(ids.Kind, string) string,
) (BlueprintScriptReconciliation, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || allocate == nil {
		return BlueprintScriptReconciliation{}, errs.New(
			errs.KindInternal,
			"Blueprint Script reconciliation input is invalid",
		)
	}
	if len(previous) > maximumBlueprintScripts || len(authored) > maximumBlueprintScripts {
		return BlueprintScriptReconciliation{}, errs.New(
			errs.KindValidationFailed,
			"Environment exceeds the 64 Script limit",
		)
	}

	servicesByName, servicesByID, err := scriptServices(environmentID, services)
	if err != nil {
		return BlueprintScriptReconciliation{}, err
	}

	currentByID := make(map[string]scriptrecord.Record, len(previous))
	blueprintByKey := make(map[string]scriptrecord.Record, len(previous))
	for _, record := range previous {
		if err := validateBlueprintScriptRecord(environmentID, record, servicesByID); err != nil {
			return BlueprintScriptReconciliation{}, err
		}
		if _, duplicate := currentByID[record.Desired.ID]; duplicate {
			return BlueprintScriptReconciliation{}, errs.New(
				errs.KindInternal,
				"durable Script projection repeats an id",
			)
		}
		currentByID[record.Desired.ID] = record
		if record.Origin == "blueprint" {
			if _, duplicate := blueprintByKey[record.ReconciliationKey]; duplicate {
				return BlueprintScriptReconciliation{}, errs.New(
					errs.KindInternal,
					"durable Blueprint Script projection repeats a reconciliation key",
				)
			}
			blueprintByKey[record.ReconciliationKey] = record
		}
	}

	keys := make([]string, 0, len(authored))
	for key := range authored {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := make(map[string]scriptrecord.Record, len(previous)+len(authored))
	for _, record := range previous {
		result[record.Desired.ID] = record
	}
	generations := make([]scriptrecord.BodyGenerationRecord, 0, len(authored))
	for _, key := range keys {
		spec := authored[key]
		if err := validateBlueprintScriptSpec(key, spec); err != nil {
			return BlueprintScriptReconciliation{}, err
		}
		service, exists := servicesByName[spec.Service]
		if !exists {
			return BlueprintScriptReconciliation{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Script references an unknown Service",
			)
		}
		if service.Desired.Replicas < 1 {
			return BlueprintScriptReconciliation{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Script target Service must have positive replicas",
			)
		}
		if service.Desired.Adapter != "" || service.BackingNetworkID != "" {
			return BlueprintScriptReconciliation{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Script target must be an operator-owned Service",
			)
		}

		execution, err := resources.resolveExecution(environmentID, spec)
		if err != nil {
			return BlueprintScriptReconciliation{}, err
		}
		current, exists := blueprintByKey[key]
		if !exists {
			if len(result) >= maximumBlueprintScripts {
				return BlueprintScriptReconciliation{}, errs.New(
					errs.KindValidationFailed,
					"Environment exceeds the 64 Script limit",
				)
			}
			scriptID := allocate(ids.KindScript, "script/"+key)
			if ids.Validate(ids.KindScript, scriptID) != nil {
				return BlueprintScriptReconciliation{}, errs.New(
					errs.KindInternal,
					"Blueprint Script allocator returned an invalid id",
				)
			}
			if _, duplicate := currentByID[scriptID]; duplicate {
				return BlueprintScriptReconciliation{}, errs.New(
					errs.KindInternal,
					"Blueprint Script allocator reused an id",
				)
			}
			current, err = newBlueprintScriptRecord(environmentID, service, key, scriptID, spec, execution)
			if err != nil {
				return BlueprintScriptReconciliation{}, err
			}
			currentByID[scriptID] = current
			generations = append(generations, scriptBodyGeneration(current))
		} else {
			if current.ServiceID != service.Desired.ID {
				return BlueprintScriptReconciliation{}, errs.New(
					errs.KindValidationFailed,
					"Blueprint Script target Service is immutable",
				)
			}
			desired := core.Script{
				ID: current.Desired.ID, Slug: spec.Slug, ServiceName: service.Desired.Name,
				Body: spec.Script, When: spec.When, Order: spec.Order, Execution: execution,
			}
			bodyChanged := desired.Body != current.Desired.Body
			updated, replaceErr := scriptrecord.ReplaceDesired(current, desired)
			if replaceErr != nil {
				return BlueprintScriptReconciliation{}, errs.Wrap(errs.KindValidationFailed, replaceErr)
			}
			current = updated
			if bodyChanged {
				generations = append(generations, scriptBodyGeneration(current))
			}
		}
		result[current.Desired.ID] = current
		delete(blueprintByKey, key)
	}

	scripts := make([]scriptrecord.Record, 0, len(result))
	slugs := make(map[string]string, len(result))
	for _, record := range result {
		if owner, duplicate := slugs[record.Desired.Slug]; duplicate && owner != record.Desired.ID {
			return BlueprintScriptReconciliation{}, errs.New(
				errs.KindNameConflict,
				"Script slug is already in use",
			)
		}
		slugs[record.Desired.Slug] = record.Desired.ID
		scripts = append(scripts, record)
	}
	sort.Slice(scripts, func(left, right int) bool {
		if scripts[left].Desired.Slug != scripts[right].Desired.Slug {
			return scripts[left].Desired.Slug < scripts[right].Desired.Slug
		}
		return scripts[left].Desired.ID < scripts[right].Desired.ID
	})
	sort.Slice(generations, func(left, right int) bool {
		if generations[left].ScriptID != generations[right].ScriptID {
			return generations[left].ScriptID < generations[right].ScriptID
		}
		return generations[left].Generation < generations[right].Generation
	})
	return BlueprintScriptReconciliation{Current: scripts, BodyGenerations: generations}, nil
}

func scriptServices(
	environmentID string,
	services []etcd.ServiceRecord,
) (map[string]etcd.ServiceRecord, map[string]etcd.ServiceRecord, error) {
	byName := make(map[string]etcd.ServiceRecord, len(services))
	byID := make(map[string]etcd.ServiceRecord, len(services))
	for _, service := range services {
		if service.EnvironmentID != environmentID || ids.Validate(ids.KindService, service.Desired.ID) != nil ||
			service.Desired.Name == "" || service.Desired.Validate() != nil {
			return nil, nil, errs.New(errs.KindInternal, "Blueprint Script Service projection is invalid")
		}
		if _, duplicate := byName[service.Desired.Name]; duplicate {
			return nil, nil, errs.New(errs.KindInternal, "Blueprint Script Service projection repeats a name")
		}
		if _, duplicate := byID[service.Desired.ID]; duplicate {
			return nil, nil, errs.New(errs.KindInternal, "Blueprint Script Service projection repeats an id")
		}
		byName[service.Desired.Name] = service
		byID[service.Desired.ID] = service
	}
	return byName, byID, nil
}

func validateBlueprintScriptSpec(key string, spec core.ScriptSpec) error {
	if core.ValidateScriptLabel("script reconciliation key", key) != nil ||
		core.ValidateScriptLabel("script slug", spec.Slug) != nil || spec.Service == "" {
		return errs.New(errs.KindValidationFailed, "Blueprint Script is invalid")
	}
	candidate := core.Script{
		ID: "script-validation", Slug: spec.Slug, ServiceName: spec.Service,
		Body: spec.Script, When: spec.When, Order: spec.Order,
	}
	if err := candidate.Validate(); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	return nil
}

func newBlueprintScriptRecord(
	environmentID string,
	service etcd.ServiceRecord,
	key string,
	scriptID string,
	spec core.ScriptSpec,
	execution *core.ScriptExecution,
) (scriptrecord.Record, error) {
	record, err := scriptrecord.NewRecord(environmentID, service.Desired.ID, core.Script{
		ID: scriptID, Slug: spec.Slug, ServiceName: service.Desired.Name,
		Body: spec.Script, When: spec.When, Order: spec.Order, Execution: execution,
	})
	if err != nil {
		return scriptrecord.Record{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	record.Origin = "blueprint"
	record.ReconciliationKey = key
	return record, nil
}

func validateBlueprintScriptRecord(
	environmentID string,
	record scriptrecord.Record,
	servicesByID map[string]etcd.ServiceRecord,
) error {
	if record.EnvironmentID != environmentID || ids.Validate(ids.KindService, record.ServiceID) != nil ||
		ids.Validate(ids.KindScript, record.Desired.ID) != nil || record.ActiveGeneration == 0 ||
		record.Desired.Validate() != nil {
		return errs.New(errs.KindInternal, "durable Script projection is invalid")
	}
	if _, exists := servicesByID[record.ServiceID]; !exists {
		return errs.New(errs.KindInternal, "durable Script projection targets an unknown Service")
	}
	service := servicesByID[record.ServiceID]
	if service.Desired.Replicas < 1 {
		return errs.New(errs.KindValidationFailed, "durable Script target Service must have positive replicas")
	}
	if service.Desired.Adapter != "" || service.BackingNetworkID != "" {
		return errs.New(errs.KindValidationFailed, "durable Script target must be an operator-owned Service")
	}
	switch record.Origin {
	case "api":
		if record.ReconciliationKey != "" {
			return errs.New(errs.KindInternal, "API Script has a reconciliation key")
		}
	case "blueprint":
		if core.ValidateScriptLabel("script reconciliation key", record.ReconciliationKey) != nil {
			return errs.New(errs.KindInternal, "Blueprint Script reconciliation key is invalid")
		}
	default:
		return errs.New(errs.KindInternal, "durable Script origin is invalid")
	}
	return nil
}

func scriptBodyGeneration(record scriptrecord.Record) scriptrecord.BodyGenerationRecord {
	digest := sha256.Sum256([]byte(record.Desired.Body))
	return scriptrecord.BodyGenerationRecord{
		ScriptID: record.Desired.ID, Generation: record.ActiveGeneration,
		BodySize: uint32(len(record.Desired.Body)), Body: record.Desired.Body,
		BodySHA256: hex.EncodeToString(digest[:]),
	}
}
