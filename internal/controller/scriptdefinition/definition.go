package scriptdefinition

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ValidateCreation validates authored Script fields before resolving the Service.
func ValidateCreation(input apiTypes.ScriptCreate) error {
	if ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil {
		return errs.New(errs.KindValidationFailed, "Script creation requires a stable Environment id")
	}
	if ids.Validate(ids.KindService, input.ServiceID) != nil {
		return errs.New(errs.KindValidationFailed, "Script creation requires a stable target Service id")
	}
	if err := (core.Script{
		ID: ids.New(ids.KindScript), Slug: input.Slug, ServiceName: "validated-after-service-resolution",
		Body: input.Body, When: core.ScriptHook(input.When), Order: input.Order,
		Execution: executionFromAPI(input.Execution),
	}).Validate(); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	return nil
}

// Response projects the stored Script without changing ownership or generation.
func Response(record etcd.ScriptRecord) apiTypes.Script {
	return apiTypes.Script{
		ID: record.Desired.ID, EnvironmentID: record.EnvironmentID, Slug: record.Desired.Slug,
		ServiceID: record.ServiceID, ServiceName: record.Desired.ServiceName,
		Body: record.Desired.Body, When: string(record.Desired.When), Order: record.Desired.Order, Origin: record.Origin,
		ReconciliationKey: record.ReconciliationKey, ActiveGeneration: record.ActiveGeneration,
		Execution: executionResponse(record.Desired.Execution),
	}
}

// CreateIntentBody captures the authored creation intent.
func CreateIntentBody(input apiTypes.ScriptCreate) idempotentintent.Value {
	fields := []idempotentintent.Field{
		idempotentintent.Field{Name: "environment_id", Value: idempotentintent.String(input.EnvironmentID)},
		idempotentintent.Field{Name: "slug", Value: idempotentintent.String(input.Slug)},
		idempotentintent.Field{Name: "script", Value: idempotentintent.String(input.Body)},
		idempotentintent.Field{Name: "service_id", Value: idempotentintent.String(input.ServiceID)},
		idempotentintent.Field{Name: "when", Value: idempotentintent.String(input.When)},
	}
	// Zero is the default and preserves pre-order creation intent identity.
	if input.Order != 0 {
		fields = append(
			fields,
			idempotentintent.Field{Name: "order", Value: idempotentintent.Integer(int64(input.Order))},
		)
	}
	// Inheritance is the create default and keeps earlier protected intent identity.
	if input.Execution != nil && input.Execution.Mode != "inherited" {
		fields = append(fields, idempotentintent.Field{Name: "execution", Value: executionIntent(*input.Execution)})
	}
	return idempotentintent.Object(fields...)
}

// EditIntentBody retains precisely the supplied patch fields.
func EditIntentBody(input apiTypes.ScriptEdit) idempotentintent.Value {
	fields := make([]idempotentintent.Field, 0, 5)
	if input.Slug != nil {
		fields = append(fields, idempotentintent.Field{Name: "slug", Value: idempotentintent.String(*input.Slug)})
	}
	if input.Body != nil {
		fields = append(fields, idempotentintent.Field{Name: "script", Value: idempotentintent.String(*input.Body)})
	}
	if input.When != nil {
		fields = append(fields, idempotentintent.Field{Name: "when", Value: idempotentintent.String(*input.When)})
	}
	if input.Order != nil {
		fields = append(
			fields,
			idempotentintent.Field{Name: "order", Value: idempotentintent.Integer(int64(*input.Order))},
		)
	}
	if input.Execution != nil {
		fields = append(fields, idempotentintent.Field{Name: "execution", Value: executionIntent(*input.Execution)})
	}
	return idempotentintent.Object(fields...)
}

// CreateDesired binds the new Script identity to the resolved Service label.
func CreateDesired(input apiTypes.ScriptCreate, scriptID, serviceName string) core.Script {
	return core.Script{
		ID: scriptID, Slug: input.Slug, ServiceName: serviceName,
		Body: input.Body, When: core.ScriptHook(input.When), Order: input.Order,
		Execution: executionFromAPI(input.Execution),
	}
}

// EditDesired replaces only authored fields and refreshes the Service label.
func EditDesired(current core.Script, serviceName string, input apiTypes.ScriptEdit) core.Script {
	desired := current
	desired.ServiceName = serviceName
	if input.Slug != nil {
		desired.Slug = *input.Slug
	}
	if input.Body != nil {
		desired.Body = *input.Body
	}
	if input.When != nil {
		desired.When = core.ScriptHook(*input.When)
	}
	if input.Order != nil {
		desired.Order = *input.Order
	}
	if input.Execution != nil {
		desired.Execution = executionFromAPI(input.Execution)
	}
	return desired
}

// ValidateEdit rejects an empty mutation.
func ValidateEdit(input apiTypes.ScriptEdit) error {
	if input.Slug == nil && input.Body == nil && input.When == nil && input.Order == nil && input.Execution == nil {
		return errs.New(errs.KindValidationFailed, "Script edit requires slug, script, when, order, or execution")
	}
	if execution := executionFromAPI(input.Execution); execution != nil {
		return execution.Validate()
	}
	return nil
}
