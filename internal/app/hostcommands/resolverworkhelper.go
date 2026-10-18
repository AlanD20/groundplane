package hostcommands

import (
	"context"
	"io"

	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	"github.com/AlanD20/groundplane/internal/agent/componentaction"
	"github.com/AlanD20/groundplane/internal/app/componentregistration"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const ResolverWorkHelperArgument = "resolver-helper"

func RunResolverWorkHelper(ctx context.Context, input io.Reader, output io.Writer) error {
	request, err := composehelper.ReadResolverRequest(ctx, input)
	if err != nil {
		return err
	}
	captured, err := resolverexecution.OpenHelperRequest(request)
	if err != nil {
		return err
	}
	catalog, err := componentregistration.NewCatalog()
	if err != nil {
		return err
	}
	response, err := composehelper.ExecuteResolverWork(
		ctx,
		runner.New(nil),
		request,
		resolverHelperCatalog{catalog: catalog},
	)
	if err != nil {
		return err
	}
	return composehelper.WriteResolverResponse(ctx, output, captured, response)
}

type resolverHelperCatalog struct{ catalog componentregistration.Catalog }

func (catalog resolverHelperCatalog) ResolveManagedResolverAction(
	action *agentpb.ComponentApply,
) (registeredcatalog.ManagedConfigActionRecipe, error) {
	if action == nil || !action.ManagedConfigContent {
		return registeredcatalog.ManagedConfigActionRecipe{}, errs.New(
			errs.KindValidationFailed,
			"DNS resolver file action has no managed configuration authority",
		)
	}
	envelope, err := componentaction.DecodeComponentAction(action)
	if err != nil {
		return registeredcatalog.ManagedConfigActionRecipe{}, err
	}
	_, _, recipe, err := catalog.catalog.ResolveManagedConfigActionEnvelope(envelope)
	return recipe, err
}

func (catalog resolverHelperCatalog) ResolveResolverObservation(
	action *agentpb.ComponentApply,
) (registeredcatalog.DNSResolverObservationRecipe, error) {
	if action == nil || action.ManagedConfigContent {
		return registeredcatalog.DNSResolverObservationRecipe{}, errs.New(
			errs.KindValidationFailed,
			"DNS resolver observation action cannot mutate configuration",
		)
	}
	envelope, err := componentaction.DecodeComponentAction(action)
	if err != nil {
		return registeredcatalog.DNSResolverObservationRecipe{}, err
	}
	_, _, recipe, err := catalog.catalog.ResolveDNSResolverObservationActionEnvelope(envelope)
	return recipe, err
}
