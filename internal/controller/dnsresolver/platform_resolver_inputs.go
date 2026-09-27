package dnsresolver

import (
	"context"
	"encoding/hex"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	platformcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"

	resolutionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/netip"
	"time"
)

func resolverInputFromProjection(
	record resolutionrecord.HostResolutionProjectionRecord,
	baselineGeneration uint64,
	resolvers []componentdns.ResolverEndpoint,
	registryAddress netip.Addr,
) (componentdns.ResolverInput, []platformcomponents.PlatformDNSHost, error) {
	baseline, err := componentdns.NewResolverBaseline(baselineGeneration, resolvers)
	if err != nil {
		return componentdns.ResolverInput{}, nil, errs.Wrap(errs.KindInternal, err)
	}
	digestBytes, err := hex.DecodeString(record.InputSHA256)
	if err != nil || len(digestBytes) != 32 {
		return componentdns.ResolverInput{}, nil, errs.New(
			errs.KindInternal,
			"host-resolution projection digest is corrupt",
		)
	}
	var digest [32]byte
	copy(digest[:], digestBytes)
	byAddress := make(map[netip.Addr][]string)
	if !registryAddress.Is4() || !(registryAddress.IsPrivate() || registryAddress == netip.MustParseAddr("127.0.0.1")) {
		return componentdns.ResolverInput{}, nil, errs.New(errs.KindInternal, "private registry address is invalid")
	}
	byAddress[registryAddress] = []string{imagefetch.RegistryHostname}
	for _, route := range record.Routes {
		if route.Hostname == imagefetch.RegistryHostname {
			return componentdns.ResolverInput{}, nil, errs.New(
				errs.KindStateConflict,
				"Route hostname is reserved for the private registry",
			)
		}
		address, parseErr := netip.ParseAddr(route.IPv4)
		if parseErr != nil {
			return componentdns.ResolverInput{}, nil, errs.New(errs.KindInternal, "host-resolution address is corrupt")
		}
		byAddress[address] = append(byAddress[address], route.Hostname)
	}
	hosts := make([]componentdns.Host, 0, len(byAddress))
	for address, names := range byAddress {
		hosts = append(hosts, componentdns.Host{Address: address, Hostnames: names})
	}
	projection, err := componentdns.NewHostResolutionProjection(record.InputRevision, digest, hosts)
	if err != nil {
		return componentdns.ResolverInput{}, nil, errs.Wrap(errs.KindInternal, err)
	}
	durable := make([]platformcomponents.PlatformDNSHost, len(projection.Hosts))
	for index, host := range projection.Hosts {
		durable[index] = platformcomponents.PlatformDNSHost{
			Address:   host.Address.String(),
			Hostnames: append([]string(nil), host.Hostnames...),
		}
	}
	input := componentdns.ResolverInput{Baseline: baseline, HostResolution: projection}
	if registryAddress.IsPrivate() {
		input.PrivateListener = registryAddress
	}
	return input, durable, nil
}

func ensureHostResolverBaseline(
	ctx context.Context,
	repository BaselineRepository,
	capture BaselineCapture,
	now time.Time,
) error {
	if _, found, err := repository.GetHostResolverBaseline(ctx); err != nil || found {
		return err
	}
	content, err := capture(ctx)
	if err != nil {
		return err
	}
	defer clear(content)
	_, err = repository.EnsureHostResolverBaseline(ctx, content, now)
	return err
}
