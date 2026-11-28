package publicingress

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"io"
	"net/netip"
	"os"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/client"
)

// verifyPortsAvailable observes Linux sockets and Docker NAT-published ports.
// Preflight never binds a socket or changes a runtime.
func verifyPortsAvailable(ctx context.Context, engine *client.Client, endpoints []netip.AddrPort) error {
	for _, path := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		if err := inspectTCPListeners(ctx, path, endpoints); err != nil {
			return err
		}
	}
	ports := make(map[uint16]bool)
	for _, endpoint := range endpoints {
		ports[endpoint.Port()] = true
	}
	for port := range ports {
		listed, err := engine.ContainerList(ctx, client.ContainerListOptions{
			Limit: 513, Filters: client.Filters{}.Add("publish", strconv.Itoa(int(port))+"/tcp"),
		})
		if err != nil || len(listed.Items) >= 513 {
			return conflict("public ingress could not verify published Docker ports")
		}
		for _, item := range listed.Items {
			for _, binding := range item.Ports {
				if binding.Type == "tcp" && binding.PublicPort != 0 {
					if err := matchOccupiedPort(netip.AddrPortFrom(binding.IP, binding.PublicPort), endpoints); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func inspectTCPListeners(ctx context.Context, path string, endpoints []netip.AddrPort) error {
	file, err := os.Open(path)
	if os.IsNotExist(err) && path == "/proc/net/tcp6" {
		for _, endpoint := range endpoints {
			if endpoint.Addr().Is6() {
				return conflict("public ingress cannot verify IPv6 listener availability")
			}
		}
		return nil
	}
	if err != nil {
		return conflict("public ingress could not verify local listening ports")
	}
	defer file.Close()
	const limit = 8 << 20
	reader := &io.LimitedReader{R: file, N: limit}
	scanner := bufio.NewScanner(reader)
	if !scanner.Scan() {
		return conflict("public ingress could not read local listening ports")
	}
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		fields := strings.Fields(scanner.Text())
		if len(fields) < 4 {
			return conflict("public ingress read an invalid local socket row")
		}
		if fields[3] == "0A" {
			local, err := parseKernelTCPAddress(fields[1])
			if err != nil {
				return err
			}
			if err := matchOccupiedPort(local, endpoints); err != nil {
				return err
			}
		}
	}
	if scanner.Err() != nil || reader.N == 0 {
		return conflict("public ingress local listening port observation is incomplete")
	}
	return nil
}

func parseKernelTCPAddress(value string) (netip.AddrPort, error) {
	address, rawPort, found := strings.Cut(value, ":")
	decoded, err := hex.DecodeString(address)
	port, portErr := strconv.ParseUint(rawPort, 16, 16)
	if !found || err != nil || portErr != nil || len(decoded) != 4 && len(decoded) != 16 || port == 0 {
		return netip.AddrPort{}, conflict("public ingress read an invalid local socket address")
	}
	for offset := 0; offset < len(decoded); offset += 4 {
		block := decoded[offset : offset+4]
		binary.NativeEndian.PutUint32(block, binary.BigEndian.Uint32(block))
	}
	normalized, _ := netip.AddrFromSlice(decoded)
	return netip.AddrPortFrom(normalized.Unmap(), uint16(port)), nil
}

func matchOccupiedPort(occupied netip.AddrPort, endpoints []netip.AddrPort) error {
	for _, endpoint := range endpoints {
		if occupied.Port() == endpoint.Port() && (!occupied.Addr().IsValid() || occupied.Addr().IsUnspecified() ||
			occupied.Addr().Unmap() == endpoint.Addr()) {
			return errs.Newf(errs.KindStateConflict, "public ingress TCP address %s is already in use", endpoint)
		}
	}
	return nil
}
