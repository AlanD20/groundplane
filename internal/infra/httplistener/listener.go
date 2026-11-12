// Package httplistener binds Controller endpoints independently of interface startup.
package httplistener

import (
	"context"
	"net"
	"net/netip"
	"syscall"

	"github.com/AlanD20/groundplane/internal/common/controllerendpoint"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// Listen permits an exact tailnet address to bind before tailscaled brings it up.
// Other addresses retain normal bind semantics; port conflicts remain errors.
func Listen(ctx context.Context, address string) (net.Listener, error) {
	config := net.ListenConfig{}
	endpoint, err := netip.ParseAddrPort(address)
	if err == nil && controllerendpoint.IsTailnet(endpoint.Addr()) {
		config.Control = func(_, _ string, raw syscall.RawConn) error {
			var optionErr error
			if err := raw.Control(func(fd uintptr) {
				optionErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_FREEBIND, 1)
			}); err != nil {
				return err
			}
			return optionErr
		}
	}
	listener, err := config.Listen(ctx, "tcp4", address)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return listener, nil
}
