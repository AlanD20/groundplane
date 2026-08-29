package app

import (
	"net"
	"time"
)

const tailscaleLocalAPISocket = "/var/run/tailscale/tailscaled.sock"

func detectTailnetDelegationDefault() bool {
	connection, err := net.DialTimeout("unix", tailscaleLocalAPISocket, 100*time.Millisecond)
	if err != nil {
		return false
	}
	_ = connection.Close()
	return true
}
