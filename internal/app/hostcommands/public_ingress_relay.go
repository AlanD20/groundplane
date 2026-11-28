package hostcommands

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/publicingress"
)

const PublicIngressRelayArgument = publicingress.RelayArgument
const PublicIngressRelayBindEnv = publicingress.RelayBindEnv
const PublicIngressRelayPeerEnv = publicingress.RelayPeerEnv

func RunPublicIngressRelay(ctx context.Context, bind, peer string) error {
	listen, source, err := publicingress.RelayAddresses(bind, peer)
	if err != nil {
		return err
	}
	return publicingress.RunRelay(ctx, listen, source)
}
