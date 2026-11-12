package config

// HTTPAddresses includes the internal endpoint without opening duplicate sockets.
func (c ControllerConfig) HTTPAddresses() []string {
	addresses := append([]string(nil), c.Listen.HTTP...)
	for _, address := range addresses {
		if address == c.Listen.InternalHTTP {
			return addresses
		}
	}
	return append(addresses, c.Listen.InternalHTTP)
}

// InternalAPI is independent of operator interface selection.
func (c ControllerConfig) InternalAPI() string { return c.Listen.InternalHTTP }
