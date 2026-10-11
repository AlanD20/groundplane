package core

// ProxyExposures selects ordinary Service forwarding. Backing containers serve
// their declared ports directly through their dedicated Zone and stable Attach
// endpoint; they are not routed through a tenant Environment's HTTP proxy.
func (service Service) ProxyExposures() []string {
	if service.Adapter != "" {
		return nil
	}
	return service.Expose
}
