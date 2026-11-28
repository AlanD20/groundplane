package transport

import "github.com/AlanD20/groundplane/internal/controller/agentchannel"

// ConfigurePublicIngress registers the optional, Host-bound result owner
// before either Agent listener starts.
func (runtime *Runtime) ConfigurePublicIngress(owner agentchannel.PublicIngressReporter) {
	runtime.publicIngressReports = owner
}
