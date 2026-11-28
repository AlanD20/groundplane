package transport

import "github.com/AlanD20/groundplane/internal/controller/agentchannel"

// ConfigureRemoteImage attaches the optional result owner before startup.
func (runtime *Runtime) ConfigureRemoteImage(owner agentchannel.RemoteImageReporter) {
	runtime.remoteImageReports = owner
}
