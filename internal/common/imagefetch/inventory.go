package imagefetch

// LocalImage describes Docker observation, not a successful GP deployment.
type LocalImage struct {
	ID             string
	Tags           []string
	Digests        []string
	SizeBytes      int64
	Created        int64
	Containers     int
	ContainerUses  []ContainerUse
	ContentIDs     []string
	RemovalBlocked string
}

// ContainerUse is live Docker identity and ownership labels, not deletion authority.
type ContainerUse struct {
	ID, Name, State          string
	Managed                  bool
	EnvironmentID, ServiceID string
}
