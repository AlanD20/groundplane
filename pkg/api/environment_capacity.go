package api

// EnvironmentNetworkCapacity is the fixed-revision Zone allocation projection.
type EnvironmentNetworkCapacity struct {
	TotalAddresses     int64 `json:"total_addresses"`
	AllocatedAddresses int64 `json:"allocated_addresses"`
	AvailableAddresses int64 `json:"available_addresses"`
	ZoneCount          int64 `json:"zone_count"`
}
