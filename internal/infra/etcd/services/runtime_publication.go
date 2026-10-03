package services

import (
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// RunningRuntimeMutation publishes independent runtime intent alongside a
// successfully acknowledged Blueprint workload. The caller owns the fenced
// Environment transaction; failed or dormant members must not call this.
func RunningRuntimeMutation(environmentID, serviceID string) (keyvalue.Mutation, error) {
	value, err := EncodeServiceRuntimeRecord(ServiceRuntimeRecord{
		EnvironmentID: environmentID, ServiceID: serviceID,
		Runtime: core.ServiceRuntime{ServiceID: serviceID, RuntimeIntent: core.ServiceRuntimeIntentRunning},
	})
	if err != nil {
		return keyvalue.Mutation{}, err
	}
	return keyvalue.Mutation{Type: keyvalue.MutationPut, Key: ServiceRuntimeKey(serviceID), Value: value}, nil
}
