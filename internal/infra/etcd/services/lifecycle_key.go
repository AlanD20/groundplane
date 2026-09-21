package services

import ()

const serviceLifecycleActivePrefix = "/v1/indexes/service-lifecycle/active/"

func ServiceLifecycleActiveKey(serviceID string) string {
	return serviceLifecycleActivePrefix + serviceID
}
