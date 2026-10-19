package componentplanning

const componentSecretReferenceIndexPrefix = "/v1/indexes/secrets/by-component/"

func ComponentSecretReferencePrefix(secretID string) string {
	return componentSecretReferenceIndexPrefix + secretID + "/"
}

func componentActiveSecretReferenceKey(secretID, componentID string) string {
	return ComponentSecretReferencePrefix(secretID) + "active/" + componentID
}
