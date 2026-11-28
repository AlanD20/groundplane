package api

import "time"

// RemoteHost is the operator view of one Host's enrollment. The currently
// running work and network availability are independent of enrollment.
type RemoteHost struct {
	ID               string     `json:"id"`
	Name             string     `json:"name"`
	State            string     `json:"state" enum:"unenrolled,enrolling,enrolled"`
	AgentID          string     `json:"agent_id,omitempty"`
	Generation       uint64     `json:"generation"`
	CredentialExpiry *time.Time `json:"credential_expiry,omitempty"`
}

type RemoteHostCreateRequest struct {
	Name string `json:"name" minLength:"1" maxLength:"253"`
}

type RemoteHostEnrollmentRequest struct {
	ExpectedGeneration uint64 `json:"expected_generation"`
	CSR                string `json:"csr" minLength:"1" maxLength:"24000"`
}

type RemoteHostEnrollment struct {
	Host        RemoteHost `json:"host"`
	Certificate string     `json:"certificate"`
	ExpiresAt   time.Time  `json:"expires_at"`
}
