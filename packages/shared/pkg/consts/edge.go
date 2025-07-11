package consts

const (
	EdgeApiAuthHeader              = "X-API-Key"
	EdgeRpcAuthHeader              = "authorization"
	EdgeRpcServiceInstanceIDHeader = "service-instance-id"

	EdgeRpcSandboxCatalogCreateEvent = "sandbox-catalog-create"
	EdgeRpcSandboxCatalogDeleteEvent = "sandbox-catalog-delete"
)

type SandboxCatalogCreateEvent struct {
	Version string `json:"version"`

	SandboxID               string `json:"sandbox_id"`
	ExecutionID             string `json:"execution_id"`
	OrchestratorID          string `json:"orchestrator_id"`
	SandboxMaxLengthInHours int64  `json:"sandbox_max_length_in_hours"` // in hours
	SandboxStartTime        string `json:"sandbox_start_time"`          // Formatted as RFC3339 (ISO 8601)
}

type SandboxCatalogDeleteEvent struct {
	Version string `json:"version"`

	SandboxID   string `json:"sandbox_id"`
	ExecutionID string `json:"execution_id"`
}
