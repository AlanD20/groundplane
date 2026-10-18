import type { TaskWork } from "./api";

const operationLabels: Record<string, string> = {
  release: "Deploy Service",
  release_restoration: "Restore previous runtime",
  service_lifecycle: "Change Service runtime",
  service_removal: "Delete Service",
  backing_provision: "Provision Backing Service",
  attach: "Connect Backing Service",
  detach: "Disconnect Backing Service",
  script: "Run Script",
  network: "Apply Zone networking",
  volume: "Manage Volume",
  component: "Apply Component",
  dns_resolver: "Apply DNS configuration",
  environment: "Provision Environment",
  hierarchy: "Remove resource hierarchy",
  removal_backup_object: "Remove captured backup objects",
  backup: "Create backup",
  backup_prune: "Remove recovery points",
  restore: "Restore backup",
  image: "Manage host image",
  runner: "Manage GitHub Runner",
  controller_software: "Activate Controller",
  agent_software: "Update Agent",
  software_preparation: "Prepare Groundplane software",
  software_staging: "Stage Groundplane artifacts",
  connector_check: "Check storage connection",
  connector_removal: "Delete Connector",
  secret_removal: "Delete Secret",
  script_removal: "Delete Script",
  backing_check: "Check Backing Service connection",
  dns_check: "Check DNS resolution",
  edge_check: "Check public access",
  runner_check: "Check GitHub Runner",
  secret_rotation: "Rotate backup encryption key",
  entry_settlement: "Save Entry values",
  entry_configuration: "Write application configuration",
  resource_definition: "Save resource definition",
  release_group: "Manage Release Group",
  agent_lifecycle: "Manage Agent installation",
  etcd_configuration: "Apply etcd configuration",
};

export function operationLabel(operation: TaskWork["operation"]): string {
  return operationLabels[operation] ?? "Resource operation";
}

export function cleanupLabel(cleanup: TaskWork["cleanup"]): string {
  switch (cleanup) {
    case "required":
      return "Cleanup required";
    case "running":
      return "Cleanup in progress";
    case "stopped":
      return "Cleanup needs attention";
    case "complete":
      return "Cleanup complete";
    default:
      return "No cleanup required";
  }
}
