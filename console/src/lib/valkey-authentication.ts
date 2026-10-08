import type { ProvisionOp, ValkeyAuthentication } from "./types";

export type ValkeyAuthenticationSelection = ValkeyAuthentication | "";
export type BackingAuthenticationCreateFields =
  | { adapter: "postgres" | "mysql" }
  | { adapter: "valkey"; authentication: ValkeyAuthentication };

export function backingAuthenticationCreateFields(
  adapter: "postgres" | "valkey" | "mysql",
  authentication: string,
): BackingAuthenticationCreateFields | undefined {
  if (adapter === "postgres" || adapter === "mysql") return { adapter };

  switch (authentication) {
    case "username_password":
    case "password":
    case "none":
      return { adapter, authentication };
    default:
      return undefined;
  }
}

export type ValkeyAuthenticationDetails = {
  label: string;
  summary: string;
  factSuffixes: string[];
  provision: ProvisionOp[];
  credentialLabel: string;
  newOwnerLabel: string;
  existingOwnerLabel: string;
};

export function valkeyAuthenticationDetails(
  authentication: string | undefined,
): ValkeyAuthenticationDetails | undefined {
  switch (authentication) {
    case "username_password":
      return {
        label: "Username + password",
        summary:
          "Each credential owner gets an independent named ACL user and password. The URL is secret; all users share the same keyspace and Pub/Sub channels.",
        factSuffixes: ["HOST", "PORT", "ROLE", "PASSWORD", "URL"],
        provision: [
          {
            op: "create_acl_user",
            detail: "ACL SETUSER <role> on >‹generated› ~* &* +@all -@admin",
          },
          { op: "save_acl", detail: "ACL SAVE" },
        ],
        credentialLabel: "Credential",
        newOwnerLabel: "Create named-user credential",
        existingOwnerLabel: "Use existing named-user credential",
      };
    case "password":
      return {
        label: "Password only",
        summary:
          "Each credential owner gets an independent password on the shared default user. No ROLE fact is exposed; removing an owner revokes only its password for future authentication.",
        factSuffixes: ["HOST", "PORT", "PASSWORD", "URL"],
        provision: [
          {
            op: "add_default_password",
            detail: "ACL SETUSER default on >‹generated› ~* &* +@all -@admin",
          },
          { op: "save_acl", detail: "ACL SAVE" },
        ],
        credentialLabel: "Credential",
        newOwnerLabel: "Create independent password",
        existingOwnerLabel: "Use existing password",
      };
    case "none":
      return {
        label: "None",
        summary:
          "No authentication is required. Any client that can reach this backing service can access the shared keyspace and Pub/Sub channels.",
        factSuffixes: ["HOST", "PORT", "URL"],
        provision: [],
        credentialLabel: "Fact ownership",
        newOwnerLabel: "Create fact owner",
        existingOwnerLabel: "Use existing fact owner",
      };
    default:
      return undefined;
  }
}
