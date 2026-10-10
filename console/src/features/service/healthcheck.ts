import type { Healthcheck } from "@/lib/types";

export type HealthcheckKind = "http" | "tcp" | "pgrep" | "custom" | "none";

export function healthcheckEnabled(value: Healthcheck | null) {
  return (
    !!value &&
    !value.disable &&
    value.test.length > 1 &&
    value.test[0] !== "NONE"
  );
}

export function healthcheckKind(value: Healthcheck | null): HealthcheckKind {
  if (!healthcheckEnabled(value)) return "none";
  const test = value!.test;
  if (test[0] === "CMD") {
    if (
      test.length === 5 &&
      test[1] === "curl" &&
      test[2] === "-fsS" &&
      test[3] === "--"
    ) {
      try {
        const url = new URL(test[4]);
        if (
          url.hostname === "127.0.0.1" &&
          ["http:", "https:"].includes(url.protocol) &&
          !url.username &&
          !url.password &&
          !url.hash
        )
          return "http";
      } catch {
        /* Other native curl invocations remain editable as Custom. */
      }
    }
    if (
      test.length === 6 &&
      test[1] === "nc" &&
      test[2] === "-z" &&
      test[3] === "--"
    )
      return "tcp";
    if (
      test.length === 5 &&
      test[1] === "pgrep" &&
      test[2] === "-f" &&
      test[3] === "--"
    )
      return "pgrep";
  }
  return "custom";
}

export function describeHealthcheck(value: Healthcheck | null): string {
  const kind = healthcheckKind(value);
  if (kind === "none") return "None";
  const test = value!.test;
  if (kind === "http") return `HTTP ${test[4]}`;
  if (kind === "tcp") return `TCP ${test[4]}:${test[5]}`;
  if (kind === "pgrep") return `Process ${test[4]}`;
  return test[0] === "CMD-SHELL"
    ? "Custom shell command"
    : `Custom command: ${test[1]}`;
}
