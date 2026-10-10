import { useEffect, useRef, useState } from "react";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import type { Healthcheck } from "@/lib/types";
import { healthcheckKind, type HealthcheckKind } from "./healthcheck";
import { useDraftDirty } from "@/lib/use-draft-dirty";

type Props = {
  value: Healthcheck | null;
  onChange: (value: Healthcheck | null) => void;
  onError: (error: string | null) => void;
  onDirtyChange?: (dirty: boolean) => void;
};

export function HealthcheckFields({
  value,
  onChange,
  onError,
  onDirtyChange,
}: Props) {
  const initialKind = healthcheckKind(value);
  const [kind, setKind] = useState<HealthcheckKind>(initialKind);
  const initialURL = initialKind === "http" ? new URL(value!.test[4]) : null;
  const [protocol, setProtocol] = useState(
    initialURL?.protocol === "https:" ? "https" : "http",
  );
  const [port, setPort] = useState(
    initialURL
      ? initialURL.port || (initialURL.protocol === "https:" ? "443" : "80")
      : "",
  );
  const [path, setPath] = useState(
    initialURL ? initialURL.pathname + initialURL.search : "/",
  );
  const [host, setHost] = useState(
    initialKind === "tcp" ? value!.test[4] : "127.0.0.1",
  );
  const [tcpPort, setTCPPort] = useState(
    initialKind === "tcp" ? value!.test[5] : "",
  );
  const [pattern, setPattern] = useState(
    initialKind === "pgrep" ? value!.test[4] : "",
  );
  const [mode, setMode] = useState(
    value?.test[0] === "CMD-SHELL" ? "shell" : "exec",
  );
  const [command, setCommand] = useState(
    initialKind === "custom"
      ? value?.test[0] === "CMD-SHELL"
        ? value.test[1]
        : JSON.stringify(value?.test.slice(1))
      : "[]",
  );

  const [interval, setInterval] = useState(value?.interval ?? "15s");
  const [timeout, setTimeoutValue] = useState(value?.timeout ?? "3s");
  const [startPeriod, setStartPeriod] = useState(value?.startPeriod ?? "20s");
  const [startInterval, setStartInterval] = useState(
    value?.startInterval ?? "",
  );
  const [retries, setRetries] = useState(String(value?.retries ?? 3));
  const inputs = [
    kind,
    protocol,
    port,
    path,
    host,
    tcpPort,
    pattern,
    mode,
    command,
    interval,
    timeout,
    startPeriod,
    startInterval,
    retries,
  ];
  const draft = JSON.stringify(inputs);
  const initial = useRef({ draft, value });
  useDraftDirty(inputs, onDirtyChange);

  function buildTest(): string[] {
    if (kind === "none") return ["NONE"];
    if (kind === "http") {
      if (!validPort(port) || !path.startsWith("/") || path.includes("#"))
        throw new Error(
          "HTTP requires a container port (1–65535) and a path beginning with /.",
        );
      const url = new URL(`${protocol}://127.0.0.1:${port}${path}`);
      return ["CMD", "curl", "-fsS", "--", url.href];
    }
    if (kind === "tcp") {
      if (!host.trim() || !validPort(tcpPort))
        throw new Error("TCP requires a host and container port (1–65535).");
      return ["CMD", "nc", "-z", "--", host.trim(), tcpPort];
    }
    if (kind === "pgrep") {
      if (!pattern.trim())
        throw new Error("Process requires a process pattern.");
      return ["CMD", "pgrep", "-f", "--", pattern];
    }
    if (mode === "shell") {
      if (!command.trim()) throw new Error("Enter the shell command.");
      return ["CMD-SHELL", command];
    }
    const args: unknown = JSON.parse(command);
    if (
      !Array.isArray(args) ||
      !args.length ||
      args.some((arg) => typeof arg !== "string") ||
      !args[0].trim()
    ) {
      throw new Error(
        'Enter a JSON array containing the executable and arguments, for example ["/app/check", "--ready"].',
      );
    }
    return ["CMD", ...args];
  }

  useEffect(() => {
    if (draft === initial.current.draft) {
      onError(null);
      if (JSON.stringify(value) !== JSON.stringify(initial.current.value))
        onChange(initial.current.value);
      return;
    }
    try {
      const test = buildTest();
      if (kind !== "none" && (!/^\d+$/.test(retries) || Number(retries) > 100))
        throw new Error(
          "Retries must be 0 (Docker default) or a whole number from 1 to 100.",
        );
      const next =
        kind === "none"
          ? {
              test,
              disable: false,
              interval: "",
              timeout: "",
              startPeriod: "",
              startInterval: "",
              retries: 0,
            }
          : {
              test,
              disable: false,
              interval,
              timeout,
              startPeriod,
              startInterval,
              retries: Number(retries),
            };
      onError(null);
      if (JSON.stringify(next) !== JSON.stringify(value)) onChange(next);
    } catch (error) {
      onError(
        error instanceof Error ? error.message : "Invalid healthcheck command.",
      );
    }
  }, [draft]);

  function field(
    id: string,
    label: string,
    text: string,
    change: (value: string) => void,
    placeholder?: string,
  ) {
    return (
      <div className="flex flex-col gap-1.5">
        <Label htmlFor={id}>{label}</Label>
        <Input
          id={id}
          value={text}
          onChange={(event) => change(event.target.value)}
          placeholder={placeholder}
        />
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
      <div className="flex flex-col gap-1.5">
        <Label htmlFor="hc-kind">Check type</Label>
        <Select
          id="hc-kind"
          value={kind}
          onValueChange={(next) => setKind(next as HealthcheckKind)}
          options={[
            { value: "none", label: "None" },
            { value: "http", label: "HTTP" },
            { value: "tcp", label: "TCP" },
            { value: "pgrep", label: "Process" },
            { value: "custom", label: "Custom command" },
          ]}
        />
      </div>
      {kind === "http" && (
        <>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="hc-protocol">Protocol</Label>
            <Select
              id="hc-protocol"
              value={protocol}
              onValueChange={setProtocol}
              options={[
                { value: "http", label: "HTTP" },
                { value: "https", label: "HTTPS" },
              ]}
            />
          </div>
          {field("hc-port", "Container port", port, setPort, "8080")}
          {field("hc-path", "Path", path, setPath, "/health")}
          <p className="sm:col-span-2 text-xs text-muted-foreground">
            Checks {protocol}://127.0.0.1:{port || "<port>"}
            {path} inside the container. Requires curl.
          </p>
        </>
      )}
      {kind === "tcp" && (
        <>
          {field("hc-host", "Host inside the container", host, setHost)}
          {field("hc-tcp-port", "Container port", tcpPort, setTCPPort, "5432")}
          <p className="sm:col-span-2 text-xs text-muted-foreground">
            Requires nc. Use the workload port, not the published host or proxy
            port.
          </p>
        </>
      )}
      {kind === "pgrep" && (
        <>
          {field("hc-pattern", "Process pattern", pattern, setPattern, "nginx")}
          <p className="sm:col-span-2 text-xs text-muted-foreground">
            Requires pgrep. Process presence is not an application readiness
            check.
          </p>
        </>
      )}
      {kind === "custom" && (
        <>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="hc-mode">Command mode</Label>
            <Select
              id="hc-mode"
              value={mode}
              onValueChange={(next) => {
                setMode(next);
                setCommand(next === "shell" ? "" : "[]");
              }}
              options={[
                { value: "exec", label: "Executable and arguments" },
                { value: "shell", label: "Shell command" },
              ]}
            />
          </div>
          <div className="sm:col-span-2 flex flex-col gap-1.5">
            <Label htmlFor="hc-command">
              {mode === "exec"
                ? "Command arguments (JSON array)"
                : "Shell command"}
            </Label>
            <Textarea
              id="hc-command"
              value={command}
              onChange={(event) => setCommand(event.target.value)}
              rows={3}
            />
          </div>
          <p className="sm:col-span-2 text-xs text-muted-foreground">
            Runs inside the workload. Exit 0 means ready; tools and any shell
            must exist in the image.
          </p>
        </>
      )}
      {kind !== "none" && (
        <>
          {field("hc-interval", "Interval", interval, setInterval, "15s")}
          {field("hc-timeout", "Timeout", timeout, setTimeoutValue, "3s")}
          {field(
            "hc-start",
            "Startup grace period",
            startPeriod,
            setStartPeriod,
            "20s",
          )}
          {field(
            "hc-start-interval",
            "Startup interval",
            startInterval,
            setStartInterval,
            "Docker default",
          )}
          {field("hc-retries", "Retries", retries, setRetries, "3")}
        </>
      )}
    </div>
  );
}

function validPort(value: string) {
  return /^\d+$/.test(value) && Number(value) > 0 && Number(value) <= 65535;
}
