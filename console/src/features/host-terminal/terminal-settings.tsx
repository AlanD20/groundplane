import { ResourcePanel, SettingsRow } from "@/components/common/resource-panel";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useStore } from "@/lib/store";
import { Save, SquareTerminal } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { parseDocument } from "yaml";

const defaultMinutes = 15;

export function HostTerminalSettings() {
  const {
    controllerConfig,
    controllerConfigLoading,
    controllerConfigError,
    refreshControllerConfig,
    setControllerConfig,
  } = useStore();
  const configuredMinutes = useMemo(
    () => readMinutes(controllerConfig?.content),
    [controllerConfig?.content],
  );
  const [minutes, setMinutes] = useState(configuredMinutes);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    void refreshControllerConfig(controller.signal).catch(() => undefined);
    return () => controller.abort();
  }, [refreshControllerConfig]);

  useEffect(() => setMinutes(configuredMinutes), [configuredMinutes]);

  async function save() {
    if (!controllerConfig || minutes < 1 || minutes > 480) return;
    setSaving(true);
    setSaveError(null);
    try {
      const document = parseDocument(controllerConfig.content);
      if (document.errors.length > 0) throw document.errors[0];
      document.setIn(["host_terminal", "input_inactivity"], `${minutes}m`);
      await setControllerConfig({
        content: String(document),
        expected_revision: controllerConfig.revision,
      });
    } catch (cause) {
      setSaveError(
        cause instanceof Error
          ? cause.message
          : "Unable to save host terminal settings.",
      );
    } finally {
      setSaving(false);
    }
  }

  return (
    <ResourcePanel
      title={
        <span className="flex items-center gap-2">
          <SquareTerminal className="size-4 text-muted-foreground" /> Host
          terminal
        </span>
      }
    >
      <SettingsRow
        title="Operator-input inactivity"
        help="The root shell terminates after this period without keyboard input. Shell output does not reset the timer. Maximum 8 hours."
      >
        <Input
          type="number"
          min={1}
          max={480}
          className="w-24"
          aria-label="Host terminal inactivity minutes"
          value={minutes}
          disabled={controllerConfigLoading || saving || !controllerConfig}
          onChange={(event) => setMinutes(Number(event.target.value))}
        />
        <span className="text-xs text-muted-foreground">minutes</span>
        <Button
          size="sm"
          disabled={
            !controllerConfig ||
            saving ||
            minutes < 1 ||
            minutes > 480 ||
            minutes === configuredMinutes
          }
          onClick={() => void save()}
        >
          <Save className="size-4" /> {saving ? "Saving…" : "Save"}
        </Button>
      </SettingsRow>
      <p className="text-xs text-muted-foreground">
        Applies after the next Controller restart. Existing terminal sessions
        keep the timeout they opened with.
      </p>
      {controllerConfigError && !controllerConfig ? (
        <p role="alert" className="text-sm text-destructive">
          {controllerConfigError}
        </p>
      ) : null}
      {saveError ? (
        <p role="alert" className="text-sm text-destructive">
          {saveError}
        </p>
      ) : null}
    </ResourcePanel>
  );
}

function readMinutes(content?: string) {
  if (!content) return defaultMinutes;
  try {
    const document = parseDocument(content);
    const value = document.getIn(["host_terminal", "input_inactivity"]);
    const match =
      typeof value === "string"
        ? value.match(/^(?:(\d+)h)?(?:(\d+)m)?$/)
        : null;
    const parsed =
      match && (match[1] || match[2])
        ? Number(match[1] ?? 0) * 60 + Number(match[2] ?? 0)
        : defaultMinutes;
    return parsed >= 1 && parsed <= 480 ? parsed : defaultMinutes;
  } catch {
    return defaultMinutes;
  }
}
