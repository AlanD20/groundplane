import { useState } from "react";
import { Bell, Send } from "lucide-react";
import { ResourcePanel } from "@/components/common/resource-panel";
import { SettingsDraft } from "@/components/common/settings-draft";
import { TaskLink } from "@/components/common/task-link";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { useVisibleRefresh } from "@/lib/use-visible-refresh";
import { newULID } from "@/lib/utils";
import {
  getTaskAlerts,
  setTaskAlerts,
  testTaskAlerts,
  type AlertSettings,
} from "./api";

type Draft = {
  revision: string;
  enabled: boolean;
  consoleURL: string;
  webhook: string;
  clear: boolean;
};

export function TaskAlertSettings({ scope }: { scope: string }) {
  return <ScopedSettings key={scope} scope={scope} />;
}

function ScopedSettings({ scope }: { scope: string }) {
  const [saved, setSaved] = useState<AlertSettings | null>(null);
  const [draft, setDraft] = useState<Draft | null>(null);
  const [busy, setBusy] = useState(false);
  const [readError, setReadError] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [testQueued, setTestQueued] = useState<string | null>(null);
  useVisibleRefresh(
    async (signal) => {
      try {
        const value = await getTaskAlerts(scope, signal);
        if (!signal.aborted) {
          setSaved(value);
          setReadError(null);
        }
      } catch (cause) {
        if (!signal.aborted) setReadError(message(cause));
      }
    },
    { key: scope },
  );

  const current =
    draft ??
    (saved
      ? {
          revision: saved.revision,
          enabled: saved.enabled,
          consoleURL: saved.console_url,
          webhook: "",
          clear: false,
        }
      : null);
  const edit = (patch: Partial<Draft>) => {
    if (current) setDraft({ ...current, ...patch });
    setError(null);
    setTestQueued(null);
  };
  async function save() {
    if (!draft || busy) return;
    setBusy(true);
    setError(null);
    try {
      const value = await setTaskAlerts(
        scope,
        {
          expected_revision: draft.revision,
          enabled: draft.enabled,
          console_url: draft.consoleURL.trim(),
          ...(draft.clear
            ? { webhook_url: "" }
            : draft.webhook.trim()
              ? { webhook_url: draft.webhook.trim() }
              : {}),
        },
        newULID(),
      );
      setSaved(value);
      setDraft(null);
      setTestQueued(null);
    } catch (cause) {
      setError(message(cause));
    } finally {
      setBusy(false);
    }
  }
  async function test() {
    if (!saved || busy || draft) return;
    setBusy(true);
    setError(null);
    try {
      const accepted = await testTaskAlerts(scope, saved.revision, newULID());
      setTestQueued(accepted.notification_id);
    } catch (cause) {
      setError(message(cause));
    } finally {
      setBusy(false);
    }
  }
  const awaitingTest =
    testQueued !== null && testQueued !== saved?.last_notification_id;
  return (
    <ResourcePanel
      title={
        <span className="flex items-center gap-2">
          <Bell className="size-4" /> Task alerts
        </span>
      }
    >
      <p className="mb-4 text-sm text-muted-foreground">
        Failed Tasks and Tasks needing attention.{" "}
        {scope === "platform"
          ? "Only Tasks without an Environment scope use this destination."
          : "Only Tasks in this Environment use this destination."}
      </p>
      {readError && (
        <p role="alert" className="mb-3 text-sm text-destructive">
          {readError}
        </p>
      )}
      {!current ? (
        <p className="text-sm text-muted-foreground">Loading alert settings…</p>
      ) : (
        <SettingsDraft
          dirty={draft !== null}
          busy={busy}
          onCancel={() => {
            setDraft(null);
            setError(null);
          }}
          actions={
            <Button disabled={busy} onClick={() => void save()}>
              {busy ? "Saving…" : "Save alerts"}
            </Button>
          }
        >
          <div className="flex items-center justify-between gap-4">
            <Label htmlFor={`alerts-enabled-${scope}`}>
              Discord notifications
            </Label>
            <Switch
              id={`alerts-enabled-${scope}`}
              checked={current.enabled}
              onCheckedChange={(enabled) => edit({ enabled })}
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor={`alerts-webhook-${scope}`}>Webhook URL</Label>
            <Input
              id={`alerts-webhook-${scope}`}
              type="password"
              autoComplete="new-password"
              value={current.webhook}
              placeholder={
                saved?.webhook_configured && !current.clear
                  ? "Webhook saved · enter a URL to replace it"
                  : "https://discord.com/api/webhooks/…"
              }
              onChange={(event) =>
                edit({ webhook: event.target.value, clear: false })
              }
            />
            <p className="text-xs text-muted-foreground">
              Saved encrypted. The existing URL is never returned to the
              Console.
            </p>
            {saved?.webhook_configured && !current.clear && (
              <Button
                variant="ghost"
                size="sm"
                onClick={() =>
                  edit({ clear: true, webhook: "", enabled: false })
                }
              >
                Remove webhook
              </Button>
            )}
            {current.clear && (
              <p className="text-xs text-muted-foreground">
                The saved webhook will be removed when you save.
              </p>
            )}
          </div>
          <div className="space-y-1.5">
            <Label htmlFor={`alerts-console-${scope}`}>
              Console URL{" "}
              <span className="text-muted-foreground">(optional)</span>
            </Label>
            <Input
              id={`alerts-console-${scope}`}
              value={current.consoleURL}
              placeholder="https://gp.example.com"
              onChange={(event) => edit({ consoleURL: event.target.value })}
            />
            <p className="text-xs text-muted-foreground">
              Adds a direct Task link to notifications.
            </p>
          </div>
        </SettingsDraft>
      )}
      <div className="mt-4 flex flex-wrap items-center justify-between gap-3 border-t border-border pt-4">
        <div className="text-sm text-muted-foreground" role="status">
          {awaitingTest
            ? "Test queued · waiting for Discord"
            : saved?.delivery_state === "never"
              ? "No delivery yet"
              : saved
                ? `Last delivery: ${saved.delivery_state}`
                : ""}
          {saved?.last_attempt_at && (
            <span className="ml-2">
              {new Date(saved.last_attempt_at).toLocaleString()}
            </span>
          )}
          {saved?.last_task_id && (
            <span className="ml-2">
              <TaskLink taskId={saved.last_task_id}>Open Task</TaskLink>
            </span>
          )}
        </div>
        <Button
          variant="outline"
          size="sm"
          disabled={busy || !!draft || !saved?.enabled || awaitingTest}
          onClick={() => void test()}
        >
          <Send className="size-3.5" /> Send test
        </Button>
      </div>
      {saved?.last_error && (
        <p className="mt-2 text-sm text-destructive">{saved.last_error}</p>
      )}
      {error && (
        <p role="alert" className="mt-3 text-sm text-destructive">
          {error}
        </p>
      )}
    </ResourcePanel>
  );
}

function message(cause: unknown) {
  return cause instanceof Error
    ? cause.message
    : "Unable to update Task alerts.";
}
