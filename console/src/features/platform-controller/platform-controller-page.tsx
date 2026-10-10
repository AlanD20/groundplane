import { useSearchParams, Link } from "react-router-dom";
import { ResourcePanel } from "@/components/common/resource-panel";
import { DetailRow } from "@/components/common/detail-row";
import { SettingsDraft } from "@/components/common/settings-draft";
import { SoftwareIdentity } from "@/features/image-delivery/software-identity";
("use client");

import { useEffect, useState } from "react";
import { FileCode2, RefreshCw, Save, ServerCog } from "lucide-react";
import { PageHeader } from "@/components/common/page-header";
import { MetaPill } from "@/components/common/meta-pill";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { CodeEditor } from "@/components/ui/code-editor";
import { useStore } from "@/lib/store";

export default function PlatformControllerPage() {
  const [search] = useSearchParams();
  const tab = ["configuration"].includes(search.get("tab") ?? "")
    ? search.get("tab")
    : "overview";
  const destination = (value: string) => {
    const next = new URLSearchParams(search);
    next.set("tab", value);
    return `?${next}`;
  };
  const {
    host,
    controllerConfig,
    controllerConfigLoading,
    controllerConfigError,
    refreshControllerConfig,
    setControllerConfig,
  } = useStore();
  const [content, setContent] = useState("");
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    void refreshControllerConfig(controller.signal).catch(() => undefined);
    return () => controller.abort();
  }, [refreshControllerConfig]);

  useEffect(() => {
    if (controllerConfig) setContent(controllerConfig.content);
  }, [controllerConfig?.revision]);

  const dirty =
    controllerConfig !== null && content !== controllerConfig.content;

  async function save() {
    if (!controllerConfig) return;
    setSaving(true);
    setSaveError(null);
    try {
      await setControllerConfig({
        content,
        expected_revision: controllerConfig.revision,
      });
    } catch (cause) {
      setSaveError(
        cause instanceof Error
          ? cause.message
          : "Unable to save Controller configuration",
      );
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title="Controller"
        icon={<ServerCog />}
        meta={
          <>
            {host ? <StatusBadge status={host.controller.status} /> : null}
            {host ? (
              <MetaPill icon={<ServerCog />}>
                {host.controller.version}
              </MetaPill>
            ) : null}
          </>
        }
      />

      {tab === "overview" && (
        <div className="grid items-start gap-5 xl:grid-cols-2">
          <ResourcePanel title="Running Controller">
            <DetailRow
              label="Installed software"
              value={
                <SoftwareIdentity
                  version={host?.controller.version}
                  digest={host?.controller.update.running_sha256}
                  unavailable="Installed identity unavailable"
                />
              }
            />
            <DetailRow
              label="Service"
              value={host?.controller.service ?? "Unavailable"}
            />
          </ResourcePanel>
          <ResourcePanel
            title="Software updates"
            actions={
              <Link
                className="text-sm text-primary hover:underline"
                to="/platform/settings?tab=updates"
              >
                Manage updates →
              </Link>
            }
          >
            <DetailRow
              label="Staged candidate"
              value={
                <SoftwareIdentity
                  version={
                    host?.controller.update.candidate?.controller_version
                  }
                  digest={
                    host?.controller.update.candidate?.controller_sha256
                  }
                  unavailable="No release staged"
                />
              }
            />
          </ResourcePanel>
          <ResourcePanel
            title="Startup configuration"
            actions={
              <Link
                className="text-sm text-primary hover:underline"
                to={destination("configuration")}
              >
                View settings →
              </Link>
            }
          >
            <DetailRow
              label="Status"
              value={
                !controllerConfig
                  ? "Unavailable"
                  : controllerConfig.restart_required
                    ? "Restart required"
                    : "Matches running configuration"
              }
            />
            <p className="text-sm text-muted-foreground">
              Applies on the next Controller restart.
            </p>
          </ResourcePanel>
        </div>
      )}
      <div hidden={tab !== "configuration"}>
        <Card>
          <CardHeader>
            <CardTitle>
              <h2 className="flex items-center gap-2">
                <FileCode2 className="size-4 text-muted-foreground" /> Startup
                configuration
              </h2>
            </CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-4">
            {controllerConfigLoading && !controllerConfig ? (
              <p className="text-sm text-muted-foreground">
                Loading Controller configuration…
              </p>
            ) : null}
            {!controllerConfigLoading &&
            controllerConfigError &&
            !controllerConfig ? (
              <p role="alert" className="text-sm text-destructive">
                {controllerConfigError}
              </p>
            ) : null}
            {controllerConfig ? (
              <SettingsDraft
                dirty={dirty}
                busy={saving}
                onCancel={() => {
                  setContent(controllerConfig.content);
                  setSaveError(null);
                }}
                actions={
                  <>
                    <Button
                      size="sm"
                      disabled={!dirty || saving}
                      onClick={() => void save()}
                    >
                      <Save className="size-4" />{" "}
                      {saving ? "Saving…" : "Save Controller config"}
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      disabled={dirty || controllerConfigLoading || saving}
                      onClick={() => {
                        setSaveError(null);
                        void refreshControllerConfig()
                          .then((config) => setContent(config.content))
                          .catch((cause: unknown) => {
                            setSaveError(
                              cause instanceof Error
                                ? cause.message
                                : "Unable to reload Controller configuration",
                            );
                          });
                      }}
                    >
                      <RefreshCw className="size-4" /> Reload file
                    </Button>
                  </>
                }
              >
                <div className="grid gap-3 rounded-lg border border-border bg-surface p-3 text-xs sm:grid-cols-2">
                  <div>
                    <p className="text-muted-foreground">File</p>
                    <p className="break-all font-mono">
                      {controllerConfig.path}
                    </p>
                  </div>
                  <div>
                    <p className="text-muted-foreground">Revision</p>
                    <p className="break-all font-mono">
                      {controllerConfig.revision}
                    </p>
                  </div>
                </div>
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="controller-config-document">
                    Controller YAML
                  </Label>
                  <CodeEditor
                    id="controller-config-document"
                    label="Controller YAML"
                    language="yaml"
                    value={content}
                    onValueChange={setContent}
                    disabled={saving || controllerConfigLoading}
                  />
                </div>
                {controllerConfig.restart_required ? (
                  <p
                    role="status"
                    className="rounded-lg border border-warning/40 bg-warning/10 px-3 py-2 text-xs text-foreground"
                  >
                    Restart required to apply these settings.
                  </p>
                ) : null}
                {saveError ? (
                  <p role="alert" className="text-xs text-destructive">
                    {saveError}
                  </p>
                ) : null}
              </SettingsDraft>
            ) : null}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
