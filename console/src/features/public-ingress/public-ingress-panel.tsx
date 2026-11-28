import { useId, useRef, useState, type FormEvent } from "react";
import { CheckCircle2, Globe2 } from "lucide-react";
import { CreationButton } from "@/components/common/creation-button";
import { ResourcePanel } from "@/components/common/resource-panel";
import { TaskLink } from "@/components/common/task-link";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { newULID } from "@/lib/utils";

export type PublicIngressRouteOption = {
  id: string;
  hostname: string;
  path: string;
  environment: string;
  service: string;
  unavailable?: string;
};

export type PublicIngressDraft = {
  enabled: boolean;
  addresses: string[];
  routeIds: string[];
};

export type PublicIngressConfiguration = PublicIngressDraft & {
  revision: string;
  state: "disabled" | "pending" | "serving" | "failed";
  taskId?: string;
  message?: string;
};

export type PublicIngressCheck = {
  scope: string;
  status: "passed" | "warning" | "blocked" | "not_checked";
  message: string;
};

export type PublicIngressReview = {
  revision: string;
  reviewToken?: string;
  canApply: boolean;
  checks: PublicIngressCheck[];
};

export type PublicIngressPanelProps = {
  configuration: PublicIngressConfiguration | null;
  routes: PublicIngressRouteOption[];
  loading?: boolean;
  loadError?: string;
  onRefresh?: () => void;
  onPreflight: (
    draft: PublicIngressDraft,
    revision: string | null,
    signal: AbortSignal,
  ) => Promise<PublicIngressReview>;
  onApply: (
    draft: PublicIngressDraft,
    review: PublicIngressReview,
    idempotencyKey: string,
  ) => Promise<{ taskId: string }>;
};

const stateText = {
  disabled: "Disabled",
  pending: "Applying",
  serving: "Serving",
  failed: "Needs attention",
};

const checkText = {
  passed: "Passed",
  warning: "Review",
  blocked: "Blocked",
  not_checked: "Not checked",
};

const errorClass =
  "rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-sm text-destructive";

type Editor = {
  enabled: boolean;
  addresses: string;
  routeIds: string[];
  revision: string | null;
};

type ReviewedDraft = {
  draft: PublicIngressDraft;
  review: PublicIngressReview;
  idempotencyKey: string;
};

export function PublicIngressPanel({
  configuration,
  routes,
  loading = false,
  loadError,
  onRefresh,
  onPreflight,
  onApply,
}: PublicIngressPanelProps) {
  const prefix = useId();
  const [editor, setEditor] = useState<Editor | null>(null);
  const [reviewed, setReviewed] = useState<ReviewedDraft | null>(null);
  const [taskId, setTaskId] = useState<string>();
  const [error, setError] = useState("");
  const [working, setWorking] = useState<"preflight" | "apply">();
  const busy = useRef(false);
  const revision = configuration?.revision ?? null;
  const stale = editor !== null && editor.revision !== revision;
  const blocked = stale || Boolean(loadError);
  const status = configuration?.state ?? "disabled";
  const submittedTask = taskId ?? configuration?.taskId;

  function cancelEditing() {
    setEditor(null);
    setReviewed(null);
    setError("");
  }

  function startEditing() {
    setEditor({
      enabled: configuration?.enabled ?? true,
      addresses: configuration?.addresses.join(", ") ?? "",
      routeIds: [...(configuration?.routeIds ?? [])],
      revision,
    });
    setReviewed(null);
    setError("");
  }

  function update(change: Partial<Omit<Editor, "revision">>) {
    setEditor((current) => (current ? { ...current, ...change } : current));
    setReviewed(null);
    setError("");
  }

  async function review(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!editor || blocked || busy.current) return;
    const addresses = [
      ...new Set(editor.addresses.split(/[\s,]+/).filter(Boolean)),
    ];
    if (editor.enabled && (!addresses.length || !editor.routeIds.length)) {
      setError("Enter a bind address and select at least one public Route.");
      return;
    }
    const draft = {
      enabled: editor.enabled,
      addresses,
      routeIds: [...editor.routeIds].sort(),
    };
    busy.current = true;
    setWorking("preflight");
    setError("");
    try {
      const result = await onPreflight(
        draft,
        editor.revision,
        AbortSignal.timeout(30_000),
      );
      setReviewed({ draft, review: result, idempotencyKey: newULID() });
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : "Unable to check ingress setup.",
      );
    } finally {
      busy.current = false;
      setWorking(undefined);
    }
  }

  async function apply() {
    if (
      !reviewed?.review.canApply ||
      !reviewed.review.reviewToken ||
      blocked ||
      busy.current
    )
      return;
    busy.current = true;
    setWorking("apply");
    setError("");
    try {
      const accepted = await onApply(
        reviewed.draft,
        reviewed.review,
        reviewed.idempotencyKey,
      );
      setTaskId(accepted.taskId);
      setEditor(null);
      setReviewed(null);
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : "Unable to submit the ingress change.",
      );
    } finally {
      busy.current = false;
      setWorking(undefined);
    }
  }

  return (
    <ResourcePanel
      title={
        <span className="flex items-center gap-2">
          <Globe2 aria-hidden className="size-4" /> Public ingress
        </span>
      }
      actions={
        !loading && !loadError ? (
          <CreationButton
            active={editor !== null}
            variant="outline"
            size="sm"
            disabled={!!working}
            onOpenChange={(open) => (open ? startEditing() : cancelEditing())}
          >
            {configuration ? "Configure ingress" : "Set up ingress"}
          </CreationButton>
        ) : undefined
      }
    >
      <p className="max-w-prose text-sm text-muted-foreground">
        Publish selected application Routes over HTTPS on this Host. The
        Groundplane Console and API stay private.
      </p>
      {loading && (
        <p role="status" className="text-sm text-muted-foreground">
          Loading ingress settings…
        </p>
      )}
      {loadError && (
        <div role="alert" className={errorClass}>
          <p>{loadError}</p>
          {onRefresh && (
            <Button variant="outline" size="sm" onClick={onRefresh}>
              Reload ingress
            </Button>
          )}
        </div>
      )}
      {!loading && (configuration || !loadError) && (
        <dl className="flex flex-wrap items-start gap-x-8 gap-y-3 text-sm">
          <div>
            <dt className="text-xs text-muted-foreground">Listener</dt>
            <dd className="mt-1">
              <Badge variant="outline">{stateText[status]}</Badge>
            </dd>
          </div>
          <div>
            <dt className="text-xs text-muted-foreground">Bind addresses</dt>
            <dd className="mt-1 break-all">
              {configuration?.addresses.join(", ") || "None"}
            </dd>
          </div>
          <div>
            <dt className="text-xs text-muted-foreground">Public Routes</dt>
            <dd className="mt-1">{configuration?.routeIds.length ?? 0}</dd>
          </div>
        </dl>
      )}
      {configuration?.message && (
        <p className="text-sm text-muted-foreground">{configuration.message}</p>
      )}
      {submittedTask && (
        <TaskLink taskId={submittedTask}>Open ingress Task</TaskLink>
      )}
      {editor && (
        <form
          onSubmit={review}
          className="space-y-4 rounded-lg border border-border p-4"
        >
          <fieldset disabled={!!working || blocked} className="space-y-4">
            <div className="flex items-center gap-2">
              <Checkbox
                id={`${prefix}-enable`}
                checked={editor.enabled}
                onChange={(event) => update({ enabled: event.target.checked })}
              />
              <Label htmlFor={`${prefix}-enable`}>
                Enable direct public HTTPS
              </Label>
            </div>
            {editor.enabled ? (
              <>
                <div className="space-y-2">
                  <Label htmlFor={`${prefix}-bind`}>Host bind addresses</Label>
                  <Input
                    id={`${prefix}-bind`}
                    value={editor.addresses}
                    onChange={(event) =>
                      update({ addresses: event.target.value })
                    }
                    aria-describedby={`${prefix}-bind-help`}
                    autoComplete="off"
                    required
                  />
                  <p
                    id={`${prefix}-bind-help`}
                    className="text-xs text-muted-foreground"
                  >
                    Enter one or more Host IP addresses separated by commas.
                    Allow inbound TCP 80 and 443 and point your public DNS at
                    this Host.
                  </p>
                </div>
                <fieldset className="space-y-2">
                  <legend className="text-sm font-medium">
                    Select public Routes
                  </legend>
                  {routes.length === 0 && (
                    <p className="text-sm text-muted-foreground">
                      Create an eligible public Route in its Environment first.
                    </p>
                  )}
                  {routes.map((route) => {
                    const checked = editor.routeIds.includes(route.id);
                    return (
                      <label
                        key={route.id}
                        className="flex items-start gap-3 rounded-md border border-border p-3"
                      >
                        <Checkbox
                          checked={checked}
                          className="mt-0.5"
                          disabled={Boolean(route.unavailable) && !checked}
                          onChange={(event) =>
                            update({
                              routeIds: event.target.checked
                                ? [...editor.routeIds, route.id]
                                : editor.routeIds.filter(
                                    (id) => id !== route.id,
                                  ),
                            })
                          }
                        />
                        <span className="min-w-0 space-y-1">
                          <span className="block break-all text-sm font-medium">
                            {route.hostname}
                            {route.path}
                          </span>
                          <span className="block text-xs text-muted-foreground">
                            {route.environment} / {route.service}
                          </span>
                          {route.unavailable && (
                            <span className="block text-xs text-muted-foreground">
                              {route.unavailable}
                            </span>
                          )}
                        </span>
                      </label>
                    );
                  })}
                  {editor.routeIds
                    .filter((id) => !routes.some((route) => route.id === id))
                    .map((id) => (
                      <label
                        key={id}
                        className="flex items-start gap-3 rounded-md border border-warning/30 p-3"
                      >
                        <Checkbox
                          checked
                          className="mt-0.5"
                          onChange={() =>
                            update({
                              routeIds: editor.routeIds.filter(
                                (routeId) => routeId !== id,
                              ),
                            })
                          }
                        />
                        <span className="min-w-0 text-sm">
                          Route no longer available{" "}
                          <code className="break-all">{id}</code>
                        </span>
                      </label>
                    ))}
                </fieldset>
              </>
            ) : (
              <p className="text-sm text-muted-foreground">
                Applying this change stops the Host’s direct public listener.
                Check other access paths before proceeding.
              </p>
            )}
          </fieldset>
          {stale && (
            <div role="alert" className={errorClass}>
              <p>
                Ingress settings changed while you were editing. Reload the
                current settings and review again.
              </p>
              <Button
                type="button"
                size="sm"
                variant="outline"
                onClick={startEditing}
              >
                Reload settings
              </Button>
            </div>
          )}
          {error && (
            <p role="alert" className={errorClass}>
              {error}
            </p>
          )}
          {reviewed && !blocked && (
            <section
              aria-label="Ingress review"
              className="space-y-3 rounded-md border border-border p-3 text-sm"
            >
              <h3 className="font-medium">Reviewed changes</h3>
              {reviewed.draft.enabled ? (
                <>
                  <p className="break-all text-muted-foreground">
                    Listen on {reviewed.draft.addresses.join(", ")} for TCP 80
                    and 443.
                  </p>
                  <ul
                    aria-label="Selected Routes"
                    className="list-inside list-disc text-muted-foreground"
                  >
                    {reviewed.draft.routeIds.map((id) => {
                      const route = routes.find(
                        (candidate) => candidate.id === id,
                      );
                      return (
                        <li className="break-all" key={id}>
                          {route ? `${route.hostname}${route.path}` : id}
                        </li>
                      );
                    })}
                  </ul>
                </>
              ) : (
                <p className="text-muted-foreground">
                  Stop direct public ingress on this Host.
                </p>
              )}
              <ul
                aria-label="Ingress checks"
                className="space-y-2 border-t border-border pt-3"
              >
                {reviewed.review.checks.map((check, index) => (
                  <li
                    key={`${check.scope}-${index}`}
                    className="flex items-start gap-2"
                  >
                    {check.status === "passed" && (
                      <CheckCircle2
                        aria-hidden
                        className="mt-0.5 size-4 shrink-0 text-primary"
                      />
                    )}
                    <div>
                      <span className="font-medium">
                        {check.scope} · {checkText[check.status]}
                      </span>
                      <p className="text-muted-foreground">{check.message}</p>
                    </div>
                  </li>
                ))}
              </ul>
            </section>
          )}
          <div className="flex flex-wrap justify-end gap-2">
            <Button
              type="button"
              variant="ghost"
              disabled={!!working}
              onClick={cancelEditing}
            >
              Cancel
            </Button>
            <Button
              type="submit"
              variant="outline"
              disabled={!!working || blocked}
            >
              {working === "preflight" ? "Checking…" : "Review changes"}
            </Button>
            {reviewed && (
              <Button
                type="button"
                disabled={
                  !!working ||
                  blocked ||
                  !reviewed.review.canApply ||
                  !reviewed.review.reviewToken
                }
                onClick={() => void apply()}
              >
                {working === "apply" ? "Submitting…" : "Apply ingress changes"}
              </Button>
            )}
          </div>
        </form>
      )}
    </ResourcePanel>
  );
}
