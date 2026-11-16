"use client";

import { useEffect, useRef, useState, type ReactNode } from "react";
import { useDraftField } from "@/lib/use-draft-field";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import type { ReleaseGroupRollbackPreviewResponse } from "./api";
import type { Environment, ReleaseGroup } from "@/lib/types";
import { releaseGroupPolicyLabel } from "./release-group-projections";
import {
  acceptRollbackPreview,
  beginRollbackPreviewRequest,
  isRollbackPreviewAccepted,
  isRollbackPreviewRequestCurrent,
  requireRollbackPreview,
  rollbackPreviewScope,
  type AcceptedRollbackPreview,
} from "./release-group-rollback-preview-authority";

type OperationDialogProps = {
  env: Environment;
  group: ReleaseGroup;
  open: boolean;
  onOpenChange: (open: boolean) => void;
};

export function DeployReleaseGroupDialog({
  env,
  group,
  open,
  onOpenChange,
  targetSelector,
}: OperationDialogProps & { targetSelector?: ReactNode }) {
  const store = useStore();
  const params = useRequiredParams("tenant");
  const [tag, setTag] = useDraftField(group.tag ?? "", group.id);

  useEffect(() => {
    if (open) setTag(group.tag ?? "");
  }, [group.id, open]);

  return (
    <TaskRunnerDialog
      runAfter
      open={open}
      onOpenChange={onOpenChange}
      variant="inline"
      title={`Deploy ${group.name} · ${env.name}`}
      description="Deploy this tag to all members in order."
      type="deploy"
      target={group.id}
      workspace={params.tenant}
      startLabel="Deploy group"
      startDisabled={!tag.trim()}
      review={
        <div className="flex flex-col gap-3">
          {targetSelector}
          <p className="text-sm text-muted-foreground">
            Services deploy in order: {group.order.join(", ")}. Each Service
            uses its configured deployment strategy.
          </p>
          <div className="flex flex-col gap-1">
            <Label htmlFor="release-group-tag">Image tag</Label>
            <Input
              id="release-group-tag"
              value={tag}
              onChange={(event) => setTag(event.target.value)}
              placeholder="v1.2.3"
            />
          </div>
          <div className="rounded-lg border border-border bg-surface p-3 text-xs text-muted-foreground">
            <span className="font-medium text-foreground">
              Failure behavior:
            </span>{" "}
            {releaseGroupPolicyLabel(group.onFailure)}
          </div>
        </div>
      }
      onDispatch={(after) =>
        store.deployReleaseGroup(env.id, group.id, tag.trim(), after)
      }
      onSettled={async () => {
        await store.refreshEnvironmentReleases(env.id);
      }}
    />
  );
}

export function RollbackReleaseGroupDialog({
  env,
  group,
  open,
  onOpenChange,
}: OperationDialogProps) {
  const store = useStore();
  const params = useRequiredParams("tenant");
  const [tag, setTag] = useState("");
  const [acceptedPreview, setAcceptedPreview] =
    useState<AcceptedRollbackPreview | null>(null);
  const [previewError, setPreviewError] = useState("");
  const [previewLoading, setPreviewLoading] = useState(false);
  const requestGeneration = useRef(0);
  const dialogSession = useRef(open ? 1 : 0);
  const previousOpen = useRef(open);
  if (previousOpen.current !== open) {
    previousOpen.current = open;
    dialogSession.current += 1;
  }
  const currentScope = rollbackPreviewScope(
    env.id,
    group.id,
    tag === "" ? undefined : tag,
    dialogSession.current,
  );
  const currentScopeRef = useRef(currentScope);
  currentScopeRef.current = currentScope;
  const currentPreview = isRollbackPreviewAccepted(
    acceptedPreview,
    currentScope,
  )
    ? acceptedPreview
    : null;

  useEffect(() => {
    requestGeneration.current += 1;
    setAcceptedPreview(null);
    setPreviewError("");
    setPreviewLoading(false);
    if (open) setTag("");
  }, [env.id, group.id, open]);

  const invalidatePreview = () => {
    dialogSession.current += 1;
    requestGeneration.current += 1;
    setAcceptedPreview(null);
    setPreviewError("");
    setPreviewLoading(false);
  };

  const loadPreview = async () => {
    dialogSession.current += 1;
    const generation = requestGeneration.current + 1;
    requestGeneration.current = generation;
    const requestedScope = rollbackPreviewScope(
      env.id,
      group.id,
      tag === "" ? undefined : tag,
      dialogSession.current,
    );
    currentScopeRef.current = requestedScope;
    const request = beginRollbackPreviewRequest(requestedScope, generation);
    const requestedTag = request.scope.tagPresent
      ? request.scope.tagValue
      : undefined;
    const requestIsCurrent = () =>
      isRollbackPreviewRequestCurrent(
        request,
        currentScopeRef.current,
        requestGeneration.current,
      );
    setAcceptedPreview(null);
    setPreviewError("");
    setPreviewLoading(true);
    try {
      const result: ReleaseGroupRollbackPreviewResponse =
        await store.previewReleaseGroupRollback(
          request.scope.environmentId,
          request.scope.groupId,
          requestedTag,
        );
      if (!requestIsCurrent()) return;
      setAcceptedPreview(acceptRollbackPreview(request.scope, result));
    } catch (error: unknown) {
      if (!requestIsCurrent()) return;
      setPreviewError(
        error instanceof Error ? error.message : "Rollback preview failed",
      );
    } finally {
      if (requestIsCurrent()) setPreviewLoading(false);
    }
  };

  const sources = currentPreview?.response.sources ?? [];

  return (
    <TaskRunnerDialog
      runAfter
      open={open}
      onOpenChange={(nextOpen) => {
        if (!nextOpen) invalidatePreview();
        onOpenChange(nextOpen);
      }}
      variant="inline"
      title={`Roll back ${group.name} · ${env.name}`}
      description="Deploy each member’s previous image in order, keeping its current configuration, replicas and release strategy. Database migrations are not reversed."
      type="rollback"
      target={group.id}
      workspace={params.tenant}
      startLabel="Roll back group"
      startDisabled={(after) =>
        after.length === 0 &&
        (previewLoading || currentPreview === null || previewError !== "")
      }
      review={(after) => (
        <div className="flex flex-col gap-3">
          <div className="flex flex-col gap-1">
            <Label htmlFor="release-group-rollback-tag">
              Image tag (optional)
            </Label>
            <Input
              id="release-group-rollback-tag"
              aria-describedby="release-group-rollback-tag-help"
              value={tag}
              onChange={(event) => {
                setTag(event.target.value);
                invalidatePreview();
              }}
              placeholder="Previously deployed tag, e.g. v1.2.2"
            />
            <p
              id="release-group-rollback-tag-help"
              className="text-xs text-muted-foreground"
            >
              Leave blank to use the image from each member's previous
              successful deployment.
            </p>
            <Button
              type="button"
              variant="secondary"
              onClick={() => {
                void loadPreview();
              }}
              disabled={previewLoading || after.length > 0}
            >
              Preview
            </Button>
          </div>
          <div className="flex flex-col gap-2 rounded-lg border border-border bg-surface p-3 text-xs text-muted-foreground">
            <p>
              <span className="font-medium text-foreground">
                Failure behavior:
              </span>{" "}
              {releaseGroupPolicyLabel(group.onFailure)}
            </p>
            {after.length > 0 && (
              <p>
                Rollback sources are selected after prerequisite Tasks succeed
                and fully settle. The current preview is not used.
              </p>
            )}
            {after.length === 0 && previewLoading && (
              <p role="status">Loading Controller-selected rollback sources…</p>
            )}
            {after.length === 0 && previewError && (
              <p role="alert" className="text-destructive">
                {previewError}
              </p>
            )}
            {(after.length === 0 ? sources : []).map((source) => (
              <dl
                key={source.service_id}
                className="grid gap-2 border-t border-border pt-2 font-mono sm:grid-cols-3"
              >
                <div className="min-w-0">
                  <dt className="font-sans text-muted-foreground">Service</dt>
                  <dd className="break-all text-foreground">
                    {source.service_id}
                  </dd>
                </div>
                <div className="min-w-0">
                  <dt className="font-sans text-muted-foreground">Release</dt>
                  <dd className="break-all text-foreground">
                    {source.release_id}
                  </dd>
                </div>
                <div className="min-w-0">
                  <dt className="font-sans text-muted-foreground">Tag</dt>
                  <dd className="break-all text-foreground">{source.tag}</dd>
                </div>
              </dl>
            ))}
          </div>
        </div>
      )}
      onDispatch={async (after) => {
        if (after.length > 0)
          return store.rollbackReleaseGroup(
            env.id,
            group.id,
            tag === "" ? undefined : tag,
            undefined,
            after,
          );
        const dispatchScope = rollbackPreviewScope(
          env.id,
          group.id,
          tag === "" ? undefined : tag,
          dialogSession.current,
        );
        const authority = requireRollbackPreview(
          acceptedPreview,
          dispatchScope,
        );
        try {
          return await store.rollbackReleaseGroup(
            env.id,
            group.id,
            tag === "" ? undefined : tag,
            authority.response.revision,
          );
        } catch (error) {
          invalidatePreview();
          setPreviewError(
            "Rollback sources changed. Load and review a new preview.",
          );
          throw error;
        }
      }}
      onSettled={async () => {
        await store.refreshEnvironmentReleases(env.id);
      }}
    />
  );
}
