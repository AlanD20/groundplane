import { useState } from "react";
import type { components, operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import { ControllerRequestError } from "@/lib/controller-request-errors";
import { useVisibleRefresh } from "@/lib/use-visible-refresh";
import {
  PublicIngressPanel,
  type PublicIngressDraft,
  type PublicIngressPanelProps,
} from "./public-ingress-panel";

type IngressResponse =
  operations["public-ingress.show"]["responses"][200]["content"]["application/json"];
type ReviewResponse =
  operations["public-ingress.preflight"]["responses"][200]["content"]["application/json"];
type ApplyResponse =
  operations["public-ingress.apply"]["responses"][202]["content"]["application/json"];

function requestDraft(draft: PublicIngressDraft) {
  return {
    enabled: draft.enabled,
    addresses: draft.addresses,
    route_ids: draft.routeIds,
  } satisfies components["schemas"]["PublicIngressDraft"];
}

function showError(cause: unknown) {
  if (cause instanceof ControllerRequestError && cause.status === 404)
    return "Public ingress is unavailable on this Host. Check the installed Controller and Host settings.";
  return cause instanceof Error
    ? cause.message
    : "Unable to read public ingress settings.";
}

export function ConnectedPublicIngress({ hostId }: { hostId?: string }) {
  const [state, setState] = useState<{
    hostId: string;
    data: IngressResponse;
  }>();
  const [failure, setFailure] = useState<{ hostId: string; message: string }>();
  const [retry, setRetry] = useState(0);
  const path = `/hosts/${encodeURIComponent(hostId ?? "")}/public-ingress`;
  const data = state?.hostId === hostId ? state?.data : undefined;
  const error = failure?.hostId === hostId ? failure?.message : undefined;
  const wire = data?.configuration;
  const configuration: PublicIngressPanelProps["configuration"] = wire
    ? {
        revision: wire.revision,
        enabled: wire.enabled,
        addresses: wire.addresses ?? [],
        routeIds: wire.route_ids ?? [],
        state: wire.state,
        ...(wire.task_id ? { taskId: wire.task_id } : {}),
        ...(wire.message ? { message: wire.message } : {}),
      }
    : null;

  useVisibleRefresh(
    async (signal) => {
      if (!hostId) return;
      try {
        const data = await controllerRequest<IngressResponse>(path, 200, {
          signal,
        });
        if (signal.aborted) return;
        setState({ hostId, data });
        setFailure(undefined);
      } catch (cause) {
        if (!signal.aborted) setFailure({ hostId, message: showError(cause) });
      }
    },
    { key: `${hostId ?? ""}:${retry}`, enabled: Boolean(hostId) },
  );

  return (
    <PublicIngressPanel
      key={hostId ?? "unavailable"}
      configuration={configuration}
      routes={data?.routes ?? []}
      loading={Boolean(hostId) && !data && !error}
      loadError={
        hostId
          ? error
          : "This Controller has not reported a Host ID for public ingress."
      }
      onRefresh={hostId ? () => setRetry((value) => value + 1) : undefined}
      onPreflight={async (draft, revision, signal) => {
        if (!hostId || revision === null)
          throw new Error("Reload ingress settings before reviewing changes.");
        const review = await controllerRequest<ReviewResponse>(
          `${path}/preflight`,
          200,
          {
            method: "POST",
            body: requestDraft(draft),
            ifMatch: JSON.stringify(revision),
            signal,
          },
        );
        return {
          revision: review.revision,
          canApply: review.can_apply,
          checks: review.checks ?? [],
          ...(review.review_token ? { reviewToken: review.review_token } : {}),
        };
      }}
      onApply={async (draft, review, idempotencyKey) => {
        if (!hostId || !review.reviewToken)
          throw new Error("Review ingress changes before applying them.");
        const body = {
          ...requestDraft(draft),
          review_token: review.reviewToken,
        } satisfies components["schemas"]["PublicIngressApplyRequest"];
        const accepted = await controllerRequest<ApplyResponse>(
          `${path}/apply`,
          202,
          {
            method: "POST",
            body,
            ifMatch: JSON.stringify(review.revision),
            idempotencyKey,
          },
        );
        return { taskId: accepted.task_id };
      }}
    />
  );
}
