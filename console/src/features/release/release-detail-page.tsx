import { subscribeTerminalTasks } from "@/features/task/terminal-observation";
import { workspaceSectionClassName } from "@/components/common/workspace-section";
import { CompactReference } from "@/components/common/compact-reference";
("use client");

import { EmptyState } from "@/components/common/empty-state";
import { ImageReference } from "@/components/common/image-reference";
import { MetaPill } from "@/components/common/meta-pill";
import { PageHeader } from "@/components/common/page-header";
import { TaskLink } from "@/components/common/task-link";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import { ArrowLeft, History } from "lucide-react";
import { useEffect, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { fetchReleaseDetail, type ReleaseDetailResponse } from "./api";

export default function ReleaseDetailPage() {
  const params = useRequiredParams("tenant", "project", "env", "id");
  const store = useStore();
  const [search] = useSearchParams();
  const environment = store.getEnvironment(
    params.tenant,
    params.project,
    params.env,
  );
  const [release, setRelease] = useState<ReleaseDetailResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const originService = environment?.services.find(
    (service) => service.id === search.get("service"),
  );
  const basePath = `/t/${params.tenant}/${params.project}/${encodeURIComponent(params.env)}`;
  const listPath = originService
    ? `${basePath}?view=overview&service=${originService.id}&serviceTab=releases`
    : `${basePath}?view=operations&panel=releases`;

  useEffect(() => {
    const controller = new AbortController();
    setRelease(null);
    setError(null);
    let version = 0;
    const refresh = () => {
      const requestVersion = ++version;
      void fetchReleaseDetail(params.id, controller.signal)
        .then((next) => {
          if (!controller.signal.aborted && requestVersion === version) {
            setRelease(next);
            setError(null);
          }
        })
        .catch((cause: unknown) => {
          if (!controller.signal.aborted && requestVersion === version)
            setError(
              cause instanceof Error
                ? cause.message
                : "Deployment could not be refreshed",
            );
        });
    };
    refresh();
    const unsubscribe = subscribeTerminalTasks((task) => {
      if (task.environment_id === environment?.id) refresh();
    });
    return () => {
      controller.abort();
      unsubscribe();
    };
  }, [params.id, environment?.id]);

  if (!environment || (error && !release)) {
    return (
      <EmptyState
        icon={<History />}
        title="Release not found"
        description={error ?? `${params.id} is outside this environment.`}
        action={
          <Link to={listPath}>
            <Button variant="outline">
              <ArrowLeft className="size-4" /> Back to releases
            </Button>
          </Link>
        }
      />
    );
  }
  if (!release)
    return (
      <EmptyState
        icon={<History />}
        title="Loading release"
        description="Reading the durable release ledger."
      />
    );
  if (release.environment_id !== environment.id) {
    return (
      <EmptyState
        icon={<History />}
        title="Release not found"
        description={`${params.id} is outside this environment.`}
        action={
          <Link to={listPath}>
            <Button variant="outline">
              <ArrowLeft className="size-4" /> Back to releases
            </Button>
          </Link>
        }
      />
    );
  }
  const service =
    environment.services.find(
      (candidate) => candidate.id === release.service_id,
    )?.name ?? release.service_id;
  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow={
          <Link
            to={listPath}
            className="inline-flex items-center gap-1 hover:text-foreground"
          >
            <ArrowLeft className="size-3" />{" "}
            {originService?.name ?? environment.name} deployments
          </Link>
        }
        title={service}
        description="Deployment image, configuration and execution"
        icon={<History />}
        meta={
          <>
            <MetaPill>
              <ImageReference value={release.tag} />
            </MetaPill>
            <MetaPill>{release.state}</MetaPill>
            <MetaPill>{release.strategy}</MetaPill>
            <MetaPill>{release.operation_kind}</MetaPill>
          </>
        }
      />
      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Image & configuration</CardTitle>
          </CardHeader>
          <CardContent className="space-y-0 divide-y divide-border text-sm [&>p]:py-4 [&>div]:py-4">
            <p>
              <span className="text-muted-foreground">Image</span>
              <br />
              <span className="break-all font-mono text-xs">
                {release.image}
              </span>
            </p>
            <p>
              <span className="text-muted-foreground">Digest</span>
              <br />
              <span className="break-all font-mono text-xs">
                {release.digest || "tag resolved without registry digest"}
              </span>
            </p>
            <p>
              <span className="text-muted-foreground">Render input</span>
              <br />
              <span className="break-all font-mono text-xs">
                {release.render_input_digest}
              </span>
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Execution</CardTitle>
          </CardHeader>
          <CardContent className="space-y-0 divide-y divide-border text-sm [&>p]:py-4 [&>div]:py-4">
            <div className="flex gap-2">
              <Badge variant={release.serving ? "success" : "outline"}>
                {release.serving ? "serving" : "not serving"}
              </Badge>
              <Badge
                variant={release.current_successful ? "success" : "outline"}
              >
                {release.current_successful ? "rollback source" : "historical"}
              </Badge>
            </div>
            <p>
              <span className="text-muted-foreground">Operation</span>
              <br />
              <span className="break-all font-mono text-xs">
                {release.operation_id}
              </span>
            </p>
            <p>
              <span className="text-muted-foreground">Originating task</span>
              <br />
              <TaskLink taskId={release.originating_task_id}>
                Inspect Task
              </TaskLink>
            </p>
            <p>
              <span className="text-muted-foreground">Attempts</span>
              <br />
              {release.attempts?.length ?? 0}
            </p>
          </CardContent>
        </Card>
      </div>
      <div className={workspaceSectionClassName(false, "space-y-3")}>
        <CompactReference value={release.id} />
      </div>
      {error && (
        <p role="alert" className="text-sm text-destructive">
          Refresh failed: {error}. The last loaded deployment remains visible.
        </p>
      )}
    </div>
  );
}
