"use client";

import { useEffect, useState } from "react";
import { ArrowLeft, History } from "lucide-react";
import { Link } from "react-router-dom";
import { EmptyState } from "@/components/common/empty-state";
import { MetaPill } from "@/components/common/meta-pill";
import { PageHeader } from "@/components/common/page-header";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import { fetchReleaseDetail, type ReleaseDetailResponse } from "./api";

export default function ReleaseDetailPage() {
  const params = useRequiredParams("tenant", "project", "env", "id");
  const store = useStore();
  const environment = store.getEnvironment(
    params.tenant,
    params.project,
    params.env,
  );
  const [release, setRelease] = useState<ReleaseDetailResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const listPath = `/t/${params.tenant}/${params.project}/${params.env}?tab=releases`;

  useEffect(() => {
    const controller = new AbortController();
    setRelease(null);
    setError(null);
    void fetchReleaseDetail(params.id, controller.signal)
      .then(setRelease)
      .catch((cause: unknown) => {
        if (!controller.signal.aborted)
          setError(
            cause instanceof Error
              ? cause.message
              : "Release detail failed to load",
          );
      });
    return () => controller.abort();
  }, [params.id]);

  if (!environment || error) {
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
            <ArrowLeft className="size-3" /> {environment.name} releases
          </Link>
        }
        title={`${service} · ${release.tag}`}
        description={release.id}
        icon={<History />}
        meta={
          <>
            <MetaPill>{release.state}</MetaPill>
            <MetaPill>{release.strategy}</MetaPill>
            <MetaPill>{release.operation_kind}</MetaPill>
          </>
        }
      />
      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Frozen release input</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-3 text-sm">
            <p>
              <span className="text-muted-foreground">Image</span>
              <br />
              <span className="font-mono text-xs">{release.image}</span>
            </p>
            <p>
              <span className="text-muted-foreground">Digest</span>
              <br />
              <span className="font-mono text-xs">
                {release.digest || "tag resolved without registry digest"}
              </span>
            </p>
            <p>
              <span className="text-muted-foreground">Render input</span>
              <br />
              <span className="font-mono text-xs">
                {release.render_input_digest}
              </span>
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Execution</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-3 text-sm">
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
              <span className="font-mono text-xs">{release.operation_id}</span>
            </p>
            <p>
              <span className="text-muted-foreground">Originating task</span>
              <br />
              <span className="font-mono text-xs">
                {release.originating_task_id}
              </span>
            </p>
            <p>
              <span className="text-muted-foreground">Attempts</span>
              <br />
              {release.attempts?.length ?? 0}
            </p>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
