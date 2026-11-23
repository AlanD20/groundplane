import { workspaceSectionClassName } from "@/components/common/workspace-section";
import { CompactReference } from "@/components/common/compact-reference";
("use client");

import { ResourceLookupState } from "@/components/common/resource-lookup-state";
import { MetaPill } from "@/components/common/meta-pill";
import { PageHeader } from "@/components/common/page-header";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import {
  ArrowLeft,
  GitCompareArrows,
  Rocket,
  ShieldCheck,
  Workflow,
} from "lucide-react";
import { Link, useNavigate } from "react-router-dom";
import { ReleaseGroupControls } from "./release-group-controls";
import {
  activeMemberTag,
  releaseGroupPolicyLabel,
  releaseGroupTag,
} from "./release-group-projections";

export default function ReleaseGroupDetailPage() {
  const params = useRequiredParams("tenant", "project", "env", "id");
  const store = useStore();
  const navigate = useNavigate();
  const environment = store.getEnvironment(
    params.tenant,
    params.project,
    params.env,
  );
  const group = environment?.releaseGroups.find(
    (candidate) => candidate.id === params.id,
  );
  const listPath = `/t/${params.tenant}/${params.project}/${params.env}?view=operations&panel=release-groups`;

  if (!environment || !group) {
    return (
      <ResourceLookupState
        icon={<Workflow />}
        resource="Deployment group"
        loading={store.tenantsLoading || store.projectsLoading}
        error={
          store.tenantError ??
          store.projectError ??
          environment?.resourceLoadError
        }
        description={`${params.id} does not exist in ${params.tenant}/${params.project}/${params.env}.`}
        action={
          <Link to={listPath}>
            <Button variant="outline">
              <ArrowLeft className="size-4" /> Back to environment
            </Button>
          </Link>
        }
      />
    );
  }

  const tag = releaseGroupTag(environment, group);
  return (
    <ReleaseGroupControls
      key={group.id}
      env={environment}
      group={group}
      onRemoved={() => navigate(listPath, { replace: true })}
    >
      {({ actions, forms }) => (
        <div className="flex flex-col gap-6">
          <PageHeader
            eyebrow={
              <Link
                to={listPath}
                className="inline-flex items-center gap-1 hover:text-foreground"
              >
                <ArrowLeft className="size-3" /> {environment.name} deployment
                groups
              </Link>
            }
            title={group.name}
            icon={<Workflow />}
            meta={
              <>
                <CompactReference value={group.id} label="Group ID" />
                <MetaPill icon={<GitCompareArrows />}>
                  {group.order.length} ordered services
                </MetaPill>
                <MetaPill icon={<ShieldCheck />}>
                  {releaseGroupPolicyLabel(group.onFailure)}
                </MetaPill>
                <MetaPill icon={<Rocket />}>
                  {tag === "mixed member tags" ? tag : `tag ${tag}`}
                </MetaPill>
              </>
            }
            actions={actions}
          />
          {forms}
          <div className="grid gap-4 lg:grid-cols-[minmax(0,1.5fr)_minmax(18rem,0.7fr)]">
            <Card>
              <CardHeader>
                <CardTitle>Release order</CardTitle>
              </CardHeader>
              <CardContent className="flex flex-col gap-2">
                {group.order.map((service, index) => (
                  <div
                    key={service}
                    className={workspaceSectionClassName(
                      false,
                      "flex flex-wrap items-center gap-3",
                    )}
                  >
                    <span className="flex size-7 shrink-0 items-center justify-center rounded-full border border-primary/30 bg-primary/10 font-mono text-xs text-primary">
                      {index + 1}
                    </span>
                    <div className="min-w-0 flex-1">
                      <p className="font-mono text-sm font-medium">{service}</p>
                      <p className="text-xs text-muted-foreground">
                        Member service record remains independent.
                      </p>
                    </div>
                    <div className="flex flex-col items-end gap-1">
                      <Badge variant="primary">
                        {activeMemberTag(environment, service) ??
                          "no active release"}
                      </Badge>
                      <Badge variant="outline">
                        {environment.services.find(
                          (candidate) => candidate.name === service,
                        )?.strategy ?? "missing"}
                      </Badge>
                    </div>
                  </div>
                ))}
              </CardContent>
            </Card>
            <div className="flex flex-col gap-4">
              <Card>
                <CardHeader>
                  <CardTitle>Failure behavior</CardTitle>
                </CardHeader>
                <CardContent className="flex flex-col gap-2 text-sm">
                  <Badge
                    variant={
                      group.onFailure === "switch_back" ? "success" : "warning"
                    }
                    className="w-fit"
                  >
                    {group.onFailure}
                  </Badge>
                  <p className="text-muted-foreground">
                    {group.onFailure === "switch_back"
                      ? "Completed members switch back if a later member fails."
                      : "Completed members stay active if a later member fails."}
                  </p>
                </CardContent>
              </Card>
            </div>
          </div>
        </div>
      )}
    </ReleaseGroupControls>
  );
}
