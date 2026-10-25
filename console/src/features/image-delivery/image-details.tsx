import type { ReactNode } from "react";
import { CopyButton } from "@/components/common/copy-button";
import { PageHeader } from "@/components/common/page-header";
import { CompactReference } from "@/components/common/compact-reference";
import { ResourcePanel } from "@/components/common/resource-panel";
import { useSearchParams } from "react-router-dom";
import { SummaryItem, SummaryStrip } from "@/components/common/resource-panel";
import { StatusBadge } from "@/components/common/status-badge";
import { TaskLink } from "@/components/common/task-link";
import { Button } from "@/components/ui/button";
import { ArrowLeft, Package, Trash2 } from "lucide-react";
import { imageSize, type HostImage } from "./api";
import {
  imageCurrentTags,
  imageName,
  previousImageReference,
  imageRepositories,
  isDigestReference,
  previousFetchReference,
  shortImageId,
  selectedImageTags,
} from "./image-presentation";
import { ImageUsage } from "./image-usage";

export function ImageDetails({
  image,
  unavailable,
  observedAt,
  onClose,
  onRemove,
  actionForm,
  platformImages,
  onInspect,
}: {
  actionForm?: ReactNode;
  image: HostImage;
  unavailable: boolean;
  observedAt?: string;
  onClose: () => void;
  onRemove: () => void;
  platformImages: HostImage[];
  onInspect: (id: string) => void;
}) {
  const [search] = useSearchParams();
  const requestedTab = search.get("imageTab");
  const tab = ["usage", "tags", "history"].includes(requestedTab ?? "")
    ? requestedTab
    : "overview";
  const currentTags = imageCurrentTags(image);
  const previousFetch = previousFetchReference(image);
  return (
    <div className="flex min-w-0 flex-col gap-5">
      <Button
        variant="ghost"
        size="content"
        className="w-fit"
        onClick={onClose}
      >
        <ArrowLeft className="size-4" /> All images
      </Button>
      <PageHeader
        title={imageName(image)}
        eyebrow="Host image"
        description={
          previousImageReference(image)
            ? `Previously known as ${previousImageReference(image)}`
            : undefined
        }
        icon={<Package />}
        actions={
          <Button
            variant="destructive"
            disabled={unavailable || !!image.removal_blocked}
            onClick={onRemove}
          >
            <Trash2 className="size-4" /> Remove image
          </Button>
        }
      />
      {actionForm}
      <div className="flex flex-wrap items-center gap-x-6 gap-y-2 rounded-lg border border-border bg-card p-4 text-xs text-muted-foreground">
        <CompactReference value={image.id} label="Image ID" />
        <span>Created {new Date(image.created_at).toLocaleString()}</span>
        {observedAt && (
          <span>Observed {new Date(observedAt).toLocaleString()}</span>
        )}
      </div>
      {unavailable && (
        <p role="status" className="text-xs text-warning">
          Usage may be out of date because inventory could not be refreshed.
        </p>
      )}
      {tab === "overview" && (
        <>
          <SummaryStrip>
            <SummaryItem label="Size">
              {imageSize(image.size_bytes)}
            </SummaryItem>
            <SummaryItem label="Containers">{image.containers}</SummaryItem>
            <SummaryItem label="Current tags">{currentTags.length}</SummaryItem>
            <SummaryItem label="Repositories">
              {imageRepositories(image).length}
            </SummaryItem>
          </SummaryStrip>
          <ResourcePanel title="Availability">
            <p className="text-sm">
              {image.removal_blocked ||
                "This image is unused and can be removed from the host."}
            </p>
            {image.protection_reason && (
              <p className="text-sm text-muted-foreground">
                {image.protection_reason}
              </p>
            )}
          </ResourcePanel>
          <ResourcePanel title="Current tags">
            {currentTags.length ? (
              currentTags.map((tag) => <Reference key={tag} value={tag} />)
            ) : (
              <div className="space-y-1 text-sm text-muted-foreground">
                <p>
                  No current Docker tag points to this image. Select it by its
                  exact digest.
                </p>
                {previousFetch && <p>Previously fetched as {previousFetch}.</p>}
              </div>
            )}
          </ResourcePanel>
          {selectedImageTags(image).length > 0 && (
            <ResourcePanel title="Pinned Service image references">
              <p className="text-sm text-muted-foreground">
                These names were saved with this exact content in Service
                configuration. They do not indicate current Docker tags or prove
                that a Service is running.
              </p>
              {selectedImageTags(image).map((reference) => (
                <Reference key={reference} value={reference} />
              ))}
            </ResourcePanel>
          )}
          {platformImages.length > 0 && (
            <ResourcePanel title="Platform images">
              <p className="text-sm text-muted-foreground">
                The index and its platform content share one table entry. Each
                exact image retains its own usage and protected removal action.
              </p>
              {platformImages.map((platform) => (
                <Button
                  key={platform.id}
                  variant="outline"
                  onClick={() => onInspect(platform.id)}
                >
                  Inspect platform image · {shortImageId(platform.id)}
                </Button>
              ))}
            </ResourcePanel>
          )}
        </>
      )}
      {tab === "usage" && (
        <ResourcePanel title="Image usage">
          <ImageUsage image={image} />
        </ResourcePanel>
      )}
      {tab === "tags" && (
        <ResourcePanel title="Tags & digests">
          <section className="space-y-2">
            <h3 className="text-sm font-medium">Local tags</h3>
            {currentTags.length ? (
              currentTags.map((tag) => <Reference key={tag} value={tag} />)
            ) : (
              <p className="text-xs text-muted-foreground">
                No current Docker tags. Available by exact digest only.
              </p>
            )}
          </section>
          <section className="space-y-2">
            <h3 className="text-sm font-medium">Exact image references</h3>
            <Reference value={image.id} />
            {image.digests.map((digest) => (
              <Reference key={digest} value={digest} />
            ))}
          </section>
        </ResourcePanel>
      )}
      {tab === "history" && (
        <ResourcePanel title="Fetch history">
          {image.fetches.length === 0 && (
            <p className="text-xs text-muted-foreground">
              No Fetch history for this image.
            </p>
          )}
          {image.fetches.map((fetch) => {
            const historicalTag =
              fetch.status === "completed" &&
              !isDigestReference(fetch.requested) &&
              !currentTags.includes(fetch.requested);
            return (
              <div
                key={fetch.task_id}
                className="space-y-2 rounded-lg border border-border p-3 text-xs"
              >
                <p className="break-all font-medium">
                  {historicalTag
                    ? "Previously fetched as "
                    : "Fetch requested "}
                  {fetch.requested}
                </p>
                <div className="flex flex-wrap items-center gap-2">
                  <StatusBadge
                    status={fetch.status}
                    label={fetch.status.replaceAll("_", " ")}
                  />
                  <span className="text-muted-foreground">
                    {new Date(fetch.requested_at).toLocaleString()}
                  </span>
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <span className="text-muted-foreground">Selected digest</span>
                  <code>{shortImageId(fetch.image)}</code>
                  <CopyButton
                    value={fetch.image}
                    label="Copy selected image digest"
                  />
                </div>
                <TaskLink taskId={fetch.task_id} />
              </div>
            );
          })}
          {image.fetches.length > 0 && (
            <p className="text-xs text-muted-foreground">
              Fetch requests are retained history, not current Docker tags. Only
              completed Tasks confirm a successful Fetch.
            </p>
          )}
        </ResourcePanel>
      )}
    </div>
  );
}

function Reference({ value }: { value: string }) {
  return (
    <div className="flex min-w-0 items-start gap-2 rounded-lg border border-border bg-background/30 p-4">
      <code className="min-w-0 flex-1 break-all text-xs">{value}</code>
      <CopyButton value={value} label="Copy reference" />
    </div>
  );
}
