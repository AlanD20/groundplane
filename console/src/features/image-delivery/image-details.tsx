import { CopyButton } from "@/components/common/copy-button";
import { Inspector } from "@/components/common/inspector";
import {
  AdvancedDetails,
  SummaryItem,
  SummaryStrip,
} from "@/components/common/resource-panel";
import { StatusBadge } from "@/components/common/status-badge";
import { TaskLink } from "@/components/common/task-link";
import { Button } from "@/components/ui/button";
import { Tabs, TabsList, TabsPanel, TabsTab } from "@/components/ui/tabs";
import { Trash2 } from "lucide-react";
import { imageSize, type HostImage } from "./api";
import { imageName, shortImageId } from "./image-presentation";
import { ImageUsage } from "./image-usage";

export function ImageDetails({
  image,
  unavailable,
  onClose,
  onRemove,
}: {
  image: HostImage;
  unavailable: boolean;
  onClose: () => void;
  onRemove: () => void;
}) {
  return (
    <Inspector
      open
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
      context="Host / Image"
      title={imageName(image)}
      footer={
        <>
          <Button
            variant="destructive"
            disabled={unavailable || !!image.removal_blocked}
            onClick={onRemove}
          >
            <Trash2 className="size-4" />
            Remove image
          </Button>
          <Button variant="outline" onClick={onClose}>
            Close
          </Button>
        </>
      }
    >
      <SummaryStrip>
        <SummaryItem label="Size">{imageSize(image.size_bytes)}</SummaryItem>
        <SummaryItem label="Containers">{image.containers}</SummaryItem>
        <SummaryItem label="Tags">{image.tags.length}</SummaryItem>
        <SummaryItem label="Created">
          {new Date(image.created_at).toLocaleDateString()}
        </SummaryItem>
      </SummaryStrip>
      {unavailable && (
        <p role="status" className="text-xs text-warning">
          Usage may be out of date while inventory is unavailable or refreshing.
        </p>
      )}
      <Tabs defaultValue="usage">
        <TabsList aria-label="Image sections">
          <TabsTab value="usage">Usage</TabsTab>
          <TabsTab value="tags">Tags & digests</TabsTab>
          <TabsTab value="history">Fetch history</TabsTab>
        </TabsList>
        <TabsPanel value="usage" className="space-y-4 pt-4">
          <ImageUsage image={image} onClose={onClose} />
        </TabsPanel>
        <TabsPanel value="tags" className="space-y-4 pt-4">
          <section className="space-y-2">
            <h3 className="text-sm font-medium">Local tags</h3>
            {image.tags.length ? (
              image.tags.map((tag) => <Reference key={tag} value={tag} />)
            ) : (
              <p className="text-xs text-muted-foreground">
                Available by digest only.
              </p>
            )}
          </section>
          <AdvancedDetails title="Exact image references">
            <Reference value={image.id} />
            {image.digests.map((digest) => (
              <Reference key={digest} value={digest} />
            ))}
          </AdvancedDetails>
        </TabsPanel>
        <TabsPanel value="history" className="space-y-3 pt-4">
          {image.fetches.length === 0 && (
            <p className="text-xs text-muted-foreground">
              No Fetch history for this image.
            </p>
          )}
          {image.fetches.map((fetch) => (
            <div
              key={fetch.task_id}
              className="space-y-2 rounded-lg border border-border p-3 text-xs"
            >
              <p className="break-all font-medium">{fetch.requested}</p>
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
                <code>
                  {shortImageId(fetch.image.split("@")[1] ?? fetch.image)}
                </code>
                <CopyButton
                  value={fetch.image}
                  label="Copy selected image digest"
                />
              </div>
              <TaskLink taskId={fetch.task_id} onClick={onClose} />
            </div>
          ))}
          {image.fetches.length > 0 && (
            <p className="text-xs text-muted-foreground">
              Requested tags are historical; only completed Tasks confirm Fetch
              success.
            </p>
          )}
        </TabsPanel>
      </Tabs>
    </Inspector>
  );
}

function Reference({ value }: { value: string }) {
  return (
    <div className="flex min-w-0 items-start gap-2 rounded-md bg-surface p-2">
      <code className="min-w-0 flex-1 break-all text-xs">{value}</code>
      <CopyButton value={value} label="Copy reference" />
    </div>
  );
}
