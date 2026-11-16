import { useRef, useState } from "react";
import { TaskLink } from "@/components/common/task-link";
import {
  TablePagination,
  useTableView,
} from "@/components/common/table-controls";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { newULID } from "@/lib/utils";
import { listImages, removeImage, type HostImage } from "./api";
import { imageName, shortImageId } from "./image-presentation";

type Removal = {
  image: HostImage;
  key: string;
  state: "ready" | "accepted" | "blocked" | "error";
  message?: string;
  taskId?: string;
};

/** A reviewable batch of ordinary protected image removals, never Docker prune. */
export function RemoveImagesDialog({
  images,
  unused,
  onClose,
  onRefresh,
}: {
  images: HostImage[];
  unused: boolean;
  onClose: () => void;
  onRefresh: () => Promise<void>;
}) {
  const initial = useRef<Removal[]>(
    images.map((image) => ({
      image,
      key: newULID(),
      state:
        image.removal_blocked || image.containers > 0 ? "blocked" : "ready",
      message:
        image.removal_blocked ||
        (image.containers > 0 ? "Used by containers" : undefined),
    })),
  );
  const [items, setItems] = useState(initial.current);
  const latest = useRef(initial.current);
  const running = useRef(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const table = useTableView(
    items,
    { name: (item) => imageName(item.image) },
    "name",
    "asc",
    "image-removal-review",
  );
  const pending = items.filter(
    (item) => item.state === "ready" || item.state === "error",
  );
  const accepted = items.filter((item) => item.state === "accepted").length;
  const blocked = items.filter((item) => item.state === "blocked").length;
  function update(id: string, patch: Partial<Removal>) {
    latest.current = latest.current.map((item) =>
      item.image.id === id ? { ...item, ...patch } : item,
    );
    setItems(latest.current);
  }
  async function submit() {
    if (running.current || !pending.length) return;
    running.current = true;
    setBusy(true);
    setError("");
    try {
      const current = await listImages();
      for (const item of pending) {
        const image = current.images.find(
          (image) => image.id === item.image.id,
        );
        if (
          item.state !== "error" &&
          (!image || image.removal_blocked || image.containers > 0)
        ) {
          update(item.image.id, {
            state: "blocked",
            message: !image
              ? "No longer in the host inventory"
              : image.removal_blocked || "Used by containers",
          });
          continue;
        }
        try {
          const response = await removeImage(item.image.id, item.key);
          update(item.image.id, {
            state: "accepted",
            taskId: response.task_id,
            message: undefined,
          });
        } catch (cause) {
          update(item.image.id, {
            state: "error",
            message:
              cause instanceof Error
                ? cause.message
                : "Unable to submit removal",
          });
        }
      }
      await onRefresh();
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : "Unable to refresh image inventory",
      );
    } finally {
      running.current = false;
      setBusy(false);
    }
  }
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open && !running.current) onClose();
      }}
    >
      <DialogContent
        className="max-w-3xl"
        showClose={!busy}
        submitOnEnter={false}
      >
        <DialogHeader>
          <DialogTitle>
            {unused ? "Remove unused images" : "Remove selected images"}
          </DialogTitle>
          <DialogDescription>
            Remove these local images and their tags. Images needed by
            containers, rollback or recovery stay protected.
          </DialogDescription>
        </DialogHeader>
        {unused && (
          <p className="text-sm text-muted-foreground">
            Images without containers across all pages and filters, captured
            when this review opened. Protected images are skipped.
          </p>
        )}
        <p className="text-sm" role="status">
          {pending.length} to submit · {blocked} protected or unavailable ·{" "}
          {accepted} Tasks submitted
        </p>
        <p className="text-xs text-muted-foreground">
          One Task per image; follow its link for completion. Registry copies
          and application data stay intact.
        </p>
        <div className="overflow-x-auto rounded-xl border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Image</TableHead>
                <TableHead>Removal</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {table.rows.map((item) => (
                <TableRow key={item.image.id}>
                  <TableCell>
                    <span className="block break-all font-medium">
                      {imageName(item.image)}
                    </span>
                    <span className="text-xs text-muted-foreground">
                      {shortImageId(item.image.id)} · {item.image.tags.length}{" "}
                      tags
                    </span>
                  </TableCell>
                  <TableCell className="max-w-sm text-xs [overflow-wrap:anywhere]">
                    {item.taskId ? (
                      <TaskLink taskId={item.taskId}>
                        Follow removal Task
                      </TaskLink>
                    ) : (
                      item.message || "Ready to remove"
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
        <TablePagination table={table} label="Images" />
        {error && (
          <p role="alert" className="text-sm text-destructive">
            {error}
          </p>
        )}
        {items.some((item) => item.state === "error") && (
          <p role="alert" className="text-sm text-destructive">
            Some requests failed or their outcome is unknown. Retry those
            requests to resolve them; already submitted Tasks will not be
            submitted again.
          </p>
        )}
        <DialogFooter>
          <Button variant="outline" disabled={busy} onClick={onClose}>
            Close
          </Button>
          {pending.length > 0 && (
            <Button
              variant="destructive"
              disabled={busy}
              onClick={() => void submit()}
            >
              {busy
                ? "Submitting…"
                : items.some((item) => item.state === "error")
                  ? "Retry requests"
                  : `Remove ${pending.length} images`}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
