import { TaskLink } from "@/components/common/task-link";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import type { operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import { newULID } from "@/lib/utils";
import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";

type Accepted =
  operations["image.fetch"]["responses"][202]["content"]["application/json"];

export function ImageFetchAction({
  onSettled,
  open,
  onOpenChange: setOpen,
}: {
  onSettled?: () => Promise<void>;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const [search] = useSearchParams();
  const [image, setImage] = useState(search.get("fetch") ?? "");
  const [accepted, setAccepted] = useState<Accepted | null>(null);
  const [completed, setCompleted] = useState(false);
  const intent = useRef<{
    image: string;
    key: string;
    accepted: Accepted | null;
  } | null>(null);

  useEffect(() => {
    if (open && intent.current?.accepted) intent.current = null;
  }, [open]);

  async function dispatch() {
    const requested = image.trim();
    if (!intent.current || intent.current.image !== requested) {
      intent.current = { image: requested, key: newULID(), accepted: null };
    }
    const current = intent.current;
    if (current.accepted) return current.accepted.task_id;
    setCompleted(false);
    setAccepted(null);
    const response = await controllerRequest<Accepted>("/images/fetch", 202, {
      method: "POST",
      body: { image: current.image },
      idempotencyKey: current.key,
    });
    current.accepted = response;
    if (intent.current === current) setAccepted(response);
    return response.task_id;
  }

  return (
    <>
      {accepted && (
        <TaskLink taskId={accepted.task_id}>
          {completed ? "Fetch completed" : "View Fetch Task"}
        </TaskLink>
      )}
      <TaskRunnerDialog
        open={open}
        onOpenChange={setOpen}
        variant="inline"
        title="Fetch image"
        type="fetch"
        target={image.trim() || "Registry image"}
        workspace="platform"
        startLabel="Fetch image"
        startDisabled={!image.trim()}
        onDispatch={dispatch}
        onCommit={() => setCompleted(true)}
        onSettled={onSettled}
        description="Use an image tag or digest."
        review={
          <div className="space-y-2">
            <Label htmlFor="fetch-image">Registry image</Label>
            <Input
              id="fetch-image"
              value={image}
              onChange={(event) => setImage(event.target.value)}
              placeholder="nginx:latest, ghcr.io/org/app:tag or registry.groundplane.internal:5000/project/api:release"
            />
            <p className="text-xs text-muted-foreground">
              After an uncertain request, keep the same image and retry to
              resolve its original acceptance.
            </p>
          </div>
        }
      />
    </>
  );
}
