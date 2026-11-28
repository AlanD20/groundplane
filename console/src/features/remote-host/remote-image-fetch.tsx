import { useId, useRef, useState, type FormEvent } from "react";
import { CreationButton } from "@/components/common/creation-button";
import { TaskLink } from "@/components/common/task-link";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { newULID } from "@/lib/utils";
import { fetchRemoteImage } from "./remote-host-api";

export function RemoteImageFetch({
  hostId,
  disabled,
}: {
  hostId: string;
  disabled: boolean;
}) {
  const inputId = useId();
  const [open, setOpen] = useState(false);
  const [input, setInput] = useState("");
  const [working, setWorking] = useState(false);
  const [error, setError] = useState("");
  const [taskId, setTaskId] = useState<string>();
  const request = useRef<{ image: string; key: string }>(undefined);
  const busy = useRef(false);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (disabled || busy.current) return;
    const image = input.trim();
    if (!/@sha256:[a-f0-9]{64}$/.test(image)) {
      setError(
        "Enter a repository reference ending in the exact SHA-256 manifest digest.",
      );
      return;
    }
    if (request.current?.image !== image)
      request.current = { image, key: newULID() };
    busy.current = true;
    setWorking(true);
    setError("");
    try {
      const result = await fetchRemoteImage(hostId, image, request.current.key);
      setTaskId(result.task_id);
      request.current = undefined;
      setOpen(false);
      setInput("");
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : "The image Fetch could not be submitted.",
      );
    } finally {
      busy.current = false;
      setWorking(false);
    }
  }

  return (
    <div className="space-y-2 border-t border-border pt-3">
      <div className="flex flex-wrap items-center gap-3">
        <CreationButton
          variant="outline"
          size="sm"
          active={open}
          disabled={working || disabled}
          onOpenChange={setOpen}
        >
          Fetch image
        </CreationButton>
        {taskId && <TaskLink taskId={taskId}>Open image Fetch Task</TaskLink>}
      </div>
      {open && (
        <form className="space-y-2" onSubmit={submit}>
          <div className="max-w-xl space-y-1.5">
            <Label htmlFor={inputId}>Image reference</Label>
            <Input
              id={inputId}
              value={input}
              required
              maxLength={512}
              spellCheck={false}
              disabled={working || disabled}
              onChange={(event) => setInput(event.target.value)}
            />
            <p className="text-sm text-muted-foreground">
              Use an exact platform manifest from Docker Hub, GitHub Container
              Registry, or Quay.
            </p>
          </div>
          {error && (
            <p role="alert" className="text-sm text-destructive">
              {error}
            </p>
          )}
          <Button type="submit" size="sm" disabled={working || disabled}>
            {working ? "Submitting…" : "Fetch on this Host"}
          </Button>
        </form>
      )}
    </div>
  );
}
