import { useId, useState } from "react";
import { SquareTerminal } from "lucide-react";
import { ArgumentList } from "@/components/common/argument-list";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import type { ServiceContainerObservation } from "@/features/service/service-observation";
import { useHostTerminal } from "./terminal-provider";

export function ContainerShellButton({
  serviceId,
  serviceName,
  containers,
}: {
  serviceId: string;
  serviceName: string;
  containers: ServiceContainerObservation[];
}) {
  const terminal = useHostTerminal();
  const id = useId();
  const running = containers.filter(
    (container) => container.state === "running",
  );
  const [open, setOpen] = useState(false);
  const [containerId, setContainerId] = useState("");
  const [user, setUser] = useState("root");
  const [directory, setDirectory] = useState("");
  const [command, setCommand] = useState(["/bin/sh"]);
  const [error, setError] = useState<string | null>(null);
  const selected = running.find((item) => item.id === containerId);
  const canSubmit =
    !!selected && !!command[0]?.trim() && !!user.trim() && !terminal.opening;

  async function launch() {
    if (!canSubmit || !selected) return;
    setError(null);
    try {
      await terminal.openContainer(
        serviceId,
        serviceName + " · " + selected.name,
        {
          container_id: selected.id,
          user: user.trim(),
          command,
          working_directory: directory,
          columns: 100,
          rows: 28,
        },
      );
      setOpen(false);
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : "Unable to open container shell",
      );
    }
  }

  return (
    <>
      <Button
        variant="outline"
        size="sm"
        disabled={!running.length}
        title={
          running.length
            ? "Open a shell in a running container"
            : "No running containers available"
        }
        onClick={() => {
          setContainerId(running[0]?.id ?? "");
          setError(null);
          setOpen(true);
        }}
      >
        <SquareTerminal className="size-3.5" /> Open shell
      </Button>
      <Dialog
        open={open}
        onOpenChange={(value) => {
          if (!terminal.opening) setOpen(value);
        }}
      >
        <DialogContent className="max-w-xl">
          <form
            className="min-w-0 space-y-5"
            onSubmit={(event) => {
              event.preventDefault();
              void launch();
            }}
          >
            <DialogHeader>
              <DialogTitle>Open shell · {serviceName}</DialogTitle>
              <DialogDescription>
                The session stays attached to the selected container. It ends if
                that container is replaced.
              </DialogDescription>
            </DialogHeader>
            <fieldset disabled={terminal.opening} className="min-w-0 space-y-4">
              <div className="space-y-1.5">
                <Label htmlFor={id + "-container"}>Container</Label>
                <Select
                  id={id + "-container"}
                  searchable
                  value={containerId}
                  options={running.map((item) => ({
                    value: item.id,
                    label:
                      item.name +
                      " · replica " +
                      item.replica +
                      " · " +
                      item.id.slice(0, 12),
                  }))}
                  onValueChange={setContainerId}
                />
              </div>
              <div className="grid gap-4 sm:grid-cols-2">
                <div className="space-y-1.5">
                  <Label htmlFor={id + "-user"}>User</Label>
                  <Input
                    id={id + "-user"}
                    value={user}
                    placeholder="root or UID:GID"
                    onChange={(event) => setUser(event.target.value)}
                  />
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor={id + "-directory"}>Working directory</Label>
                  <Input
                    id={id + "-directory"}
                    value={directory}
                    placeholder="Container default"
                    onChange={(event) => setDirectory(event.target.value)}
                  />
                </div>
              </div>
              <ArgumentList
                id={id + "-command"}
                label="Command"
                values={command}
                onChange={setCommand}
                description="First item is the executable; each following item is one literal argument."
                emptyLabel="Choose an executable"
                resetLabel="Clear command"
              />
            </fieldset>
            {error && (
              <p role="alert" className="text-sm text-destructive">
                {error}
              </p>
            )}
            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                disabled={terminal.opening}
                onClick={() => setOpen(false)}
              >
                Cancel
              </Button>
              <Button type="submit" disabled={!canSubmit}>
                {terminal.opening ? "Opening…" : "Open shell"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </>
  );
}
