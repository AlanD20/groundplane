import { useId } from "react";
import { Plus, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { newULID } from "@/lib/utils";
import type {
  FirstDeploymentAttachment,
  FirstDeploymentBackingOption,
} from "./first-deployment-attachment-model";

export function FirstDeploymentAttachments({
  attachments,
  options,
  onChange,
}: {
  attachments: FirstDeploymentAttachment[];
  options: FirstDeploymentBackingOption[];
  onChange: (attachments: FirstDeploymentAttachment[]) => void;
}) {
  const prefix = useId();

  function updateAttachment(
    id: string,
    change: Partial<FirstDeploymentAttachment>,
  ) {
    onChange(
      attachments.map((attachment) =>
        attachment.id === id ? { ...attachment, ...change } : attachment,
      ),
    );
  }

  return (
    <section className="space-y-3" aria-labelledby={`${prefix}-heading`}>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 id={`${prefix}-heading`} className="text-sm font-medium">
          Backing connections
        </h3>
        {options.length > 0 && (
          <Button
            type="button"
            size="sm"
            variant="outline"
            onClick={() =>
              onChange([
                ...attachments,
                { id: newULID(), name: "", backing: "" },
              ])
            }
          >
            <Plus aria-hidden className="size-4" /> Add connection
          </Button>
        )}
      </div>
      <p className="text-xs text-muted-foreground">
        {options.length > 0
          ? "Connect this Service to existing shared infrastructure and optionally provide its connection URL as an environment variable."
          : "There are no Backing Services available in this setup. You can add a connection from the Service after deployment."}
      </p>
      {attachments.map((attachment, index) => {
        const id = `${prefix}-${attachment.id}`;
        const option = options.find(
          ({ value }) => value === attachment.backing,
        );
        return (
          <div
            key={attachment.id}
            className="space-y-2 rounded-lg border border-border p-3"
          >
            <div className="grid grid-cols-[minmax(0,1fr)_auto] items-end gap-2 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto]">
              <div className="col-span-2 space-y-1.5 sm:col-span-1">
                <Label htmlFor={`${id}-name`}>Connection name</Label>
                <Input
                  id={`${id}-name`}
                  value={attachment.name}
                  placeholder="app-database"
                  maxLength={255}
                  onChange={(event) =>
                    updateAttachment(attachment.id, {
                      name: event.target.value,
                    })
                  }
                  required
                />
              </div>
              <div className="space-y-1.5">
                <Label htmlFor={`${id}-backing`}>Backing Service</Label>
                <Select
                  id={`${id}-backing`}
                  value={option?.value ?? null}
                  options={options}
                  onValueChange={(backing) => {
                    const selected = options.find(
                      ({ value }) => value === backing,
                    );
                    updateAttachment(attachment.id, {
                      backing,
                      environmentKey: selected?.urlFact
                        ? attachment.environmentKey
                        : "",
                    });
                  }}
                  disabled={options.length === 0}
                />
              </div>
              <Button
                type="button"
                size="icon"
                variant="ghost"
                aria-label={`Remove connection ${index + 1}`}
                onClick={() =>
                  onChange(attachments.filter(({ id }) => id !== attachment.id))
                }
              >
                <Trash2 aria-hidden className="size-4" />
              </Button>
            </div>
            {option?.description && (
              <p className="text-xs text-muted-foreground">
                {option.description}
              </p>
            )}
            {option?.urlFact && (
              <div className="space-y-1.5 sm:max-w-sm">
                <Label htmlFor={`${id}-variable`}>
                  Connection URL variable (optional)
                </Label>
                <Input
                  id={`${id}-variable`}
                  value={attachment.environmentKey ?? ""}
                  placeholder="SERVICE_URL"
                  onChange={(event) =>
                    updateAttachment(attachment.id, {
                      environmentKey: event.target.value,
                    })
                  }
                />
              </div>
            )}
          </div>
        );
      })}
    </section>
  );
}
