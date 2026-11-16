import { TaskLink } from "@/components/common/task-link";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { BlueprintReview } from "./blueprint-review";
import type { BlueprintValidationResponse } from "./api";
import { useId } from "react";

export function validExpectedBlueprintRevision(value: string): boolean {
  return value === "0" || /^task_[0-9A-HJKMNP-TV-Z]{26}$/.test(value);
}

export function BlueprintTaskReview({
  validation,
  after,
  revision,
  onRevisionChange,
}: {
  validation: BlueprintValidationResponse | null;
  after: string[];
  revision: string;
  onRevisionChange: (revision: string) => void;
}) {
  const id = useId();
  return (
    <div className="space-y-4">
      {validation ? (
        <BlueprintReview validation={validation} />
      ) : (
        <p className="text-sm text-muted-foreground">
          Mutable resources and the execution plan will be validated after
          prerequisite Tasks succeed and fully settle.
        </p>
      )}
      {after.length > 0 && (
        <>
          <div className="space-y-2">
            <Label htmlFor={id}>Expected Blueprint revision</Label>
            <Input
              id={id}
              value={revision}
              onChange={(event) => onRevisionChange(event.target.value)}
              aria-invalid={!validExpectedBlueprintRevision(revision)}
            />
            <p className="text-xs text-muted-foreground">
              If a prerequisite applies this Environment, use its Task ID. The
              revision must match when this request prepares; it is not rebased
              automatically.
            </p>
            <Select
              aria-label="Use prerequisite as expected Blueprint revision"
              value={after.includes(revision) ? revision : null}
              onValueChange={onRevisionChange}
              placeholder="Use prerequisite Task ID…"
              options={after.map((task) => ({ value: task, label: task }))}
            />
          </div>
          <ul className="space-y-1">
            {after.map((task) => (
              <li key={task}>
                <TaskLink taskId={task}>{task}</TaskLink>
              </li>
            ))}
          </ul>
        </>
      )}
    </div>
  );
}
