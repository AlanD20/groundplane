import { Disclosure } from "@/components/common/disclosure";
import { useRef } from "react";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { newULID } from "@/lib/utils";
import { removeImage, type HostImage } from "./api";
import { imageName, shortImageId } from "./image-presentation";

export function RemoveImageDialog({
  image,
  onClose,
  onSettled,
}: {
  image: HostImage;
  onClose: () => void;
  onSettled: () => Promise<void>;
}) {
  const key = useRef(newULID());
  return (
    <TaskRunnerDialog
      open
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
      variant="inline"
      title="Remove host image"
      type="remove"
      target={`${imageName(image)} · ${shortImageId(image.id)}`}
      workspace="platform"
      startLabel="Remove image"
      destructive
      description="Remove this image and all its local tags. Registry content and application data are unchanged."
      review={
        <div className="space-y-3 text-sm">
          <p>
            {image.tags.length} local tags will be removed with this image. You
            can fetch it again later if it remains available in its registry.
          </p>
          {image.tags.length > 0 && (
            <Disclosure
              summaryClassName="cursor-pointer text-muted-foreground"
              summary={<>Show affected tags</>}
            >
              <ul className="mt-2 space-y-1">
                {image.tags.map((tag) => (
                  <li key={tag} className="break-all font-mono text-xs">
                    {tag}
                  </li>
                ))}
              </ul>
            </Disclosure>
          )}
          <p>
            Images needed by containers, rollback or recovery cannot be removed.
          </p>
        </div>
      }
      onDispatch={async () =>
        (await removeImage(image.id, key.current)).task_id
      }
      onSettled={onSettled}
    />
  );
}
