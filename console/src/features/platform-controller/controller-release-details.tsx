import { CompactReference } from "@/components/common/compact-reference";
import { DetailRow } from "@/components/common/detail-row";
import { workspaceSectionClassName } from "@/components/common/workspace-section";
import type { HostInfo } from "../platform-host/host-model";

export type ControllerRelease = NonNullable<
  HostInfo["controller"]["update"]["candidate"]
>;

export function ControllerReleaseDetails({
  release,
}: {
  release: ControllerRelease;
}) {
  return (
    <div className={workspaceSectionClassName()}>
      <DetailRow
        label="Controller version"
        value={release.controller_version}
      />
      <DetailRow
        label="Compatibility"
        value={`Storage epoch ${release.storage_epoch} | Channel schema ${release.channel_schema}`}
      />
      <DetailRow
        label="Release"
        value={
          <CompactReference
            value={release.release}
            label="Release manifest"
            hideLabel
          />
        }
      />
      <DetailRow
        label="Binary"
        value={
          <CompactReference
            value={release.controller_sha256}
            label="Controller binary"
            hideLabel
          />
        }
      />
      <DetailRow
        label="Agent image"
        value={
          <CompactReference
            value={release.agent_image}
            label="Agent image"
            hideLabel
          />
        }
      />
    </div>
  );
}
