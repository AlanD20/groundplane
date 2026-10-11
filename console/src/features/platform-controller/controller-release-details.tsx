import { DetailRow } from "@/components/common/detail-row";
import { workspaceSectionClassName } from "@/components/common/workspace-section";
import { SoftwareIdentity } from "@/features/image-delivery/software-identity";
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
        label="Staged Controller"
        value={
          <SoftwareIdentity
            version={release.controller_version}
            sourceBuild={release.controller_source?.kind === "source_ref"}
            sourceCommit={release.controller_source?.commit}
            digest={release.controller_sha256}
          />
        }
      />
      <DetailRow
        label="Compatibility"
        value={`Storage epoch ${release.storage_epoch} | Channel schema ${release.channel_schema}`}
      />
      <DetailRow
        label="Release"
        value={
          <SoftwareIdentity
            artifactReference={release.release}
            artifactLabel="Release manifest"
            unavailable="Release identity unavailable"
          />
        }
      />
      <DetailRow
        label="Staged Agent"
        value={
          <SoftwareIdentity
            artifactReference={release.agent_image}
            unavailable="Agent image identity unavailable"
          />
        }
      />
    </div>
  );
}
