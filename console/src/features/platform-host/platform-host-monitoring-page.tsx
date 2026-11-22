import { PageHeader } from "@/components/common/page-header";
import { ScopedMonitoring } from "@/features/monitoring/scoped-monitoring";
import { useStore } from "@/lib/store";
import { Activity } from "lucide-react";

export default function PlatformHostMonitoringPage() {
  const { host } = useStore();
  return (
    <div className="space-y-6">
      <PageHeader
        title="Monitoring"
        eyebrow="Platform · Host"
        icon={<Activity />}
      />
      <ScopedMonitoring
        scope="host"
        id="local"
        name={host?.hostname ?? "Host"}
      />
    </div>
  );
}
