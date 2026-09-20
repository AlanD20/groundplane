import { Router as RouterIcon } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export function EnvironmentRouterUnavailable() {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <RouterIcon className="size-4 text-muted-foreground" /> Router
        </CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-xs text-muted-foreground">
          Router components are not part of this Environment projection. The
          Controller has not published an authoritative Caddy or Cloudflare
          Tunnel record, so no local controls are shown.
        </p>
      </CardContent>
    </Card>
  );
}
