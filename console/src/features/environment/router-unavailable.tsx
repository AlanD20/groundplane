import { Router as RouterIcon } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export function EnvironmentRouterUnavailable() {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <RouterIcon className="size-4 text-muted-foreground" /> Proxy & tunnel
        </CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-xs text-muted-foreground">
          Proxy and tunnel configuration is unavailable.
        </p>
      </CardContent>
    </Card>
  );
}
