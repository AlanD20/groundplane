import { Badge } from "@/components/ui/badge";
import { useStore } from "@/lib/store";

export function EmptySecretValueBadge({ empty }: { empty?: boolean }) {
  const { showEmptySecretBadges } = useStore();
  return empty && showEmptySecretBadges ? (
    <Badge variant="warning">empty value</Badge>
  ) : null;
}
