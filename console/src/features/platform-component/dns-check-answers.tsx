import { useState } from "react";
import {
  CollectionToolbar,
  TablePagination,
  TableSortHead,
  useTableView,
} from "@/components/common/table-controls";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { DNSCheckAnswer } from "./dns-check-api";

const outcomes: Record<DNSCheckAnswer["outcome"], string> = {
  resolved: "Resolved",
  name_not_found: "Name not found (NXDOMAIN)",
  no_answer: "No matching answer",
  refused: "Query refused",
  server_error: "DNS server error",
  timeout: "Timed out",
  query_failed: "Query failed",
  canceled: "Canceled",
};

export function DNSCheckAnswers({
  answers,
  taskId,
}: {
  answers: DNSCheckAnswer[];
  taskId: string;
}) {
  const [query, setQuery] = useState("");
  const search = query.toLowerCase();
  const items = answers.filter((answer) =>
    [
      answer.name,
      answer.domain,
      answer.server,
      outcomes[answer.outcome],
      ...(answer.answers ?? []).map((sample) => sample.value),
    ].some((value) => value.toLowerCase().includes(search)),
  );
  const table = useTableView(
    items,
    {
      query: (answer) => answer.name,
      server: (answer) => answer.server,
      outcome: (answer) => outcomes[answer.outcome],
      time: (answer) => answer.elapsed_ms,
    },
    "query",
    "asc",
    `${taskId}/${query}`,
  );
  return (
    <div className="space-y-3">
      <CollectionToolbar
        query={query}
        onQueryChange={setQuery}
        label="DNS answers"
      />
      <div className="overflow-x-auto rounded-lg border border-border">
        <Table className="min-w-[600px] text-xs">
          <TableHeader>
            <TableRow>
              <TableSortHead sort={table} field="query">
                Query
              </TableSortHead>
              <TableSortHead sort={table} field="server">
                Server
              </TableSortHead>
              <TableSortHead sort={table} field="outcome">
                Outcome / answer
              </TableSortHead>
              <TableSortHead sort={table} field="time">
                Time
              </TableSortHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {table.rows.map((answer) => (
              <TableRow
                key={`${answer.domain}/${answer.name}/${answer.server}/${answer.through_resolver}`}
              >
                <TableCell className="max-w-52 break-all">
                  <span className="font-medium">{answer.name}</span>
                  <span className="mt-1 block text-muted-foreground">
                    {answer.type} ·{" "}
                    {answer.domain === "." ? "Default upstream" : answer.domain}
                  </span>
                </TableCell>
                <TableCell className="max-w-40 break-all">
                  {answer.server}
                  <span className="mt-1 block text-muted-foreground">
                    {answer.through_resolver
                      ? "Via CoreDNS"
                      : "Direct upstream"}
                    {answer.used_tcp ? " · TCP" : ""}
                  </span>
                </TableCell>
                <TableCell className="max-w-60 break-all space-y-1">
                  <Badge
                    variant={
                      answer.outcome === "resolved"
                        ? "success"
                        : ["no_answer", "name_not_found"].includes(
                              answer.outcome,
                            )
                          ? "warning"
                          : "danger"
                    }
                  >
                    {outcomes[answer.outcome]}
                  </Badge>
                  {(answer.answers ?? []).map((sample, index) => (
                    <div key={index} title={`${sample.name} ${sample.type}`}>
                      {sample.value}
                    </div>
                  ))}
                  {answer.total_answers > (answer.answers?.length ?? 0) && (
                    <p className="text-muted-foreground">
                      Showing {answer.answers?.length ?? 0} of{" "}
                      {answer.total_answers} answers
                    </p>
                  )}
                  {answer.rcode !== undefined &&
                    answer.outcome !== "resolved" && (
                      <p className="text-muted-foreground">
                        DNS response code {answer.rcode}
                      </p>
                    )}
                </TableCell>
                <TableCell className="whitespace-nowrap tabular-nums">
                  {answer.elapsed_ms} ms
                </TableCell>
              </TableRow>
            ))}
            {!table.total && (
              <TableRow>
                <TableCell colSpan={4} className="text-muted-foreground">
                  No answers match this search.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
      <TablePagination table={table} label="Answers" />
    </div>
  );
}
