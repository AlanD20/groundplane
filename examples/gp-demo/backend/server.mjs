import http from "node:http";
import os from "node:os";

const startedAt = new Date().toISOString();
let requests = 0;
const server = http.createServer(async (request, response) => {
  const url = new URL(request.url, "http://localhost");
  response.setHeader("Content-Type", "application/json");
  response.setHeader("Cache-Control", "no-store");
  const send = (status, data) => {
    response.writeHead(status);
    response.end(JSON.stringify(data));
  };
  if (request.method === "GET" && url.pathname === "/api/health") {
    return send(200, { status: "ok" });
  }
  if (request.method === "GET" && url.pathname === "/api/info") {
    requests += 1;
    return send(200, {
      message: "Hello from the backend running on Groundplane!",
      service: "api",
      version: "0.1.0",
      hostname: os.hostname(),
      startedAt,
      time: new Date().toISOString(),
      requests,
    });
  }
  if (request.method === "POST" && url.pathname === "/api/echo") {
    let bytes = 0;
    const chunks = [];
    for await (const chunk of request) {
      bytes += chunk.length;
      if (bytes > 4096) return send(413, { error: "Message is too large." });
      chunks.push(chunk);
    }
    try {
      const input = JSON.parse(Buffer.concat(chunks).toString());
      if (typeof input?.message !== "string" || input.message.length > 500) {
        return send(400, { error: "Send a message of up to 500 characters." });
      }
      return send(200, {
        message: input.message,
        handledBy: "api",
        time: new Date().toISOString(),
      });
    } catch {
      return send(400, { error: "Send a JSON object containing a message." });
    }
  }
  send(404, { error: "Endpoint not found." });
});
server.listen(3000, "0.0.0.0", () =>
  console.log("Demo API listening on port 3000"),
);
process.on("SIGTERM", () => server.close(() => process.exit(0)));
