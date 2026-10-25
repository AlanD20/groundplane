async function callAPI(path, options) {
  const response = await fetch(path, options);
  const body = await response.json();
  if (!response.ok) throw new Error(body.error || `HTTP ${response.status}`);
  return body;
}
const info = document.querySelector("#info");
const status = document.querySelector("#status");
const refresh = document.querySelector("#refresh");
async function loadInfo() {
  refresh.disabled = true;
  status.textContent = "Connecting…";
  try {
    info.textContent = JSON.stringify(await callAPI("/api/info"), null, 2);
    status.textContent = "API connected";
    status.dataset.state = "ok";
  } catch (error) {
    status.textContent = "API unreachable";
    status.dataset.state = "error";
    info.textContent = error.message;
  } finally {
    refresh.disabled = false;
  }
}
refresh.addEventListener("click", loadInfo);
document
  .querySelector("#echo-form")
  .addEventListener("submit", async (event) => {
    event.preventDefault();
    const button = event.currentTarget.querySelector("button");
    const output = document.querySelector("#echo");
    button.disabled = true;
    try {
      output.textContent = JSON.stringify(
        await callAPI("/api/echo", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            message: document.querySelector("#message").value,
          }),
        }),
        null,
        2,
      );
    } catch (error) {
      output.textContent = error.message;
    } finally {
      button.disabled = false;
    }
  });
loadInfo();
