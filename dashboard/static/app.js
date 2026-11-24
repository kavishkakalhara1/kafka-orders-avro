const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];
const money = (value) =>
  Number(value || 0).toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
const escapeHtml = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (character) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        character
      ],
  );
let state = null;
let selectedTopic = "all";
let view = "overview";
let frozenRecords = null;
let busy = false;
let toastTimer;
const icons = () => window.lucide?.createIcons();

function toast(message, error = false) {
  const element = $("#toast");
  element.textContent = message;
  element.className = error ? "toast error" : "toast";
  element.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => {
    element.hidden = true;
  }, 4500);
}

async function command(path, body) {
  const response = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const result = await response.json();
  if (!response.ok) throw new Error(result.error || "Command failed");
  return result;
}

async function action(task) {
  if (busy) return;
  busy = true;
  renderControls();
  try {
    await task();
  } catch (error) {
    toast(error.message, true);
  } finally {
    busy = false;
    await refresh();
  }
}

function setView(next) {
  view = next;
  const names = {
    overview: ["Overview", "Pipeline overview"],
    records: ["Record explorer", "Record explorer"],
    schema: ["Schema registry", "Schema registry"],
    events: ["Event log", "Application events"],
  };
  $("#breadcrumb").textContent = names[next][0];
  $("#page-title").textContent = names[next][1];
  $$(".nav-item").forEach((button) =>
    button.classList.toggle("active", button.dataset.view === next),
  );
  $("#overview-view").hidden = next !== "overview";
  $("#records-section").hidden = !["overview", "records"].includes(next);
  $("#schema-view").hidden = next !== "schema";
  $("#events-view").hidden = next !== "events";
  if (next === "events") renderEvents();
  icons();
}

function renderControls() {
  const ready = state?.ready && !busy;
  const workers = state?.workers || {};
  const bothRunning = workers.consumer?.running && workers.retry?.running;
  $("#start-workers").disabled = !ready || bothRunning;
  $("#start-workers").innerHTML = bothRunning
    ? '<i data-lucide="check"></i> Workers running'
    : '<i data-lucide="play"></i> Start workers';
  $("#produce-button").disabled = !ready || workers.producer?.running;
  $("#produce-button").innerHTML = workers.producer?.running
    ? '<i data-lucide="radio"></i> Producing...'
    : '<i data-lucide="send"></i> Produce orders';
  $("#stop-producer").disabled = busy || !workers.producer?.running;
  $$(".worker-toggle").forEach((button) => {
    const running = workers[button.dataset.worker]?.running;
    button.disabled = busy || (!running && !state?.ready);
    button.title = `${running ? "Stop" : "Start"} ${button.dataset.worker} worker`;
    button.setAttribute("aria-label", button.title);
    button.innerHTML = `<i data-lucide="${running ? "pause" : "play"}"></i>`;
  });
  icons();
}

function render() {
  const counts = state.counts;
  $("#connection-dot").className = `status-dot ${state.ready ? "" : "offline"}`;
  $("#connection-label").textContent = state.ready
    ? "Cluster connected"
    : "Service unavailable";
  $("#error-banner").hidden = !state.health.error;
  $("#error-banner").textContent = state.health.error || "";
  for (const name of ["producer", "consumer", "retry"]) {
    const worker = state.workers[name];
    const label = $(`#${name}-status`);
    label.textContent = worker.running
      ? "Running"
      : worker.exit_code
        ? "Failed"
        : "Stopped";
    label.className = `node-status ${worker.running ? "running" : ""}`;
  }
  $("#broker-status").textContent = state.health.kafka ? "Healthy" : "Offline";
  $("#broker-status").className =
    `node-status ${state.health.kafka ? "running" : ""}`;
  $("#orders-topic").textContent = state.topics.orders;
  $("#namespace-label").textContent = state.topics.orders;
  $("#producer-count").textContent = `${counts.order_produced || 0} sent`;
  $("#retry-policy").textContent =
    `${state.config.max_retries} retries / jitter`;
  $("#metric-produced").textContent = counts.order_produced || 0;
  $("#metric-processed").textContent = state.averages.global.count;
  $("#metric-retries").textContent = counts.retry_scheduled || 0;
  $("#metric-dlq").textContent = counts.dead_lettered || 0;
  $("#retry-note").textContent = `${counts.retry_released || 0} released`;
  $("#global-average").textContent = money(state.averages.global.average);
  $("#sample-count").textContent =
    `${state.averages.global.count} valid orders`;
  const products = Object.entries(state.averages.products);
  $("#product-averages").innerHTML = products.length
    ? products
        .map(
          ([product, stats]) =>
            `<div class="product-stat"><span>${escapeHtml(product)} <small>(${stats.count})</small></span><strong>$${money(stats.average)}</strong></div>`,
        )
        .join("")
    : '<div class="empty-products">No product aggregates yet</div>';
  renderChart();
  renderRecords();
  if (view === "events") renderEvents();
  $("#schema-subject").textContent = `${state.topics.orders}-value`;
  $("#schema-badge").textContent = state.schema_id
    ? `Registered / ID ${state.schema_id}`
    : "Awaiting registry";
  $("#schema-json").textContent = JSON.stringify(state.schema, null, 2);
  $("#offset-group").textContent = state.consumer_group || "";
  $("#offset-rows").innerHTML = (state.offsets || [])
    .map(
      (offset) =>
        `<tr><td>${offset.partition}</td><td>${offset.committed ?? "Not committed"}</td><td>${offset.end}</td><td class="${offset.lag ? "amber" : ""}">${offset.lag}</td></tr>`,
    )
    .join("");
  $("#offset-updated").textContent = state.offsets_updated_at
    ? `Updated ${new Date(state.offsets_updated_at).toLocaleTimeString("en-GB")}`
    : "Awaiting broker offsets";
  $("#wire-schema-id").textContent = state.schema_id
    ? `ID ${state.schema_id}`
    : "Schema ID";
  $("#footer-policy").textContent =
    `${state.config.max_retries} retries max · ${state.config.backoff_base}s base / ${state.config.backoff_max}s cap · Avro`;
  const errors = state.events.filter((event) => event.level === "ERROR").length;
  $("#error-count").hidden = !errors;
  $("#error-count").textContent = errors;
  renderControls();
}

function renderChart() {
  const values = state.trend;
  $("#chart-empty").hidden = values.length > 0;
  if (!values.length) {
    $("#chart-line").setAttribute("d", "");
    $("#chart-area").setAttribute("d", "");
    return;
  }
  const points = values.length === 1 ? [values[0], values[0]] : values;
  const minimum = Math.min(...points) * 0.85;
  const maximum = Math.max(...points) * 1.15 || 1;
  const path = points
    .map(
      (price, index) =>
        `${index ? "L" : "M"}${(index * 600) / (points.length - 1)},${88 - ((price - minimum) / (maximum - minimum)) * 76}`,
    )
    .join(" ");
  $("#chart-line").setAttribute("d", path);
  $("#chart-area").setAttribute("d", `${path} L600,100 L0,100 Z`);
}

function topicKind(record) {
  return (
    Object.keys(state.topics).find(
      (key) => state.topics[key] === record.topic,
    ) || "orders"
  );
}

function currentRecords() {
  return frozenRecords || state?.records || [];
}

function filteredRecords() {
  const query = $("#search").value.trim().toLowerCase();
  return currentRecords().filter(
    (record) =>
      (selectedTopic === "all" || topicKind(record) === selectedTopic) &&
      (!query || JSON.stringify(record).toLowerCase().includes(query)),
  );
}

function recordStatus(record, kind) {
  if (kind === "dlq") return "Dead-lettered";
  if (kind === "retry") {
    const returned = currentRecords().some(
      (item) =>
        item.topic === state.topics.orders &&
        item.key === record.key &&
        item.headers["retry-count"] === record.headers["retry-count"],
    );
    if (returned) return "Released";
    const seconds = Math.ceil(
      Number(record.headers["retry-at"]) - Date.now() / 1000,
    );
    return seconds > 0 ? `Due in ${seconds}s` : "Due / awaiting worker";
  }
  return record.headers["retry-count"] ? "Retry delivery" : "Received";
}

function renderRecords() {
  if (!state) return;
  const records = filteredRecords();
  $("#record-total").textContent = currentRecords().length;
  $("#tab-all").textContent = currentRecords().length;
  for (const topic of ["orders", "retry", "dlq"])
    $("#tab-" + topic).textContent = currentRecords().filter(
      (record) => topicKind(record) === topic,
    ).length;
  $("#record-rows").innerHTML = records
    .map((record) => {
      const kind = topicKind(record);
      const payload = record.payload;
      const validObject = payload && typeof payload === "object";
      const product = validObject
        ? payload.product || "Missing product"
        : "Malformed payload";
      const price =
        validObject && typeof payload.price === "number"
          ? "$" + money(payload.price)
          : "--";
      return `<tr><td title="${escapeHtml(record.key)}">${escapeHtml(record.key || "(no key)")}</td><td><span class="badge ${kind}">${kind === "dlq" ? "dead letter" : kind}</span></td><td title="${escapeHtml(record.headers["failure-reason"] || product)}">${escapeHtml(product)}</td><td>${price}</td><td class="offset">${Number(record.headers["retry-count"] || 0) + 1}</td><td class="offset">${record.partition} : ${record.offset}</td><td><span class="state-label ${kind}">${escapeHtml(recordStatus(record, kind))}</span></td><td><button class="icon-button inspect-button" data-id="${escapeHtml(record.id)}" title="Inspect record ${escapeHtml(record.key)}" aria-label="Inspect record ${escapeHtml(record.key)}"><i data-lucide="arrow-up-right"></i></button></td></tr>`;
    })
    .join("");
  $("#records-empty").hidden = records.length > 0;
  $("#empty-message").textContent = $("#search").value
    ? "No records match this search"
    : "No records in this topic";
  $("#records-caption").textContent =
    `${records.length} records${frozenRecords ? " / display paused" : ""}`;
  icons();
}

function renderEvents() {
  if (!state) return;
  const events = state.events.filter(
    (event) => !$("#errors-only").checked || event.level === "ERROR",
  );
  $("#event-list").innerHTML = events.length
    ? events
        .map((event) => {
          const details = { ...event };
          for (const key of [
            "id",
            "event",
            "timestamp",
            "level",
            "logger",
            "source",
          ])
            delete details[key];
          return `<div class="event ${event.level === "ERROR" ? "error" : ""}"><time>${event.timestamp ? new Date(event.timestamp).toLocaleTimeString("en-GB") : "--:--:--"}</time><span class="event-source">${escapeHtml(event.source)}</span><div><strong>${escapeHtml(event.event)}</strong><p>${escapeHtml(JSON.stringify(details))}</p></div></div>`;
        })
        .join("")
    : '<div class="empty-state"><h3>No events to display</h3></div>';
}

function inspect(id) {
  const record = currentRecords().find((entry) => entry.id === id);
  if (!record) return;
  $("#detail-title").textContent = `Order ${record.key || "(no key)"}`;
  $("#detail-location").textContent =
    `${record.topic} / partition ${record.partition} / offset ${record.offset}`;
  $("#detail-payload").textContent = JSON.stringify(record.payload, null, 2);
  $("#detail-base64").textContent = record.base64;
  $("#detail-headers").innerHTML =
    Object.entries(record.headers)
      .map(
        ([name, value]) =>
          `<dt>${escapeHtml(name)}</dt><dd>${escapeHtml(value)}</dd>`,
      )
      .join("") || "<dt>Headers</dt><dd>None</dd>";
  $("#record-dialog").showModal();
}

async function refresh() {
  try {
    const response = await fetch("/api/state", {
      signal: AbortSignal.timeout(5000),
    });
    if (!response.ok) throw new Error("Dashboard unavailable");
    state = await response.json();
    render();
  } catch (error) {
    if (state) state.ready = false;
    $("#connection-dot").className = "status-dot offline";
    $("#connection-label").textContent = "Dashboard disconnected";
    $("#error-banner").hidden = false;
    $("#error-banner").textContent =
      "Connection to the dashboard was lost. Reconnecting...";
    renderControls();
  }
  $("#clock").textContent = new Date().toLocaleTimeString("en-GB");
}

$$(".nav-item").forEach((button) =>
  button.addEventListener("click", () => setView(button.dataset.view)),
);
$$(".tab").forEach((button) =>
  button.addEventListener("click", () => {
    selectedTopic = button.dataset.topic;
    $$(".tab").forEach((tab) => {
      tab.classList.toggle("active", tab === button);
      tab.setAttribute("aria-selected", String(tab === button));
    });
    renderRecords();
  }),
);
$("#start-workers").addEventListener("click", () =>
  action(async () => {
    for (const worker of ["consumer", "retry"])
      if (!state.workers[worker].running)
        await command("/api/control", { worker, action: "start" });
    toast("Consumer and retry worker started");
  }),
);
$$(".worker-toggle").forEach((button) =>
  button.addEventListener("click", () =>
    action(async () => {
      const worker = button.dataset.worker;
      const operation = state.workers[worker].running ? "stop" : "start";
      await command("/api/control", { worker, action: operation });
      toast(
        `${worker === "consumer" ? "Consumer" : "Retry worker"} ${operation === "start" ? "started" : "stopped"}`,
      );
    }),
  ),
);
$("#stop-producer").addEventListener("click", () =>
  action(async () => {
    await command("/api/control", { worker: "producer", action: "stop" });
    toast("Producer stopped");
  }),
);
$("#produce-form").addEventListener("submit", (event) => {
  event.preventDefault();
  action(async () => {
    await command("/api/produce", {
      mode: $("#mode").value,
      count: $("#continuous").checked ? 0 : Number($("#count").value),
      rate: Number($("#rate").value),
      failures: Number($("#failures").value),
    });
    toast("Producer started");
  });
});
$("#mode").addEventListener("change", () => {
  $("#failures-field").hidden = $("#mode").value !== "transient";
});
$("#continuous").addEventListener("change", () => {
  $("#count").disabled = $("#continuous").checked;
});
$("#search").addEventListener("input", renderRecords);
$("#errors-only").addEventListener("change", renderEvents);
$("#record-rows").addEventListener("click", (event) => {
  const button = event.target.closest(".inspect-button");
  if (button) inspect(button.dataset.id);
});
$("#pause-feed").addEventListener("click", () => {
  frozenRecords = frozenRecords ? null : [...(state?.records || [])];
  $("#pause-feed").innerHTML =
    `<i data-lucide="${frozenRecords ? "play" : "pause"}"></i>`;
  $("#pause-feed").title = frozenRecords
    ? "Resume record display"
    : "Pause record display";
  $("#pause-feed").setAttribute("aria-label", $("#pause-feed").title);
  $("#feed-label").textContent = frozenRecords ? "Display paused" : "Live feed";
  renderRecords();
});
$("#export-records").addEventListener("click", () => {
  const query = new URLSearchParams({
    topic: selectedTopic,
    search: $("#search").value.trim(),
  });
  const link = document.createElement("a");
  link.href = `/api/export?${query}`;
  link.download = `orderflow-${selectedTopic}.json`;
  document.body.append(link);
  link.click();
  link.remove();
  toast("Download requested");
});
$("#close-dialog").addEventListener("click", () => $("#record-dialog").close());
$("#record-dialog").addEventListener("click", (event) => {
  if (event.target === $("#record-dialog")) {
    const rect = event.target.getBoundingClientRect();
    if (
      event.clientX < rect.left ||
      event.clientX > rect.right ||
      event.clientY < rect.top ||
      event.clientY > rect.bottom
    )
      event.target.close();
  }
});
icons();
renderControls();
async function poll() {
  await refresh();
  setTimeout(poll, 1000);
}
poll();
