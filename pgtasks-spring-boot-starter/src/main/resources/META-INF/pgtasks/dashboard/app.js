/*
 * pgtasks dashboard: a page over the tasks Actuator endpoint, with no build step and no
 * dependencies. Every text from the server goes into the page as text (textContent or text
 * nodes), never as HTML. Values the endpoint hides (show-values) stay hidden: the page only shows
 * what the endpoint returns.
 */
"use strict";

// Every call the page makes to the endpoint. Tests read this table: keep one route per line.
// routes:begin
const ROUTES = {
  overview: ["GET", ""],
  topic: ["GET", "/topics/{topic}"],
  deadLetters: ["GET", "/topics/{topic}/dead-letters"],
  revive: ["POST", "/topics/{topic}/revive"],
  discard: ["POST", "/topics/{topic}/discard"],
  tasks: ["GET", "/topics/{topic}/tasks"],
  release: ["POST", "/topics/{topic}/release"],
  purge: ["POST", "/topics/{topic}/purge"],
  deleteTopic: ["DELETE", "/topics/{topic}"],
  task: ["GET", "/tasks/{id}"],
};
// routes:end

const HIDDEN = "******";
const REFRESH_MS = 10000;
const OUTCOMES = ["succeeded", "filtered", "failed", "redundant", "recreated"];
const STATES = ["scheduled", "retrying", "ready", "active", "suspended", "failed"];

const state = {
  config: null,
  deadLetters: {}, // per topic: { cursors: [0], selected: Set }
  // per topic: { filter: null (no search yet) or the search's fields, cursors: [0], page, stale }. A search can
  // read a topic's whole backlog, so it runs when asked for, on a page change and after a change, not on every refresh.
  searches: {},
  timer: null,
  seq: 0, // the latest render wins
};

// ---------------------------------------------------------------------------------------------
// DOM helpers: text only.

function h(tag, props, ...children) {
  const el = document.createElement(tag);
  for (const [key, value] of Object.entries(props || {})) {
    if (value === null || value === undefined || value === false) {
      continue;
    }
    if (key === "class") {
      el.className = value;
    } else if (key.startsWith("on") && typeof value === "function") {
      el.addEventListener(key.slice(2), value);
    } else if (key === "checked" || key === "disabled" || key === "value" || key === "selected" || key === "required") {
      el[key] = value;
    } else if (key === "href") {
      el.setAttribute("href", safeHref(value));
    } else {
      el.setAttribute(key, value === true ? "" : String(value));
    }
  }
  add(el, children);
  return el;
}

function add(el, children) {
  for (const child of children.flat(Infinity)) {
    if (child === null || child === undefined || child === false) {
      continue;
    }
    el.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return el;
}

/** Links are in-page routes only. */
function safeHref(value) {
  return typeof value === "string" && value.startsWith("#/") ? value : "#/";
}

function link(route, ...text) {
  return h("a", { href: route }, ...text);
}

function topicLink(name) {
  return link("#/topics/" + encodeURIComponent(name), name);
}

function taskLink(id) {
  return id === null || id === undefined ? "—" : link("#/tasks/" + encodeURIComponent(id), "#" + id);
}

function chip(text, kind, title) {
  return h("span", { class: "chip" + (kind ? " " + kind : ""), title }, text);
}

function panel(title, options, ...body) {
  const head = h("div", { class: "panel-head" }, h("h2", null, title), options && options.meta, options && options.actions
    ? h("div", { class: "actions" }, options.actions) : null);
  return h("section", { class: "panel" }, head, body, options && options.note ? h("div", { class: "panel-note" }, options.note) : null);
}

function table(columns, rows, empty) {
  if (!rows.length) {
    return h("div", { class: "empty" }, empty || "Nothing here.");
  }
  return h("div", { class: "table-wrap" }, h("table", null,
    h("thead", null, h("tr", null, columns.map(column => h("th", { class: column.num ? "num" : null, scope: "col" }, column.label)))),
    h("tbody", null, rows.map(row => h("tr", null, columns.map(column => {
      const value = column.value(row);
      if (value instanceof Node && value.nodeName === "TD") {
        return value;
      }
      return h("td", { class: column.num ? "num" : column.class || null }, value === null || value === undefined ? "—" : value);
    }))))));
}

function count(value, hot) {
  if (value === null || value === undefined) {
    return h("td", { class: "num zero" }, "—");
  }
  return h("td", { class: "num" + (value === 0 ? " zero" : hot ? " hot" : "") }, number(value));
}

function number(value) {
  return typeof value === "number" ? value.toLocaleString() : String(value);
}

/** A value the endpoint may hide: shown as a marker, never as the asterisks. */
function value(text) {
  if (text === HIDDEN) {
    return h("span", { class: "hidden-value", title: "Hidden by management.endpoint.tasks.show-values" }, "hidden");
  }
  return text === null || text === undefined ? "—" : text;
}

// ---------------------------------------------------------------------------------------------
// Time

function duration(ms) {
  const s = Math.round(Math.abs(ms) / 1000);
  if (s < 60) return s + " s";
  const m = Math.round(s / 60);
  if (m < 60) return m + " min";
  const hours = Math.round(m / 6) / 10;
  if (hours < 48) return hours + " h";
  return Math.round(hours / 2.4) / 10 + " d";
}

function age(iso) {
  return iso ? Date.now() - Date.parse(iso) : null;
}

function time(iso) {
  if (!iso) {
    return "—";
  }
  const ms = age(iso);
  const text = Math.abs(ms) < 1000 ? "now" : ms > 0 ? duration(ms) + " ago" : "in " + duration(ms);
  return h("time", { datetime: iso, title: new Date(iso).toLocaleString() }, text);
}

function since(iso) {
  return iso ? h("time", { datetime: iso, title: "since " + new Date(iso).toLocaleString() }, duration(age(iso))) : "—";
}

// ---------------------------------------------------------------------------------------------
// The endpoint

class ApiError extends Error {
  constructor(status, message, body) {
    super(message);
    this.status = status;
    this.body = body;
  }
}

async function loadConfig() {
  const response = await fetch("config.json", { cache: "no-store", credentials: "same-origin", headers: { Accept: "application/json" } });
  if (!response.ok) {
    throw new ApiError(response.status, "The dashboard's configuration couldn't be loaded (" + response.status + ")");
  }
  state.config = await response.json();
}

function csrfHeaders() {
  const config = state.config || {};
  if (config.csrf && config.csrf.header && config.csrf.token) {
    return { [config.csrf.header]: config.csrf.token };
  }
  // Spring Security's CookieCsrfTokenRepository, when the page isn't behind its CSRF filter.
  const cookie = document.cookie.split("; ").find(entry => entry.startsWith("XSRF-TOKEN="));
  return cookie ? { "X-XSRF-TOKEN": decodeURIComponent(cookie.slice("XSRF-TOKEN=".length)) } : {};
}

function url(name, params, query) {
  const [, template] = ROUTES[name];
  const path = template.replace(/\{(\w+)\}/g, (_, key) => encodeURIComponent(String(params[key])));
  const search = new URLSearchParams();
  for (const [key, val] of Object.entries(query || {})) {
    if (val !== null && val !== undefined && val !== "") {
      search.set(key, String(val));
    }
  }
  const text = search.toString();
  return state.config.endpoint + path + (text ? "?" + text : "");
}

/** Calls one of the ROUTES. Writes send the CSRF token, and retry once with a fresh one on 403. */
async function api(name, params, options) {
  if (!state.config || !state.config.endpoint) {
    throw new ApiError(0, "The tasks endpoint isn't exposed");
  }
  const [method] = ROUTES[name];
  const body = options && options.body;
  if (method !== "GET") {
    Object.values(state.searches).forEach(search => {
      search.stale = true;
    });
  }
  const target = url(name, params || {}, options && options.query);
  const headers = { Accept: "application/json" };
  if (method !== "GET") {
    Object.assign(headers, csrfHeaders());
  }
  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
  }
  const response = await fetch(target, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
    cache: "no-store",
    credentials: state.config.endpoint.includes("://") ? "include" : "same-origin",
    redirect: "manual",
  });
  if (response.type === "opaqueredirect") {
    // Spring Security sends a browser without a session to its login page.
    throw new ApiError(401, describe(401, null));
  }
  if (response.status === 403 && method !== "GET" && !(options && options.retried)) {
    await loadConfig();
    return api(name, params, Object.assign({}, options, { retried: true }));
  }
  let data = null;
  const text = await response.text();
  if (text) {
    try {
      data = JSON.parse(text);
    } catch (e) {
      data = null;
    }
  }
  if (!response.ok) {
    throw new ApiError(response.status, describe(response.status, data), data);
  }
  return data;
}

function describe(status, data) {
  const detail = data && typeof data.error === "string" ? data.error : null;
  switch (status) {
    case 401: return "Sign in first: the tasks endpoint needs authentication.";
    case 403: return "Not allowed: the tasks endpoint refused this (a role, or a missing CSRF token).";
    case 404: return detail || "Not found.";
    case 409: return detail || "Someone else changed this first.";
    default: return detail || "The endpoint answered " + status + ".";
  }
}

// ---------------------------------------------------------------------------------------------
// Feedback

function toast(message, bad) {
  const el = h("div", { class: "toast" + (bad ? " bad" : "") }, message);
  document.getElementById("toasts").append(el);
  setTimeout(() => el.remove(), bad ? 8000 : 4000);
}

function failure(error) {
  return h("div", { class: "notice bad" }, error && error.message ? error.message : String(error));
}

/** A dialog with a form; resolves with what submit returns, or null when cancelled. */
function dialog(build) {
  const el = document.getElementById("dialog");
  return new Promise(resolve => {
    const error = h("div", { class: "error", hidden: true });
    let done = false;
    const close = result => {
      if (!done) {
        done = true;
        el.onclose = null;
        el.close();
        resolve(result);
      }
    };
    const content = build({
      error,
      cancel: () => close(null),
      fail: message => {
        error.textContent = message;
        error.hidden = false;
      },
      close,
    });
    const form = h("form", { method: "dialog", novalidate: true }, content.body, error,
      h("div", { class: "buttons" },
        h("button", { type: "button", class: "ghost", onclick: () => close(null) }, "Cancel"),
        content.submit));
    form.addEventListener("submit", async event => {
      event.preventDefault();
      if (content.valid && !content.valid()) {
        return;
      }
      content.submit.disabled = true;
      try {
        const result = await content.onsubmit();
        if (result !== undefined) {
          close(result);
        }
      } catch (e) {
        error.textContent = e.message || String(e);
        error.hidden = false;
      } finally {
        content.submit.disabled = false;
      }
    });
    el.replaceChildren(form);
    el.onclose = () => close(null);
    el.showModal();
    const first = form.querySelector("input, select");
    (first || content.submit).focus();
  });
}

/**
 * Asks before an action. With typed, the button stays disabled until that text is typed: for
 * what can't be undone.
 */
function ask({ title, message, action, danger, typed, run }) {
  return dialog(({ fail }) => {
    const submit = h("button", { type: "submit", class: danger ? "danger solid" : "primary", disabled: Boolean(typed) }, action);
    const body = [h("h2", null, title), add(h("p", null), [message].flat())];
    if (typed) {
      const input = h("input", { autocomplete: "off", spellcheck: "false", "aria-label": "Type " + typed + " to confirm" });
      input.addEventListener("input", () => {
        submit.disabled = input.value !== typed;
      });
      body.push(h("label", { class: "field" }, h("span", null, "Type ", h("code", null, typed), " to confirm"), input));
    }
    return {
      body,
      submit,
      onsubmit: async () => {
        try {
          return await run();
        } catch (e) {
          fail(e.message);
          return undefined;
        }
      },
    };
  });
}

// ---------------------------------------------------------------------------------------------
// Views

const main = () => document.getElementById("main");

async function render(options) {
  const seq = ++state.seq;
  const quiet = options && options.quiet;
  try {
    if (!state.config) {
      await loadConfig();
    }
    if (!state.config.endpoint) {
      main().replaceChildren(h("div", { class: "notice warn" },
        "The tasks endpoint isn't exposed, so there is nothing to show. Add it with ",
        h("code", null, "management.endpoints.web.exposure.include=health,tasks"), "."));
      return;
    }
    const route = location.hash.replace(/^#\/?/, "").split("/").map(part => decodeURIComponent(part));
    let view;
    if (route[0] === "topics" && route[1]) {
      view = await topicView(route[1]);
    } else if (route[0] === "tasks" && route[1]) {
      view = await taskView(route[1]);
    } else {
      view = await overviewView();
    }
    if (seq !== state.seq) {
      return;
    }
    const scroll = window.scrollY;
    main().replaceChildren(add(document.createDocumentFragment(), [view]));
    if (quiet) {
      window.scrollTo(0, scroll);
    } else {
      window.scrollTo(0, 0);
    }
  } catch (e) {
    if (seq !== state.seq) {
      return;
    }
    if (!quiet) {
      main().replaceChildren(failure(e));
    } else {
      toast(e.message || String(e), true);
    }
  }
}

function refresh() {
  return render({ quiet: true });
}

// --- Overview

async function overviewView() {
  const data = await api("overview");
  showNode(data.node);
  const topics = data.topics || [];
  const total = key => topics.reduce((sum, topic) => sum + (topic.queue ? topic.queue[key] || 0 : 0), 0);
  const oldest = topics.map(topic => topic.queue && topic.queue.oldestReadyAt).filter(Boolean).sort()[0];
  const stats = h("div", { class: "stats" },
    stat("Topics", topics.length),
    stat("Ready", total("ready")),
    stat("Active", total("active")),
    stat("Oldest ready", oldest ? since(oldest) : "—"),
    stat("Failing", total("failing"), total("failing") ? "warn" : null),
    stat("Dead letters", total("deadLetters"), total("deadLetters") ? "bad" : null));

  const byName = new Map(topics.map(topic => [topic.name, topic]));
  const modules = Object.entries(data.modules || {});
  const counted = topics.map(topic => topic.queue && topic.queue.countedAt).filter(Boolean).sort()[0];
  const stale = counted && age(counted) > 3 * 60 * 1000;
  const topicPanels = modules.length ? modules.map(([module, names]) => panel(module, {
    meta: h("span", { class: "muted" }, names.length + (names.length === 1 ? " topic" : " topics")),
  }, topicTable(names.map(name => byName.get(name)).filter(Boolean)))) : [h("div", { class: "notice" }, "No topics yet.")];

  return [
    stats,
    stale ? h("div", { class: "notice warn" }, "The counts are ", since(counted),
      " old: is any node running housekeeping? (", h("code", null, "pgtasks.queue.snapshot.age"), ")") : null,
    topicPanels,
    nodesPanel(data.node, data.nodes || []),
    counted ? h("p", { class: "muted" }, "Counts as of ", time(counted), ", from the housekeeping leader's snapshot.") : null,
  ];
}

function stat(label, val, kind) {
  return h("div", { class: "stat" + (kind ? " " + kind : "") }, h("div", { class: "label" }, label),
    h("div", { class: "value" }, typeof val === "number" ? number(val) : val));
}

function topicStatus(topic) {
  return h("span", { class: "chips" },
    topic.processing ? chip("running", "ok", "This node processes it") : chip("not here", null, "This node doesn't process it"));
}

function topicTable(topics) {
  return table([
    { label: "Topic", value: topic => h("td", { class: "wrap" }, topicLink(topic.name)) },
    { label: "Status", value: topicStatus },
    { label: "Ready", num: true, value: topic => count(topic.queue && topic.queue.ready) },
    { label: "Oldest ready", num: true, value: topic => topic.queue && topic.queue.oldestReadyAt ? since(topic.queue.oldestReadyAt) : "—" },
    { label: "Active", num: true, value: topic => count(topic.queue && topic.queue.active) },
    { label: "Scheduled", num: true, value: topic => count(topic.queue && topic.queue.scheduled) },
    { label: "Retrying", num: true, value: topic => count(topic.queue && topic.queue.retrying) },
    { label: "Suspended", num: true, value: topic => count(topic.queue && topic.queue.suspended) },
    { label: "Failing", num: true, value: topic => count(topic.queue && topic.queue.failing, true) },
    { label: "Dead letters", num: true, value: topic => count(topic.queue && topic.queue.deadLetters, true) },
  ], topics, "No topics.");
}

function showNode(node) {
  const el = document.getElementById("node");
  if (!node) {
    el.replaceChildren();
    return;
  }
  el.replaceChildren(h("span", null, node.name + " #" + node.id),
    node.running ? chip("running", "ok") : chip("stopped", "bad"),
    node.leader ? chip("leader", "info", "This node leads housekeeping") : null);
}

function nodesPanel(self, nodes) {
  const term = (self && self.leaseTermSeconds ? self.leaseTermSeconds : 60) * 1000;
  return panel("Nodes", {
    meta: h("span", { class: "muted" }, nodes.length + " registered"),
    note: self ? ["This node processes ", self.processing && self.processing.length ? self.processing.join(", ") : "no topics",
      ". A node whose heartbeat is older than " + duration(term) + " has stopped or lost the database."] : null,
  }, table([
    { label: "Node", value: node => h("td", { class: "wrap" }, node.name, " ", h("span", { class: "muted" }, "#" + node.id),
      self && node.id === self.id ? [" ", chip("this node", "info")] : null) },
    { label: "Version", value: node => node.version },
    { label: "Heartbeat", value: node => h("td", { class: "nowrap" }, time(node.heartbeatAt),
      age(node.heartbeatAt) > term ? [" ", chip("late", "warn")] : null) },
    { label: "Started", value: node => time(node.startedAt) },
    { label: "Leads", value: node => node.leases && node.leases.length ? h("span", { class: "chips" }, node.leases.map(lease => chip(lease, "info"))) : "—" },
  ], nodes, "No nodes registered."));
}

// --- Topic

function deadLetterState(topic) {
  if (!state.deadLetters[topic]) {
    state.deadLetters[topic] = { cursors: [0], selected: new Set() };
  }
  return state.deadLetters[topic];
}

async function topicView(name) {
  const paging = deadLetterState(name);
  const after = paging.cursors[paging.cursors.length - 1];
  const search = searchState(name);
  const [topic, dead, overview] = await Promise.all([
    api("topic", { topic: name }),
    api("deadLetters", { topic: name }, { query: { after: after || null, limit: 50 } }),
    api("overview").catch(() => null),
    loadSearch(name, search),
  ]);
  if (overview) {
    showNode(overview.node);
  }
  const queue = topic.queue;
  const head = h("div", null,
    h("div", { class: "crumbs" }, link("#/", "Overview"), topic.module ? " / " + topic.module : null),
    h("div", { class: "page-head" }, h("h1", null, topic.name), topicStatus(topic),
      h("div", { class: "actions" },
        h("button", { type: "button", class: "danger", onclick: () => purge(name) }, "Purge…"),
        h("button", { type: "button", class: "danger", onclick: () => deleteTopic(name) }, "Delete topic…"))));

  const stats = queue ? h("div", { class: "stats" },
    stat("Ready", queue.ready),
    stat("Oldest ready", queue.oldestReadyAt ? since(queue.oldestReadyAt) : "—"),
    stat("Due", queue.due),
    stat("Active", queue.active),
    stat("Scheduled", queue.scheduled),
    stat("Retrying", queue.retrying),
    stat("Suspended", queue.suspended),
    stat("Failing", queue.failing, queue.failing ? "warn" : null),
    stat("Failing since", queue.oldestFailingAt ? since(queue.oldestFailingAt) : "—"),
    stat("Dead letters", queue.deadLetters, queue.deadLetters ? "bad" : null))
    : h("div", { class: "notice" }, "No counts yet: the housekeeping leader takes them every minute.");

  return [
    head,
    stats,
    queue && queue.countedAt ? h("p", { class: "muted" }, "Counts as of ", time(queue.countedAt), ".") : null,
    deadLettersPanel(name, dead, paging),
    tasksPanel(name, search, queue ? queue.suspended : null),
    settingsPanel(topic.settings),
  ];
}

function deadLettersPanel(topic, page, paging) {
  const letters = page.deadLetters || [];
  const visible = new Set(letters.map(letter => letter.id));
  for (const id of paging.selected) {
    if (!visible.has(id)) paging.selected.delete(id);
  }
  const selectedCount = () => paging.selected.size;
  const reviveSelected = h("button", { type: "button", class: "small", disabled: !selectedCount(), onclick: () => byIds(topic, "revive") }, "Revive selected");
  const discardSelected = h("button", { type: "button", class: "small danger", disabled: !selectedCount(), onclick: () => byIds(topic, "discard") }, "Discard selected…");
  const update = () => {
    reviveSelected.disabled = discardSelected.disabled = !selectedCount();
    all.checked = letters.length > 0 && selectedCount() === letters.length;
  };
  const all = h("input", { type: "checkbox", "aria-label": "Select all on this page", disabled: !letters.length });
  all.addEventListener("change", () => {
    letters.forEach(letter => all.checked ? paging.selected.add(letter.id) : paging.selected.delete(letter.id));
    main().querySelectorAll("input[data-dead]").forEach(box => {
      box.checked = all.checked;
    });
    update();
  });
  all.checked = letters.length > 0 && selectedCount() === letters.length;
  const rows = table([
    { label: all, value: letter => {
      const box = h("input", { type: "checkbox", "data-dead": "", checked: paging.selected.has(letter.id), "aria-label": "Select task " + letter.id });
      box.addEventListener("change", () => {
        box.checked ? paging.selected.add(letter.id) : paging.selected.delete(letter.id);
        update();
      });
      return box;
    } },
    { label: "Task", value: letter => taskLink(letter.id) },
    { label: "Identifier", value: letter => h("td", { class: "wrap mono" }, letter.identifier) },
    { label: "Attempts", num: true, value: letter => count(letter.attempt) },
    { label: "Failed first", value: letter => time(letter.firstFailedAt) },
    { label: "Error", value: letter => h("td", { class: "wrap mono" }, errorSummary(letter.error)) },
  ], letters, paging.cursors.length > 1 ? "No more dead letters." : "No dead letters.");
  const pager = h("div", { class: "pager" },
    h("button", { type: "button", class: "small", disabled: paging.cursors.length <= 1, onclick: () => {
      paging.cursors = [0];
      paging.selected.clear();
      refresh();
    } }, "First"),
    h("button", { type: "button", class: "small", disabled: paging.cursors.length <= 1, onclick: () => {
      paging.cursors.pop();
      paging.selected.clear();
      refresh();
    } }, "Previous"),
    h("button", { type: "button", class: "small", disabled: page.next === null || page.next === undefined, onclick: () => {
      paging.cursors.push(page.next);
      paging.selected.clear();
      refresh();
    } }, "Next"),
    h("span", { class: "muted" }, "Page " + paging.cursors.length + ", lowest id first"));
  return panel("Dead letters", {
    actions: [reviveSelected, discardSelected,
      h("button", { type: "button", class: "small", onclick: () => everyDeadLetter(topic, "revive") }, "Revive all…"),
      h("button", { type: "button", class: "small danger", onclick: () => everyDeadLetter(topic, "discard") }, "Discard all…")],
  }, rows, pager);
}

async function byIds(topic, action) {
  const paging = deadLetterState(topic);
  const ids = [...paging.selected].sort((a, b) => a - b);
  if (!ids.length) {
    return;
  }
  const revive = action === "revive";
  const run = () => api(action, { topic }, { body: { ids: ids.join(",") } });
  let result;
  if (revive) {
    try {
      result = await run();
    } catch (e) {
      toast(e.message, true);
      return;
    }
  } else {
    result = await ask({
      title: "Discard " + ids.length + " dead letter(s)",
      message: ["Give up on tasks ", ids.map(id => "#" + id).join(", "), ". They finish as failed; this can't be undone."],
      action: "Discard",
      danger: true,
      run,
    });
  }
  if (result) {
    paging.selected.clear();
    toast((revive ? "Revived " + result.revived : "Discarded " + result.discarded) + " dead letter(s).");
    refresh();
  }
}

async function everyDeadLetter(topic, action) {
  const revive = action === "revive";
  const result = await ask({
    title: (revive ? "Revive" : "Discard") + " every dead letter",
    message: revive ? ["Give every dead letter of ", h("strong", null, topic), " another round of attempts."]
      : ["Give up on every dead letter of ", h("strong", null, topic), ". They finish as failed; this can't be undone."],
    action: revive ? "Revive all" : "Discard all",
    danger: !revive,
    typed: revive ? null : topic,
    run: () => api(action, { topic }, { body: { all: "true" } }),
  });
  if (result) {
    deadLetterState(topic).cursors = [0];
    toast((revive ? "Revived " + result.revived : "Discarded " + result.discarded) + " dead letter(s).");
    refresh();
  }
}

// --- Unfinished tasks: search

function searchState(topic) {
  if (!state.searches[topic]) {
    state.searches[topic] = { filter: null, cursors: [0], page: null, stale: true };
  }
  return state.searches[topic];
}

/** The search's fields as the endpoint takes them, as query parameters or in a body. */
function filterFields(filter) {
  const fields = {};
  for (const key of ["identifier", "reference", "header", "createdFrom", "createdUntil"]) {
    if (filter[key]) fields[key] = filter[key];
  }
  if (filter.state && filter.state.length) fields.state = filter.state.join(",");
  return fields;
}

async function loadSearch(topic, search) {
  if (!search.filter || (!search.stale && search.page)) {
    return;
  }
  const after = search.cursors[search.cursors.length - 1];
  search.page = await api("tasks", { topic }, { query: Object.assign({ after: after || null, limit: 50 }, filterFields(search.filter)) });
  search.stale = false;
}

function filterChips(filter) {
  const fields = filterFields(filter);
  const names = { identifier: "identifier", reference: "reference", state: "state", header: "header", createdFrom: "created from",
    createdUntil: "created before" };
  const chips = Object.entries(fields).map(([key, val]) => chip(names[key] + ": " + val, "info"));
  return h("span", { class: "chips" }, chips.length ? chips : chip("every unfinished task", "info"));
}

function tasksPanel(topic, search, suspended) {
  const open = h("button", { type: "button", class: "small", onclick: () => searchTasks(topic) }, search.filter ? "Change search…" : "Search…");
  // From the last counts, which may be a minute old; the endpoint says how many it released.
  const releaseAll = suspended ? h("button", { type: "button", class: "small", onclick: () => releaseEvery(topic) }, "Release all suspended…") : null;
  if (!search.filter) {
    return panel("Unfinished tasks", {
      actions: [open, h("button", { type: "button", class: "small", onclick: () => setSearch(topic, {}) }, "List all"), releaseAll],
      note: "Find tasks by identifier, reference, state, a header or when they were pushed. "
        + "Without an identifier or a reference a search reads the topic's whole backlog, so it runs when you ask, not on every refresh.",
    }, h("div", { class: "empty" }, "No search yet."));
  }
  const tasks = (search.page && search.page.tasks) || [];
  const rows = table([
    { label: "Task", value: task => taskLink(task.id) },
    { label: "Identifier", value: task => h("td", { class: "wrap mono" }, task.identifier) },
    { label: "State", value: task => chip(task.state, task.state === "failed" ? "bad" : task.state === "retrying" ? "warn" : "info") },
    { label: "Attempt", num: true, value: task => count(task.attempt) },
    { label: "Runs at", value: task => time(task.runAt) },
    { label: "Created", value: task => time(task.createdAt) },
    { label: "Reference", value: task => h("td", { class: "wrap mono" }, task.reference || "—") },
    { label: h("span", { class: "sr-only" }, "Actions"), value: task => task.state === "suspended"
      ? h("button", { type: "button", class: "small", onclick: () => releaseOne(topic, task.id) }, "Release")
      : h("td", null) },
  ], tasks, search.cursors.length > 1 ? "No more tasks." : "No unfinished task matches.");
  const page = search.page || {};
  const go = cursors => {
    search.cursors = cursors;
    search.stale = true;
    refresh();
  };
  const pager = h("div", { class: "pager" },
    h("button", { type: "button", class: "small", disabled: search.cursors.length <= 1, onclick: () => go([0]) }, "First"),
    h("button", { type: "button", class: "small", disabled: search.cursors.length <= 1, onclick: () => go(search.cursors.slice(0, -1)) }, "Previous"),
    h("button", { type: "button", class: "small", disabled: page.next === null || page.next === undefined,
      onclick: () => go(search.cursors.concat([page.next])) }, "Next"),
    h("button", { type: "button", class: "small ghost", onclick: () => go(search.cursors) }, "Reload"),
    h("span", { class: "muted" }, "Page " + search.cursors.length + ", lowest id first"));
  return panel("Unfinished tasks", {
    meta: filterChips(search.filter),
    actions: [open, h("button", { type: "button", class: "small ghost", onclick: () => setSearch(topic, null) }, "Clear"), releaseAll],
  }, rows, pager);
}

/** Releases one suspended task: it becomes ready and runs again. */
async function releaseOne(topic, id) {
  let result;
  try {
    result = await api("release", { topic }, { body: { ids: String(id) } });
  } catch (e) {
    toast(e.message, true);
    return;
  }
  toast(result.released ? "Released task #" + id + "." : "Task #" + id + " is no longer suspended.");
  searchState(topic).stale = true;
  refresh();
}

async function releaseEvery(topic) {
  const result = await ask({
    title: "Release every suspended task",
    message: ["Make every suspended task of ", h("strong", null, topic), " ready, so it runs again with the attempts it has."],
    action: "Release all",
    run: () => api("release", { topic }, { body: { all: "true" } }),
  });
  if (result) {
    toast("Released " + result.released + " suspended task(s).");
    searchState(topic).stale = true;
    refresh();
  }
}

function setSearch(topic, filter) {
  const search = searchState(topic);
  search.filter = filter;
  search.cursors = [0];
  search.page = null;
  search.stale = true;
  refresh();
}

async function searchTasks(topic) {
  const current = searchState(topic).filter || {};
  const filter = await dialog(({ fail }) => {
    const identifier = h("input", { name: "identifier", autocomplete: "off", spellcheck: "false", value: current.identifier || "" });
    const reference = h("input", { name: "reference", autocomplete: "off", spellcheck: "false", value: current.reference || "" });
    const header = h("input", { name: "header", autocomplete: "off", spellcheck: "false", placeholder: "name=value", value: current.header || "" });
    const from = h("input", { name: "createdFrom", type: "datetime-local",
      value: current.createdFrom ? localInput(new Date(current.createdFrom)) : "" });
    const until = h("input", { name: "createdUntil", type: "datetime-local",
      value: current.createdUntil ? localInput(new Date(current.createdUntil)) : "" });
    const states = STATES.map(name => h("input", { type: "checkbox", value: name, checked: (current.state || []).includes(name),
      "aria-label": name }));
    const submit = h("button", { type: "submit", class: "primary" }, "Search");
    return {
      body: [h("h2", null, "Search unfinished tasks"),
        h("p", { class: "muted" }, "Every field you fill in must match; leave them all empty to list every unfinished task."),
        h("div", { class: "row" },
          h("label", { class: "field" }, h("span", null, "Identifier"), identifier),
          h("label", { class: "field" }, h("span", null, "Reference"), reference)),
        h("fieldset", { class: "field" }, h("legend", null, "State (any of)"),
          h("div", { class: "chips" }, states.map(box => h("label", { class: "toggle" }, box, box.value)))),
        h("label", { class: "field" }, h("span", null, "Header"), header),
        h("div", { class: "row" },
          h("label", { class: "field" }, h("span", null, "Created from (your time)"), from),
          h("label", { class: "field" }, h("span", null, "Created before (your time)"), until))],
      submit,
      valid: () => {
        if (header.value.trim() && header.value.indexOf("=") < 1) {
          fail("A header is name=value.");
          return false;
        }
        for (const input of [from, until]) {
          if (input.value && isNaN(new Date(input.value).getTime())) {
            fail("Pick a time, or leave it empty.");
            return false;
          }
        }
        return true;
      },
      onsubmit: () => ({
        identifier: identifier.value.trim() || null,
        reference: reference.value.trim() || null,
        header: header.value.trim() || null,
        state: states.filter(box => box.checked).map(box => box.value),
        createdFrom: from.value ? new Date(from.value).toISOString() : null,
        createdUntil: until.value ? new Date(until.value).toISOString() : null,
      }),
    };
  });
  if (filter) {
    setSearch(topic, filter);
  }
}

function localInput(date) {
  const pad = n => String(n).padStart(2, "0");
  return date.getFullYear() + "-" + pad(date.getMonth() + 1) + "-" + pad(date.getDate()) + "T" + pad(date.getHours()) + ":" + pad(date.getMinutes());
}

async function purge(topic) {
  const result = await dialog(({ fail }) => {
    const outcome = h("select", { name: "outcome" }, OUTCOMES.map(o => h("option", { value: o }, o)));
    const before = h("input", { name: "before", type: "datetime-local", required: true, value: localInput(new Date(Date.now() - 30 * 86400000)) });
    const typed = h("input", { autocomplete: "off", spellcheck: "false", "aria-label": "Type " + topic + " to confirm" });
    const submit = h("button", { type: "submit", class: "danger solid", disabled: true }, "Purge");
    typed.addEventListener("input", () => {
      submit.disabled = typed.value !== topic;
    });
    return {
      body: [h("h2", null, "Purge " + topic),
        h("p", null, "Deletes finished tasks with this outcome that finished before the time, as retention does. This can't be undone."),
        h("div", { class: "row" },
          h("label", { class: "field" }, h("span", null, "Outcome"), outcome),
          h("label", { class: "field" }, h("span", null, "Finished before (your time)"), before)),
        h("label", { class: "field" }, h("span", null, "Type ", h("code", null, topic), " to confirm"), typed)],
      submit,
      valid: () => {
        if (!before.value || isNaN(new Date(before.value).getTime())) {
          fail("Pick a time.");
          return false;
        }
        return true;
      },
      onsubmit: () => api("purge", { topic }, { body: { outcome: outcome.value, before: new Date(before.value).toISOString() } }),
    };
  });
  if (result) {
    toast("Purged " + number(result.purged) + " task(s).");
    refresh();
  }
}

async function deleteTopic(topic) {
  const result = await ask({
    title: "Delete " + topic,
    message: ["Deletes the topic with its history, on every node. It must have no unfinished tasks. ",
      "A node whose configuration registers it creates it again when it starts, so remove it there first."],
    action: "Delete topic",
    danger: true,
    typed: topic,
    run: () => api("deleteTopic", { topic }),
  });
  if (result) {
    toast("Deleted " + topic + " and " + number(result.deletedTasks) + " finished task(s).");
    location.hash = "#/";
  }
}

const SOURCES = {
  "default": ["default", null],
  "global-defaults": ["global defaults", "info"],
  "module-defaults": ["module defaults", "info"],
  "topic": ["topic", "ok"],
  "override": ["override", "warn"],
};

function settingsPanel(settings) {
  const rows = Object.entries(settings || {}).map(([key, setting]) => ({ key, value: setting.value, source: setting.source }));
  return panel("Settings", {
    note: "Where each came from: the library's default, TaskGlobals, module defaults, the topic's code, or a pgtasks.topics.* override.",
  }, table([
    { label: "Setting", value: row => h("td", { class: "mono" }, row.key) },
    { label: "Value", value: row => h("td", { class: "wrap mono" }, row.value) },
    { label: "From", value: row => chip(...(SOURCES[row.source] || [row.source, null])) },
  ], rows, "This node doesn't run the topic, so it doesn't know its settings."));
}

// --- Task

/** The endpoint gives an error as an object with a type, a message and a stack; older ones as JSON text. */
function parseError(text) {
  if (!text || text === HIDDEN) {
    return null;
  }
  if (typeof text === "object") {
    return text;
  }
  try {
    const parsed = JSON.parse(text);
    return parsed && typeof parsed === "object" ? parsed : { message: String(parsed) };
  } catch (e) {
    return { message: text };
  }
}

function errorSummary(text) {
  if (text === HIDDEN) {
    return value(text);
  }
  const error = parseError(text);
  if (!error) {
    return "—";
  }
  return [error.type || "", error.type && error.message ? ": " : "", value(error.message || "")];
}

function errorBlock(text) {
  if (text === HIDDEN) {
    return value(text);
  }
  const error = parseError(text);
  if (!error) {
    return "—";
  }
  const stack = error.stack || error.stackTrace;
  return h("div", null,
    error.type ? h("div", { class: "mono" }, h("strong", null, error.type)) : null,
    error.message !== undefined ? h("pre", null, error.message === HIDDEN ? value(HIDDEN) : String(error.message)) : null,
    stack ? h("details", null, h("summary", null, "Stack"), h("pre", null, Array.isArray(stack) ? stack.join("\n") : String(stack))) : null);
}

async function taskView(idText) {
  if (!/^\d+$/.test(idText)) {
    return h("div", { class: "notice bad" }, "A task id is a number.");
  }
  let task;
  try {
    task = await api("task", { id: idText });
  } catch (e) {
    if (e.status === 404) {
      return [h("div", { class: "crumbs" }, link("#/", "Overview")),
        h("div", { class: "notice" }, "No task #" + idText + ". Finished tasks are deleted after their topic's retention.")];
    }
    throw e;
  }
  const hidden = [task.payload, task.output].includes(HIDDEN) || Object.values(task.headers || {}).includes(HIDDEN)
    || (task.error && task.error.includes(HIDDEN));
  const fields = [
    ["Topic", topicLink(task.topic)],
    ["Identifier", h("span", { class: "mono" }, task.identifier)],
    ["State", chip(task.state, task.state === "failed" ? "bad" : task.finished ? "ok" : "info")],
    ["Attempt", number(task.attempt)],
    ["Failures", task.failures ? [number(task.failures), task.firstFailedAt ? [", first ", time(task.firstFailedAt)] : null] : "0"],
    ["Run at", time(task.runAt)],
    ["Priority", number(task.priority)],
    ["Node", task.node === null || task.node === undefined ? "—" : "#" + task.node],
    ["Created", time(task.createdAt)],
    ["Completed", time(task.completedAt)],
    ["Reference", task.reference ? h("span", { class: "mono" }, task.reference) : "—"],
    ["Parent", taskLink(task.parentId)],
    ["Root", taskLink(task.rootId)],
    ["Payload type", task.payloadType ? h("span", { class: "mono" }, task.payloadType) : "—"],
  ];
  const headers = Object.entries(task.headers || {});
  return [
    h("div", { class: "crumbs" }, link("#/", "Overview"), " / ", topicLink(task.topic)),
    h("div", { class: "page-head" }, h("h1", null, "Task #" + task.id)),
    hidden ? h("div", { class: "notice" }, "Payloads, outputs and error messages are hidden by ",
      h("code", null, "management.endpoint.tasks.show-values"), ". Error types are still shown.") : null,
    h("div", { class: "grid" },
      panel("Task", null, h("div", { class: "panel-body" }, h("dl", { class: "kv" }, fields.map(([k, v]) => [h("dt", null, k), h("dd", null, v)])))),
      h("div", null,
        panel("Error", null, h("div", { class: "panel-body" }, errorBlock(task.error))),
        panel("Payload", null, h("div", { class: "panel-body" }, task.payload === HIDDEN ? value(HIDDEN) : task.payload === null ? "—" : h("pre", null, task.payload))),
        task.output !== null && task.output !== undefined
          ? panel("Output", null, h("div", { class: "panel-body" }, task.output === HIDDEN ? value(HIDDEN) : h("pre", null, task.output))) : null,
        panel("Headers", null, table([
          { label: "Name", value: ([name]) => h("td", { class: "mono" }, name) },
          { label: "Value", value: ([, val]) => h("td", { class: "wrap mono" }, value(val)) },
        ], headers, "No headers.")))),
  ];
}

// ---------------------------------------------------------------------------------------------
// Chrome: theme, live refresh, task lookup

const THEMES = ["auto", "light", "dark"];

function storage(key, val) {
  try {
    if (val === undefined) {
      return localStorage.getItem("pgtasks." + key);
    }
    localStorage.setItem("pgtasks." + key, val);
  } catch (e) {
    // Private windows and blocked storage: the page works without it.
  }
  return null;
}

function applyTheme(theme) {
  if (theme === "light" || theme === "dark") {
    document.documentElement.setAttribute("data-theme", theme);
  } else {
    document.documentElement.removeAttribute("data-theme");
  }
  const button = document.getElementById("theme");
  button.textContent = theme === "light" ? "Light" : theme === "dark" ? "Dark" : "Auto";
  button.setAttribute("aria-label", "Theme: " + button.textContent);
}

function live(on) {
  clearInterval(state.timer);
  state.timer = on ? setInterval(() => {
    if (!document.hidden && !document.getElementById("dialog").open) {
      refresh();
    }
  }, REFRESH_MS) : null;
}

function start() {
  let theme = storage("theme") || "auto";
  applyTheme(theme);
  document.getElementById("theme").addEventListener("click", () => {
    theme = THEMES[(THEMES.indexOf(theme) + 1) % THEMES.length];
    storage("theme", theme);
    applyTheme(theme);
  });
  const refreshBox = document.getElementById("refresh");
  refreshBox.checked = storage("live") !== "off";
  live(refreshBox.checked);
  refreshBox.addEventListener("change", () => {
    storage("live", refreshBox.checked ? "on" : "off");
    live(refreshBox.checked);
  });
  document.getElementById("lookup").addEventListener("submit", event => {
    event.preventDefault();
    const input = document.getElementById("lookup-id");
    const id = input.value.trim().replace(/^#/, "");
    if (/^\d+$/.test(id)) {
      location.hash = "#/tasks/" + id;
      input.value = "";
    } else {
      toast("A task id is a number.", true);
    }
  });
  window.addEventListener("hashchange", () => {
    render();
    main().focus({ preventScroll: true });
  });
  render();
}

start();
