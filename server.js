#!/usr/bin/env node
"use strict";
// Bot Boss - a Grok-Bot-style command center for Claude Code agents.
// Launcher model: the server owns the agents it spawns via
//   claude -p --input-format stream-json --output-format stream-json
// and drives them over stdin/stdout. Pure Node stdlib, no dependencies.

const http = require("http");
const fs = require("fs");
const path = require("path");
const os = require("os");
const crypto = require("crypto");
const { spawn } = require("child_process");

const PORT = Number(process.env.PORT || 4177);
const HOME = os.homedir();
const PUBLIC_DIR = path.join(__dirname, "public");
const CLAUDE_BIN = process.env.CLAUDE_BIN || "claude";
const DATA_DIR = process.env.BOSS_DATA_DIR || path.join(HOME, ".bot-boss", "bots");
const SEED_DIR = path.join(__dirname, "seed");
const SEED_MARKER = path.join(path.dirname(DATA_DIR), ".seeded");

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
/** @type {Map<string, Agent>} */
const agents = new Map();
/** @type {Set<http.ServerResponse>} SSE clients */
const sseClients = new Set();

// ---------------------------------------------------------------------------
// Agent lifecycle
// ---------------------------------------------------------------------------
class Agent {
  constructor({ name, label, description, cwd, model, permissionMode }) {
    this.id = crypto.randomUUID().slice(0, 8);
    this.name = name || `bot-${this.id}`;
    this.label = (label || "").trim(); // optional short title/tag, display only
    this.description = (description || "").trim(); // persistent role/rules (system prompt)
    this.cwd = cwd || HOME;
    this.model = model || "claude-sonnet-4-5";
    this.permissionMode = permissionMode || "acceptEdits";
    // stable session id we control up front, so the conversation is resumable
    this.sessionId = crypto.randomUUID();
    this.status = "starting"; // starting | working | ready | blocked | dead
    this.createdAt = Date.now();
    this.lastActivity = Date.now();
    this.costUSD = 0;
    this.tokensIn = 0;  // input tokens incl. cache reads/creations
    this.tokensOut = 0; // output tokens
    this.transcript = []; // {role, kind, text, tool, id, ts}
    this.proc = null;
    this.started = false; // whether this.sessionId has been established yet
    this._stdoutBuf = "";
    this._liveMsg = null; // coalescing buffer for a streaming assistant message
    this._seq = 0;        // monotonic id source for streamed entries
    this._removed = false;
  }

  // reconstruct a persisted bot (process not running until next message)
  static fromDisk(d) {
    const a = new Agent(d);
    a.id = d.id;
    a.label = (d.label || "").trim();
    a.description = (d.description || "").trim();
    a.sessionId = d.sessionId;
    a.createdAt = d.createdAt || Date.now();
    a.lastActivity = d.lastActivity || a.createdAt;
    a.costUSD = d.costUSD || 0;
    a.tokensIn = d.tokensIn || 0;
    a.tokensOut = d.tokensOut || 0;
    a.transcript = d.transcript || [];
    a.started = d.started !== false; // an existing bot has an established session
    a.status = "ready"; // idle and resumable
    return a;
  }

  // spawn the underlying `claude -p` process. An already-started session is
  // continued with --resume; a fresh one is created with --session-id.
  _spawn() {
    const args = [
      "-p",
      "--input-format", "stream-json",
      "--output-format", "stream-json",
      "--include-partial-messages",
      "--verbose",
      "--model", this.model,
      "--permission-mode", this.permissionMode,
      this.started ? "--resume" : "--session-id", this.sessionId,
    ];
    // persistent per-bot role/rules, layered onto the default system prompt
    if (this.description) args.push("--append-system-prompt", this.description);
    this.started = true;
    const proc = spawn(CLAUDE_BIN, args, {
      cwd: this.cwd,
      env: process.env,
      stdio: ["pipe", "pipe", "pipe"],
    });
    this.proc = proc;
    this.status = "working";
    this._stdoutBuf = "";

    proc.stdout.on("data", (chunk) => this._onStdout(chunk));
    proc.stderr.on("data", (chunk) => {
      const s = chunk.toString();
      // stderr is mostly diagnostics; only surface if it looks like an error
      if (/error|not found|fatal/i.test(s)) {
        this._push({ role: "system", kind: "error", text: s.trim() });
      }
    });
    proc.on("exit", () => {
      this.proc = null;
      // an idle exit just means the turn ended; keep the bot resumable.
      if (this.status !== "ready") { this.status = "ready"; broadcast({ type: "agent_status", id: this.id, status: this.status }); }
      this.save();
    });
  }

  start(task) {
    // Only spawn if there is a first task. With no task, stay idle/ready; the
    // process spawns lazily on the first message (send() handles that), so we
    // don't leave an idle process sitting at "working" with nothing to do.
    if (task && task.trim()) {
      this.send(task); // send() spawns the process if needed
    } else {
      this.status = "ready";
    }
    broadcast({ type: "agent_added", agent: this.summary() });
    this.save();
  }

  send(text) {
    // (re)spawn a bot whose process has exited (e.g. after a server restart)
    if (!this.proc) this._spawn();
    const msg = {
      type: "user",
      message: { role: "user", content: [{ type: "text", text }] },
    };
    try {
      this.proc.stdin.write(JSON.stringify(msg) + "\n");
    } catch (e) {
      return false;
    }
    this._push({ role: "user", kind: "text", text });
    this.status = "working";
    this.lastActivity = Date.now();
    broadcast({ type: "agent_status", id: this.id, status: this.status });
    return true;
  }

  // kill the process synchronously and detach its listeners, so no buffered
  // output or exit handler can touch state after a deliberate stop/respawn.
  _killProc() {
    if (!this.proc) return;
    try { this.proc.stdout.removeAllListeners(); } catch (e) {}
    try { this.proc.stderr.removeAllListeners(); } catch (e) {}
    try { this.proc.removeAllListeners("exit"); } catch (e) {}
    try { this.proc.stdin.end(); } catch (e) {}
    try { this.proc.kill("SIGTERM"); } catch (e) {}
    this.proc = null;
  }

  stop() { this._killProc(); }

  // wipe all context: kill the process, start a brand-new session, clear history
  reset() {
    this._killProc();
    this.sessionId = crypto.randomUUID(); // fresh, unstarted session
    this.started = false;
    this.transcript = [];
    this.costUSD = 0; this.tokensIn = 0; this.tokensOut = 0;
    this.createdAt = Date.now();   // age reflects the new session
    this.lastActivity = Date.now();
    this._liveMsg = null; this._stdoutBuf = "";
    this.status = "ready";
    this.save();
    broadcast({ type: "agent_reset", id: this.id, agent: this.summary() });
  }

  // edit name / working dir / model. cwd/model changes take effect on the next
  // spawn, so end the current process (session + history kept via --resume).
  update({ name, label, description, cwd, model, permissionMode }) {
    if (typeof name === "string" && name.trim()) this.name = name.trim();
    if (typeof label === "string") this.label = label.trim();
    let respawn = false;
    if (description != null && description.trim() !== this.description) { this.description = description.trim(); respawn = true; }
    if (cwd != null && cwd !== this.cwd) { this.cwd = cwd; respawn = true; }
    if (model != null && model !== this.model) { this.model = model; respawn = true; }
    if (permissionMode != null && permissionMode !== this.permissionMode) { this.permissionMode = permissionMode; respawn = true; }
    if (respawn && this.proc) this.stop();
    this.save();
    broadcast({ type: "agent_updated", id: this.id, agent: this.summary() });
  }

  // --- persistence ---
  toDisk() {
    return {
      id: this.id, name: this.name, label: this.label, description: this.description,
      cwd: this.cwd, model: this.model,
      permissionMode: this.permissionMode, sessionId: this.sessionId,
      createdAt: this.createdAt, lastActivity: this.lastActivity,
      costUSD: this.costUSD, tokensIn: this.tokensIn, tokensOut: this.tokensOut,
      started: this.started, status: this.status, transcript: this.transcript,
    };
  }
  save() {
    if (this._removed) return;
    try {
      fs.mkdirSync(DATA_DIR, { recursive: true });
      fs.writeFileSync(path.join(DATA_DIR, this.id + ".json"), JSON.stringify(this.toDisk()));
    } catch (e) {}
  }
  remove() {
    this._removed = true;
    try { fs.unlinkSync(path.join(DATA_DIR, this.id + ".json")); } catch (e) {}
  }

  _onStdout(chunk) {
    this._stdoutBuf += chunk.toString();
    let idx;
    while ((idx = this._stdoutBuf.indexOf("\n")) >= 0) {
      const line = this._stdoutBuf.slice(0, idx).trim();
      this._stdoutBuf = this._stdoutBuf.slice(idx + 1);
      if (!line) continue;
      let ev;
      try { ev = JSON.parse(line); } catch (e) { continue; }
      this._handleEvent(ev);
    }
  }

  _handleEvent(ev) {
    this.lastActivity = Date.now();
    switch (ev.type) {
      case "system":
        if (ev.subtype === "init") {
          this.sessionId = ev.session_id;
          if (ev.model) this.model = ev.model;
        }
        break;

      case "stream_event": {
        // live token deltas for a smooth typing effect
        const e = ev.event;
        if (e && e.type === "content_block_delta" && e.delta) {
          if (e.delta.type === "text_delta" && e.delta.text) {
            this._appendLive("text", e.delta.text);
          } else if (e.delta.type === "thinking_delta" && e.delta.thinking) {
            this._appendLive("thinking", e.delta.thinking);
          }
        }
        break;
      }

      case "assistant": {
        // authoritative message; may repeat with same id as blocks complete
        const content = (ev.message && ev.message.content) || [];
        for (const block of content) {
          if (block.type === "text") {
            this._commitLive("text", block.text);
          } else if (block.type === "thinking") {
            this._commitLive("thinking", block.thinking);
          } else if (block.type === "tool_use") {
            this._push({
              role: "assistant", kind: "tool_use",
              tool: block.name,
              text: summarizeToolInput(block.name, block.input),
              id: block.id,
            });
          }
        }
        break;
      }

      case "user": {
        // tool results echoed back as user messages
        const content = (ev.message && ev.message.content) || [];
        for (const block of content) {
          if (block.type === "tool_result") {
            this._push({
              role: "tool", kind: "tool_result",
              toolUseId: block.tool_use_id,
              text: stringifyToolResult(block.content),
            });
          }
        }
        break;
      }

      case "result": {
        this._liveMsg = null;
        if (typeof ev.total_cost_usd === "number") this.costUSD += ev.total_cost_usd;
        const u = ev.usage || {};
        this.tokensIn += (u.input_tokens || 0) + (u.cache_read_input_tokens || 0) + (u.cache_creation_input_tokens || 0);
        this.tokensOut += (u.output_tokens || 0);
        this.status = "ready"; // waiting for your next message
        broadcast({ type: "agent_status", id: this.id, status: this.status, costUSD: this.costUSD, tokensIn: this.tokensIn, tokensOut: this.tokensOut });
        this.save();
        break;
      }

      case "rate_limit_event": {
        // only surface real problems; "allowed"/"allowed_warning" are fine
        const info = ev.rate_limit_info || {};
        if (info.status && !String(info.status).startsWith("allowed")) {
          this._push({ role: "system", kind: "error", text: `rate limit: ${info.status} (${info.overageDisabledReason || info.rateLimitType || ""})` });
        }
        break;
      }
    }
  }

  _appendLive(kind, text) {
    if (!this._liveMsg || this._liveMsg.kind !== kind) {
      // stable, unique id kept through finalize so the client updates in place
      this._liveMsg = { role: "assistant", kind, text: "", id: "live-" + (++this._seq), ts: Date.now(), live: true };
      this.transcript.push(this._liveMsg);
    }
    this._liveMsg.text += text;
    broadcast({ type: "agent_delta", id: this.id, entry: this._liveMsg });
  }

  _commitLive(kind, text) {
    if (this._liveMsg && this._liveMsg.kind === kind && this._liveMsg.live) {
      // finalize the streamed entry in place; keep its id so the client matches it
      this._liveMsg.text = text;
      this._liveMsg.live = false;
      broadcast({ type: "agent_delta", id: this.id, entry: this._liveMsg });
      this._liveMsg = null;
      this.save();
    } else {
      // block with no preceding stream (e.g. deltas batched); id auto-assigned
      this._push({ role: "assistant", kind, text });
    }
  }

  _push(entry) {
    entry.ts = entry.ts || Date.now();
    entry.id = entry.id || crypto.randomUUID().slice(0, 8);
    this.transcript.push(entry);
    broadcast({ type: "agent_msg", id: this.id, entry });
    this.save();
  }

  summary() {
    return {
      id: this.id, name: this.name, label: this.label, description: this.description,
      cwd: this.cwd, model: this.model,
      permissionMode: this.permissionMode, status: this.status,
      sessionId: this.sessionId, createdAt: this.createdAt,
      lastActivity: this.lastActivity, costUSD: this.costUSD,
      tokensIn: this.tokensIn, tokensOut: this.tokensOut,
      owned: true, msgCount: this.transcript.length,
    };
  }
}

function summarizeToolInput(name, input) {
  if (!input) return name;
  if (name === "Bash") return input.command || "";
  if (name === "Read" || name === "Edit" || name === "Write") return input.file_path || "";
  if (name === "Grep") return input.pattern || "";
  if (name === "Glob") return input.pattern || "";
  const keys = Object.keys(input);
  return keys.length ? JSON.stringify(input).slice(0, 300) : name;
}

function stringifyToolResult(content) {
  if (typeof content === "string") return content.slice(0, 2000);
  if (Array.isArray(content)) {
    return content.map((c) => (typeof c === "string" ? c : c.text || "")).join("\n").slice(0, 2000);
  }
  return JSON.stringify(content || "").slice(0, 2000);
}

// list candidate working directories for the New Agent form.
// Override with BOSS_DIRS (colon-separated absolute paths); otherwise scan
// the immediate subdirectories of $HOME.
function listDirs() {
  const envDirs = (process.env.BOSS_DIRS || "").split(":").map((s) => s.trim()).filter(Boolean);
  if (envDirs.length) return envDirs;
  const dirs = [HOME];
  try {
    for (const d of fs.readdirSync(HOME, { withFileTypes: true })) {
      if (d.isDirectory() && !d.name.startsWith(".")) dirs.push(path.join(HOME, d.name));
    }
  } catch (e) {}
  return dirs.sort();
}

// expand a leading ~ to $HOME
// resolve an input path to absolute. Absolute paths pass through; ~ expands to
// HOME; anything else is treated as relative to HOME.
function expandPath(p) {
  if (!p) return p;
  p = String(p).trim();
  if (!p) return p;
  if (p === "~") return HOME;
  if (p.startsWith("~/") || p.startsWith("~\\")) return path.join(HOME, p.slice(2));
  if (path.isAbsolute(p)) return p;
  return path.join(HOME, p);
}

// validate that a path resolves to an existing directory
function checkDir(p) {
  const resolved = expandPath(p);
  if (!resolved) return { ok: false, reason: "Enter a path (absolute, or relative to your home dir)", resolved };
  try {
    if (!fs.statSync(resolved).isDirectory()) return { ok: false, reason: "Path is not a directory", resolved };
    return { ok: true, resolved };
  } catch (e) {
    return { ok: false, reason: "Directory does not exist", resolved };
  }
}

// ---------------------------------------------------------------------------
// SSE
// ---------------------------------------------------------------------------
function broadcast(obj) {
  const data = `data: ${JSON.stringify(obj)}\n\n`;
  for (const res of sseClients) {
    try { res.write(data); } catch (e) {}
  }
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------
function sendJSON(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { "Content-Type": "application/json" });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve) => {
    let b = "";
    req.on("data", (c) => (b += c));
    req.on("end", () => {
      try { resolve(b ? JSON.parse(b) : {}); } catch (e) { resolve({}); }
    });
  });
}

const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, `http://localhost:${PORT}`);
  const p = u.pathname;

  // --- API ---
  if (p === "/api/state") {
    return sendJSON(res, 200, {
      agents: [...agents.values()].map((a) => a.summary()),
      dirs: listDirs(),
      models: process.env.BOSS_MODELS
        ? process.env.BOSS_MODELS.split(",").map((s) => s.trim()).filter(Boolean)
        : [
            // convenience aliases (Claude Code resolves to the current release)
            "opus", "sonnet", "haiku",
            // pinned model IDs
            "claude-opus-5", "claude-sonnet-5", "claude-haiku-4-5",
            "claude-fable-5", "claude-opus-4-8", "claude-sonnet-4-5",
          ],
    });
  }

  if (p === "/api/check-dir") {
    return sendJSON(res, 200, checkDir(u.searchParams.get("path") || ""));
  }

  if (p === "/api/events") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    });
    res.write("retry: 3000\n\n");
    sseClients.add(res);
    req.on("close", () => sseClients.delete(res));
    return;
  }

  if (p === "/api/agents" && req.method === "POST") {
    const body = await readBody(req);
    const chk = checkDir(body.cwd || "");
    if (!chk.ok) return sendJSON(res, 400, { error: chk.reason + (chk.resolved ? `: ${chk.resolved}` : "") });
    const a = new Agent({
      name: body.name, label: body.label, description: body.description,
      cwd: chk.resolved, model: body.model, permissionMode: body.permissionMode,
    });
    agents.set(a.id, a);
    a.start(body.task || "");
    return sendJSON(res, 200, a.summary());
  }

  let m;
  if ((m = p.match(/^\/api\/agents\/([^/]+)$/))) {
    const a = agents.get(m[1]);
    if (req.method === "GET") {
      if (a) return sendJSON(res, 200, { ...a.summary(), transcript: a.transcript });
      return sendJSON(res, 404, { error: "no such bot" });
    }
    if (req.method === "DELETE") {
      if (a) { a.stop(); a.remove(); agents.delete(a.id); broadcast({ type: "agent_removed", id: a.id }); }
      return sendJSON(res, 200, { ok: true });
    }
  }

  if ((m = p.match(/^\/api\/agents\/([^/]+)\/message$/)) && req.method === "POST") {
    const a = agents.get(m[1]);
    const body = await readBody(req);
    if (!a) return sendJSON(res, 404, { error: "no such bot" });
    const ok = a.send(body.text || "");
    return sendJSON(res, ok ? 200 : 400, { ok });
  }

  if ((m = p.match(/^\/api\/agents\/([^/]+)\/stop$/)) && req.method === "POST") {
    const a = agents.get(m[1]);
    if (a) a.stop();
    return sendJSON(res, 200, { ok: true });
  }

  if ((m = p.match(/^\/api\/agents\/([^/]+)\/reset$/)) && req.method === "POST") {
    const a = agents.get(m[1]);
    if (!a) return sendJSON(res, 404, { error: "no such bot" });
    a.reset();
    return sendJSON(res, 200, { ok: true });
  }

  if ((m = p.match(/^\/api\/agents\/([^/]+)\/update$/)) && req.method === "POST") {
    const a = agents.get(m[1]);
    if (!a) return sendJSON(res, 404, { error: "no such bot" });
    const body = await readBody(req);
    if (body.cwd != null) {
      const chk = checkDir(body.cwd);
      if (!chk.ok) return sendJSON(res, 400, { error: chk.reason + (chk.resolved ? `: ${chk.resolved}` : "") });
      body.cwd = chk.resolved;
    }
    a.update(body);
    return sendJSON(res, 200, a.summary());
  }

  // --- static files ---
  let file = p === "/" ? "/index.html" : p;
  const fp = path.join(PUBLIC_DIR, path.normalize(file).replace(/^(\.\.[/\\])+/, ""));
  if (fp.startsWith(PUBLIC_DIR) && fs.existsSync(fp) && fs.statSync(fp).isFile()) {
    const ext = path.extname(fp);
    const types = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css" };
    res.writeHead(200, { "Content-Type": types[ext] || "text/plain", "Cache-Control": "no-cache" });
    return fs.createReadStream(fp).pipe(res);
  }

  res.writeHead(404);
  res.end("not found");
});

// load persisted bots (processes stay stopped until their next message)
function loadBots() {
  try {
    for (const f of fs.readdirSync(DATA_DIR)) {
      if (!f.endsWith(".json")) continue;
      try {
        const d = JSON.parse(fs.readFileSync(path.join(DATA_DIR, f), "utf8"));
        agents.set(d.id, Agent.fromDisk(d));
      } catch (e) {}
    }
  } catch (e) { /* no data dir yet */ }
}

// On first run only, create the default bots shipped in seed/. A marker file
// records that seeding happened, so deleting a seeded bot later does not
// resurrect it on the next start.
function seedDefaults() {
  if (fs.existsSync(SEED_MARKER)) return;
  let files = [];
  try { files = fs.readdirSync(SEED_DIR).filter((f) => f.endsWith(".json")); } catch (e) {}
  for (const f of files) {
    try {
      const seed = JSON.parse(fs.readFileSync(path.join(SEED_DIR, f), "utf8"));
      if (seed.cwd) { const chk = checkDir(seed.cwd); seed.cwd = chk.ok ? chk.resolved : HOME; }
      const a = new Agent(seed); // fresh id + resumable sessionId, no history
      a.status = "ready"; // idle and resumable; no process runs until first message
      agents.set(a.id, a);
      a.save();
    } catch (e) { console.error("seed failed for", f, e.message); }
  }
  try {
    fs.mkdirSync(path.dirname(SEED_MARKER), { recursive: true });
    fs.writeFileSync(SEED_MARKER, new Date().toISOString() + "\n");
  } catch (e) {}
}

server.listen(PORT, "127.0.0.1", () => {
  loadBots();
  seedDefaults();
  console.log(`\n  Bot Boss running at  http://localhost:${PORT}  (${agents.size} saved bot(s))\n`);
  // heartbeat to keep SSE alive
  setInterval(() => broadcast({ type: "ping", t: Date.now() }), 20000);
});
