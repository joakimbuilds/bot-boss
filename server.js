#!/usr/bin/env node
"use strict";
// Bot Boss - a Grok-Bot-style command center for Claude Code agents.
// Launcher model: the server owns background agents it spawns via
//   claude -p --input-format stream-json --output-format stream-json
// and also surfaces your manually-opened terminal tabs read-only via
//   claude agents --json  (+ their JSONL transcripts).
// Pure Node stdlib, no dependencies.

const http = require("http");
const fs = require("fs");
const path = require("path");
const os = require("os");
const crypto = require("crypto");
const { spawn, execFile } = require("child_process");

const PORT = Number(process.env.PORT || 4177);
const HOME = os.homedir();
const PROJECTS_DIR = path.join(HOME, ".claude", "projects");
const PUBLIC_DIR = path.join(__dirname, "public");
const CLAUDE_BIN = process.env.CLAUDE_BIN || "claude";

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
/** @type {Map<string, Agent>} */
const agents = new Map();
/** @type {Set<http.ServerResponse>} SSE clients */
const sseClients = new Set();

let peersCache = [];
let peersUpdatedAt = 0;

// ---------------------------------------------------------------------------
// Agent lifecycle
// ---------------------------------------------------------------------------
class Agent {
  constructor({ name, cwd, model, permissionMode }) {
    this.id = crypto.randomUUID().slice(0, 8);
    this.name = name || `agent-${this.id}`;
    this.cwd = cwd || HOME;
    this.model = model || "claude-sonnet-4-5";
    this.permissionMode = permissionMode || "acceptEdits";
    this.sessionId = null;
    this.status = "starting"; // starting | working | ready | blocked | dead
    this.createdAt = Date.now();
    this.lastActivity = Date.now();
    this.costUSD = 0;
    this.transcript = []; // {role, kind, text, tool, id, ts}
    this.proc = null;
    this._stdoutBuf = "";
    this._liveMsg = null; // coalescing buffer for a streaming assistant message
  }

  start(task) {
    const args = [
      "-p",
      "--input-format", "stream-json",
      "--output-format", "stream-json",
      "--include-partial-messages",
      "--verbose",
      "--model", this.model,
      "--permission-mode", this.permissionMode,
    ];
    const proc = spawn(CLAUDE_BIN, args, {
      cwd: this.cwd,
      env: process.env,
      stdio: ["pipe", "pipe", "pipe"],
    });
    this.proc = proc;
    this.status = "working";

    proc.stdout.on("data", (chunk) => this._onStdout(chunk));
    proc.stderr.on("data", (chunk) => {
      const s = chunk.toString();
      // stderr is mostly diagnostics; only surface if it looks like an error
      if (/error|not found|fatal/i.test(s)) {
        this._push({ role: "system", kind: "error", text: s.trim() });
      }
    });
    proc.on("exit", (code) => {
      this.status = "dead";
      this._push({ role: "system", kind: "exit", text: `agent exited (code ${code})` });
      broadcast({ type: "agent_status", id: this.id, status: this.status });
    });

    if (task && task.trim()) this.send(task);
    broadcast({ type: "agent_added", agent: this.summary() });
  }

  send(text) {
    if (!this.proc || this.status === "dead") return false;
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

  stop() {
    if (this.proc && this.status !== "dead") {
      try { this.proc.stdin.end(); } catch (e) {}
      try { this.proc.kill("SIGTERM"); } catch (e) {}
    }
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
            this._commitLive("text", block.text, ev.message.id);
          } else if (block.type === "thinking") {
            this._commitLive("thinking", block.thinking, ev.message.id);
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
              text: stringifyToolResult(block.content),
            });
          }
        }
        break;
      }

      case "result": {
        this._liveMsg = null;
        if (typeof ev.total_cost_usd === "number") this.costUSD += ev.total_cost_usd;
        this.status = "ready"; // waiting for your next message
        broadcast({ type: "agent_status", id: this.id, status: this.status, costUSD: this.costUSD });
        break;
      }

      case "rate_limit_event": {
        const info = ev.rate_limit_info || {};
        if (info.status && info.status !== "allowed") {
          this._push({ role: "system", kind: "error", text: `rate limit: ${info.status} (${info.overageDisabledReason || info.rateLimitType || ""})` });
        }
        break;
      }
    }
  }

  _appendLive(kind, text) {
    if (!this._liveMsg || this._liveMsg.kind !== kind) {
      this._liveMsg = { role: "assistant", kind, text: "", id: "live-" + Date.now(), ts: Date.now(), live: true };
      this.transcript.push(this._liveMsg);
    }
    this._liveMsg.text += text;
    broadcast({ type: "agent_delta", id: this.id, entry: this._liveMsg });
  }

  _commitLive(kind, text, msgId) {
    if (this._liveMsg && this._liveMsg.kind === kind && this._liveMsg.live) {
      this._liveMsg.text = text;
      this._liveMsg.live = false;
      this._liveMsg.id = msgId || this._liveMsg.id;
      broadcast({ type: "agent_delta", id: this.id, entry: this._liveMsg });
      this._liveMsg = null;
    } else {
      this._push({ role: "assistant", kind, text, id: msgId });
    }
  }

  _push(entry) {
    entry.ts = entry.ts || Date.now();
    entry.id = entry.id || crypto.randomUUID().slice(0, 8);
    this.transcript.push(entry);
    broadcast({ type: "agent_msg", id: this.id, entry });
  }

  summary() {
    return {
      id: this.id, name: this.name, cwd: this.cwd, model: this.model,
      permissionMode: this.permissionMode, status: this.status,
      sessionId: this.sessionId, createdAt: this.createdAt,
      lastActivity: this.lastActivity, costUSD: this.costUSD,
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

// ---------------------------------------------------------------------------
// Peers (read-only manual terminal tabs)
// ---------------------------------------------------------------------------
function refreshPeers() {
  execFile(CLAUDE_BIN, ["agents", "--json"], { maxBuffer: 4 * 1024 * 1024 }, (err, stdout) => {
    if (err) return;
    try {
      const list = JSON.parse(stdout);
      peersCache = list.map((s) => ({
        id: s.sessionId,
        sessionId: s.sessionId,
        name: s.name || s.sessionId.slice(0, 8),
        cwd: s.cwd,
        kind: s.kind,
        status: s.status || "idle", // idle | busy
        pid: s.pid,
        createdAt: s.startedAt,
        owned: false,
      }));
      peersUpdatedAt = Date.now();
      broadcast({ type: "peers", peers: peersCache });
    } catch (e) {}
  });
}

// find <sessionId>.jsonl anywhere under ~/.claude/projects
function findTranscriptFile(sessionId) {
  try {
    const dirs = fs.readdirSync(PROJECTS_DIR);
    for (const d of dirs) {
      const p = path.join(PROJECTS_DIR, d, sessionId + ".jsonl");
      if (fs.existsSync(p)) return p;
    }
  } catch (e) {}
  return null;
}

function readPeerTranscript(sessionId) {
  const file = findTranscriptFile(sessionId);
  if (!file) return [];
  const out = [];
  let raw;
  try { raw = fs.readFileSync(file, "utf8"); } catch (e) { return []; }
  for (const line of raw.split("\n")) {
    if (!line.trim()) continue;
    let o;
    try { o = JSON.parse(line); } catch (e) { continue; }
    const msg = o.message;
    if (!msg) continue;
    const role = msg.role || o.type;
    const content = msg.content;
    let text = "";
    let kind = "text";
    if (typeof content === "string") {
      text = content;
    } else if (Array.isArray(content)) {
      for (const b of content) {
        if (b.type === "text") text += b.text;
        else if (b.type === "thinking") { kind = "thinking"; text += b.thinking; }
        else if (b.type === "tool_use") { kind = "tool_use"; text += `${b.name}: ${summarizeToolInput(b.name, b.input)}`; }
        else if (b.type === "tool_result") { kind = "tool_result"; text += stringifyToolResult(b.content); }
      }
    }
    if (!text.trim()) continue;
    out.push({ role: role === "user" ? "user" : (kind === "tool_result" ? "tool" : "assistant"), kind, text: text.slice(0, 8000), ts: o.timestamp ? Date.parse(o.timestamp) : undefined });
  }
  return out.slice(-200);
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
      peers: peersCache,
      dirs: listDirs(),
      models: ["claude-opus-4-8", "claude-sonnet-4-5", "claude-haiku-4-5"],
    });
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
    const a = new Agent({
      name: body.name, cwd: body.cwd, model: body.model,
      permissionMode: body.permissionMode,
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
      // maybe a peer
      return sendJSON(res, 200, { owned: false, transcript: readPeerTranscript(m[1]) });
    }
    if (req.method === "DELETE") {
      if (a) { a.stop(); agents.delete(a.id); broadcast({ type: "agent_removed", id: a.id }); }
      return sendJSON(res, 200, { ok: true });
    }
  }

  if ((m = p.match(/^\/api\/agents\/([^/]+)\/message$/)) && req.method === "POST") {
    const a = agents.get(m[1]);
    const body = await readBody(req);
    if (!a) return sendJSON(res, 404, { error: "no such agent" });
    const ok = a.send(body.text || "");
    return sendJSON(res, ok ? 200 : 400, { ok });
  }

  if ((m = p.match(/^\/api\/agents\/([^/]+)\/stop$/)) && req.method === "POST") {
    const a = agents.get(m[1]);
    if (a) a.stop();
    return sendJSON(res, 200, { ok: true });
  }

  if ((m = p.match(/^\/api\/peers\/([^/]+)\/transcript$/))) {
    return sendJSON(res, 200, { transcript: readPeerTranscript(m[1]) });
  }

  // --- static files ---
  let file = p === "/" ? "/index.html" : p;
  const fp = path.join(PUBLIC_DIR, path.normalize(file).replace(/^(\.\.[/\\])+/, ""));
  if (fp.startsWith(PUBLIC_DIR) && fs.existsSync(fp) && fs.statSync(fp).isFile()) {
    const ext = path.extname(fp);
    const types = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css" };
    res.writeHead(200, { "Content-Type": types[ext] || "text/plain" });
    return fs.createReadStream(fp).pipe(res);
  }

  res.writeHead(404);
  res.end("not found");
});

server.listen(PORT, "127.0.0.1", () => {
  console.log(`\n  Bot Boss running at  http://localhost:${PORT}\n`);
  refreshPeers();
  setInterval(refreshPeers, 4000);
  // heartbeat to keep SSE alive
  setInterval(() => broadcast({ type: "ping", t: Date.now() }), 20000);
});
