# Bot Boss

A Grok-Bot-style web command center for Claude Code agents, running on your Mac.

> Unofficial, community-built tool. Not affiliated with, endorsed by, or
> sponsored by Anthropic. "Claude" and "Claude Code" are trademarks of
> Anthropic, used here only to describe compatibility.

Messaging-app layout: a sidebar of agents (like chat contacts), a chat pane,
and a details panel. **Launcher model**: Bot Boss owns the agents it spawns,
so read and reply are fully clean. Your manually-opened terminal tabs also show
up, read-only.

## Run

```sh
git clone https://github.com/<you>/bot-boss.git
cd bot-boss
npm start          # or: node server.js
```

Then open http://localhost:4177

No dependencies (pure Node stdlib). Requires the `claude` CLI on your PATH.

## How it works

- **Owned agents** are `claude -p --input-format stream-json --output-format
  stream-json` child processes. Bot Boss writes your messages to stdin and
  renders the streamed events (thinking, text, tool calls) live. Each agent is
  multi-turn: it stays alive waiting for your next message.
- **Terminal tabs** (read-only) come from `claude agents --json`; their
  transcripts are read from `~/.claude/projects/*/<sessionId>.jsonl`.
- Sessions that need you (`working` / `blocked`) sort to the top of the sidebar.

## New Agent

Click **+ New Agent**: pick a working dir, model, permission mode, and a first
task. For agents that should run tools without prompting, use
`bypassPermissions`. `default` will stall on the first tool call (nothing can
answer the prompt in headless mode).

## Config

- `PORT` (default 4177)
- `CLAUDE_BIN` (default `claude`)
- `BOSS_DIRS` (colon-separated absolute paths for the New Agent dir picker;
  defaults to the immediate subdirectories of `$HOME`)

## Notes

- Dispatched agents consume your Claude quota, same as any session.
- This is the **launcher** design: it does not inject input into your existing
  manual tabs (that path needs fragile AppleScript / private daemon sockets).
  To control a session from here, dispatch it from here.
