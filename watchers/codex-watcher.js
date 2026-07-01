'use strict';
/**
 * Codex session watcher — tails ~/.codex/sessions/**\/*.jsonl
 * and forwards events to the obs-dashboard at http://localhost:3000
 */

const fs   = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const os   = require('node:os');

const SESSIONS_DIR  = path.join(os.homedir(), '.codex', 'sessions');
const DASHBOARD     = { host: 'localhost', port: 3000, path: '/api/events' };
const POLL_NEW_FILES_MS = 2000;

// Track byte position per file so we only read new lines
const filePositions = new Map(); // filePath → byteOffset
const activeSessions = new Map(); // sessionId → { model, context_window, cwd }

function postEvent(agent, session_id, type, data) {
  const body = JSON.stringify({
    agent, session_id, type,
    timestamp: new Date().toISOString(),
    data,
  });
  const req = http.request({
    ...DASHBOARD, method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
  }, () => {});
  req.on('error', () => {});
  req.end(body);
}

function processLine(filePath, raw) {
  let obj;
  try { obj = JSON.parse(raw); } catch { return; }

  const ts = obj.timestamp;
  const type = obj.type;
  const p = obj.payload || {};

  // Derive session_id from file path (rollout-<date>T<time>-<uuid>.jsonl)
  const basename = path.basename(filePath, '.jsonl');
  // Extract UUID portion: last segment after final hyphen-group
  const sessionId = basename.replace(/^rollout-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-/, '');

  if (type === 'session_meta') {
    const meta = {
      model: p.model,
      cwd: p.cwd,
      label: p.cwd ? p.cwd.split('/').pop() : undefined,
      cli_version: p.cli_version,
      model_context_window: p.model_context_window,
    };
    activeSessions.set(sessionId, meta);
    postEvent('codex', sessionId, 'session_start', meta);
    return;
  }

  if (type === 'turn_context') {
    const sess = activeSessions.get(sessionId) || {};
    const updated = { ...sess, model: p.model, context_window_size: parseInt(p.model_context_window||0)||null };
    activeSessions.set(sessionId, updated);
    postEvent('codex', sessionId, 'turn_context', {
      model: p.model,
      context_window_size: parseInt(p.model_context_window||0)||null,
      cwd: p.cwd,
      reasoning_effort: p.effort,
    });
    return;
  }

  if (type === 'event_msg') {
    const sub = p.type;

    if (sub === 'task_started') {
      postEvent('codex', sessionId, 'turn_start', {
        turn_id: p.turn_id,
        context_window_size: parseInt(p.model_context_window||0)||null,
      });
      return;
    }

    if (sub === 'task_complete') {
      postEvent('codex', sessionId, 'turn_end', {
        turn_id: p.turn_id,
        duration_ms: p.duration_ms,
        time_to_first_token_ms: p.time_to_first_token_ms,
      });
      return;
    }

    if (sub === 'token_count') {
      const total = p.info?.total_token_usage || {};
      const last  = p.info?.last_token_usage  || {};
      const sess  = activeSessions.get(sessionId) || {};
      postEvent('codex', sessionId, 'after_provider_response', {
        model: sess.model,
        context_window_size: sess.context_window_size,
        usage: {
          input_tokens:         last.input_tokens || 0,
          output_tokens:        last.output_tokens || 0,
          cache_read_input_tokens: last.cached_input_tokens || 0,
          reasoning_tokens:     last.reasoning_output_tokens || 0,
        },
        cumulative: {
          input_tokens:         total.input_tokens || 0,
          output_tokens:        total.output_tokens || 0,
          cached_input_tokens:  total.cached_input_tokens || 0,
          reasoning_tokens:     total.reasoning_output_tokens || 0,
          total_tokens:         total.total_tokens || 0,
        },
      });
      return;
    }

    if (sub === 'mcp_tool_call_end') {
      // Emit start (increments tool_call_count) then end (captures duration)
      postEvent('codex', sessionId, 'tool_execution_start', {
        tool_name: p.invocation?.tool,
        tool_call_id: p.call_id,
        server: p.invocation?.server,
      });
      postEvent('codex', sessionId, 'tool_execution_end', {
        tool_name: p.invocation?.tool,
        tool_call_id: p.call_id,
        duration_ms: p.duration,
        server: p.invocation?.server,
        args: p.invocation?.arguments,
      });
      return;
    }

    if (sub === 'user_message') {
      postEvent('codex', sessionId, 'turn_start', {
        user_prompt: p.message ? String(p.message).slice(0, 300) : undefined,
      });
      return;
    }

    if (sub === 'agent_message') {
      postEvent('codex', sessionId, 'agent_message', {
        phase: p.phase,
        text: p.message ? String(p.message).slice(0, 300) : undefined,
      });
      return;
    }
  }
}

function tailFile(filePath) {
  const stat = fs.statSync(filePath);
  const offset = filePositions.get(filePath) ?? 0;
  if (stat.size <= offset) return;

  const stream = fs.createReadStream(filePath, { start: offset, encoding: 'utf8' });
  let buf = '';
  stream.on('data', chunk => { buf += chunk; });
  stream.on('end', () => {
    const lines = buf.split('\n');
    // Last segment may be a partial line if file was mid-write; don't consume it yet
    const partial = lines[lines.length - 1];
    if (partial.trim()) lines.pop(); else lines.pop();
    const consumed = buf.length - partial.length;
    filePositions.set(filePath, offset + consumed);
    for (const line of lines) {
      const trimmed = line.trim();
      if (trimmed) processLine(filePath, trimmed);
    }
  });
  stream.on('error', () => {});
}

function scanDirectory(dir) {
  if (!fs.existsSync(dir)) return;
  try {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) scanDirectory(full);
      else if (entry.isFile() && entry.name.endsWith('.jsonl')) tailFile(full);
    }
  } catch {}
}

// Initial scan of all existing sessions (for any live/recent session)
scanDirectory(SESSIONS_DIR);

// Poll for new files and new lines in known files
setInterval(() => scanDirectory(SESSIONS_DIR), POLL_NEW_FILES_MS);

console.log(`Codex watcher started — watching ${SESSIONS_DIR}`);
