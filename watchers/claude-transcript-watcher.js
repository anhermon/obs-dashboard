'use strict';
/**
 * Claude Code transcript watcher
 *
 * Reads ~/.claude/projects/**\/*.jsonl and forwards token-usage data to the
 * obs-dashboard at http://localhost:3000.
 *
 * On startup it back-fills ALL historical transcripts (batched, 50 ms delay).
 * Then it polls every 3 s for new turns in live sessions.
 *
 * State is persisted to .transcript-watcher-state.json next to this file so
 * that restarts never double-count tokens.
 *
 * Path taxonomy handled:
 *   <projDir>/<uuid>.jsonl                         -> claude-code
 *   <projDir>/<uuid>/subagents/<agent>.jsonl        -> claude-code:subagent
 *   <projDir>/<uuid>/subagents/workflows/wf_ID/agent -> claude-code:workflow
 *   paperclip projDir, any depth                    -> cowork
 */

const fs   = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const os   = require('node:os');

// ─── Constants ────────────────────────────────────────────────────────────────

const PROJECTS_DIR      = path.join(os.homedir(), '.claude', 'projects');
const DASHBOARD         = { host: 'localhost', port: 3000, path: '/api/events' };
const POLL_INTERVAL_MS  = 3000;
const BATCH_SIZE        = 100;   // events per POST during backfill (server max = 100)
const BATCH_DELAY_MS    = 10;    // ms between backfill batches
const STATE_FILE        = path.join(__dirname, '.transcript-watcher-state.json');
const UUID_RE           = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// ─── In-memory state ──────────────────────────────────────────────────────────

/** filePath → number (byte offset last successfully read + posted) */
const filePositions  = new Map();

/**
 * `${filePath}:${messageUuid}` — prevents double-posting a single assistant
 * message even if the watcher restarts and re-reads a partial chunk.
 */
const seenMessages   = new Set();

/**
 * sessionId → metadata collected from the transcript.
 * Built during processing; NOT persisted (rebuilt on demand from file content).
 */
const sessionMeta    = new Map();

/**
 * sessionIds for which we have already posted a session_start event.
 * Persisted to avoid re-setting working_dir with stale / synthetic data on
 * watcher restarts.
 */
const postedSessions = new Set();

// ─── State persistence ────────────────────────────────────────────────────────

function loadState() {
  try {
    const raw  = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    const fps  = raw.filePositions  || {};
    const sms  = raw.seenMessages   || [];
    const ps   = raw.postedSessions || [];
    for (const [k, v] of Object.entries(fps)) filePositions.set(k, v);
    for (const k of sms) seenMessages.add(k);
    for (const k of ps)  postedSessions.add(k);
    console.log(
      `State loaded: ${filePositions.size} files, ` +
      `${seenMessages.size} messages seen, ${postedSessions.size} sessions started`
    );
  } catch {
    // No state file → fresh start, process all history
  }
}

function saveState() {
  try {
    const state = {
      version:        2,
      savedAt:        new Date().toISOString(),
      filePositions:  Object.fromEntries(filePositions),
      seenMessages:   [...seenMessages],
      postedSessions: [...postedSessions],
    };
    fs.writeFileSync(STATE_FILE, JSON.stringify(state), 'utf8');
  } catch (e) {
    console.error('Failed to save state:', e.message);
  }
}

// ─── Path / identity helpers ──────────────────────────────────────────────────

/** First component of the path relative to PROJECTS_DIR */
function getProjDirName(filePath) {
  const rel = path.relative(PROJECTS_DIR, filePath);
  return rel.split('/')[0] || '';
}

/**
 * Derive the agent name from the file's location within the projects tree.
 *
 * Priority:
 *   1. Path contains /subagents/ → subagent or workflow variant
 *   2. Project-dir name contains "paperclip" or "cowork" → cowork
 *   3. Everything else → claude-code
 */
function deriveAgent(filePath) {
  const rel     = path.relative(PROJECTS_DIR, filePath);
  const parts   = rel.split('/');
  const projDir = parts[0] || '';

  // Subagent paths: <projDir>/<session-dir>/subagents/[workflows/wf_*/]<file>
  if (parts.includes('subagents')) {
    const subIdx = parts.indexOf('subagents');
    const below  = parts.slice(subIdx + 1).join('/');
    // workflow subagents live under subagents/workflows/wf_<id>/
    if (below.includes('workflows') || below.startsWith('wf_') || below.includes('/wf_')) {
      return 'claude-code:workflow';
    }
    return 'claude-code:subagent';
  }

  if (projDir.includes('paperclip') || projDir.includes('cowork')) {
    return 'cowork';
  }

  return 'claude-code';
}

/**
 * Decode an encoded project-directory name into a human-readable label.
 *
 * Claude Code encodes the filesystem path as:
 *   /  →  -   (single dash)
 *   /. →  --  (double dash, for hidden directories)
 *
 * Strategy:
 *   1. Strip the encoded home-directory prefix.
 *   2. Take the last 2 dash-delimited tokens of what remains as the label.
 *   This gives "dark-factory" for "-Users-alice-dev-dark-factory", etc.
 */
function labelFromProjDir(projDir) {
  if (!projDir) return 'unknown';
  if (projDir.includes('paperclip')) return 'paperclip';
  if (projDir.includes('cowork'))    return 'cowork';

  // Build encoded home prefix: /Users/alice → 'Users-alice'
  const homeEncoded = os.homedir().replace(/^\//, '').replace(/\//g, '-');

  // Strip leading '-' then the home prefix
  let rest = projDir.replace(/^-/, '');
  if (rest.startsWith(homeEncoded)) {
    rest = rest.slice(homeEncoded.length).replace(/^-/, '');
  }

  if (!rest) return 'home';

  // Handle hidden-dir markers: '--foo' encodes '/.foo'. Preserve the joined
  // token to avoid over-splitting.
  // e.g. '--paperclip-instances' → already caught above
  // For regular paths: split on '-' and take the last 2 meaningful tokens.
  const tokens = rest.split('-').filter(Boolean);
  if (tokens.length === 0) return 'unknown';
  if (tokens.length <= 2)  return tokens.join('-');
  return tokens.slice(-2).join('-');
}

/** Session ID = the filename without the .jsonl extension. */
function sessionIdFromFile(filePath) {
  return path.basename(filePath, '.jsonl');
}

// ─── HTTP ─────────────────────────────────────────────────────────────────────

function postBatch(events) {
  return new Promise((resolve) => {
    const body = JSON.stringify(events);
    const req  = http.request(
      {
        ...DASHBOARD,
        method: 'POST',
        headers: {
          'Content-Type':   'application/json',
          'Content-Length': Buffer.byteLength(body),
        },
      },
      (res) => { res.resume(); resolve(true); }
    );
    req.on('error', (err) => {
      console.error('POST /api/events error:', err.message);
      resolve(false);
    });
    req.end(body);
  });
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Post events in batches of BATCH_SIZE with BATCH_DELAY_MS between them.
 * Returns the number of events successfully sent.
 */
async function flushEvents(events) {
  if (!events.length) return 0;
  let total = 0;
  for (let i = 0; i < events.length; i += BATCH_SIZE) {
    const batch = events.slice(i, i + BATCH_SIZE);
    const ok    = await postBatch(batch);
    if (ok) total += batch.length;
    if (i + BATCH_SIZE < events.length) await sleep(BATCH_DELAY_MS);
  }
  return total;
}

// ─── File reading ─────────────────────────────────────────────────────────────

/**
 * Read bytes from filePath starting at `fromOffset`.
 * Returns full lines and the new byte offset (does NOT include any trailing
 * partial line that the writer may still be appending).
 */
function readChunk(filePath, fromOffset) {
  try {
    const stat = fs.statSync(filePath);
    if (stat.size <= fromOffset) return { lines: [], newOffset: fromOffset };

    const fd  = fs.openSync(filePath, 'r');
    const buf = Buffer.alloc(stat.size - fromOffset);
    fs.readSync(fd, buf, 0, buf.length, fromOffset);
    fs.closeSync(fd);

    const text  = buf.toString('utf8');
    const raw   = text.split('\n');

    // The last element may be an incomplete line being written right now.
    // Pop it unconditionally (if non-empty it's partial; if empty it's noise).
    const partial = raw.pop() ?? '';

    const consumed = Buffer.byteLength(text.slice(0, text.length - partial.length), 'utf8');
    const newOffset = fromOffset + consumed;

    const lines = raw.filter((l) => l.trim());
    return { lines, newOffset };
  } catch {
    return { lines: [], newOffset: fromOffset };
  }
}

// ─── Event building ───────────────────────────────────────────────────────────

/**
 * Build dashboard events from a set of transcript lines.
 *
 * @param {string}   filePath    - absolute path to the .jsonl file
 * @param {string[]} lines       - lines to process
 * @param {boolean}  twoPass     - if true, scan for metadata before events
 *                                 (used when reading a file from the beginning)
 * @returns {object[]}           - array of dashboard event objects
 */
function buildEventsFromLines(filePath, lines, twoPass) {
  const projDir   = getProjDirName(filePath);
  const agent     = deriveAgent(filePath);
  const sessionId = sessionIdFromFile(filePath);

  // Retrieve or initialise per-session metadata
  const meta = sessionMeta.get(sessionId) || {
    agent,
    label:  null,
    cwd:    null,
    model:  null,
  };

  // ── Pass 1: collect metadata from the full chunk (startup only) ─────────────
  if (twoPass) {
    for (const line of lines) {
      let obj;
      try { obj = JSON.parse(line); } catch { continue; }
      if (!obj || !obj.type) continue;

      if (obj.type === 'ai-title' && obj.title && !meta.label) {
        meta.label = obj.title;
      }
      if (obj.cwd && !meta.cwd) {
        meta.cwd = obj.cwd;
      }
      if (!meta.model) {
        // Pull model from assistant message or top-level field
        if (obj.message?.model) meta.model = obj.message.model;
      }
    }
  }

  // Fall back label derivation from directory name
  if (!meta.label) meta.label = labelFromProjDir(projDir);

  // Effective cwd for the server's label-extraction logic:
  //   server reads label = data.cwd.split('/').pop()
  //   Use the real cwd when available; otherwise synthesise a path whose
  //   basename is our computed label so the server stores the right thing.
  const effectiveCwd = meta.cwd || (meta.label ? `/~/${meta.label}` : undefined);

  const events = [];

  // ── Pass 2 (or single pass for polling): collect token events ────────────────
  for (const line of lines) {
    let obj;
    try { obj = JSON.parse(line); } catch { continue; }
    if (!obj || !obj.type) continue;

    // Keep picking up metadata in polling mode (single-pass)
    if (!twoPass) {
      if (obj.type === 'ai-title' && obj.title) {
        meta.label = obj.title;
        // effectiveCwd won't update here since it's a const above, but future
        // events in this poll batch will use the updated meta for new sessions.
      }
      if (obj.cwd && !meta.cwd) meta.cwd = obj.cwd;
      if (!meta.model && obj.message?.model) meta.model = obj.message.model;
    }

    // Only process assistant messages with usage data
    if (obj.type !== 'assistant') continue;

    const usage   = obj.message?.usage;
    const msgUuid = obj.uuid;
    if (!usage || !msgUuid) continue;

    // Deduplication: skip messages already posted
    const key = `${filePath}:${msgUuid}`;
    if (seenMessages.has(key)) continue;
    seenMessages.add(key);

    const msgModel = obj.message?.model || meta.model;
    const ts       = obj.timestamp || new Date().toISOString();

    // Emit session_start exactly once per session (first token event).
    // We skip re-posting if the session was already started in a previous run
    // so we don't corrupt the working_dir column with synthetic data.
    if (!postedSessions.has(sessionId)) {
      events.push({
        agent,
        session_id: sessionId,
        type:       'session_start',
        timestamp:  ts,
        data: {
          model: msgModel || meta.model,
          // Only include cwd when we have something meaningful to say;
          // the server derives the run label from cwd.split('/').pop().
          ...(effectiveCwd ? { cwd: effectiveCwd } : {}),
        },
      });
      postedSessions.add(sessionId);
    }

    // Token usage event — this is the core payload the dashboard needs.
    events.push({
      agent,
      session_id: sessionId,
      type:       'after_provider_response',
      timestamp:  ts,
      data: {
        model: msgModel,
        usage: {
          input_tokens:                usage.input_tokens                || 0,
          output_tokens:               usage.output_tokens               || 0,
          cache_read_input_tokens:     usage.cache_read_input_tokens     ||
                                       usage.cached_input_tokens         || 0,
          cache_creation_input_tokens: usage.cache_creation_input_tokens || 0,
          reasoning_tokens:            usage.reasoning_tokens            ||
                                       usage.reasoning_output_tokens     || 0,
        },
      },
    });
  }

  sessionMeta.set(sessionId, meta);
  return events;
}

// ─── Directory scanning ────────────────────────────────────────────────────────

/** Recursively collect every *.jsonl file under `dir`. */
function findJsonlFiles(dir, result = []) {
  if (!fs.existsSync(dir)) return result;
  try {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        findJsonlFiles(full, result);
      } else if (entry.isFile() && entry.name.endsWith('.jsonl')) {
        result.push(full);
      }
    }
  } catch { /* permission errors etc. */ }
  return result;
}

// ─── Startup backfill ─────────────────────────────────────────────────────────

async function startupBackfill() {
  console.log(`Scanning ${PROJECTS_DIR} …`);
  const files = findJsonlFiles(PROJECTS_DIR);
  console.log(`Found ${files.length} transcript files`);

  const allEvents = [];
  let skipped = 0;

  for (const filePath of files) {
    try {
      const savedOffset = filePositions.get(filePath) ?? 0;
      const { lines, newOffset } = readChunk(filePath, savedOffset);

      if (!lines.length) { skipped++; continue; }

      // Use two-pass (metadata scan before events) only when reading from the
      // beginning of a file so we capture ai-title even when it appears after
      // the first assistant turn.
      const isFullRead = savedOffset === 0;
      const events     = buildEventsFromLines(filePath, lines, isFullRead);

      allEvents.push(...events);
      filePositions.set(filePath, newOffset);
    } catch (e) {
      console.error(`Error reading ${filePath}:`, e.message);
    }
  }

  console.log(
    `Backfill: ${allEvents.length} events from ${files.length - skipped} files ` +
    `(${skipped} already up-to-date)`
  );

  if (allEvents.length) {
    const posted = await flushEvents(allEvents);
    console.log(`Backfill: posted ${posted}/${allEvents.length} events`);
  }

  saveState();
  console.log('Backfill complete — starting live poll');
}

// ─── Live poll ────────────────────────────────────────────────────────────────

async function poll() {
  let files;
  try { files = findJsonlFiles(PROJECTS_DIR); } catch { return; }

  const newEvents = [];

  for (const filePath of files) {
    const savedOffset = filePositions.get(filePath) ?? 0;
    const { lines, newOffset } = readChunk(filePath, savedOffset);

    if (!lines.length) continue;

    // Brand-new file discovered during polling: do a two-pass read
    const isFirstRead = !filePositions.has(filePath) || savedOffset === 0;
    const events      = buildEventsFromLines(filePath, lines, isFirstRead);

    if (events.length) newEvents.push(...events);
    filePositions.set(filePath, newOffset);
  }

  if (newEvents.length) {
    const posted = await flushEvents(newEvents);
    if (posted) {
      console.log(`Poll: posted ${posted} new event(s)`);
      saveState();
    }
  }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log('Claude transcript watcher starting…');
  console.log(`  Projects dir : ${PROJECTS_DIR}`);
  console.log(`  Dashboard    : http://${DASHBOARD.host}:${DASHBOARD.port}`);
  console.log(`  State file   : ${STATE_FILE}`);

  if (!fs.existsSync(PROJECTS_DIR)) {
    console.error(`Projects directory not found: ${PROJECTS_DIR}`);
    process.exit(1);
  }

  loadState();
  await startupBackfill();

  // Save state periodically as a safety net between polls
  setInterval(saveState, 60_000);
  setInterval(poll, POLL_INTERVAL_MS);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
