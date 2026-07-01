'use strict';

const express = require('express');
const { WebSocketServer } = require('ws');
const { DatabaseSync } = require('node:sqlite');
const http = require('node:http');
const path = require('node:path');
const crypto = require('node:crypto');

const PORT = 3000;

// --- Database ---
const db = new DatabaseSync(path.join(__dirname, 'obs.db'));
db.exec('PRAGMA journal_mode = WAL');
db.exec('PRAGMA foreign_keys = ON');

db.exec(`
  CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    agent TEXT NOT NULL,
    label TEXT,
    model TEXT,
    started_at INTEGER NOT NULL,
    ended_at INTEGER,
    duration_ms INTEGER,
    turn_count INTEGER DEFAULT 0,
    event_count INTEGER DEFAULT 0,
    tool_call_count INTEGER DEFAULT 0,
    input_tokens INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    cache_read_tokens INTEGER DEFAULT 0,
    cache_write_tokens INTEGER DEFAULT 0,
    reasoning_tokens INTEGER DEFAULT 0,
    cost_usd REAL DEFAULT 0,
    context_window_size INTEGER,
    status TEXT DEFAULT 'running',
    system_prompt TEXT,
    working_dir TEXT
  );

  CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    type TEXT NOT NULL,
    ts INTEGER NOT NULL,
    seq INTEGER NOT NULL DEFAULT 0,
    data TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(id)
  );

  CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id, seq);
  CREATE INDEX IF NOT EXISTS idx_runs_started_at ON runs(started_at DESC);
`);

// Add new columns to existing DBs without dropping data
const existingCols = db.prepare("PRAGMA table_info(runs)").all().map(r => r.name);
const newCols = {
  reasoning_tokens: 'INTEGER DEFAULT 0',
  context_window_size: 'INTEGER',
};
for (const [col, def] of Object.entries(newCols)) {
  if (!existingCols.includes(col)) {
    db.exec(`ALTER TABLE runs ADD COLUMN ${col} ${def}`);
  }
}

const stmts = {
  upsertRun: db.prepare(`
    INSERT INTO runs (id, agent, label, model, started_at, status, system_prompt, working_dir)
    VALUES (?, ?, ?, ?, ?, 'running', ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      label = COALESCE(excluded.label, label),
      model = COALESCE(excluded.model, model),
      system_prompt = COALESCE(excluded.system_prompt, system_prompt),
      working_dir = COALESCE(excluded.working_dir, working_dir)
  `),
  endRun:            db.prepare(`UPDATE runs SET ended_at = ?, duration_ms = ? - started_at, status = 'done' WHERE id = ?`),
  updateModel:       db.prepare(`UPDATE runs SET model = ? WHERE id = ?`),
  updateSystemPrompt:db.prepare(`UPDATE runs SET system_prompt = ? WHERE id = ?`),
  updateWorkingDir:  db.prepare(`UPDATE runs SET working_dir = ?, label = COALESCE(label, ?) WHERE id = ?`),
  updateContextWin:  db.prepare(`UPDATE runs SET context_window_size = ? WHERE id = ? AND (context_window_size IS NULL OR context_window_size < ?)`),
  addTokens: db.prepare(`
    UPDATE runs SET
      input_tokens       = input_tokens + ?,
      output_tokens      = output_tokens + ?,
      cache_read_tokens  = cache_read_tokens + ?,
      cache_write_tokens = cache_write_tokens + ?,
      reasoning_tokens   = reasoning_tokens + ?,
      cost_usd           = cost_usd + ?
    WHERE id = ?
  `),
  incrementTurns:    db.prepare(`UPDATE runs SET turn_count = turn_count + 1 WHERE id = ?`),
  incrementToolCalls:db.prepare(`UPDATE runs SET tool_call_count = tool_call_count + 1 WHERE id = ?`),
  incrementEvents:   db.prepare(`UPDATE runs SET event_count = event_count + 1 WHERE id = ?`),
  insertEvent:       db.prepare(`INSERT INTO events (run_id, type, ts, seq, data) VALUES (?, ?, ?, ?, ?)`),
  getRuns:           db.prepare(`SELECT * FROM runs ORDER BY started_at DESC LIMIT 200`),
  getRun:            db.prepare(`SELECT * FROM runs WHERE id = ?`),
  getEvents:         db.prepare(`SELECT * FROM events WHERE run_id = ? ORDER BY seq ASC`),
  getRunSeq:         db.prepare(`SELECT COALESCE(MAX(seq), -1) + 1 AS next_seq FROM events WHERE run_id = ?`),
};

// --- Analytics queries ---
const analyticsStmts = {
  byAgent: db.prepare(`
    SELECT agent,
      COUNT(*) AS run_count,
      COALESCE(SUM(turn_count),0) AS total_turns,
      COALESCE(SUM(tool_call_count),0) AS total_tools,
      COALESCE(SUM(input_tokens),0) AS total_input,
      COALESCE(SUM(output_tokens),0) AS total_output,
      COALESCE(SUM(cache_read_tokens),0) AS total_cached,
      COALESCE(SUM(reasoning_tokens),0) AS total_reasoning,
      COALESCE(SUM(cost_usd),0) AS total_cost,
      COALESCE(AVG(NULLIF(duration_ms,0)),0) AS avg_duration_ms
    FROM runs GROUP BY agent ORDER BY total_cost DESC
  `),
  topTools: db.prepare(`
    SELECT json_extract(data,'$.tool_name') AS tool_name,
      COUNT(*) AS call_count,
      json_extract(data,'$.agent') AS agent
    FROM events
    WHERE type IN ('tool_execution_start','tool_call_start','pre_tool_call')
      AND json_extract(data,'$.tool_name') IS NOT NULL
    GROUP BY tool_name ORDER BY call_count DESC LIMIT 30
  `),
  toolDurations: db.prepare(`
    SELECT json_extract(data,'$.tool_name') AS tool_name,
      COUNT(*) AS call_count,
      COALESCE(AVG(CAST(json_extract(data,'$.duration_ms') AS REAL)),0) AS avg_dur_ms,
      COALESCE(SUM(CAST(json_extract(data,'$.duration_ms') AS REAL)),0) AS total_dur_ms
    FROM events
    WHERE type IN ('tool_execution_end','tool_call_end','post_tool_call')
      AND json_extract(data,'$.tool_name') IS NOT NULL
    GROUP BY tool_name ORDER BY total_dur_ms DESC LIMIT 30
  `),
  recentRuns: db.prepare(`SELECT * FROM runs ORDER BY started_at DESC LIMIT 20`),
};

// --- Event processor ---
function processEvent(raw) {
  const { agent, session_id, type, timestamp, data = {} } = raw;
  const ts = timestamp ? new Date(timestamp).getTime() : Date.now();
  const run_id = session_id || crypto.randomUUID();

  const existing = stmts.getRun.get(run_id);
  if (!existing) {
    stmts.upsertRun.run(
      run_id,
      agent || 'unknown',
      data.label || data.cwd ? (data.cwd||'').split('/').pop()||run_id.slice(0,8) : run_id.slice(0,8),
      data.model || null,
      ts,
      data.system_prompt || null,
      data.cwd || null
    );
  }

  const seq = stmts.getRunSeq.get(run_id).next_seq;
  stmts.insertEvent.run(run_id, type, ts, seq, JSON.stringify(data));
  stmts.incrementEvents.run(run_id);

  if (type === 'session_start' || type === 'agent_start') {
    if (data.model)         stmts.updateModel.run(data.model, run_id);
    if (data.system_prompt) stmts.updateSystemPrompt.run(data.system_prompt, run_id);
    if (data.cwd)           stmts.updateWorkingDir.run(data.cwd, data.cwd, run_id);
    if (data.context_window_size) stmts.updateContextWin.run(data.context_window_size, run_id, data.context_window_size);
  }

  if (type === 'turn_start' || type === 'agent_message')
    stmts.incrementTurns.run(run_id);
  if (type === 'turn_context' && data.context_window_size)
    stmts.updateContextWin.run(data.context_window_size, run_id, data.context_window_size);

  if (type === 'tool_execution_start' || type === 'tool_call_start' || type === 'pretooluse')
    stmts.incrementToolCalls.run(run_id);

  // model_select: record the model switch
  if (type === 'model_select' && data.model)
    stmts.updateModel.run(data.model, run_id);

  // before_provider_request: use approx tokens for context window if no exact count yet
  if (type === 'before_provider_request' && data.approx_input_tokens)
    stmts.updateContextWin.run(data.approx_input_tokens, run_id, data.approx_input_tokens);

  if (['after_provider_response','post_api_request','llm_response'].includes(type)) {
    const usage = data.usage || {};
    stmts.addTokens.run(
      usage.input_tokens || usage.prompt_tokens || 0,
      usage.output_tokens || usage.completion_tokens || 0,
      usage.cache_read_input_tokens || usage.cached_input_tokens || 0,
      usage.cache_creation_input_tokens || 0,
      usage.reasoning_tokens || usage.reasoning_output_tokens || 0,
      data.cost_usd || data.cost || 0,
      run_id
    );
    if (data.model) stmts.updateModel.run(data.model, run_id);
    if (data.context_window_size)
      stmts.updateContextWin.run(data.context_window_size, run_id, data.context_window_size);
  }

  if (type === 'session_end' || type === 'agent_end')
    stmts.endRun.run(ts, ts, run_id);

  return { run_id, event: { id: seq, run_id, type, ts, seq, data }, run: stmts.getRun.get(run_id) };
}

// --- HTTP + WebSocket ---
const app = express();
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

const wsClients = new Set();
function broadcast(msg) {
  const str = typeof msg === 'string' ? msg : JSON.stringify(msg);
  for (const ws of wsClients) if (ws.readyState === 1) ws.send(str);
}

app.post('/api/events', (req, res) => {
  try {
    const items = Array.isArray(req.body) ? req.body : [req.body];
    const results = items.map(processEvent);
    broadcast({ type: 'batch', events: results });
    res.json({ ok: true, processed: results.length });
  } catch (err) {
    console.error('Event error:', err.message);
    res.status(400).json({ error: err.message });
  }
});

app.get('/api/runs', (_req, res) => res.json(stmts.getRuns.all()));

app.get('/api/runs/:id', (req, res) => {
  const run = stmts.getRun.get(req.params.id);
  if (!run) return res.status(404).json({ error: 'not found' });
  res.json(run);
});

app.get('/api/runs/:id/events', (req, res) => {
  const evs = stmts.getEvents.all(req.params.id).map(e => ({
    ...e, data: e.data ? JSON.parse(e.data) : {}
  }));
  res.json(evs);
});

app.get('/api/analytics', (_req, res) => {
  res.json({
    by_agent:       analyticsStmts.byAgent.all(),
    top_tools:      analyticsStmts.topTools.all(),
    tool_durations: analyticsStmts.toolDurations.all(),
    recent_runs:    analyticsStmts.recentRuns.all(),
  });
});

app.delete('/api/runs', (_req, res) => {
  db.exec('DELETE FROM events; DELETE FROM runs;');
  broadcast({ type: 'clear' });
  res.json({ ok: true });
});

// --- rtk token savings ---
const rtkDbPath = path.join(require('os').homedir(), 'Library', 'Application Support', 'rtk', 'history.db');
app.get('/api/rtk', (_req, res) => {
  try {
    const rtkDb = new DatabaseSync(rtkDbPath);
    const summary = rtkDb.prepare(`
      SELECT
        COUNT(*) AS total_commands,
        COALESCE(SUM(input_tokens),0) AS total_input,
        COALESCE(SUM(output_tokens),0) AS total_output,
        COALESCE(SUM(saved_tokens),0) AS total_saved,
        COALESCE(AVG(savings_pct),0) AS avg_savings_pct,
        COALESCE(SUM(exec_time_ms),0) AS total_time_ms
      FROM commands
    `).get();
    const recent = rtkDb.prepare(`
      SELECT timestamp, original_cmd, rtk_cmd, input_tokens, output_tokens, saved_tokens, savings_pct, exec_time_ms, project_path
      FROM commands ORDER BY id DESC LIMIT 50
    `).all();
    rtkDb.close();
    res.json({ summary, recent });
  } catch (e) {
    res.json({ summary: null, recent: [], error: e.message });
  }
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

wss.on('connection', ws => {
  wsClients.add(ws);
  ws.send(JSON.stringify({ type: 'init', runs: stmts.getRuns.all() }));
  ws.on('close', () => wsClients.delete(ws));
});

server.listen(PORT, () => {
  console.log(`\n🔭  Obs Dashboard  →  http://localhost:${PORT}\n`);
  console.log('POST /api/events    — receive events');
  console.log('GET  /api/analytics — aggregate stats');
  console.log('WS   ws://localhost:3000\n');
});
