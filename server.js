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

// Safe columns for list endpoints — excludes system_prompt and working_dir
const RUN_SAFE_COLS = `id, agent, label, model, started_at, ended_at, duration_ms,
  turn_count, event_count, tool_call_count,
  input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
  reasoning_tokens, cost_usd, context_window_size, status`;

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
  getRuns:           db.prepare(`SELECT ${RUN_SAFE_COLS} FROM runs ORDER BY started_at DESC LIMIT 200`),
  getRun:            db.prepare(`SELECT * FROM runs WHERE id = ?`),
  getRunSafe:        db.prepare(`SELECT ${RUN_SAFE_COLS} FROM runs WHERE id = ?`),
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
  recentRuns: db.prepare(`SELECT ${RUN_SAFE_COLS} FROM runs ORDER BY started_at DESC LIMIT 20`),
  totalStats: db.prepare(`
    SELECT
      COUNT(*) AS runs,
      COUNT(CASE WHEN input_tokens > 0 OR output_tokens > 0 THEN 1 END) AS sessions_with_tokens,
      COALESCE(SUM(input_tokens), 0) AS input_tokens,
      COALESCE(SUM(output_tokens), 0) AS output_tokens,
      COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
      COALESCE(SUM(cache_write_tokens), 0) AS cache_write_tokens,
      COALESCE(SUM(reasoning_tokens), 0) AS reasoning_tokens,
      COALESCE(SUM(cost_usd), 0) AS cost_usd,
      COUNT(CASE WHEN status = 'running' THEN 1 END) AS active_runs
    FROM runs
  `),
  byAgentDetailed: db.prepare(`
    SELECT
      agent,
      COUNT(*) AS runs,
      COALESCE(SUM(input_tokens), 0) AS input_tokens,
      COALESCE(SUM(output_tokens), 0) AS output_tokens,
      COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
      COALESCE(SUM(cache_write_tokens), 0) AS cache_write_tokens,
      COALESCE(SUM(reasoning_tokens), 0) AS reasoning_tokens,
      COALESCE(SUM(cost_usd), 0) AS cost_usd,
      COUNT(CASE WHEN status = 'running' THEN 1 END) AS active_runs
    FROM runs GROUP BY agent ORDER BY cost_usd DESC, input_tokens DESC
  `),
  topCacheConsumers: db.prepare(`
    SELECT ${RUN_SAFE_COLS}
    FROM runs WHERE cache_read_tokens > 0
    ORDER BY cache_read_tokens DESC LIMIT 5
  `),
  dailyTokens: db.prepare(`
    SELECT
      date(started_at / 1000, 'unixepoch', 'localtime') AS date,
      COALESCE(SUM(input_tokens), 0) AS input_tokens,
      COALESCE(SUM(output_tokens), 0) AS output_tokens,
      COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
      COALESCE(SUM(cache_write_tokens), 0) AS cache_write_tokens,
      COALESCE(SUM(reasoning_tokens), 0) AS reasoning_tokens,
      COUNT(*) AS runs
    FROM runs
    WHERE started_at >= ?
    GROUP BY date ORDER BY date ASC
  `),
  agentSummary: db.prepare(`
    SELECT
      agent,
      COUNT(*) AS runs,
      COALESCE(SUM(input_tokens), 0) AS input_tokens,
      COALESCE(SUM(output_tokens), 0) AS output_tokens,
      COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
      COALESCE(SUM(cache_write_tokens), 0) AS cache_write_tokens,
      COALESCE(SUM(reasoning_tokens), 0) AS reasoning_tokens,
      COALESCE(SUM(cost_usd), 0) AS cost_usd,
      COALESCE(SUM(turn_count), 0) AS total_turns,
      COALESCE(SUM(tool_call_count), 0) AS total_tool_calls,
      COALESCE(AVG(NULLIF(duration_ms, 0)), 0) AS avg_duration_ms,
      COUNT(CASE WHEN status = 'running' THEN 1 END) AS active_runs,
      MAX(started_at) AS last_seen_at
    FROM runs WHERE agent = ? GROUP BY agent
  `),
  agentTopRuns: db.prepare(`
    SELECT ${RUN_SAFE_COLS} FROM runs WHERE agent = ? ORDER BY started_at DESC LIMIT 10
  `),
  agentRecentDailyTokens: db.prepare(`
    SELECT
      date(started_at / 1000, 'unixepoch', 'localtime') AS date,
      COALESCE(SUM(input_tokens), 0) AS input_tokens,
      COALESCE(SUM(output_tokens), 0) AS output_tokens,
      COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
      COALESCE(SUM(cache_write_tokens), 0) AS cache_write_tokens,
      COUNT(*) AS runs
    FROM runs
    WHERE agent = ? AND started_at >= ?
    GROUP BY date ORDER BY date ASC
  `),
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

  if (type === 'model_select' && data.model)
    stmts.updateModel.run(data.model, run_id);

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

  // Strip sensitive columns before broadcasting
  return { run_id, event: { id: seq, run_id, type, ts, seq, data }, run: stmts.getRunSafe.get(run_id) };
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
    if (items.length > 100) return res.status(429).json({ error: 'batch too large (max 100 events)' });
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
    ...e,
    data: (() => { try { return e.data ? JSON.parse(e.data) : {}; } catch { return {}; } })(),
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

// --- Summary helpers ---
function cacheHitRate(cache_read, input, cache_write) {
  const total = (input || 0) + (cache_read || 0) + (cache_write || 0);
  return total > 0 ? Math.round((cache_read || 0) / total * 10000) / 100 : 0;
}

function totalTokensForRow(r) {
  return (r.input_tokens || 0) + (r.output_tokens || 0) +
    (r.cache_read_tokens || 0) + (r.cache_write_tokens || 0) +
    (r.reasoning_tokens || 0);
}

function isSubscriptionAgent(agent) {
  // claude-code runs under Anthropic Pro/Max subscription — cost_usd not meaningful
  return agent === 'claude-code' || agent.startsWith('claude-code:') ||
    agent === 'claude-code:subagent';
}

function buildOptimizationInsights(total, byAgent) {
  const insights = [];
  const globalHitRate = cacheHitRate(total.cache_read_tokens, total.input_tokens, total.cache_write_tokens);

  if (total.sessions_with_tokens === 0) {
    insights.push({
      type: 'missing_data',
      message: 'No token data found. Run the claude-transcript-watcher to backfill usage from transcript files.',
      impact: 'high',
    });
    return insights;
  }

  if (globalHitRate < 15 && total.cache_write_tokens > 5000) {
    insights.push({
      type: 'low_cache_hit_rate',
      message: `Global cache hit rate is ${globalHitRate.toFixed(1)}%. System prompts are being written to cache but reads are low — consider consolidating repeated context into a single long-lived system prompt.`,
      impact: 'high',
    });
  } else if (globalHitRate >= 50) {
    insights.push({
      type: 'good_cache_usage',
      message: `Cache hit rate is ${globalHitRate.toFixed(1)}% — prompt caching is working well.`,
      impact: 'low',
    });
  }

  if (total.cache_write_tokens === 0 && total.input_tokens > 50000) {
    insights.push({
      type: 'no_cache_writes',
      message: 'No cache writes detected despite high input token volume. Enable prompt caching (cache_control breakpoints) to reduce costs on repeated context.',
      impact: 'high',
    });
  }

  const subagentRows = byAgent.filter(r => r.agent && r.agent.includes('subagent'));
  if (subagentRows.length > 0) {
    const subagentRuns = subagentRows.reduce((s, r) => s + r.runs, 0);
    insights.push({
      type: 'subagent_activity',
      message: `${subagentRuns} subagent run(s) detected across ${subagentRows.length} agent variant(s). Subagents share cache with their parent when using the same system prompt.`,
      impact: 'low',
    });
  }

  if (total.reasoning_tokens > 0) {
    const reasoningPct = Math.round(total.reasoning_tokens / Math.max(total.output_tokens, 1) * 100);
    if (reasoningPct > 40) {
      insights.push({
        type: 'high_reasoning',
        message: `Reasoning tokens are ${reasoningPct}% of output tokens. Extended thinking is active — review if all tasks require deep reasoning.`,
        impact: 'medium',
      });
    }
  }

  const agentsWithNoTokens = byAgent.filter(r => r.input_tokens === 0 && r.runs >= 3);
  if (agentsWithNoTokens.length > 0) {
    insights.push({
      type: 'missing_token_data',
      message: `${agentsWithNoTokens.map(r => r.agent).join(', ')} have runs with no token data. Ensure hooks post after_provider_response events or the transcript watcher is running.`,
      impact: 'medium',
    });
  }

  return insights;
}

app.get('/api/summary', (_req, res) => {
  const cutoff14d = Date.now() - 14 * 24 * 60 * 60 * 1000;

  const raw = analyticsStmts.totalStats.get();
  const total = {
    runs: raw.runs,
    sessions_with_tokens: raw.sessions_with_tokens,
    input_tokens: raw.input_tokens,
    output_tokens: raw.output_tokens,
    cache_read_tokens: raw.cache_read_tokens,
    cache_write_tokens: raw.cache_write_tokens,
    reasoning_tokens: raw.reasoning_tokens,
    total_tokens: totalTokensForRow(raw),
    cache_hit_rate_pct: cacheHitRate(raw.cache_read_tokens, raw.input_tokens, raw.cache_write_tokens),
    cost_usd: raw.cost_usd,
    active_runs: raw.active_runs,
  };

  const byAgentRaw = analyticsStmts.byAgentDetailed.all();
  const by_agent = byAgentRaw.map(r => ({
    agent: r.agent,
    runs: r.runs,
    input_tokens: r.input_tokens,
    output_tokens: r.output_tokens,
    cache_read_tokens: r.cache_read_tokens,
    cache_write_tokens: r.cache_write_tokens,
    reasoning_tokens: r.reasoning_tokens,
    total_tokens: totalTokensForRow(r),
    cache_hit_rate_pct: cacheHitRate(r.cache_read_tokens, r.input_tokens, r.cache_write_tokens),
    cost_usd: r.cost_usd,
    active_runs: r.active_runs,
    is_subscription: isSubscriptionAgent(r.agent),
  }));

  const top_cache_consumers = analyticsStmts.topCacheConsumers.all().map(r => ({
    ...r,
    cache_hit_rate_pct: cacheHitRate(r.cache_read_tokens, r.input_tokens, r.cache_write_tokens),
  }));

  const cache_efficiency_by_agent = by_agent
    .filter(r => r.cache_read_tokens > 0 || r.cache_write_tokens > 0)
    .map(r => ({
      agent: r.agent,
      cache_read_tokens: r.cache_read_tokens,
      cache_write_tokens: r.cache_write_tokens,
      cache_hit_rate_pct: r.cache_hit_rate_pct,
      runs: r.runs,
    }))
    .sort((a, b) => b.cache_hit_rate_pct - a.cache_hit_rate_pct);

  const daily_tokens = analyticsStmts.dailyTokens.all(cutoff14d);

  const optimization_insights = buildOptimizationInsights(total, byAgentRaw);

  res.json({
    generated_at: Date.now(),
    total,
    by_agent,
    top_cache_consumers,
    cache_efficiency_by_agent,
    daily_tokens,
    optimization_insights,
  });
});

app.get('/api/agent/:agent', (req, res) => {
  const agent = req.params.agent;
  const cutoff14d = Date.now() - 14 * 24 * 60 * 60 * 1000;

  const summary = analyticsStmts.agentSummary.get(agent);
  if (!summary) return res.status(404).json({ error: `No runs found for agent: ${agent}` });

  const top_runs = analyticsStmts.agentTopRuns.all(agent);
  const daily_tokens = analyticsStmts.agentRecentDailyTokens.all(agent, cutoff14d);

  const cache_hit_rate_pct = cacheHitRate(
    summary.cache_read_tokens, summary.input_tokens, summary.cache_write_tokens
  );

  res.json({
    generated_at: Date.now(),
    agent,
    summary: {
      ...summary,
      total_tokens: totalTokensForRow(summary),
      cache_hit_rate_pct,
      is_subscription: isSubscriptionAgent(agent),
    },
    top_runs: top_runs.map(r => ({
      ...r,
      cache_hit_rate_pct: cacheHitRate(r.cache_read_tokens, r.input_tokens, r.cache_write_tokens),
    })),
    daily_tokens,
    optimization_insights: buildOptimizationInsights(
      { ...summary, sessions_with_tokens: summary.runs, active_runs: summary.active_runs },
      [summary]
    ),
  });
});

app.delete('/api/runs', (_req, res) => {
  db.exec('BEGIN; DELETE FROM events; DELETE FROM runs; COMMIT;');
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
  ws.on('error', () => wsClients.delete(ws));
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`\n🔭  Obs Dashboard  →  http://localhost:${PORT}\n`);
  console.log('POST /api/events    — receive events');
  console.log('GET  /api/analytics — aggregate stats');
  console.log('WS   ws://localhost:3000\n');
});
