#!/usr/bin/env node
'use strict';

/**
 * obs-cli — Observability Dashboard CLI
 * Connects to http://localhost:3000 (obs-dashboard server)
 *
 * Commands: summary | agents | recent [n] | optimize | export | runs [agent]
 * Flags:    --json (raw JSON output), --help / -h
 */

const http = require('node:http');

const BASE = 'http://localhost:3000';

// ---------------------------------------------------------------------------
// Pricing (claude-sonnet-4-6 rates, per million tokens)
// ---------------------------------------------------------------------------
const PRICE = {
  input:       3.00,   // $/M input tokens
  output:     15.00,   // $/M output tokens
  cache_read:  0.30,   // $/M cache read tokens
  cache_write: 3.75,   // $/M cache write tokens (1.25x input)
};

/** Agents that run under a subscription — cost shown as "sub". */
function isSubscription(agent) {
  if (!agent) return false;
  const base = agent.split(':')[0];
  return ['codex', 'hermes', 'pi', 'cursor'].includes(base);
}

/**
 * Compute API cost from token counts using PRICE table.
 * Returns null for subscription agents.
 */
function computeCost(row) {
  if (isSubscription(row.agent)) return null;
  return (
    ((row.input_tokens       || 0) / 1e6) * PRICE.input  +
    ((row.output_tokens      || 0) / 1e6) * PRICE.output +
    ((row.cache_read_tokens  || 0) / 1e6) * PRICE.cache_read  +
    ((row.cache_write_tokens || 0) / 1e6) * PRICE.cache_write
  );
}

function totalTokens(row) {
  return (row.input_tokens       || 0) +
         (row.output_tokens      || 0) +
         (row.cache_read_tokens  || 0) +
         (row.cache_write_tokens || 0) +
         (row.reasoning_tokens   || 0);
}

function cacheHitPct(row) {
  const denom = (row.input_tokens || 0) +
                (row.cache_read_tokens || 0) +
                (row.cache_write_tokens || 0);
  if (!denom) return null;
  return ((row.cache_read_tokens || 0) / denom) * 100;
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------
function apiFetch(urlPath) {
  return new Promise((resolve, reject) => {
    const req = http.get(`${BASE}${urlPath}`, res => {
      let raw = '';
      res.on('data', c => { raw += c; });
      res.on('end', () => {
        if (res.statusCode === 404) {
          return reject(Object.assign(new Error(`404: ${urlPath}`), { status: 404 }));
        }
        try { resolve(JSON.parse(raw)); }
        catch {
          reject(new Error(`Non-JSON response from ${urlPath} (status ${res.statusCode})`));
        }
      });
    });
    req.on('error', err => {
      reject(new Error(
        `Cannot reach dashboard at ${BASE}: ${err.message}\n` +
        `Is the server running?  cd ~/dev/obs-dashboard && node server.js`
      ));
    });
  });
}

/**
 * getSummary() — tries /api/summary (new server) then falls back to
 * building equivalent data from /api/analytics + /api/runs (old server).
 *
 * Returns a normalised object matching the /api/summary shape:
 *   { total, by_agent, top_cache_consumers, cache_efficiency_by_agent,
 *     daily_tokens, optimization_insights }
 */
async function getSummary() {
  // Try the new endpoint first
  try {
    const data = await apiFetch('/api/summary');
    if (data && data.total) return data;
  } catch (err) {
    if (!String(err.message).startsWith('404')) throw err;
  }

  // --- Fallback: build from /api/analytics and /api/runs ---
  const [analytics, runs] = await Promise.all([
    apiFetch('/api/analytics'),
    apiFetch('/api/runs'),
  ]);

  // analytics.by_agent uses different field names; normalise them
  const agentRows = (analytics.by_agent || []).map(r => ({
    agent:               r.agent,
    runs:                r.run_count || 0,
    input_tokens:        r.total_input || 0,
    output_tokens:       r.total_output || 0,
    cache_read_tokens:   r.total_cached || 0,   // analytics only has read side
    cache_write_tokens:  0,                      // not exposed in /api/analytics
    reasoning_tokens:    r.total_reasoning || 0,
    cost_usd:            r.total_cost || 0,
    active_runs:         0,
  }));

  // Augment with cache_write from individual runs
  const runsByAgent = {};
  for (const r of runs) {
    if (!runsByAgent[r.agent]) runsByAgent[r.agent] = [];
    runsByAgent[r.agent].push(r);
  }
  for (const row of agentRows) {
    const agRuns = runsByAgent[row.agent] || [];
    row.cache_write_tokens = agRuns.reduce((s, r) => s + (r.cache_write_tokens || 0), 0);
    row.active_runs = agRuns.filter(r => r.status === 'running').length;
  }

  // Compute total across all agents
  const total = agentRows.reduce((acc, r) => ({
    runs:               acc.runs + r.runs,
    sessions_with_tokens: acc.sessions_with_tokens + (r.input_tokens > 0 || r.output_tokens > 0 ? r.runs : 0),
    input_tokens:       acc.input_tokens + r.input_tokens,
    output_tokens:      acc.output_tokens + r.output_tokens,
    cache_read_tokens:  acc.cache_read_tokens + r.cache_read_tokens,
    cache_write_tokens: acc.cache_write_tokens + r.cache_write_tokens,
    reasoning_tokens:   acc.reasoning_tokens + r.reasoning_tokens,
    cost_usd:           acc.cost_usd + r.cost_usd,
    active_runs:        acc.active_runs + r.active_runs,
  }), {
    runs: 0, sessions_with_tokens: 0,
    input_tokens: 0, output_tokens: 0,
    cache_read_tokens: 0, cache_write_tokens: 0,
    reasoning_tokens: 0, cost_usd: 0, active_runs: 0,
  });

  const totalDenom = (total.input_tokens || 0) + (total.cache_read_tokens || 0) + (total.cache_write_tokens || 0);
  total.total_tokens = (total.input_tokens || 0) + (total.output_tokens || 0) +
                       (total.cache_read_tokens || 0) + (total.cache_write_tokens || 0) +
                       (total.reasoning_tokens || 0);
  total.cache_hit_rate_pct = totalDenom > 0
    ? Math.round((total.cache_read_tokens / totalDenom) * 10000) / 100 : 0;

  // Enrich by_agent with derived fields
  const by_agent = agentRows.map(r => {
    const denom = (r.input_tokens || 0) + (r.cache_read_tokens || 0) + (r.cache_write_tokens || 0);
    return {
      ...r,
      total_tokens: totalTokens(r),
      cache_hit_rate_pct: denom > 0 ? Math.round((r.cache_read_tokens / denom) * 10000) / 100 : 0,
      is_subscription: isSubscription(r.agent),
    };
  });

  const cache_efficiency_by_agent = by_agent
    .filter(r => r.cache_read_tokens > 0 || r.cache_write_tokens > 0)
    .sort((a, b) => b.cache_hit_rate_pct - a.cache_hit_rate_pct);

  // Build basic insights
  const optimization_insights = buildLocalInsights(total, by_agent);

  return { total, by_agent, cache_efficiency_by_agent, daily_tokens: [], optimization_insights };
}

/** Minimal insight builder for when /api/summary isn't available. */
function buildLocalInsights(total, byAgent) {
  const insights = [];
  const hitRate = total.cache_hit_rate_pct || 0;

  if (total.sessions_with_tokens === 0) {
    insights.push({
      type: 'missing_data',
      message: 'No token data found. Run watchers/claude-transcript-watcher.js to backfill usage.',
      impact: 'high',
    });
    return insights;
  }

  if (hitRate < 15 && total.cache_write_tokens > 5000) {
    insights.push({
      type: 'low_cache_hit_rate',
      message: `Global cache hit rate is ${hitRate.toFixed(1)}%. Cache writes exist but reads are low — consolidate repeated context into a stable system prompt.`,
      impact: 'high',
    });
  } else if (hitRate >= 50) {
    insights.push({
      type: 'good_cache_usage',
      message: `Cache hit rate is ${hitRate.toFixed(1)}% — prompt caching is working well.`,
      impact: 'low',
    });
  }

  if (total.cache_write_tokens === 0 && total.input_tokens > 50000) {
    insights.push({
      type: 'no_cache_writes',
      message: 'No cache writes detected despite high input volume. Add cache_control breakpoints to enable prompt caching.',
      impact: 'high',
    });
  }

  const noTokens = byAgent.filter(r => r.input_tokens === 0 && r.runs >= 3);
  if (noTokens.length) {
    insights.push({
      type: 'missing_token_data',
      message: `${noTokens.map(r => r.agent).join(', ')} have runs with no token data. Ensure the transcript watcher is running.`,
      impact: 'medium',
    });
  }

  return insights;
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------
function fmtNum(n) {
  if (!n && n !== 0) return '-';
  if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return String(n);
}

function fmtCost(v) {
  if (v === null || v === undefined) return 'sub';
  if (v <= 0)   return '$0.00';
  if (v >= 100) return '$' + v.toFixed(0);
  if (v >= 1)   return '$' + v.toFixed(2);
  return '$' + v.toFixed(4);
}

function fmtPct(n) {
  if (n === null || n === undefined || n === '') return '-';
  return Number(n).toFixed(1) + '%';
}

function fmtDur(ms) {
  if (!ms) return '-';
  const s = ms / 1000;
  if (s < 60)  return s.toFixed(0) + 's';
  const m = Math.floor(s / 60);
  const rs = Math.round(s % 60);
  if (m < 60)  return `${m}m${rs}s`;
  const h = Math.floor(m / 60);
  return `${h}h${m % 60}m`;
}

function fmtDate(ts) {
  if (!ts) return '-';
  return new Date(ts).toISOString().slice(0, 16).replace('T', ' ');
}

function fmtCacheHitPct(row) {
  const pct = cacheHitPct(row);
  if (pct === null) return '-';
  return fmtPct(pct);
}

/** Render an aligned plain-text table. aligns: 'left'|'right' per column. */
function printTable(headers, rows, aligns = []) {
  if (!rows.length) { console.log('  (no data)'); return; }

  // Stringify all cells up front so width calculation and rendering are consistent
  const strRows = rows.map(r => r.map(c => String(c ?? '')));
  const strHeaders = headers.map(c => String(c ?? ''));
  const allCells = [strHeaders, ...strRows];
  const widths = strHeaders.map((_, i) =>
    Math.max(...allCells.map(r => (r[i] || '').length))
  );

  const renderRow = row =>
    row.map((c, i) => {
      const w = widths[i];
      return aligns[i] === 'right' ? c.padStart(w) : c.padEnd(w);
    }).join('  ');

  const sep = widths.map(w => '-'.repeat(w)).join('  ');
  console.log(renderRow(strHeaders));
  console.log(sep);
  for (const row of strRows) console.log(renderRow(row));
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

async function cmdSummary(json) {
  const data = await getSummary();
  if (json) { console.log(JSON.stringify(data, null, 2)); return; }

  const { total, optimization_insights } = data;

  // Recompute estimated cost via CLI pricing (server may have stale/zero values)
  const cliCost = (data.by_agent || []).reduce((sum, r) => {
    const c = computeCost(r);
    return sum + (c || 0);
  }, 0);

  console.log('\n=== Token Summary ===\n');
  printTable(
    ['Metric', 'Value'],
    [
      ['Total Runs',            String(total.runs)],
      ['Sessions with Tokens',  String(total.sessions_with_tokens || 0)],
      ['Active Runs',           String(total.active_runs || 0)],
      ['Input Tokens',          fmtNum(total.input_tokens)],
      ['Output Tokens',         fmtNum(total.output_tokens)],
      ['Cache Read Tokens',     fmtNum(total.cache_read_tokens)],
      ['Cache Write Tokens',    fmtNum(total.cache_write_tokens)],
      ['Reasoning Tokens',      fmtNum(total.reasoning_tokens)],
      ['Total Tokens',          fmtNum(total.total_tokens)],
      ['Cache Hit Rate',        fmtPct(total.cache_hit_rate_pct)],
      ['Estimated Cost',        fmtCost(cliCost > 0 ? cliCost : null)],
    ],
    ['left', 'right']
  );

  if (optimization_insights && optimization_insights.length) {
    console.log('\n=== Insights ===\n');
    for (const ins of optimization_insights) {
      const tag = ins.impact === 'high' ? '[!]' : ins.impact === 'medium' ? '[~]' : '[-]';
      console.log(`${tag} ${ins.message}`);
    }
  }
  console.log('');
}

async function cmdAgents(json) {
  const data = await getSummary();
  if (json) { console.log(JSON.stringify(data.by_agent, null, 2)); return; }

  console.log('\n=== Agent Breakdown ===\n');
  const rows = (data.by_agent || []).map(r => [
    r.agent,
    r.runs,
    fmtNum(r.total_tokens || totalTokens(r)),
    fmtNum(r.input_tokens),
    fmtNum(r.output_tokens),
    fmtNum(r.cache_read_tokens),
    fmtPct(r.cache_hit_rate_pct ?? cacheHitPct(r)),
    isSubscription(r.agent) ? 'sub' : fmtCost(computeCost(r)),
  ]);

  if (!rows.length) { console.log('  No agents found.'); return; }
  printTable(
    ['Agent', 'Runs', 'Total', 'Input', 'Output', 'CacheRead', 'Cache%', 'Cost'],
    rows,
    ['left','right','right','right','right','right','right','right']
  );
  console.log('');
}

async function cmdRecent(n, json) {
  const runs = await apiFetch('/api/runs');
  const slice = runs.slice(0, n);
  if (json) { console.log(JSON.stringify(slice, null, 2)); return; }

  console.log(`\n=== Recent ${n} Sessions ===\n`);
  const rows = slice.map(r => [
    r.id.slice(0, 8),
    r.agent || '-',
    (r.label || '-').slice(0, 22),
    fmtNum(totalTokens(r)),
    fmtCacheHitPct(r),
    isSubscription(r.agent) ? 'sub' : fmtCost(computeCost(r)),
    fmtDur(r.duration_ms),
    fmtDate(r.started_at),
  ]);

  if (!rows.length) { console.log('  No runs found.'); return; }
  printTable(
    ['ID',     'Agent', 'Label',   'Tokens', 'Cache%', 'Cost', 'Dur',   'Started'],
    rows,
    ['left', 'left',  'left',   'right',  'right',  'right','right',  'left']
  );
  console.log('');
}

async function cmdOptimize(json) {
  const data = await getSummary();
  const out = {
    optimization_insights:     data.optimization_insights,
    cache_efficiency_by_agent: data.cache_efficiency_by_agent,
    total: {
      cache_hit_rate_pct:  data.total.cache_hit_rate_pct,
      cache_read_tokens:   data.total.cache_read_tokens,
      cache_write_tokens:  data.total.cache_write_tokens,
      input_tokens:        data.total.input_tokens,
    },
  };
  if (json) { console.log(JSON.stringify(out, null, 2)); return; }

  const { total, cache_efficiency_by_agent, optimization_insights } = data;

  console.log('\n=== Cache Efficiency Analysis ===\n');
  console.log(`  Global cache hit rate : ${fmtPct(total.cache_hit_rate_pct)}`);
  console.log(`  Cache reads           : ${fmtNum(total.cache_read_tokens)}`);
  console.log(`  Cache writes          : ${fmtNum(total.cache_write_tokens)}`);
  console.log(`  Input tokens          : ${fmtNum(total.input_tokens)}`);

  if (cache_efficiency_by_agent && cache_efficiency_by_agent.length) {
    console.log('\nPer-agent cache stats:\n');
    printTable(
      ['Agent', 'CacheRead', 'CacheWrite', 'HitRate', 'Runs'],
      cache_efficiency_by_agent.map(r => [
        r.agent,
        fmtNum(r.cache_read_tokens),
        fmtNum(r.cache_write_tokens),
        fmtPct(r.cache_hit_rate_pct),
        r.runs,
      ]),
      ['left','right','right','right','right']
    );
  }

  if (optimization_insights && optimization_insights.length) {
    console.log('\nInsights:\n');
    for (const ins of optimization_insights) {
      const severity = ins.impact === 'high' ? '[HIGH]' : ins.impact === 'medium' ? '[MED] ' : '[LOW] ';
      console.log(`  ${severity} ${ins.message}\n`);
    }
  }

  console.log('Actionable tips:\n');
  const hitRate = total.cache_hit_rate_pct || 0;
  if (hitRate < 10 && (total.cache_write_tokens || 0) > 0) {
    console.log('  * Cache writes exist but reads are low. Ensure sessions reuse the same');
    console.log('    system prompt text so Claude can serve subsequent calls from cache.\n');
  }
  if ((total.cache_write_tokens || 0) === 0 && (total.input_tokens || 0) > 10000) {
    console.log('  * No cache writes detected. Add cache_control breakpoints to your system');
    console.log('    prompt to enable prompt caching ($3.75/M write, $0.30/M read).\n');
  }
  if (hitRate >= 50) {
    console.log('  * Cache hit rate is healthy (>=50%). Keep system prompts stable to');
    console.log('    maintain high cache reuse.\n');
  }
  if ((total.reasoning_tokens || 0) > (total.output_tokens || 0) * 0.4) {
    console.log('  * Extended thinking is consuming >40% of output budget. Consider');
    console.log('    disabling it for routine tasks to reduce latency and cost.\n');
  }
  console.log('');
}

async function cmdExport() {
  // Always NDJSON — inherently machine-readable, --json flag has no effect
  const runs = await apiFetch('/api/runs');
  for (const run of runs) {
    process.stdout.write(JSON.stringify(run) + '\n');
  }
}

async function cmdRuns(agent, json) {
  let runs;
  if (agent) {
    // Try /api/agent/:agent (new server) first
    try {
      const data = await apiFetch(`/api/agent/${encodeURIComponent(agent)}`);
      // top_runs is capped at 10 by server; fall through to client-filter for more
      if (data.top_runs && data.top_runs.length > 0) {
        runs = data.top_runs;
      } else {
        runs = [];
      }
    } catch (err) {
      if (String(err.message).startsWith('404')) {
        // Endpoint not available or agent not found — filter from /api/runs instead
        const all = await apiFetch('/api/runs');
        runs = all.filter(r => r.agent === agent);
      } else {
        throw err;
      }
    }
  } else {
    runs = await apiFetch('/api/runs');
  }

  if (json) { console.log(JSON.stringify(runs, null, 2)); return; }

  const title = agent ? `Runs: ${agent}` : 'All Runs (latest 200)';
  console.log(`\n=== ${title} ===\n`);

  const rows = runs.map(r => [
    r.id.slice(0, 8),
    r.agent || '-',
    (r.label || '-').slice(0, 22),
    fmtNum(totalTokens(r)),
    fmtCacheHitPct(r),
    isSubscription(r.agent) ? 'sub' : fmtCost(computeCost(r)),
    r.status || '-',
    fmtDate(r.started_at),
  ]);

  if (!rows.length) { console.log('  No runs found.'); return; }
  printTable(
    ['ID',   'Agent', 'Label',  'Tokens', 'Cache%', 'Cost', 'Status', 'Started'],
    rows,
    ['left','left', 'left',  'right',  'right',  'right','left',   'left']
  );
  console.log('');
}

// ---------------------------------------------------------------------------
// Help
// ---------------------------------------------------------------------------
function showHelp() {
  console.log(`
obs-cli — Observability Dashboard CLI
Connects to: ${BASE}

Usage:
  obs <command> [args] [--json]

Commands:
  summary           Total token stats, cache efficiency, estimated cost
  agents            Per-agent breakdown: runs, tokens, cache%, cost
  recent [n]        Last N sessions with key metrics  (default: 10)
  optimize          Cache efficiency analysis with actionable tips
  export            NDJSON dump of all runs (pipe to jq)
  runs [agent]      List all runs, or filter by agent name

Options:
  --json            Output raw JSON instead of formatted tables
  --help, -h        Show this help

Pricing (claude-sonnet-4-6):
  Input:       $3.00/M    Output:      $15.00/M
  Cache read:  $0.30/M    Cache write:  $3.75/M
  Subscription agents (codex/hermes/pi/cursor) shown as "sub"
  Priced agents: claude-code, cowork, and variants

Examples:
  obs summary
  obs agents
  obs agents --json | jq '.[] | select(.agent == "claude-code")'
  obs recent 20
  obs recent --json | jq '.[] | {id, agent, label}'
  obs runs claude-code
  obs optimize
  obs export | jq 'select(.input_tokens > 50000)'
`);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
// Suppress EPIPE so piping to head/grep doesn't blow up
process.stdout.on('error', err => { if (err.code === 'EPIPE') process.exit(0); });
process.stderr.on('error', err => { if (err.code === 'EPIPE') process.exit(0); });

async function main() {
  const argv = process.argv.slice(2);
  const json  = argv.includes('--json');
  const args  = argv.filter(a => a !== '--json');
  const cmd   = args[0];

  if (!cmd || cmd === '--help' || cmd === '-h') {
    showHelp();
    return;
  }

  try {
    switch (cmd) {
      case 'summary':
        await cmdSummary(json);
        break;

      case 'agents':
        await cmdAgents(json);
        break;

      case 'recent': {
        const n = Math.max(1, parseInt(args[1]) || 10);
        await cmdRecent(n, json);
        break;
      }

      case 'optimize':
        await cmdOptimize(json);
        break;

      case 'export':
        await cmdExport(json);
        break;

      case 'runs': {
        const agent = args[1] || null;
        await cmdRuns(agent, json);
        break;
      }

      default:
        console.error(`Unknown command: "${cmd}"\nRun: obs --help`);
        process.exit(1);
    }
  } catch (err) {
    console.error(`\nError: ${err.message}`);
    process.exit(1);
  }
}

main();
