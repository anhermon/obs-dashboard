'use strict';
const http = require('node:http');

const payload = process.env.HOOK_PAYLOAD || '{}';
const agent = process.env.HOOK_AGENT || 'claude-code';

let p;
try { p = JSON.parse(payload); } catch { process.exit(0); }

const typeMap = {
  PreToolUse:       'tool_execution_start',
  PostToolUse:      'tool_execution_end',
  Stop:             'agent_end',
  SubagentStop:     'agent_end',
  UserPromptSubmit: 'turn_start',
  PreCompact:       'pre_compact',
  Notification:     'notification',
};

const type = typeMap[p.hook_event_name] ?? (p.hook_event_name ?? 'unknown').toLowerCase();
const label = p.cwd ? p.cwd.split('/').pop() : undefined;

const data = {};
if (p.hook_event_name) data.hook = p.hook_event_name;
if (p.cwd)            data.cwd = p.cwd;
if (label)            data.label = label;
if (p.tool_name)      data.tool_name = p.tool_name;
if (p.tool_input)     data.args = p.tool_input;
if (p.tool_result != null) data.result = String(p.tool_result).slice(0, 600);
if (p.model)          data.model = p.model;
if (p.user_prompt)    data.user_prompt = String(p.user_prompt).slice(0, 300);

const body = JSON.stringify({
  agent,
  session_id: p.session_id,
  type,
  timestamp: new Date().toISOString(),
  data,
});

const req = http.request({
  host: 'localhost', port: 3000,
  path: '/api/events', method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(body),
  },
}, () => {});
req.on('error', () => {});
req.end(body);
