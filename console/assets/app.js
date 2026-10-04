'use strict';
// oarkflow operator console.
//
// Every request goes through api() with the bearer token from sessionStorage, so
// the console is subject to exactly the same auth and ACL as curl. Nothing is
// cached across reloads and nothing is stored beyond the tab.
//
// All broker-derived text is inserted with textContent, never innerHTML, so a
// message payload or a consumer-supplied name cannot inject markup or script.

const TOKEN_KEY = 'oarkflow.admin.token';
const $ = (id) => document.getElementById(id);

let token = sessionStorage.getItem(TOKEN_KEY) || '';
let current = 'overview';
let timer = null;
let inflight = 0;

// ---------------------------------------------------------------- transport

function apiHeaders() {
  const h = { 'Accept': 'application/json' };
  if (token) h['Authorization'] = 'Bearer ' + token;
  return h;
}

async function api(path, opts) {
  opts = opts || {};
  const init = { method: opts.method || 'GET', headers: apiHeaders() };
  if (opts.body !== undefined) {
    init.headers['Content-Type'] = 'application/json';
    init.body = JSON.stringify(opts.body);
  }
  inflight++;
  setConn('busy');
  try {
    const res = await fetch(path, init);
    const text = await res.text();
    let data = null;
    if (text) { try { data = JSON.parse(text); } catch (_) { data = text; } }
    if (!res.ok) {
      const err = new Error(brokerMessage(data, res.status));
      err.status = res.status;
      err.data = data;
      // 401/403 means the token is wrong or lacks permission. Surfacing that as a
      // dead session is more useful than leaving a console full of empty tables.
      if (res.status === 401) { signOut(err.message); }
      throw err;
    }
    setConn('ok');
    return data;
  } catch (e) {
    if (e.status === undefined) setConn('bad');
    if (e.status === undefined) throw new Error('cannot reach broker: ' + e.message);
    throw e;
  } finally {
    if (--inflight === 0) setConn(token ? 'ok' : 'bad');
  }
}

function brokerMessage(data, status) {
  if (data && typeof data === 'object') {
    if (data.message) return data.message;
    if (data.error) return data.error;
  }
  if (typeof data === 'string' && data.trim()) return data.trim().slice(0, 300);
  return 'request failed with status ' + status;
}

function setConn(state) {
  const dot = $('dot'), label = $('conn-label');
  if (!dot) return;
  dot.className = 'dot' + (state === 'ok' ? ' ok' : state === 'bad' ? ' bad' : '');
  label.textContent = state === 'busy' ? 'working' : state === 'ok' ? 'connected' : state === 'bad' ? 'unreachable' : 'idle';
}

function banner(msg, kind) {
  const el = $('banner');
  if (!msg) { el.hidden = true; return; }
  el.hidden = false;
  el.textContent = msg;
  el.className = 'banner' + (kind === 'info' ? ' info' : '');
}

// ---------------------------------------------------------------- helpers

function el(tag, attrs, kids) {
  const n = document.createElement(tag);
  if (attrs) for (const k in attrs) {
    const v = attrs[k];
    if (v === null || v === undefined || v === false) continue;
    if (k === 'text') n.textContent = String(v);
    else if (k === 'class') n.className = v;
    else if (k.slice(0, 2) === 'on') n.addEventListener(k.slice(2), v);
    else n.setAttribute(k, v === true ? '' : String(v));
  }
  if (kids) (Array.isArray(kids) ? kids : [kids]).forEach((c) => {
    if (c === null || c === undefined || c === false) return;
    n.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
  });
  return n;
}

function table(cols, rows, emptyText) {
  if (!rows || rows.length === 0) return el('div', { class: 'empty', text: emptyText || 'nothing here' });
  const thead = el('thead', null, el('tr', null, cols.map((c) =>
    el('th', { class: c.num ? 'num' : null, text: c.label }))));
  const tbody = el('tbody', null, rows.map((r) =>
    el('tr', null, cols.map((c) => {
      const v = c.render ? c.render(r) : r[c.key];
      const td = el('td', { class: (c.num ? 'num ' : '') + (c.cls || '') });
      if (v instanceof Node) td.appendChild(v);
      else td.textContent = v === null || v === undefined || v === '' ? '—' : String(v);
      return td;
    }))));
  return el('div', { class: 'panel-body flush' }, el('table', null, [thead, tbody]));
}

function card(k, v, s) {
  return el('div', { class: 'card' }, [
    el('div', { class: 'k', text: k }),
    el('div', { class: 'v', text: v }),
    s ? el('div', { class: 's', text: s }) : null,
  ]);
}

function panel(title, actions, body) {
  return el('div', { class: 'panel' }, [
    el('h2', null, [el('span', { text: title }), actions ? el('span', { class: 'row' }, actions) : null]),
    body,
  ]);
}

function num(n) {
  if (n === null || n === undefined || n === '') return '—';
  const x = Number(n);
  if (!isFinite(x)) return String(n);
  if (Math.abs(x) >= 1e9) return (x / 1e9).toFixed(2) + 'B';
  if (Math.abs(x) >= 1e6) return (x / 1e6).toFixed(2) + 'M';
  if (Math.abs(x) >= 1e4) return (x / 1e3).toFixed(1) + 'k';
  return x.toLocaleString();
}

function bytes(n) {
  n = Number(n) || 0;
  const u = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return (i === 0 ? n : n.toFixed(1)) + ' ' + u[i];
}

function when(s) {
  if (!s || s.startsWith('0001-01-01')) return '—';
  const d = new Date(s);
  if (isNaN(d)) return '—';
  const secs = (Date.now() - d.getTime()) / 1000;
  if (secs < 60) return Math.max(0, Math.round(secs)) + 's ago';
  if (secs < 3600) return Math.round(secs / 60) + 'm ago';
  if (secs < 86400) return Math.round(secs / 3600) + 'h ago';
  return d.toLocaleString();
}

function stateTag(s) {
  const t = String(s || '').toLowerCase();
  let cls = '';
  if (['running', 'ok', 'active', 'ready', 'healthy', 'completed', 'succeeded', 'idle'].indexOf(t) >= 0) cls = 'ok';
  else if (['paused', 'draining', 'scheduled', 'queued', 'pending', 'retrying', 'degraded'].indexOf(t) >= 0) cls = 'warn';
  else if (['failed', 'error', 'banned', 'stopped', 'cancelled', 'dead'].indexOf(t) >= 0) cls = 'err';
  return el('span', { class: 'tag ' + cls, text: s || '—' });
}

// Payloads cross the wire as base64 because Message.Payload is []byte. Showing
// the raw base64 to an operator is useless, so try to decode and fall back.
function decodePayload(p) {
  if (typeof p !== 'string' || p.length === 0) return payloadText(p);
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(p) || p.length % 4 !== 0) return p;
  try {
    const bin = atob(p);
    const bytes = Uint8Array.from(bin, (c) => c.charCodeAt(0));
    const text = new TextDecoder().decode(bytes);
    // Reject binary that decoded to control characters: it was not really text.
    if (/[\x00-\x08\x0e-\x1f]/.test(text)) return p;
    return text;
  } catch (_) {
    return p;
  }
}

function payloadText(p) {
  if (p === null || p === undefined) return '';
  if (typeof p === 'string') {
    // The API may hand back a payload as a JSON string or as a structured value.
    try {
      const v = JSON.parse(p);
      return typeof v === 'object' ? JSON.stringify(v, null, 2) : String(v);
    } catch (_) { return p; }
  }
  try { return JSON.stringify(p, null, 2); } catch (_) { return String(p); }
}

// ---------------------------------------------------------------- modal

let modalResolve = null;

function confirm2(title, body, okLabel) {
  $('modal-title').textContent = title;
  const b = $('modal-body');
  b.textContent = '';
  b.appendChild(typeof body === 'string' ? el('p', { class: 'muted', text: body }) : body);
  $('modal-ok').textContent = okLabel || 'Confirm';
  $('modal').hidden = false;
  return new Promise((res) => { modalResolve = res; });
}

function closeModal(v) {
  $('modal').hidden = true;
  const r = modalResolve;
  modalResolve = null;
  if (r) r(v);
}

// ---------------------------------------------------------------- views

const VIEWS = {};

// /stats nests everything under "store", and its queues/topics/dlq maps are
// name -> count, not rich objects. Reading st.queues directly yields undefined and
// renders a table of dashes that looks like "no data" rather than a bug.
function storeStats(st) {
  return (st && st.store) || {};
}

function counts(map) {
  const out = [];
  for (const name in map || {}) {
    const v = map[name];
    if (v !== null && typeof v === 'object') out.push({ name: name, detail: v });
    else out.push({ name: name, count: v });
  }
  return out.sort((a, b) => a.name.localeCompare(b.name));
}

function totalCounts(list) {
  return list.reduce((a, r) => a + (typeof r.count === 'number' ? r.count : 0), 0);
}

VIEWS.overview = async function (root) {
  const st = await api('/stats');
  const ss = storeStats(st);
  const q = counts(ss.queues), t = counts(ss.topics), d = counts(ss.dlq);
  const dead = ss.dead_letter !== undefined ? Number(ss.dead_letter) : totalCounts(d);

  root.appendChild(el('div', { class: 'cards' }, [
    card('Queues', num(q.length), 'declared'),
    card('Topics', num(t.length), 'with retention'),
    card('Messages', num(ss.messages), num(ss.leased) + ' leased'),
    card('Dead letters', num(dead), dead ? 'need attention' : 'clean'),
  ]));

  let health = 'unknown';
  try {
    const h = await api('/healthz');
    health = (h && (h.status || h.state)) || String(h);
  } catch (_) { /* a degraded broker must still render */ }

  const dl = el('dl', { class: 'kv' });
  const add = (k, v) => { dl.appendChild(el('dt', { text: k })); dl.appendChild(el('dd', { text: v })); };
  add('Broker state', st.broker_state || 'unknown');
  add('Health', health);
  add('Mode', st.mode || 'standalone');
  add('Queued messages', num(ss.messages));
  add('Leased', num(ss.leased));
  add('Dead letters', num(dead));
  if (st.error) add('Last error', st.error);
  root.appendChild(panel('Broker', null, el('div', { class: 'panel-body' }, dl)));
};

VIEWS.queues = async function (root) {
  const st = await api('/stats');
  const rows = counts(storeStats(st).queues).filter((r) => !/\.dlq$/.test(r.name));

  root.appendChild(panel('Queues (' + rows.length + ')', null, table([
    { label: 'Queue', render: (r) => el('span', { class: 'mono', text: r.name }) },
    { label: 'Ready depth', num: true, render: (r) => num(r.count) },
    {
      label: '', cls: 'actions', render: (r) => el('span', null, [
        el('button', { class: 'btn sm', text: 'Inspect', onclick: () => inspectQueue(r.name) }),
        el('button', { class: 'btn sm', text: 'Pause', onclick: () => queueAction(r.name, 'pause') }),
        el('button', { class: 'btn sm', text: 'Resume', onclick: () => queueAction(r.name, 'resume') }),
        el('button', {
          class: 'btn sm danger', text: 'Purge',
          onclick: async () => {
            if (!await confirm2('Purge ' + r.name, 'Every message in this queue is deleted. This cannot be undone.', 'Purge')) return;
            await act('/queues/' + encodeURIComponent(r.name) + '/purge', 'POST', r.name + ' purged');
          },
        }),
      ]),
    },
  ], rows, 'no queues declared yet')));

  const dlqs = counts(storeStats(st).dlq).filter((r) => Number(r.count) > 0);
  if (dlqs.length) {
    root.appendChild(panel('Queues with dead letters', null, table([
      { label: 'Queue', render: (r) => el('span', { class: 'mono', text: r.name }) },
      { label: 'Dead', num: true, render: (r) => num(r.count) },
    ], dlqs, '')));
  }
};

async function queueAction(name, action) {
  await act('/queues/' + encodeURIComponent(name) + '/' + action, 'POST', name + ' ' + action + 'd');
}

async function inspectQueue(name) {
  const body = el('div');
  body.appendChild(el('p', { class: 'muted', text: 'loading…' }));
  const m = confirm2('Queue ' + name, body, 'Close');
  try {
    // There is no /queues/{name} detail route, so depth comes from /stats.
    const st = await api('/stats');
    body.textContent = '';
    const dl = el('dl', { class: 'kv' });
    dl.appendChild(el('dt', { text: 'ready depth' }));
    dl.appendChild(el('dd', { text: num((storeStats(st).queues || {})[name]) }));
    dl.appendChild(el('dt', { text: 'dead letters' }));
    dl.appendChild(el('dd', { text: num((storeStats(st).dlq || {})[name]) }));
    body.appendChild(dl);
    try {
      const msgs = await api('/queues/' + encodeURIComponent(name) + '/messages?limit=20');
      if (Array.isArray(msgs) && msgs.length) {
        body.appendChild(el('h3', { class: 'small muted', text: 'Ready messages' }));
        body.appendChild(table([
          { label: 'ID', render: (r) => el('span', { class: 'mono', text: r.id }) },
          { label: 'Key', render: (r) => el('span', { class: 'mono', text: r.key || '—' }) },
          { label: 'Payload', render: (r) => el('pre', { class: 'payload', text: decodePayload(r.payload).slice(0, 400) }) },
          { label: 'Age', render: (r) => when(r.created_at || r.createdAt) },
        ], msgs, 'no ready messages'));
      }
    } catch (_) { /* message peek is best-effort */ }
  } catch (e) {
    body.textContent = '';
    body.appendChild(el('p', { class: 'err', text: e.message }));
  }
  await m;
}

VIEWS.topics = async function (root) {
  const st = await api('/stats');
  const rows = counts(storeStats(st).topics);
  root.appendChild(panel('Topics (' + rows.length + ')', null, table([
    { label: 'Topic', render: (r) => el('span', { class: 'mono', text: r.name }) },
    { label: 'Retained', num: true, render: (r) => num(r.count) },
    { label: '', cls: 'actions', render: (r) => el('button', { class: 'btn sm', text: 'Read', onclick: () => readTopic(r.name) }) },
  ], rows, 'no topics have been published to yet')));
};

async function readTopic(name) {
  const body = el('div');
  body.appendChild(el('p', { class: 'muted', text: 'loading…' }));
  const m = confirm2('Topic ' + name, body, 'Close');
  let cursor = '';
  const render = async (after) => {
    body.textContent = '';
    const path = '/v1/topics/' + encodeURIComponent(name) + '?limit=20' + (after ? '&after=' + encodeURIComponent(after) : '');
    try {
      const msgs = await api(path);
      if (!Array.isArray(msgs) || !msgs.length) {
        body.appendChild(el('p', { class: 'muted', text: after ? 'end of retention' : 'no retained messages' }));
        return;
      }
      cursor = msgs[msgs.length - 1].id;
      body.appendChild(table([
        { label: 'ID', render: (r) => el('span', { class: 'mono', text: r.id }) },
        { label: 'Key', render: (r) => el('span', { class: 'mono', text: r.key || '—' }) },
        { label: 'Payload', render: (r) => el('pre', { class: 'payload', text: decodePayload(r.payload).slice(0, 300) }) },
        { label: 'Age', render: (r) => when(r.created_at || r.createdAt) },
      ], msgs, 'no retained messages'));
      body.appendChild(el('div', { class: 'row', style: 'margin-top:12px' },
        el('button', { class: 'btn sm', text: 'Next 20 →', onclick: () => render(cursor) })));
    } catch (e) {
      body.appendChild(el('p', { class: 'err', text: e.message }));
    }
  };
  await render('');
  await m;
}

VIEWS.streams = async function (root) {
  const st = await api('/stats');
  const rows = counts(storeStats(st).streams);
  root.appendChild(panel('Streams (' + rows.length + ')', null, table([
    { label: 'Stream', render: (r) => el('span', { class: 'mono', text: r.name }) },
    { label: 'Messages', num: true, render: (r) => num(typeof r.count === 'number' ? r.count : (r.detail && r.detail.messages)) },
    { label: '', cls: 'actions', render: (r) => el('span', null, [
      el('button', { class: 'btn sm', text: 'Inspect', onclick: () => inspectStream(r.name) }),
      el('button', { class: 'btn sm', text: 'Lag', onclick: () => showLag(r.name) }),
    ]) },
  ], rows, 'no streams created yet')));
};

async function showLag(name) {
  const body = el('div', null, el('p', { class: 'muted', text: 'loading\u2026' }));
  const m = confirm2('Lag for ' + name, body, 'Close');
  try {
    const d = await api('/lag?stream=' + encodeURIComponent(name));
    body.textContent = '';
    body.appendChild(el('pre', { class: 'payload', text: JSON.stringify(d, null, 2) }));
  } catch (e) {
    body.textContent = '';
    body.appendChild(el('p', { class: 'err', text: e.message }));
  }
  await m;
}

async function inspectStream(name) {
  const body = el('div', null, el('p', { class: 'muted', text: 'loading…' }));
  const m = confirm2('Stream ' + name, body, 'Close');
  try {
    const d = await api('/streams/' + encodeURIComponent(name));
    body.textContent = '';
    const dl = el('dl', { class: 'kv' });
    for (const k in d) {
      const v = d[k];
      if (v === null || typeof v !== 'object') {
        dl.appendChild(el('dt', { text: k }));
        dl.appendChild(el('dd', { text: typeof v === 'number' ? num(v) : String(v) }));
      }
    }
    body.appendChild(dl);
  } catch (e) {
    body.textContent = '';
    body.appendChild(el('p', { class: 'err', text: e.message }));
  }
  await m;
}

VIEWS.consumers = async function (root) {
  const list = await api('/v1/consumers');
  const rows = Array.isArray(list) ? list : (list && list.consumers) || [];
  // The API names the identifier consumer_id and never returns a queue, so the
  // id fallback matters: building an action URL from r.id silently produced
  // /v1/consumers/undefined/pause.
  const idOf = (c) => c.consumer_id || c.id || c.name || '';
  const sorted = rows.slice().sort((a, b) => idOf(a).localeCompare(idOf(b)));

  root.appendChild(panel('Consumers (' + sorted.length + ')', null, table([
    { label: 'ID', render: (r) => el('span', { class: 'mono', text: idOf(r) }) },
    { label: 'Group', render: (r) => el('span', { class: 'mono', text: r.group || '—' }) },
    { label: 'State', render: (r) => (r.banned ? stateTag('banned') : r.draining ? stateTag('draining') : r.paused ? stateTag('paused') : stateTag(r.state)) },
    { label: 'Ack mode', key: 'ack_mode' },
    { label: 'In flight', num: true, key: 'inflight' },
    { label: 'Leased', num: true, key: 'leased' },
    { label: 'Acked', num: true, key: 'acked' },
    { label: 'Nacked', num: true, key: 'nacked' },
    { label: 'Failed', num: true, key: 'failed' },
    { label: 'Heartbeat', render: (r) => when(r.last_heartbeat) },
    { label: '', cls: 'actions', render: (r) => consumerActions(r, idOf(r)) },
  ], sorted, 'no remote consumers registered')));
};

function consumerActions(r, id) {
  if (!id) return el('span', { class: 'muted small', text: 'no id' });
  const path = encodeURIComponent(id);
  const go = (action, label) => () => act('/v1/consumers/' + path + '/' + action, 'POST', label + ' ' + id);
  const kids = [];
  if (r.banned) {
    kids.push(el('button', { class: 'btn sm', text: 'Unban', onclick: go('unban', 'unbanned') }));
  } else {
    kids.push(el('button', { class: 'btn sm', text: 'Pause', onclick: go('pause', 'paused') }));
    kids.push(el('button', { class: 'btn sm', text: 'Drain', onclick: go('drain', 'draining') }));
    kids.push(el('button', { class: 'btn sm danger', text: 'Ban', onclick: go('ban', 'banned') }));
  }
  kids.push(el('button', {
    class: 'btn sm danger', text: 'Remove',
    onclick: async () => {
      if (!await confirm2('Remove consumer ' + id, 'It must re-register before it can receive messages again.', 'Remove')) return;
      await act('/v1/consumers/' + path + '/unregister', 'POST', 'removed ' + id);
    },
  }));
  return el('span', null, kids);
}

VIEWS.tasks = async function (root) {
  const list = await api('/v1/tasks?limit=200');
  const tasks = Array.isArray(list) ? list : (list && list.tasks) || [];
  const byStatus = {};
  tasks.forEach((t) => { const s = t.status || 'unknown'; byStatus[s] = (byStatus[s] || 0) + 1; });

  root.appendChild(el('div', { class: 'cards' },
    Object.keys(byStatus).sort().map((s) => card(s, num(byStatus[s])))));

  const search = el('input', { type: 'text', placeholder: 'filter by id, name or queue', style: 'min-width:220px' });
  const wrap = el('div');
  const draw = () => {
    const q = search.value.trim().toLowerCase();
    const rows = tasks.filter((t) => !q ||
      String(t.id || '').toLowerCase().indexOf(q) >= 0 ||
      String(t.name || '').toLowerCase().indexOf(q) >= 0 ||
      String(t.queue || '').toLowerCase().indexOf(q) >= 0);
    wrap.textContent = '';
    wrap.appendChild(table([
      { label: 'ID', render: (t) => el('span', { class: 'mono', text: t.id }) },
      { label: 'Name', key: 'name' },
      { label: 'Queue', render: (t) => el('span', { class: 'mono', text: t.queue || '—' }) },
      { label: 'Status', render: (t) => stateTag(t.status) },
      { label: 'Attempts', num: true, key: 'attempts' },
      { label: 'Updated', render: (t) => when(t.updated_at || t.updatedAt) },
      { label: '', cls: 'actions', render: (t) => taskActions(t) },
    ], rows, q ? 'no tasks match that filter' : 'no tasks submitted yet'));
  };
  search.addEventListener('input', draw);

  root.appendChild(panel('Tasks (' + tasks.length + ')',
    [search, el('button', { class: 'btn sm', text: 'Submit task', onclick: () => submitTask() })],
    el('div', { class: 'panel-body flush' }, wrap)));
  draw();
}

function taskActions(t) {
  const id = encodeURIComponent(t.id);
  const kids = [
    el('button', { class: 'btn sm', text: 'Detail', onclick: () => taskDetail(t.id) }),
  ];
  const s = String(t.status || '').toLowerCase();
  const terminal = ['completed', 'succeeded', 'failed', 'cancelled'].indexOf(s) >= 0;
  if (!terminal && s !== 'running') {
    kids.push(el('button', { class: 'btn sm', text: 'Cancel', onclick: () => act('/v1/tasks/' + id + '/cancel', 'POST', 'cancelled ' + t.id) }));
  }
  if (terminal) {
    kids.push(el('button', {
      class: 'btn sm', text: 'Retry',
      onclick: () => act('/v1/tasks/' + id + '/retry', 'POST', 'retrying ' + t.id),
    }));
  }
  return el('span', null, kids);
}

async function taskDetail(id) {
  const body = el('div', null, el('p', { class: 'muted', text: 'loading…' }));
  const m = confirm2('Task', body, 'Close');
  try {
    const t = await api('/v1/tasks/' + encodeURIComponent(id));
    body.textContent = '';
    const dl = el('dl', { class: 'kv' });
    for (const k in t) {
      const v = t[k];
      if (v === null || v === undefined) continue;
      if (typeof v === 'object') continue;
      dl.appendChild(el('dt', { text: k }));
      dl.appendChild(el('dd', { text: String(v) }));
    }
    body.appendChild(dl);
    if (t.last_error) body.appendChild(el('pre', { class: 'payload', text: String(t.last_error) }));
    try {
      const r = await api('/v1/tasks/' + encodeURIComponent(id) + '/result');
      if (r !== null && r !== undefined && JSON.stringify(r) !== '{}') {
        body.appendChild(el('h3', { class: 'small muted', text: 'Result' }));
        body.appendChild(el('pre', { class: 'payload', text: payloadText(r) }));
      }
    } catch (_) { /* no result recorded yet */ }
  } catch (e) {
    body.textContent = '';
    body.appendChild(el('p', { class: 'err', text: e.message }));
  }
  await m;
}

async function submitTask() {
  const name = el('input', { type: 'text', placeholder: 'task name', style: 'flex:1' });
  const queue = el('input', { type: 'text', placeholder: 'queue', style: 'flex:1' });
  const meta = el('textarea', { placeholder: '{"key":"value"}' });
  const body = el('div', null, [
    el('div', { class: 'row' }, [name, queue]),
    el('div', { style: 'margin-top:10px' }, meta),
  ]);
  if (!await confirm2('Submit task', body, 'Submit')) return;
  let md = {};
  const raw = meta.value.trim();
  if (raw) {
    try { md = JSON.parse(raw); } catch (e) { banner('metadata is not valid JSON: ' + e.message); return; }
  }
  const payload = { name: name.value.trim(), queue: queue.value.trim(), metadata: md };
  if (!payload.name) { banner('task name is required'); return; }
  try {
    const res = await api('/v1/tasks/submit', { method: 'POST', body: payload });
    banner('submitted task ' + ((res && (res.id || res.task_id)) || ''), 'info');
    render();
  } catch (e) { banner(e.message); }
}

VIEWS.dlq = async function (root) {
  const st = await api('/stats');
  const all = counts(storeStats(st).dlq);
  const rows = all.filter((r) => Number(r.count) > 0);

  root.appendChild(el('div', { class: 'cards' }, [
    card('Dead letters', num(storeStats(st).dead_letter !== undefined ? storeStats(st).dead_letter : totalCounts(all)),
      rows.length ? 'need attention' : 'clean'),
  ]));

  root.appendChild(panel('Dead letters by queue', null, table([
    { label: 'Queue', render: (r) => el('span', { class: 'mono', text: r.name }) },
    { label: 'Dead', num: true, render: (r) => num(r.count) },
    { label: '', cls: 'actions', render: (r) => el('button', { class: 'btn sm', text: 'Inspect', onclick: () => inspectDLQ(r.name) }) },
  ], rows, 'no dead letters - nothing has exhausted its retries')));
};

async function inspectDLQ(name) {
  const body = el('div', null, el('p', { class: 'muted', text: 'loading\u2026' }));
  const m = confirm2('Dead letters in ' + name, body, 'Close');
  try {
    const msgs = await api('/dlq/' + encodeURIComponent(name) + '/messages?limit=50');
    body.textContent = '';
    body.appendChild(table([
      { label: 'ID', render: (r) => el('span', { class: 'mono', text: r.id }) },
      { label: 'Attempts', num: true, key: 'attempts' },
      { label: 'Error', render: (r) => el('span', { text: r.error || r.last_error || '\u2014' }) },
      { label: 'Age', render: (r) => when(r.created_at) },
      { label: 'Payload', render: (r) => el('pre', { class: 'payload', text: decodePayload(r.payload).slice(0, 300) }) },
    ], Array.isArray(msgs) ? msgs : [], 'no dead letters in this queue'));
  } catch (e) {
    body.textContent = '';
    body.appendChild(el('p', { class: 'err', text: e.message }));
  }
  await m;
}

VIEWS.cluster = async function (root) {
  const body = el('pre', { class: 'payload', text: 'loading…' });
  root.appendChild(panel('Cluster status', null, el('div', { class: 'panel-body' }, body)));
  try {
    const d = await api('/cluster/status');
    body.textContent = JSON.stringify(d, null, 2);
  } catch (e) {
    body.textContent = '';
    body.appendChild(el('p', { class: 'err', text: e.message }));
  }
};

VIEWS.audit = async function (root) {
  const data = await api('/audit?limit=200');
  const rows = Array.isArray(data) ? data : (data && (data.events || data.entries)) || [];
  root.appendChild(panel('Audit log (' + rows.length + ')', null, table([
    { label: 'Time', render: (r) => el('span', { class: 'mono', text: (r.time || r.at || '').replace('T', ' ').slice(0, 19) }) },
    { label: 'Principal', key: 'principal' },
    { label: 'Method', key: 'method' },
    { label: 'Path', render: (r) => el('span', { class: 'mono', text: r.path }) },
    { label: 'Status', num: true, render: (r) => (r.allowed === false ? stateTag('denied') : stateTag(String(r.status || 'ok'))) },
    { label: 'Reason', key: 'reason' },
  ], rows, 'no admin mutations recorded in this window')));
};

// ---------------------------------------------------------------- plumbing

async function act(path, method, okMsg) {
  try {
    await api(path, { method: method });
    banner(okMsg, 'info');
  } catch (e) {
    banner(e.message);
  }
  render();
}

async function render() {
  const root = $('view');
  const fn = VIEWS[current];
  if (!fn) return;
  $('view-title').textContent = current.charAt(0).toUpperCase() + current.slice(1);
  try {
    const frag = document.createDocumentFragment();
    await fn(frag);
    root.textContent = '';
    root.appendChild(frag);
  } catch (e) {
    root.textContent = '';
    root.appendChild(panel('Could not load ' + current, null,
      el('div', { class: 'panel-body' }, el('p', { class: 'err', text: e.message }))));
  }
  refreshBrokerPill();
}

async function refreshBrokerPill() {
  const pill = $('broker-pill');
  try {
    const d = await api('/v1/broker');
    const s = (d && d.state) || 'unknown';
    pill.textContent = 'state ' + s;
    pill.className = 'pill ' + (s === 'running' ? 'running' : s === 'paused' ? 'paused' : '');
    $('btn-pause').textContent = s === 'paused' ? 'Resume broker' : 'Pause broker';
  } catch (_) {
    pill.textContent = 'state unknown';
    pill.className = 'pill';
  }
}

function loop() {
  clearInterval(timer);
  if ($('auto').checked) timer = setInterval(render, 5000);
}

function signOut(msg) {
  token = '';
  sessionStorage.removeItem(TOKEN_KEY);
  clearInterval(timer);
  $('app').hidden = true;
  $('gate').hidden = false;
  if (msg) $('gate-error').textContent = msg;
  $('token').value = '';
}

async function signIn(t) {
  token = t;
  sessionStorage.setItem(TOKEN_KEY, t);
  $('gate-error').textContent = '';
  try {
    await api('/stats');
  } catch (e) {
    token = '';
    sessionStorage.removeItem(TOKEN_KEY);
    $('gate-error').textContent = e.message;
    return;
  }
  $('gate').hidden = true;
  $('app').hidden = false;
  render();
  loop();
}

document.addEventListener('DOMContentLoaded', function () {
  $('gate-form').addEventListener('submit', function (ev) {
    ev.preventDefault();
    signIn($('token').value.trim());
  });
  $('signout').addEventListener('click', function () { signOut(); });
  $('btn-refresh').addEventListener('click', render);
  $('auto').addEventListener('change', loop);
  $('modal-cancel').addEventListener('click', function () { closeModal(false); });
  $('modal-ok').addEventListener('click', function () { closeModal(true); });
  document.addEventListener('keydown', function (ev) { if (ev.key === 'Escape' && !$('modal').hidden) closeModal(false); });
  $('nav').addEventListener('click', function (ev) {
    const b = ev.target.closest('.nav-item');
    if (!b) return;
    current = b.dataset.view;
    Array.prototype.forEach.call(document.querySelectorAll('.nav-item'), (n) => n.classList.toggle('active', n === b));
    banner('');
    render();
  });
  $('btn-pause').addEventListener('click', async function () {
    const paused = $('broker-pill').textContent.indexOf('paused') >= 0;
    if (paused) { brokerAction('resume'); return; }
    const ok = await confirm2('Pause broker',
      'New publishes are rejected with 409 and in-process pool workers stop. ' +
      'Remote consumers are NOT stopped: anything already queued can still be drained. ' +
      'Pause individual queues if you need a hard stop.', 'Pause');
    if (ok) brokerAction('pause');
  });

  if (token) signIn(token); else $('gate').hidden = false;
});

function brokerAction(action) {
  return async function () {
    await api('/v1/broker', { method: 'POST', body: { action: action } });
    banner('broker ' + (action === 'pause' ? 'paused' : 'resumed'), 'info');
    render();
  };
}
