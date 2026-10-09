'use strict';
// Devovour relay: a tiny WebSocket room server. It only forwards presence and events between players in the same room.
const http = require('http');
const { WebSocketServer } = require('ws');
const ALLOWED = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean); // e.g. https://yourgame.netlify.app
const MAX_ROOM = +process.env.MAX_ROOM || 32;
const server = http.createServer((req, res) => { res.writeHead(200, { 'content-type': 'text/plain', 'access-control-allow-origin': '*' }); res.end('devovour relay ok'); });
const wss = new WebSocketServer({ server, maxPayload: 8192 });
const rooms = new Map(); let seq = 1;
const bc = (R, from, obj) => { const s = JSON.stringify(obj); for (const [id, c] of R) if (id !== from && c.ws.readyState === 1) c.ws.send(s); };
wss.on('connection', (ws, req) => {
  const origin = req.headers.origin || ''; if (ALLOWED.length && !ALLOWED.includes(origin)) { ws.close(1008, 'origin'); return; }
  const u = new URL(req.url, 'http://x'), name = (u.searchParams.get('room') || 'main').toLowerCase().replace(/[^a-z0-9_.-]/g, '').slice(0, 48) || 'main';
  let R = rooms.get(name); if (!R) rooms.set(name, R = new Map()); if (R.size >= MAX_ROOM) { ws.close(1013, 'full'); return; }
  const id = 'p' + (seq++).toString(36) + Math.random().toString(36).slice(2, 5), me = { ws, pr: {}, tokens: 80, t: Date.now(), alive: true };
  R.set(id, me);
  ws.send(JSON.stringify({ t: 'welcome', id, peers: Object.fromEntries([...R].filter(([k]) => k !== id).map(([k, v]) => [k, v.pr])) }));
  bc(R, id, { t: 'join', id, pr: {} });
  ws.on('pong', () => { me.alive = true; });
  ws.on('message', buf => {
    const now = Date.now(); me.tokens = Math.min(80, me.tokens + (now - me.t) / 1000 * 60); me.t = now; if (--me.tokens < 0) return; // ~60 msgs/s per player
    let m; try { m = JSON.parse(buf); } catch (e) { return; }
    if (m.t === 'pres' && m.pr && typeof m.pr === 'object' && !Array.isArray(m.pr)) { me.pr = m.pr; bc(R, id, { t: 'pres', id, pr: m.pr }); }
    else if (m.t === 'emit' && typeof m.topic === 'string' && /^[a-z][a-z0-9_.-]{0,47}$/.test(m.topic)) bc(R, id, { t: 'emit', id, topic: m.topic, data: m.data });
  });
  ws.on('close', () => { R.delete(id); bc(R, id, { t: 'leave', id }); if (!R.size) rooms.delete(name); });
});
setInterval(() => { for (const R of rooms.values()) for (const c of R.values()) { if (!c.alive) { c.ws.terminate(); continue; } c.alive = false; try { c.ws.ping(); } catch (e) { } } }, 25000);
server.listen(process.env.PORT || 8787, () => console.log('devovour relay listening on', process.env.PORT || 8787));
