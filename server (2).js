// ARENA FPS online - servidor sem dependências (HTTP + WebSocket manual)
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = process.env.PORT || 3000, MAX_PLAYERS = 10, MAX_ROOMS = 30;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const PUB = __dirname;

const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); return res.end('ok'); }
  fs.readFile(path.join(PUB, 'index.html'), (err, data) => {
    if (err) { res.writeHead(500); return res.end('erro'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); res.end(data);
  });
});

// ---------- WebSocket mínimo ----------
function frame(op, payload) {
  const len = payload.length; let h;
  if (len < 126) h = Buffer.from([0x80 | op, len]);
  else if (len < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(len, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([h, payload]);
}
function send(c, obj) { if (c.alive && !c.socket.destroyed) c.socket.write(frame(1, Buffer.from(JSON.stringify(obj)))); }

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) return socket.destroy();
  const acc = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = { socket, alive: true, buf: Buffer.alloc(0), id: crypto.randomBytes(4).toString('hex'), room: null, name: '', msgs: 0, t0: Date.now() };
  socket.on('data', d => {
    c.buf = Buffer.concat([c.buf, d]);
    if (c.buf.length > 100000) return close(c);
    for (;;) {
      const b = c.buf; if (b.length < 2) return;
      const op = b[0] & 15; let len = b[1] & 127, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > 20000) return close(c);
      const masked = b[1] & 128; if (!masked) return close(c);
      if (b.length < off + 4 + len) return;
      const mask = b.slice(off, off + 4), data = Buffer.from(b.slice(off + 4, off + 4 + len));
      for (let i = 0; i < len; i++) data[i] ^= mask[i & 3];
      c.buf = b.slice(off + 4 + len);
      if (op === 8) return close(c);
      if (op === 9) socket.write(frame(10, data));
      else if (op === 1) { try { onMsg(c, JSON.parse(data.toString())); } catch (e) {} }
    }
  });
  socket.on('close', () => close(c)); socket.on('error', () => close(c));
});

// ---------- Salas ----------
const rooms = new Map();
const num = (v, m) => (typeof v === 'number' && isFinite(v)) ? Math.max(-m, Math.min(m, v)) : 0;
const cleanName = s => String(s || '').replace(/[^\p{L}\p{N} _\-.]/gu, '').trim().slice(0, 14) || 'Jogador';
function newCode() { const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; let s; do { s = ''; for (let i = 0; i < 4; i++) s += A[Math.floor(Math.random() * A.length)]; } while (rooms.has(s)); return s; }
function mkRoom(pub) { const r = { code: newCode(), pub, players: new Map(), seed: (Math.random() * 1e9) | 0 }; rooms.set(r.code, r); return r; }
function join(c, room) {
  room.players.set(c.id, c); c.room = room;
  c.st = { x: 0, y: 0, z: 30, yaw: 0, pitch: 0, w: 1, hp: 100, dead: 0 }; c.kills = 0; c.score = 0;
  send(c, { t: 'joined', id: c.id, code: room.code, pub: room.pub });
  broadcast(room, { t: 'info', msg: c.name + ' entrou' });
}
function leave(c) {
  const r = c.room; if (!r) return; r.players.delete(c.id); c.room = null;
  broadcast(r, { t: 'left', id: c.id, name: c.name, msg: c.name + ' saiu' });
  if (r.players.size === 0) rooms.delete(r.code);
}
function close(c) { if (!c.alive) return; c.alive = false; leave(c); try { c.socket.destroy(); } catch (e) {} }
function broadcast(room, obj, except) { for (const p of room.players.values()) if (p.id !== except) send(p, obj); }

function onMsg(c, m) {
  if (++c.msgs > 60 * 60 * 10) return close(c);
  if (m.t === 'join') {
    if (c.room) return;
    c.name = cleanName(m.name);
    let room;
    if (m.mode === 'create') { if (rooms.size >= MAX_ROOMS) return send(c, { t: 'err', msg: 'Servidor cheio, tenta mais tarde' }); room = mkRoom(false); }
    else if (m.mode === 'code') {
      room = rooms.get(String(m.code || '').toUpperCase().trim());
      if (!room) return send(c, { t: 'err', msg: 'Sala não encontrada' });
      if (room.players.size >= MAX_PLAYERS) return send(c, { t: 'err', msg: 'Sala cheia (10/10)' });
    } else {
      for (const r of rooms.values()) if (r.pub && r.players.size < MAX_PLAYERS) { room = r; break; }
      if (!room) { if (rooms.size >= MAX_ROOMS) return send(c, { t: 'err', msg: 'Servidor cheio, tenta mais tarde' }); room = mkRoom(true); }
    }
    join(c, room);
  } else if (m.t === 'st' && c.room) {
    const s = c.st; s.x = num(m.x, 200); s.y = num(m.y, 100); s.z = num(m.z, 200); s.yaw = num(m.yaw, 100); s.pitch = num(m.pitch, 3);
    s.w = Math.max(0, Math.min(9, m.w | 0)); s.hp = Math.max(0, Math.min(300, m.hp | 0)); s.dead = m.dead ? 1 : 0;
  } else if (m.t === 'shot' && c.room) {
    broadcast(c.room, { t: 'shot', id: c.id, x: num(m.x, 200), y: num(m.y, 100), z: num(m.z, 200), dx: num(m.dx, 1), dy: num(m.dy, 1), dz: num(m.dz, 1) }, c.id);
  } else if (m.t === 'kill' && c.room) { c.kills++; c.score = Math.max(c.score, Math.min(1e7, m.score | 0)); }
}

// Envia o estado de cada sala 15x por segundo
setInterval(() => {
  for (const r of rooms.values()) {
    const list = [];
    for (const p of r.players.values()) list.push({ id: p.id, n: p.name, k: p.kills, s: p.score, ...p.st });
    const msg = { t: 'snap', p: list };
    for (const p of r.players.values()) send(p, msg);
  }
}, 66);

server.listen(PORT, () => console.log('ARENA FPS online na porta ' + PORT));
