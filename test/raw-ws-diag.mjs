// 原生 WS 客户端诊断：raw TLS + 手工升级 + 手工解析 WS 帧
// 用途：确认 101 之后客户端 TCP 上到底有没有字节到达（区分"帧格式错"vs"完全没发"）
import tls from 'node:tls';
import crypto from 'node:crypto';

const HOST = 'nali.lovie.me';
const PATH = '/vless?ed=2048';

function buildVlessRequest(host, port, payload) {
  const uuid = Buffer.from('d342d11ed4244583b36e524ab1f0afa4', 'hex');
  const hostBuf = Buffer.from(host);
  return Buffer.concat([
    Buffer.from([0x00]), uuid, Buffer.from([0x00, 0x01]),
    Buffer.from([0x00, 0x00]), // options length 0
    Buffer.from([0x01]),
    Buffer.from([(port >> 8) & 0xff, port & 0xff]),
    Buffer.from([0x02, hostBuf.length]), hostBuf,
    Buffer.from(payload),
  ]);
}

function maskFrame(payload) {
  const mask = crypto.randomBytes(4);
  const masked = Buffer.alloc(payload.length);
  for (let i = 0; i < payload.length; i++) masked[i] = payload[i] ^ mask[i % 4];
  let header;
  if (payload.length < 126) header = Buffer.from([0x82, 0x80 | payload.length]);
  else if (payload.length < 65536) header = Buffer.from([0x82, 0x80 | 126, payload.length >> 8, payload.length & 0xff]);
  else header = Buffer.from([0x82, 0x80 | 127, 0, 0, 0, 0, ...Number(payload.length).toString(16).padStart(16, '0').match(/../g).map(h => parseInt(h, 16))]);
  return Buffer.concat([header, mask, masked]);
}

function parseServerFrame(buf, state) {
  // 服务端帧不掩码。返回 {frames, rest}
  const frames = [];
  let i = 0;
  while (i + 2 <= buf.length) {
    const fin = (buf[i] & 0x80) !== 0;
    const opcode = buf[i] & 0x0f;
    let len = buf[i + 1] & 0x7f;
    let idx = i + 2;
    if (len === 126) { if (i + 4 > buf.length) break; len = buf.readUInt16BE(i + 2); idx = i + 4; }
    else if (len === 127) { if (i + 10 > buf.length) break; len = Number(buf.readBigUInt64BE(i + 2)); idx = i + 10; }
    if (idx + len > buf.length) break;
    frames.push({ fin, opcode, len, payload: buf.subarray(idx, idx + len) });
    i = idx + len;
  }
  return { frames, rest: buf.subarray(i) };
}

const key = crypto.randomBytes(16).toString('base64');
const sock = tls.connect({ host: HOST, port: 443, servername: HOST }, () => {
  const req =
    `GET ${PATH} HTTP/1.1\r\n` +
    `Host: ${HOST}\r\n` +
    `Upgrade: websocket\r\n` +
    `Connection: Upgrade\r\n` +
    `Sec-WebSocket-Key: ${key}\r\n` +
    `Sec-WebSocket-Version: 13\r\n` +
    `User-Agent: raw-diag/1.0\r\n\r\n`;
  sock.write(req);
});

let upgradeDone = false;
let buf = Buffer.alloc(0);
let byteCount = 0;
const t0 = Date.now();

sock.on('data', (d) => {
  buf = Buffer.concat([buf, d]);
  if (!upgradeDone) {
    const idx = buf.indexOf('\r\n\r\n');
    if (idx === -1) return;
    const head = buf.subarray(0, idx).toString();
    console.log('=== UPGRADE RESPONSE ===');
    console.log(head.split('\r\n').slice(0, 12).join('\n'));
    upgradeDone = true;
    buf = buf.subarray(idx + 4);
    // 升级完成后发送 VLESS 请求（example.com:80）
    const vless = buildVlessRequest('example.com', 80, 'GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n');
    sock.write(maskFrame(vless));
    console.log('=== VLESS request sent (first-message mode, no early data) ===');
  }
  byteCount += d.length;
  const { frames, rest } = parseServerFrame(buf);
  buf = Buffer.from(rest);
  for (const f of frames) {
    console.log(`[+${Date.now() - t0}ms] FRAME opcode=${f.opcode} fin=${f.fin} len=${f.len} hex=${f.payload.subarray(0, 24).toString('hex')}${f.len > 24 ? '...' : ''}${f.opcode === 1 ? ' text=' + f.payload.toString().slice(0, 60) : ''}`);
  }
  if (frames.length === 0 && buf.length > 0) {
    console.log(`[+${Date.now() - t0}ms] partial bytes (${buf.length}): ${buf.subarray(0, 32).toString('hex')}`);
  }
});

sock.on('error', (e) => console.log('SOCK-ERR:', e.message));
sock.on('close', () => console.log(`SOCK CLOSED totalBytes=${byteCount} elapsed=${Date.now() - t0}ms`));

setTimeout(() => {
  console.log(`--- 12s elapsed: total=${byteCount} bytes, pending=${buf.length} ---`);
  sock.destroy();
  process.exit(0);
}, 12000);
