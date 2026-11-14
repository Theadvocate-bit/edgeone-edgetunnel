// 端到端测试：模拟 v2ray 客户端通过 ed=2048 early data 建立 VLESS 隧道
const BASE = 'nali.lovie.me';
const UUID = '7dd59004-77a1-4aed-888d-2676cb90f757';

function b64url(buf) {
  return buf.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function buildVlessRequest(host, port, payload) {
  const uuid = Buffer.from(UUID.replace(/-/g, ''), 'hex');
  const hostBuf = Buffer.from(host);
  return Buffer.concat([
    Buffer.from([0x00]), uuid, Buffer.from([0x00, 0x01]),
    Buffer.from([(port >> 8) & 0xff, port & 0xff]),
    Buffer.from([0x02, hostBuf.length]), hostBuf,
    Buffer.from(payload),
  ]);
}

function wssConnect(path, protocol) {
  return new Promise((resolve, reject) => {
    const url = `wss://${BASE}${path}`;
    const ws = new WebSocket(url, protocol ? [protocol] : undefined);
    const state = { ws, messages: [], done: false };
    ws.binaryType = 'arraybuffer';
    const timer = setTimeout(() => {
      if (!state.done) { state.done = true; try { ws.close(); } catch {} reject(new Error('connect/open timeout 15s')); }
    }, 15000);
    ws.addEventListener('open', () => { clearTimeout(timer); resolve(state); });
    ws.addEventListener('message', (ev) => {
      state.messages.push(Buffer.from(ev.data instanceof ArrayBuffer ? new Uint8Array(ev.data) : ev.data));
    });
    ws.addEventListener('error', (ev) => {
      if (!state.done) { state.done = true; clearTimeout(timer); reject(new Error('ws error: ' + (ev.message || 'unknown'))); }
    });
    ws.addEventListener('close', (ev) => { state.closed = { code: ev.code, reason: ev.reason }; });
  });
}

function waitForBytes(state, n, timeoutMs) {
  return new Promise((resolve, reject) => {
    const t0 = Date.now();
    const iv = setInterval(() => {
      const total = Buffer.concat(state.messages);
      if (total.length >= n) { clearInterval(iv); resolve(total); }
      else if (Date.now() - t0 > timeoutMs) { clearInterval(iv); reject(new Error(`wait data timeout, got ${total.length} bytes`)); }
    }, 100);
  });
}

// ---- Test 1: ed=2048 early data 路径 → example.com:80 HTTP 请求 ----
(async () => {
  console.log('== Test 1: VLESS over WSS (ed=2048 early data) -> example.com:80 ==');
  try {
    const req = buildVlessRequest('example.com', 80, 'GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n');
    const state = await wssConnect('/vless?ed=2048', b64url(req));
    console.log('WS opened ✅');
    const data = await waitForBytes(state, 10, 20000);
    console.log(`received ${data.length} bytes, first 4: ${data.subarray(0, 4).toString('hex')}`);
    const body = data.subarray(2).toString('latin1');
    const ok = data[0] === 0 && data[1] === 0 && body.startsWith('HTTP/1.1');
    console.log(ok ? 'TUNNEL WORKS ✅ -> ' + body.split('\r\n')[0] : 'UNEXPECTED DATA ❌: ' + body.slice(0, 120));
    try { state.ws.close(); } catch {}
    process.exit(ok ? 0 : 1);
  } catch (e) {
    console.log('TUNNEL FAILED ❌:', e.message);
    process.exit(1);
  }
})();
