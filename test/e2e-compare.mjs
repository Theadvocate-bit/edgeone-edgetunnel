// 对比测试：early-data 路径 vs 升级后首消息路径
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
    const ws = new WebSocket(`wss://${BASE}${path}`, protocol ? [protocol] : undefined);
    const state = { ws, messages: [], closed: null };
    ws.binaryType = 'arraybuffer';
    const timer = setTimeout(() => { try { ws.close(); } catch {} reject(new Error('open timeout 15s')); }, 15000);
    ws.addEventListener('open', () => { clearTimeout(timer); resolve(state); });
    ws.addEventListener('message', (ev) => {
      state.messages.push(Buffer.from(ev.data instanceof ArrayBuffer ? new Uint8Array(ev.data) : ev.data));
    });
    ws.addEventListener('error', (ev) => { if (!state.resolved) reject(new Error('ws error: ' + (ev.message || 'unknown'))); });
    ws.addEventListener('close', (ev) => { state.closed = { code: ev.code, reason: ev.reason }; });
  });
}

function waitForBytes(state, n, timeoutMs) {
  return new Promise((resolve) => {
    const t0 = Date.now();
    const iv = setInterval(() => {
      const total = Buffer.concat(state.messages);
      if (total.length >= n) { clearInterval(iv); resolve(total); }
      else if (Date.now() - t0 > timeoutMs) {
        clearInterval(iv);
        console.log(`  [timeout] got ${total.length} bytes, closed=${JSON.stringify(state.closed)}`);
        resolve(total);
      }
    }, 100);
  });
}

async function runTest(label, path, useEarlyData) {
  console.log(`== ${label} ==`);
  const req = buildVlessRequest('example.com', 80, 'GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n');
  try {
    const state = await wssConnect(path, useEarlyData ? b64url(req) : undefined);
    console.log('WS opened ✅ (101 handshake ok)');
    if (!useEarlyData) state.ws.send(req);
    const data = await waitForBytes(state, 10, 15000);
    if (data.length >= 3 && data[0] === 0 && data[1] === 0) {
      console.log(`TUNNEL WORKS ✅ received ${data.length} bytes -> ${data.subarray(2).toString('latin1').split('\r\n')[0]}`);
    } else if (data.length > 0) {
      console.log(`received ${data.length} bytes but unexpected: ${data.subarray(0, 60).toString('latin1')}`);
    } else {
      console.log('NO DATA ❌');
    }
    try { state.ws.close(); } catch {}
  } catch (e) {
    console.log('FAILED ❌:', e.message);
  }
}

await runTest('A: early-data (sec-websocket-protocol)', '/vless?ed=2048', true);
await runTest('B: first-message (open 后 send)', '/vless', false);
process.exit(0);
