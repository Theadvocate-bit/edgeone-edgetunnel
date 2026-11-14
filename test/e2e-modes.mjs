// v8 双模式线上测试：handlers + pair 各测一次 early-data 隧道
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

async function runTest(label, path, useEarlyData) {
  console.log(`== ${label} ==`);
  const req = buildVlessRequest('example.com', 80, 'GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n');
  await new Promise((resolve) => {
    let settled = false;
    const done = () => { if (!settled) { settled = true; resolve(); } };
    const ws = new WebSocket(`wss://${BASE}${path}`, useEarlyData ? [b64url(req)] : undefined);
    ws.binaryType = 'arraybuffer';
    const t0 = Date.now();
    let opened = false;
    ws.addEventListener('open', () => { opened = true; console.log('  101 handshake ok'); if (!useEarlyData) ws.send(req); });
    ws.addEventListener('message', (ev) => {
      const d = Buffer.from(ev.data instanceof ArrayBuffer ? new Uint8Array(ev.data) : ev.data);
      const text = d.toString('latin1');
      if (d[0] === 0 && d[1] === 0 && text.slice(2).startsWith('HTTP/1.1')) {
        console.log(`  TUNNEL WORKS ✅ ${d.length} bytes -> ${text.slice(2).split('\r\n')[0]}`);
        settled = true; try { ws.close(); } catch {}
      } else if (d.length < 100 && /^[\x20-\x7e]+$/.test(text)) {
        console.log(`  server text: ${text}`); // ready / OPEN-ERR / MSG-ERR 等
      } else {
        console.log(`  binary ${d.length} bytes head=${d.subarray(0, 4).toString('hex')}`);
      }
      if (Date.now() - t0 > 15000) done();
    });
    ws.addEventListener('error', (ev) => { console.log('  ws error:', ev.message || 'unknown'); done(); });
    ws.addEventListener('close', (ev) => {
      console.log(`  closed code=${ev.code} (opened=${opened}, ${Date.now() - t0}ms)`);
      done();
    });
    setTimeout(() => { console.log('  result: NO DATA (15s)'); try { ws.close(); } catch {} done(); }, 16000);
  });
}

await runTest('handlers mode (默认)', '/vless?ed=2048', true);
await runTest('pair mode (?ws-mode=pair)', '/vless?ed=2048&ws-mode=pair', true);
process.exit(0);
