// 冒烟测试：模拟 EdgeOne Node Functions 官方 WS API（返回 { websocket: {onopen,onmessage,onclose,onerror} }），
// 验证 VLESS over WS -> net TCP 全链路
import { onRequest } from '../node-functions/[[default]].js';

class MockWS {
  constructor() {
    this.sent = [];
    this.readyState = 1;
    this.closed = false;
  }
  send(d) { this.sent.push(Buffer.isBuffer(d) ? d : Buffer.from(new Uint8Array(d))); }
  close() { this.readyState = 3; this.closed = true; }
}

// 构造 VLESS 请求头: version + uuid(16) + optLen + command + port(2) + addrType + addr + payload
function buildVlessRequest(host, port, payload) {
  const uuidStr = 'd342d11e-d424-4583-b36e-524ab1f0afa4';
  const uuid = Buffer.from(uuidStr.replace(/-/g, ''), 'hex');
  const hostBuf = Buffer.from(host);
  return Buffer.concat([
    Buffer.from([0x00]),        // version
    uuid,                       // 16 bytes uuid
    Buffer.from([0x00]),        // opt length = 0
    Buffer.from([0x01]),        // command: TCP
    Buffer.from([(port >> 8) & 0xff, port & 0xff]), // port big-endian
    Buffer.from([0x02, hostBuf.length]), // addrType=2 domain
    hostBuf,
    Buffer.from(payload),
  ]);
}

async function main() {
  const request = new Request('https://test.edgeone.app/vless?ed=2048', {
    headers: { upgrade: 'websocket', 'sec-websocket-protocol': '' },
  });
  const context = { request, env: {}, waitUntil: () => {} };

  const result = await onRequest(context);
  // 官方 API：升级请求应返回 { websocket: {onopen,onmessage,...} }
  console.log('upgrade result keys =', Object.keys(result || {}));
  if (!result || !result.websocket) {
    console.log('SMOKE TEST FAIL ❌  (未返回官方 websocket handler 对象)');
    process.exit(1);
  }

  const ws = new MockWS();
  await result.websocket.onopen(ws, request);
  console.log('onopen done');

  const vlessBuf = buildVlessRequest('example.com', 80,
    'GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n');
  await result.websocket.onmessage(ws, vlessBuf, true);

  await new Promise(r => setTimeout(r, 6000));

  const total = Buffer.concat(ws.sent);
  console.log('received bytes from remote:', total.length);
  console.log('body preview:', total.toString('utf8').slice(0, 200));
  if (total.length > 2 && total.toString('utf8', 2).includes('HTTP/1.1')) {
    console.log('SMOKE TEST PASS ✅  (VLESS 头剥离 + vless resp header 前缀 + TCP 全链路 ok)');
    process.exit(0);
  }
  console.log('SMOKE TEST FAIL ❌');
  process.exit(1);
}

main().catch(e => { console.error(e); process.exit(1); });
