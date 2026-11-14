// 冒烟测试：模拟 EdgeOne Node Functions 运行时，验证 VLESS over WS -> net TCP 全链路
import { onRequest } from '../node-functions/[[default]].js';

class MockWS {
  constructor() {
    this.listeners = {};
    this.sent = [];
    this.readyState = 1;
  }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  accept() {}
  send(d) { this.sent.push(Buffer.from(new Uint8Array(d))); }
  close() { this.readyState = 3; (this.listeners['close'] || []).forEach(f => f()); }
  emitMessage(data) { (this.listeners['message'] || []).forEach(f => f({ data })); }
  emitError(e) { (this.listeners['error'] || []).forEach(f => f(e)); }
}

class WebSocketPair {
  constructor() {
    this.socket = new MockWS();   // 服务端
    this.client = new MockWS();   // 客户端
    // 模拟平台内置 pair：Node 的 Response 构造器不允许 101，这里用普通对象模拟
    this.response = { status: 101, headers: new Headers() };
  }
}
globalThis.WebSocketPair = WebSocketPair;

// 构造 VLESS 请求头: version + uuid(16) + optLen + command + port(2) + addrType + addr + payload
function buildVlessRequest(host, port, payload) {
  const uuidStr = 'd342d11e-d424-4583-b36e-524ab1f0afa4';
  const uuid = Buffer.from(uuidStr.replace(/-/g, ''), 'hex');
  const hostBuf = Buffer.from(host);
  const head = Buffer.concat([
    Buffer.from([0x00]),        // version
    uuid,                       // 16 bytes uuid
    Buffer.from([0x00]),        // opt length = 0
    Buffer.from([0x01]),        // command: TCP
    Buffer.from([(port >> 8) & 0xff, port & 0xff]), // port big-endian
    Buffer.from([0x02, hostBuf.length]), // addrType=2 domain
    hostBuf,
  ]);
  return Buffer.concat([head, Buffer.from(payload)]);
}

async function main() {
  const request = new Request('https://test.edgeone.app/vless', {
    headers: { Upgrade: 'websocket', 'sec-websocket-protocol': '' },
    // @ts-ignore 双工
    duplex: 'half',
  });
  const context = { request, env: {}, waitUntil: () => {} };

  const response = await onRequest(context);
  console.log('upgrade response status =', response.status); // 期望 101

  const serverWS = globalThis.__lastPair.socket;
  const clientWS = globalThis.__lastPair.client;

  const vlessBuf = buildVlessRequest('example.com', 80,
    'GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n');

  // 函数监听 pair.socket（服务端），客户端发来的消息要在服务端 socket 上触发
  serverWS.emitMessage(vlessBuf.buffer.slice(vlessBuf.byteOffset, vlessBuf.byteOffset + vlessBuf.byteLength));

  await new Promise(r => setTimeout(r, 5000));

  const total = Buffer.concat(serverWS.sent);
  const text = total.toString('utf8', Math.min(total.length, 64));
  console.log('received bytes from remote:', total.length);
  console.log('first 64 bytes (after vless resp header):', text.slice(0, 200));
  if (total.length > 24 && total.toString().includes('HTTP/1.1')) {
    console.log('SMOKE TEST PASS ✅  (VLESS -> net TCP -> response ok)');
  } else {
    console.log('SMOKE TEST FAIL ❌');
    process.exit(1);
  }
  process.exit(0);
}

// hook: 拦截 WebSocketPair 以拿到 client 侧
const OrigPair = WebSocketPair;
globalThis.WebSocketPair = class extends OrigPair {
  constructor() {
    super();
    globalThis.__lastPair = this;
  }
};

main().catch(e => { console.error(e); process.exit(1); });
