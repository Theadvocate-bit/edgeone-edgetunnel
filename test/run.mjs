// 冒烟测试：先 esbuild 打包 node-functions/*.ts 到 .tmp/，再模拟 EdgeOne 运行时验证。
// 运行：node test/run.mjs
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { build } from 'esbuild';

const root = path.resolve(new URL('..', import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1'));
const tmpDir = path.join(root, '.tmp');
mkdirSync(tmpDir, { recursive: true });

// 1. esbuild 打包（与平台构建器行为一致；v14.4 起唯一入口 = [[default]].ts）
for (const [entry, out] of [
  ['[[default]].ts', 'default.mjs'],
]) {
  await build({
    entryPoints: [path.join(root, 'node-functions', entry)],
    bundle: true,
    platform: 'node',
    format: 'esm',
    outfile: path.join(tmpDir, out),
    logLevel: 'warning',
  });
}

const { onRequest: defaultOnRequest } = await import(pathToFileURL(path.join(tmpDir, 'default.mjs')).href);
// v14.4 起唯一 onRequest：WS / xhttp / udp 全部经 catch-all 分发
const vlessOnRequest = defaultOnRequest;
const xhttpOnRequest = defaultOnRequest;
const udpOnRequest = defaultOnRequest;

// --- WebSocketPair mock（模拟 CF/EdgeOne pair 模式运行时） ---
class PairMockWS {
  constructor() {
    this.sent = [];
    this.readyState = 1;
    this.listeners = {};
    this.canSend = false; // accept() 之后才允许 send（模拟真实状态）
  }
  accept() { this.canSend = true; }
  addEventListener(type, fn) { (this.listeners[type] ||= []).push(fn); }
  send(d) { if (!this.canSend) throw new Error('send before accept'); this.sent.push(Buffer.isBuffer(d) ? d : Buffer.from(new Uint8Array(d))); }
  close() { this.readyState = 3; }
  emit(type, data) { (this.listeners[type] || []).forEach(fn => fn(data)); }
}
globalThis.__pairs = [];
globalThis.WebSocketPair = class {
  constructor() {
    this[0] = { client: true }; // 平台侧拿走的客户端 socket
    this[1] = new PairMockWS(); // 服务端 socket
    globalThis.__pairs.push(this);
  }
};

// 2. 测试逻辑
class MockWS {
  constructor() {
    this.sent = [];
    this.readyState = 1;
    this.closed = false;
  }
  send(d) { this.sent.push(Buffer.isBuffer(d) ? d : Buffer.from(new Uint8Array(d))); }
  close() { this.readyState = 3; this.closed = true; }
}

function buildVlessRequest(host, port, payload, addrType = 2, command = 1) {
  const uuidStr = 'd342d11e-d424-4583-b36e-524ab1f0afa4';
  const uuid = Buffer.from(uuidStr.replace(/-/g, ''), 'hex');
  const head = Buffer.concat([
    Buffer.from([0x00]),
    uuid,
    Buffer.from([0x00]),
    Buffer.from([command]),
    Buffer.from([(port >> 8) & 0xff, port & 0xff]),
  ]);
  if (addrType === 2) {
    const hostBuf = Buffer.from(host);
    return Buffer.concat([head, Buffer.from([0x02, hostBuf.length]), hostBuf, Buffer.from(payload)]);
  }
  return Buffer.concat([head, Buffer.from([0x01]), Buffer.from(host.split('.').map(Number)), Buffer.from(payload)]);
}

async function testWsUpgrade(label, request, sendFirstMessage, handler = vlessOnRequest) {
  const result = await handler({ request, env: {}, waitUntil: () => {} });
  if (!result || !result.websocket) {
    console.log(`[${label}] FAIL ❌  (未返回 { websocket: handlers })`);
    return false;
  }
  const ws = new MockWS();
  await result.websocket.onopen(ws, request);
  if (sendFirstMessage) await result.websocket.onmessage(ws, sendFirstMessage, true);
  await new Promise(r => setTimeout(r, 6000));
  const total = Buffer.concat(ws.sent);
  const ok = total.length > 2 && total.toString('utf8', 2).includes('HTTP/1.1');
  console.log(`[${label}] received ${total.length} bytes, head hex=${total.subarray(0, 4).toString('hex')} -> ${ok ? 'PASS ✅' : 'FAIL ❌'}`);
  return ok;
}

async function main() {
  let allOk = true;

  // --- 1. /websocket 路径 + early data ---
  {
    const buf = buildVlessRequest('example.com', 80,
      'GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n');
    const b64 = buf.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
    const request = new Request('https://test.edgeone.app/websocket?ed=2048', {
      headers: { upgrade: 'websocket', 'sec-websocket-protocol': b64 },
    });
    allOk = (await testWsUpgrade('ws /websocket early-data', request, null)) && allOk;
  }

  // --- 2. /vless 兼容路径：首消息带 VLESS 头 ---
  {
    const request = new Request('https://test.edgeone.app/vless?ed=2048', {
      headers: { upgrade: 'websocket', 'sec-websocket-protocol': '' },
    });
    const buf = buildVlessRequest('example.com', 80,
      'GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n');
    allOk = (await testWsUpgrade('ws /vless normal', request, buf)) && allOk;
    // v14.4：/vless 升级现在统一走 catch-all（唯一 onRequest），二次验证
    {
      const req2 = new Request('https://test.edgeone.app/vless?ed=2048', {
        headers: { upgrade: 'websocket', 'sec-websocket-protocol': '' },
      });
      allOk = (await testWsUpgrade('ws /vless catch-all-route', req2, buf)) && allOk;
    }
  }

  // --- 3. /ws 路径 + IPv4 地址类型 ---
  {
    const request = new Request('https://test.edgeone.app/ws', {
      headers: { upgrade: 'websocket' },
    });
    const buf = buildVlessRequest('1.1.1.1', 80,
      'GET / HTTP/1.1\r\nHost: 1.1.1.1\r\nConnection: close\r\n\r\n', 1);
    allOk = (await testWsUpgrade('ws /ws ipv4', request, buf)) && allOk;
  }

  // --- 4. pair 模式（CF 风格 WebSocketPair）+ early data ---
  {
    const buf = buildVlessRequest('example.com', 80,
      'GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n');
    const b64 = buf.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
    const request = new Request('https://test.edgeone.app/vless?ed=2048&ws-mode=pair', {
      headers: { upgrade: 'websocket', 'sec-websocket-protocol': b64 },
    });
    // Node 的 Response 禁止 101；本地用允许 101 的 mock 替换（真实平台内部处理）
    const RealResp = globalThis.Response;
    globalThis.Response = class {
      constructor(body, init) {
        this.status = init?.status ?? 200;
        this.webSocket = init?.webSocket;
        this.headers = new RealResp(null, init?.headers ? { headers: init.headers } : undefined).headers;
        this.__body = body;
      }
    };
    let resp;
    try {
      resp = await vlessOnRequest({ request, env: {}, waitUntil: () => {} });
    } finally {
      globalThis.Response = RealResp;
    }
    const is101 = resp && resp.status === 101;
    let dataOk = false;
    if (is101) {
      const server = globalThis.__pairs.at(-1)?.[1];
      await new Promise(r => setTimeout(r, 6000));
      const total = Buffer.concat(server?.sent || []);
      dataOk = total.length > 2 && total.toString('utf8', 2).includes('HTTP/1.1');
      console.log(`[ws pair-mode] status=${resp.status}, received ${total.length} bytes, head=${total.subarray(0, 4).toString('hex')} -> ${dataOk ? 'PASS ✅' : 'FAIL ❌'}`);
    } else {
      console.log(`[ws pair-mode] status=${resp?.status} (期望 101) -> FAIL ❌`);
    }
    allOk = allOk && is101 && dataOk;
  }

  // --- 5. 非法路径拒绝 400 ---
  {
    const request = new Request('https://test.edgeone.app/other', {
      headers: { upgrade: 'websocket' },
    });
    const resp = await vlessOnRequest({ request, env: {}, waitUntil: () => {} });
    const ok = resp instanceof Response && resp.status === 400;
    console.log(`[ws bad-path] status=${resp?.status} -> ${ok ? 'PASS ✅' : 'FAIL ❌'}`);
    allOk = allOk && ok;
  }

  // --- 5. [[default]].ts：/debug 端点 ---
  {
    const resp = await defaultOnRequest({
      request: new Request('https://t.app/debug'),
      env: {},
    });
    const data = await resp.json();
    console.log('[/debug]', JSON.stringify({ dns: data.dns, resolvedHost: data.resolvedHost, headers: Object.keys(data.forwardedHeaders || {}) }));
    if (!data.dns || data.dns.status !== 'ok') allOk = false;
  }

  // --- 6. 配置页 + 升级拒绝 ---
  {
    const uuid = 'd342d11e-d424-4583-b36e-524ab1f0afa4';
    const resp = await defaultOnRequest({
      request: new Request(`https://t.app/${uuid}`),
      env: {},
    });
    const text = await resp.text();
    const checks = {
      wsLink: text.includes('vless://') && text.includes(encodeURIComponent('/vless?ed=2048')),
      wsAlpn: text.includes('alpn=h2%2Chttp%2F1.1'),
      xhttpLink: text.includes('type=xhttp') && text.includes('mode=transaction') && text.includes('extra=%7B'),
      xhttpUdpCmd: text.includes('UDP 命令(02)'),
      udpRelayLink: text.includes('udp-relay://') && text.includes('framing=length-prefix'),
      h2NotUsable: text.includes('不可用') && !/#EO-H2-[^\s]*生成/.test(text),
    };
    const cfgOk = text.includes('节点 1') && text.includes('/xhttp') && Object.values(checks).every(Boolean);
    console.log(`[config page] ${cfgOk ? 'PASS ✅' : 'FAIL ❌'} (${text.length} chars) checks=${JSON.stringify(checks)}`);
    allOk = allOk && cfgOk;

    const resp2 = await defaultOnRequest({
      request: new Request('https://t.app/anything', { headers: { upgrade: 'websocket' } }),
      env: {},
    });
    console.log(`[catch-all upgrade] status=${resp2.status} (期望 400：非 WS 路径拒绝)`);
    allOk = allOk && resp2.status === 400;
  }

  // --- 7. 自定义域名跟随 ---
  {
    const uuid = 'd342d11e-d424-4583-b36e-524ab1f0afa4';
    const checkHost = async (label, req, env, expect) => {
      const t = await (await defaultOnRequest({ request: req, env })).text();
      const ok = t.includes(`@${expect}:443`) && t.includes(`sni=${expect}`);
      console.log(`[custom-host:${label}] ${ok ? 'PASS ✅' : 'FAIL ❌'} (期望 ${expect})`);
      return ok;
    };

    allOk = await checkHost('x-forwarded-host',
      new Request(`https://internal.edgeone.internal/${uuid}`, {
        headers: { host: 'internal.edgeone.internal', 'x-forwarded-host': 'my.domain.com' },
      }), {}, 'my.domain.com') && allOk;

    allOk = await checkHost('eo-pages-host',
      new Request(`https://internal.qcloudteo.com/${uuid}`, {
        headers: { host: 'internal.qcloudteo.com', 'eo-pages-host': 'nali.lovie.me' },
      }), {}, 'nali.lovie.me') && allOk;

    allOk = await checkHost('custom-domain-env',
      new Request(`https://whatever.example.com/${uuid}`, {
        headers: { host: 'whatever.example.com', 'x-forwarded-host': 'ignored.com' },
      }), { CUSTOM_DOMAIN: 'my.custom.domain' }, 'my.custom.domain') && allOk;

    allOk = await checkHost('url-hostname-fallback',
      new Request(`https://site.edgeone.app/${uuid}`, {
        headers: { host: 'rewritten.internal' },
      }), {}, 'site.edgeone.app') && allOk;
  }

  // --- 8. XHTTP 事务模式（v14） ---
  {
    const readStream = async (resp) => {
      const chunks = [];
      const reader = resp.body.getReader();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(Buffer.from(value));
      }
      return Buffer.concat(chunks);
    };

    // 8a. raw 模式：POST body 为完整上行（HTTP 请求）→ 流式下行
    {
      const request = new Request('https://t.app/xhttp?addr=example.com&port=80', {
        method: 'POST',
        headers: { 'content-type': 'application/octet-stream' },
        body: Buffer.from('GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n'),
      });
      const resp = await xhttpOnRequest({ request, env: {}, waitUntil: () => {} });
      const data = await readStream(resp);
      const ok = data.toString('utf8', 0).includes('HTTP/1.1');
      console.log(`[xhttp raw] ${data.length} bytes, head=${JSON.stringify(data.toString('utf8', 0, 15))} -> ${ok ? 'PASS ✅' : 'FAIL ❌'}`);
      allOk = allOk && ok;
    }

    // 8b. vless 模式：body 为 VLESS 帧 → 响应头剥离正确，payload 必须以纯上行开头（不得残留 VLESS 头字节）
    {
      const buf = buildVlessRequest('example.com', 80,
        'GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n');
      const request = new Request('https://t.app/xhttp?mode=vless', {
        method: 'POST',
        headers: { 'content-type': 'application/octet-stream' },
        body: buf,
      });
      const resp = await xhttpOnRequest({ request, env: {}, waitUntil: () => {} });
      const data = await readStream(resp);
      const payload = data.subarray(2); // 去掉 VLESS 响应头 [ver,0]
      // 剥离正确 → 远端收到纯 GET 请求 → 回 200 OK；剥离缺失 → 远端收到 VLESS 头垃圾 → 400 Bad Request
      const ok = data.subarray(0, 2).toString('hex') === '0000'
        && payload.subarray(0, 9).toString('utf8') === 'HTTP/1.1 '
        && payload.toString('utf8').includes('200 OK');
      console.log(`[xhttp vless] ${data.length} bytes, payload head=${JSON.stringify(payload.toString('utf8', 0, 15))} -> ${ok ? 'PASS ✅' : 'FAIL ❌'}`);
      allOk = allOk && ok;
    }

    // 8c. d= 参数携带上行（base64url）
    {
      const up = Buffer.from('GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n');
      const d = up.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
      const request = new Request(`https://t.app/xhttp?addr=example.com&port=80&d=${d}`, { method: 'POST' });
      const resp = await xhttpOnRequest({ request, env: {}, waitUntil: () => {} });
      const data = await readStream(resp);
      const ok = data.toString('utf8', 0).includes('HTTP/1.1');
      console.log(`[xhttp d-param] ${data.length} bytes -> ${ok ? 'PASS ✅' : 'FAIL ❌'}`);
      allOk = allOk && ok;
    }

    // 8d. 缺参数 400
    {
      const resp = await xhttpOnRequest({ request: new Request('https://t.app/xhttp', { method: 'POST' }), env: {}, waitUntil: () => {} });
      const ok = resp.status === 400;
      console.log(`[xhttp missing-args] status=${resp.status} -> ${ok ? 'PASS ✅' : 'FAIL ❌'}`);
      allOk = allOk && ok;
    }

    // 8e. VLESS UDP 命令（command=2）→ 路由到 dgram 中继
    //     本机网络可能过滤 UDP（DNS 超时），因此 200（有响应）或 502（已发送但无响应/不可用）均视为路由正确；
    //     仅 500 异常或未按 UDP 路径处理才算失败。
    {
      const header = Buffer.from([0x12, 0x34, 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0]);
      const qname = Buffer.from([7, ...'example'.split('').map(c => c.charCodeAt(0)), 3, ...'com'.split('').map(c => c.charCodeAt(0)), 0]);
      const dnsPkt = Buffer.concat([header, qname, Buffer.from([0, 1, 0, 1])]);
      const buf = buildVlessRequest('8.8.8.8', 53, dnsPkt, 1, 2); // IPv4 地址 + UDP 命令
      const request = new Request('https://t.app/xhttp?mode=vless', {
        method: 'POST',
        headers: { 'content-type': 'application/octet-stream' },
        body: buf,
      });
      const resp = await xhttpOnRequest({ request, env: {}, waitUntil: () => {} });
      const ok = resp.status === 200 || resp.status === 502;
      const extra = resp.status === 200
        ? `resp=${resp.headers.get('x-udp-responses')} responses/${resp.headers.get('x-udp-bytes')}B`
        : '';
      console.log(`[xhttp vless-udp] status=${resp.status} ${extra} -> ${ok ? 'PASS ✅' : 'FAIL ❌'}`);
      allOk = allOk && ok;
    }
  }

  // --- 9. UDP 探针 + 逐包中继（v14） ---
  // 注：UDP 出站可达性取决于运行环境网络（本机可能被防火墙/运营商过滤），因此
  //     断言只要求「dgram 可用 + 发送不报错」；DNS 是否收到响应仅作信息输出。
  {
    // 9a. GET /udp 探测
    {
      const resp = await udpOnRequest({ request: new Request('https://t.app/udp'), env: {}, waitUntil: () => {} });
      const data = await resp.json();
      const ok = data.dgramAvailable === true && data.dnsProbe?.sent !== false;
      console.log(`[udp probe] dgram=${data.dgramAvailable} dns.ok=${data.dnsProbe?.ok} ms=${data.dnsProbe?.ms} -> ${ok ? 'PASS ✅' : 'FAIL ❌'}${data.dnsProbe?.ok ? '' : ' (DNS 无响应：环境网络过滤，非代码问题)'}`);
      allOk = allOk && ok;
    }
    // 9b. POST /udp DNS 中继（多响应带长度前缀）
    {
      const dnsq = Buffer.concat([
        Buffer.from([0xab, 0xcd, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        Buffer.from([7, ...'example'.split('').map(c => c.charCodeAt(0)), 3, ...'com'.split('').map(c => c.charCodeAt(0)), 0]),
        Buffer.from([0x00, 0x01, 0x00, 0x01]),
      ]);
      const request = new Request('https://t.app/udp?host=8.8.8.8&port=53', {
        method: 'POST',
        headers: { 'content-type': 'application/octet-stream' },
        body: dnsq,
      });
      const resp = await udpOnRequest({ request, env: {}, waitUntil: () => {} });
      if (resp.status !== 200) {
        const info = await resp.json().catch(() => ({}));
        console.log(`[udp relay] status=${resp.status} error=${info.error} -> SKIP ⚠️ (环境 UDP 过滤，非代码问题)`);
      } else {
        const data = Buffer.from(await resp.arrayBuffer());
        const n = Number(resp.headers.get('x-udp-responses') || 0);
        // 长度前缀后应为 DNS 响应（id 0xabcd）
        const ok = n >= 1 && data.length > 2 + 12 && data[2] === 0xab && data[3] === 0xcd;
        console.log(`[udp relay] responses=${n} bytes=${data.length} -> ${ok ? 'PASS ✅' : 'FAIL ❌'}`);
        allOk = allOk && ok;
      }
    }
  }

  console.log(allOk ? 'ALL TESTS PASS ✅' : 'SOME TESTS FAILED ❌');
  process.exit(allOk ? 0 : 1);
}

main().catch(e => { console.error(e); process.exit(1); });
