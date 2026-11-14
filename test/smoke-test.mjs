// 冒烟测试（node --experimental-strip-types 运行）：
// 1. vless.ts：模拟 EdgeOne 官方 WS API（升级返回 { websocket: {onopen,onmessage,...} }），
//    验证 VLESS over WS -> net TCP 全链路（普通 + early data 两种路径）
// 2. [[default]].ts：验证 /debug 与 /<UUID> HTTP 端点
import { onRequest as vlessOnRequest } from '../node-functions/vless.ts';
import { onRequest as defaultOnRequest } from '../node-functions/[[default]].ts';

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
function buildVlessRequest(host, port, payload, addrType = 2) {
  const uuidStr = 'd342d11e-d424-4583-b36e-524ab1f0afa4';
  const uuid = Buffer.from(uuidStr.replace(/-/g, ''), 'hex');
  const head = Buffer.concat([
    Buffer.from([0x00]),                 // version
    uuid,                                // 16 bytes uuid
    Buffer.from([0x00]),                 // opt length = 0
    Buffer.from([0x01]),                 // command: TCP
    Buffer.from([(port >> 8) & 0xff, port & 0xff]), // port big-endian
  ]);
  if (addrType === 2) {
    const hostBuf = Buffer.from(host);
    return Buffer.concat([head, Buffer.from([0x02, hostBuf.length]), hostBuf, Buffer.from(payload)]);
  }
  // addrType=1 ipv4（地址类型 1 字节 + 4 字节 IP，无长度前缀）
  return Buffer.concat([head, Buffer.from([0x01]), Buffer.from(host.split('.').map(Number)), Buffer.from(payload)]);
}

async function testWsUpgrade(label, request, sendFirstMessage) {
  const result = await vlessOnRequest({ request, env: {}, waitUntil: () => {} });
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

  // --- 1. 普通 WS 路径：首消息带 VLESS 头 ---
  {
    const request = new Request('https://test.edgeone.app/vless?ed=2048', {
      headers: { upgrade: 'websocket', 'sec-websocket-protocol': '' },
    });
    const buf = buildVlessRequest('example.com', 80,
      'GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n');
    allOk = (await testWsUpgrade('ws-normal', request, buf)) && allOk;
  }

  // --- 2. ed=2048 early data：VLESS 首包经 base64url 放进 sec-websocket-protocol ---
  {
    const buf = buildVlessRequest('example.com', 80,
      'GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n');
    const b64 = buf.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
    const request = new Request('https://test.edgeone.app/vless?ed=2048', {
      headers: { upgrade: 'websocket', 'sec-websocket-protocol': b64 },
    });
    allOk = (await testWsUpgrade('ws-early-data', request, null)) && allOk;
  }

  // --- 3. IPv4 地址类型 ---
  {
    const request = new Request('https://test.edgeone.app/vless', {
      headers: { upgrade: 'websocket' },
    });
    const buf = buildVlessRequest('1.1.1.1', 80,
      'GET / HTTP/1.1\r\nHost: 1.1.1.1\r\nConnection: close\r\n\r\n', 1);
    allOk = (await testWsUpgrade('ws-ipv4', request, buf)) && allOk;
  }

  // --- 4. [[default]].ts：/debug 端点 ---
  {
    const resp = await defaultOnRequest({
      request: new Request('https://t.app/debug'),
      env: {},
    });
    const data = await resp.json();
    console.log('[/debug]', JSON.stringify(data));
    if (!data.dns || data.dns.status !== 'ok') allOk = false;
  }

  // --- 5. [[default]].ts：/<UUID> 配置页 + 升级拒绝 ---
  {
    const uuid = 'd342d11e-d424-4583-b36e-524ab1f0afa4';
    const resp = await defaultOnRequest({
      request: new Request(`https://t.app/${uuid}`),
      env: {},
    });
    const text = await resp.text();
    const cfgOk = text.includes('vless://') && text.includes('/vless?ed=2048');
    console.log(`[config page] ${cfgOk ? 'PASS ✅' : 'FAIL ❌'} (${text.length} chars)`);
    allOk = allOk && cfgOk;

    const resp2 = await defaultOnRequest({
      request: new Request('https://t.app/anything', { headers: { upgrade: 'websocket' } }),
      env: {},
    });
    console.log(`[catch-all upgrade] status=${resp2.status} (期望 426)`);
    allOk = allOk && resp2.status === 426;
  }

  // --- 6. 自定义域名跟随：Host 被平台改写时的域名解析链 ---
  {
    const uuid = 'd342d11e-d424-4583-b36e-524ab1f0afa4';
    const checkHost = async (label, req, env, expect) => {
      const t = await (await defaultOnRequest({ request: req, env })).text();
      // vlessMain 里 address/sni/host 都用同一 hostName
      const ok = t.includes(`@${expect}:443`) && t.includes(`sni=${expect}`);
      console.log(`[custom-host:${label}] ${ok ? 'PASS ✅' : 'FAIL ❌'} (期望 ${expect})`);
      return ok;
    };

    // a. Host 被改写为内部地址，但 X-Forwarded-Host 保留自定义域名
    allOk = await checkHost('x-forwarded-host',
      new Request(`https://internal.edgeone.internal/${uuid}`, {
        headers: { host: 'internal.edgeone.internal', 'x-forwarded-host': 'my.domain.com' },
      }), {}, 'my.domain.com') && allOk;

    // b. 设置 CUSTOM_DOMAIN 环境变量后强制生效（最高优先级）
    allOk = await checkHost('custom-domain-env',
      new Request(`https://whatever.example.com/${uuid}`, {
        headers: { host: 'whatever.example.com', 'x-forwarded-host': 'ignored.com' },
      }), { CUSTOM_DOMAIN: 'my.custom.domain' }, 'my.custom.domain') && allOk;

    // c. 无转发头时回退到 url.hostname（而非被改写的 Host 头）
    allOk = await checkHost('url-hostname-fallback',
      new Request(`https://site.edgeone.app/${uuid}`, {
        headers: { host: 'rewritten.internal' },
      }), {}, 'site.edgeone.app') && allOk;
  }

  console.log(allOk ? 'ALL TESTS PASS ✅' : 'SOME TESTS FAILED ❌');
  process.exit(allOk ? 0 : 1);
}

main().catch(e => { console.error(e); process.exit(1); });
