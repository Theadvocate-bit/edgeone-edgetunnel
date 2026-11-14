// 冒烟测试：先 esbuild 打包 node-functions/*.ts 到 .tmp/，再模拟 EdgeOne 运行时验证。
// 运行：node test/run.mjs
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { build } from 'esbuild';

const root = path.resolve(new URL('..', import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1'));
const tmpDir = path.join(root, '.tmp');
mkdirSync(tmpDir, { recursive: true });

// 1. esbuild 打包（与平台构建器行为一致）
for (const [entry, out] of [
  ['websocket.ts', 'websocket.mjs'],
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

const { onRequest: vlessOnRequest } = await import(pathToFileURL(path.join(tmpDir, 'websocket.mjs')).href);
const { onRequest: defaultOnRequest } = await import(pathToFileURL(path.join(tmpDir, 'default.mjs')).href);

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

function buildVlessRequest(host, port, payload, addrType = 2) {
  const uuidStr = 'd342d11e-d424-4583-b36e-524ab1f0afa4';
  const uuid = Buffer.from(uuidStr.replace(/-/g, ''), 'hex');
  const head = Buffer.concat([
    Buffer.from([0x00]),
    uuid,
    Buffer.from([0x00]),
    Buffer.from([0x01]),
    Buffer.from([(port >> 8) & 0xff, port & 0xff]),
  ]);
  if (addrType === 2) {
    const hostBuf = Buffer.from(host);
    return Buffer.concat([head, Buffer.from([0x02, hostBuf.length]), hostBuf, Buffer.from(payload)]);
  }
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

  // --- 4. 非法路径拒绝 400 ---
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

  console.log(allOk ? 'ALL TESTS PASS ✅' : 'SOME TESTS FAILED ❌');
  process.exit(allOk ? 0 : 1);
}

main().catch(e => { console.error(e); process.exit(1); });
