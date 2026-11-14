// EdgeOne Pages Node Functions —— HTTP 兜底路由（catch-all）
// 注意：WebSocket 升级统一由 websocket.ts（官方形态文件名）处理。
// 本文件只服务 HTTP 端点：
//   - /debug        自诊断端点（DNS / 出站 TCP 探测 / 请求头回显）
//   - /<UUID>       v2ray / clash-meta 配置文本
//   - 其它          404（/ 静态首页优先）

import dns from 'node:dns';
import net from 'node:net';
import { getFullConfig } from './config.ts';
import { getWsDiag } from './websocket.ts';

let userID = 'd342d11e-d424-4583-b36e-524ab1f0afa4';
let proxyIP = '';

// 实例亲和性探针：模块级计数器（跨请求存活 => 同一实例 => 有状态会话可行）
let affinityCount = 0;
const AFFINITY_BOOT = Math.random().toString(36).slice(2, 8);

/**
 * @param {any} context EdgeOne Node Functions EventContext
 */
export async function onRequest(context: any) {
  const { request, env } = context;
  try {
    userID = env?.UUID || userID;
    proxyIP = env?.PROXYIP || proxyIP;

    // 升级请求不应到达这里；明确拒绝，避免平台对 catch-all 的 WS 处理出现 500
    const upgradeHeader: any = request.headers.get('upgrade');
    if (upgradeHeader?.toLowerCase() === 'websocket') {
      return new Response(
        `WebSocket endpoint is /vless. Use wss://<host>/vless?ed=2048\n`,
        { status: 426, headers: { 'Content-Type': 'text/plain;charset=utf-8' } }
      );
    }

    const url = new URL(request.url);
    switch (url.pathname) {
      case '/affinity': {
        // 实例亲和性探针：连续访问时 count 递增且 boot 相同 => 有状态会话可行
        affinityCount++;
        // 顺带探测缓冲请求体是否可用（POST 时）
        let bufferedBody: string | null = null;
        if ((request as any).method === 'POST') {
          try { bufferedBody = (await request.text()).slice(0, 200); } catch (e: any) { bufferedBody = 'ERR: ' + e?.message; }
        }
        return new Response(JSON.stringify({ count: affinityCount, boot: AFFINITY_BOOT, bufferedBody }, null, 2), {
          status: 200,
          headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' },
        });
      }
      case '/debug': {
        // 自诊断端点：检查 DNS 与出站 TCP 是否可用
        const diag = await handleDebug();
        diag.resolvedHost = resolveHostName(request, env); // 供确认配置生成所用的域名
        diag.requestUrl = request.url;
        // 回显平台转发时保留的头（用于诊断自定义域名跟随）
        const fwdHeaders: Record<string, string> = {};
        (request.headers as any).forEach?.((v: string, k: string) => {
          if (/host|forward|origin|referer|x-real|eo-/i.test(k)) fwdHeaders[k] = v;
        });
        diag.forwardedHeaders = fwdHeaders;
        diag.wsDiagnostics = getWsDiag();
        return new Response(JSON.stringify(diag, null, 2), {
          status: 200,
          headers: { 'Content-Type': 'application/json;charset=utf-8' },
        });
      }
      case `/${userID}`: {
        const hostName = resolveHostName(request, env);
        return new Response(getFullConfig(userID, hostName), {
          status: 200,
          headers: { 'Content-Type': 'text/plain;charset=utf-8' },
        });
      }
      case '/stream-out': {
        // 流式响应能力探针：每 500ms 发一块，共 6 块（3 秒）
        const stream = new ReadableStream({
          start(controller) {
            const enc = new TextEncoder();
            let i = 0;
            const t = setInterval(() => {
              i++;
              try {
                controller.enqueue(enc.encode(`chunk ${i} @${Date.now()}\n`));
                if (i >= 6) { clearInterval(t); controller.close(); }
              } catch { clearInterval(t); }
            }, 500);
          },
        });
        return new Response(stream, {
          status: 200,
          headers: { 'Content-Type': 'text/plain', 'X-Accel-Buffering': 'no', 'Cache-Control': 'no-store' },
        });
      }
      case '/stream-in': {
        // 流式请求体能力探针：读 POST body 的每个 chunk 并记录到达时间
        const bodyType = typeof (request as any).body;
        const reqKeys = Object.keys(request || {});
        const chunks: number[] = [];
        const times: number[] = [];
        const start = Date.now();
        const reader = (request.body as any)?.getReader?.();
        if (!reader) {
          return new Response(JSON.stringify({ bodyType, reqKeys, error: 'no request.body stream' }), { status: 200, headers: { 'Content-Type': 'application/json' } });
        }
        let bytes = 0;
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          bytes += value?.byteLength || 0;
          chunks.push(value?.byteLength || 0);
          times.push(Date.now() - start);
        }
        return new Response(JSON.stringify({ chunks, times, bytes, totalMs: Date.now() - start }, null, 2), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      default: {
        // `/` 静态首页优先；这里兜底其它未匹配路径
        return new Response('Not found', { status: 404 });
      }
    }
  } catch (err: any) {
    return new Response(String(err), { status: 500 });
  }
}

// ---------------------------------------------------------------------------
// 域名解析（配置生成用：address / sni / host 必须跟随用户实际访问的域名）
// ---------------------------------------------------------------------------

/**
 * 解析客户端配置应使用的域名。
 * 优先级：CUSTOM_DOMAIN 环境变量 > eo-pages-host（EdgeOne 平台保留的原始域名）
 *        > X-Forwarded-Host > url.hostname > Host 头。
 *
 * 背景：Pages 平台把请求转发给函数时，Host 头会被改写为内部回源地址，
 * 但平台会在 eo-pages-host 头中保留用户实际访问的域名。
 */
function resolveHostName(request: any, env: any): string {
  // 1. 显式指定（自定义域名场景最可靠，推荐）
  const custom = env?.CUSTOM_DOMAIN || env?.DOMAIN;
  if (custom) {
    return String(custom).trim().replace(/^https?:\/\//, '').replace(/\/.*$/, '');
  }
  // 2. EdgeOne 平台保留的原始访问域名
  const eoh = request.headers.get('eo-pages-host');
  if (eoh) {
    return eoh.trim();
  }
  // 3. 反代平台保留的原始域名
  const xfh = request.headers.get('x-forwarded-host');
  if (xfh) {
    return xfh.split(',')[0].trim();
  }
  // 4. 请求 URL 的 hostname（排除明显的内部地址）
  try {
    const u = new URL(request.url);
    if (u.hostname && !u.hostname.endsWith('.internal')) {
      return u.hostname;
    }
  } catch { /* ignore */ }
  // 5. 兜底 Host 头
  return request.headers.get('host') || '';
}

// ---------------------------------------------------------------------------
// 自诊断：/debug
// ---------------------------------------------------------------------------

/**
 * 探测 DNS 解析与出站 TCP 可达性（部署后访问 /debug 即可排查"连接不上"）
 */
async function handleDebug() {
  const results: any = {
    node: process?.version || 'unknown',
    time: new Date().toISOString(),
    env: { uuidSet: Boolean(userID), proxyipSet: Boolean(proxyIP) },
  };

  // DNS
  try {
    const addrs = await dns.promises.resolve('example.com');
    results.dns = { status: 'ok', example: addrs[0] };
  } catch (e: any) {
    results.dns = { status: 'fail', error: e?.message || String(e) };
  }

  // 出站 TCP 探测（5s 超时）
  results.tcp = {} as Record<string, string>;
  const probes: Array<[string, string, number]> = [
    ['example.com:80', 'example.com', 80],
    ['1.1.1.1:443', '1.1.1.1', 443],
    ['www.baidu.com:443', 'www.baidu.com', 443],
  ];
  for (const [name, host, port] of probes) {
    results.tcp[name] = await tcpProbe(host, port, 5000);
  }
  return results;
}

function tcpProbe(host: string, port: number, timeoutMs: number): Promise<string> {
  return new Promise((resolve) => {
    const start = Date.now();
    const sock = net.connect({ host, port });
    const done = (r: string) => {
      try { sock.destroy(); } catch { /* ignore */ }
      resolve(r);
    };
    sock.setTimeout(timeoutMs, () => done(`timeout(${timeoutMs}ms)`));
    sock.once('connect', () => done(`ok ${Date.now() - start}ms`));
    sock.once('error', (e: any) => done(`error: ${e?.message || e}`));
  });
}
