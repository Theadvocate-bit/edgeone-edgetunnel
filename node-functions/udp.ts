// EdgeOne Pages Node Functions —— UDP 路线探针 + 逐包中继（v14）
//
// 平台约束下的 UDP 能力验证与利用：
//   1. GET /udp          —— 探测运行时是否提供 node:dgram，并实测 UDP 出站（向 8.8.8.8:53 发 DNS 查询）
//   2. POST /udp?host=&port= —— 逐包 UDP 中继：body=载荷 → dgram 发送 → 收集响应（静默 400ms 或超时）→ 返回
//
// Hysteria2 说明：Hysteria2 需要 QUIC 服务端（持久 UDP socket + QUIC 协议栈 + 连接级状态）。
// 本平台沙箱逐请求重建、无跨请求状态、无 QUIC 运行时 —— 无法承载 Hysteria2 服务端。
// UDP 路线的可用形态是本端点的「逐包无状态中继」，适用于 DNS 等一问一答型 UDP 流量。

import { readBodyBuffer, buildDnsQuery } from './util.ts';

const RESPONSE_QUIET_MS = 400;
const EXCHANGE_TIMEOUT_MS = 5000;

function json(obj: any, status = 200): Response {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { 'Content-Type': 'application/json;charset=utf-8', 'Cache-Control': 'no-store' },
  });
}

interface UdpExchangeResult {
  sent: boolean;
  ok: boolean;
  chunks: Buffer[];
  bytes: number;
  ms: number;
  error?: string;
}

function udpExchange(
  dgram: any,
  pkt: Buffer,
  host: string,
  port: number,
  timeoutMs: number
): Promise<UdpExchangeResult> {
  return new Promise((resolve) => {
    const started = Date.now();
    const chunks: Buffer[] = [];
    let settled = false;
    let quietTimer: any = null;
    let sock: any = null;
    let sentOk = false;

    const settle = (error?: string) => {
      if (settled) return;
      settled = true;
      clearTimeout(quietTimer);
      try { sock?.close(); } catch { /* ignore */ }
      const total = chunks.reduce((n, c) => n + c.length, 0);
      resolve({
        sent: sentOk,
        ok: chunks.length > 0,
        chunks,
        bytes: total,
        ms: Date.now() - started,
        ...(error ? { error } : {}),
      });
    };

    try {
      sock = dgram.createSocket('udp4');
    } catch (e: any) {
      settle('createSocket failed: ' + (e?.message || e));
      return;
    }

    sock.on('message', (m: Buffer) => {
      chunks.push(Buffer.from(m));
      clearTimeout(quietTimer);
      quietTimer = setTimeout(() => settle(), RESPONSE_QUIET_MS);
    });
    sock.on('error', (e: any) => settle(e?.message || 'socket error'));
    sock.send(pkt, port, host, (err: any) => {
      if (err) settle(err?.message || 'send failed');
      else sentOk = true;
    });
    setTimeout(() => settle(chunks.length ? undefined : 'timeout'), timeoutMs);
  });
}

/** 多响应拼接：每段前加 2 字节大端长度前缀（客户端可据此分帧） */
function concatWithLengthPrefix(chunks: Buffer[]): Buffer {
  const parts: Buffer[] = [];
  for (const c of chunks) {
    parts.push(Buffer.from([(c.length >> 8) & 0xff, c.length & 0xff]), c);
  }
  return Buffer.concat(parts);
}

/**
 * @param {any} context EdgeOne Node Functions EventContext
 */
export async function onRequest(context: any) {
  const { request } = context;

  // 动态导入：平台运行时可能不提供 dgram（不能在模块顶层 import，否则整函数加载失败）
  let dgram: any = null;
  try {
    const m: any = await import('node:dgram');
    dgram = m?.default || m;
  } catch { dgram = null; }

  if (!dgram || typeof dgram.createSocket !== 'function') {
    return json({
      dgramAvailable: false,
      note: '平台运行时未提供 node:dgram，UDP 路线不可行（Hysteria2/QUIC 亦不可行）',
    });
  }

  try {
    const url = new URL(request.url);
    const host = (url.searchParams.get('host') || '').trim();
    const port = Number(url.searchParams.get('port') || 0);

    // 探针：无参数 GET → 实测 DNS over UDP（多解析器重试，规避瞬时丢包）
    if (request.method === 'GET' && !host) {
      const resolvers = ['8.8.8.8', '1.1.1.1', '223.5.5.5'];
      let probe: UdpExchangeResult | null = null;
      let usedResolver = '';
      for (const resolver of resolvers) {
        const r = await udpExchange(dgram, buildDnsQuery('example.com'), resolver, 53, 2500);
        if (r.ok) { probe = r; usedResolver = resolver; break; }
        probe = r;
      }
      return json({
        dgramAvailable: true,
        resolver: usedResolver || null,
        dnsProbe: {
          sent: probe!.sent,
          ok: probe!.ok,
          responses: probe!.chunks.length,
          bytes: probe!.bytes,
          ms: probe!.ms,
          ...(probe!.error ? { error: probe!.error } : {}),
        },
        verdict: probe!.ok
          ? `UDP 出站可用（经 ${usedResolver} 验证）：逐包中继可行（DNS 等一问一答型流量）；Hysteria2 仍不可行（需 QUIC + 持久会话）`
          : probe!.sent
            ? 'UDP 已发送但未收到响应：可能为环境网络对 UDP 的过滤；dgram 本身可用，逐包中继代码已就绪'
            : 'dgram 发送失败：平台可能封禁 UDP 出站，UDP 路线不可行',
      });
    }

    // 中继：POST/GET + host/port
    if (!host || !port) {
      return json({ error: '缺少 host/port', usage: 'POST /udp?host=<ip>&port=<n>  body=<UDP 载荷>' }, 400);
    }
    const { buf, how } = await readBodyBuffer(request);
    if (!buf || !buf.length) {
      return json({ error: '缺少 UDP 载荷（body 为空）', bodyHow: how }, 400);
    }
    const r = await udpExchange(dgram, buf, host, port, EXCHANGE_TIMEOUT_MS);
    if (!r.ok) {
      return json(
        { sent: r.sent, ok: false, responses: 0, ms: r.ms, host, port, bodyHow: how, ...(r.error ? { error: r.error } : {}) },
        502
      );
    }
    return new Response(concatWithLengthPrefix(r.chunks), {
      status: 200,
      headers: {
        'Content-Type': 'application/octet-stream',
        'X-UDP-Responses': String(r.chunks.length),
        'X-UDP-Bytes': String(r.bytes),
        'X-UDP-Ms': String(r.ms),
        'Cache-Control': 'no-store',
      },
    });
  } catch (err: any) {
    return json({ error: err?.message || String(err) }, 500);
  }
}
