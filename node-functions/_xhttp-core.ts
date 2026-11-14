// EdgeOne Pages Node Functions —— XHTTP 事务模式（v14）
//
// 平台实测约束（README「最终结论」）：
//   - 流式响应可用      → 下载方向：响应体逐块流式下发 ✅
//   - 流式请求体不可用  → 上行方向：只能整包缓冲（arrayBuffer/_rawBodyBuffer/text 三路探测）
//   - 沙箱逐请求重建    → TCP 会话无法跨请求存活 → 无法实现 xray xhttp 的会话语义
//
// 因此本端点是「一请求 = 一事务」的自包含隧道（非 xray 兼容，需自定义客户端）：
//
//   POST /xhttp?addr=<host>&port=<n>&mode=raw[&d=<base64url 上行>]
//     body: 完整上行字节（如一条完整 HTTP 请求）
//     响应: 流式下行字节（目标服务器的应答）
//
//   POST /xhttp?mode=vless   （VLESS 帧：地址/端口从 VLESS 头解析，回传带 VLESS 响应头）
//
//   GET  /xhttp              （端点说明 JSON）
//
// 超时保护：连接 8s / 空闲 15s / 总时长 25s（平台 30s 墙钟内安全收尾）。

import net from 'node:net';
import { processVlessHeader } from './_vless-core.ts';
import { readBodyBuffer, b64urlToBuffer } from './_util.ts';

const CONNECT_TIMEOUT = 8000;
const IDLE_TIMEOUT = 15000;
const HARD_DEADLINE = 25000;

interface TunnelResult {
  stream: ReadableStream;
  dialAddr: string;
  dialPort: number;
  uplinkHow: string;
  uplinkBytes: number;
}

function runTunnel(
  address: string,
  port: number,
  uplink: Buffer | null,
  mode: string,
  proxyIP: string,
  vlessRespHeader: Buffer | null = null
): TunnelResult {
  let vlessHeaderPending: Buffer | null = vlessRespHeader;
  let retried = false;
  let gotData = false;

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      let socket: net.Socket | null = null;
      let idleTimer: any = null;
      let hardTimer: any = null;
      let connTimer: any = null;
      let closed = false;

      const clearTimers = () => {
        for (const t of [idleTimer, hardTimer, connTimer]) clearTimeout(t);
        idleTimer = hardTimer = connTimer = null;
      };
      const finish = () => {
        if (closed) return;
        closed = true;
        clearTimers();
        try { socket?.destroy(); } catch { /* ignore */ }
        try { controller.close(); } catch { /* ignore */ }
      };
      const armIdle = () => {
        clearTimeout(idleTimer);
        idleTimer = setTimeout(finish, IDLE_TIMEOUT);
      };

      const dial = (host: string, p: number, isRetry: boolean) => {
        let s: net.Socket;
        try {
          s = net.connect({ host, port: p });
        } catch {
          finish();
          return;
        }
        socket = s;
        s.setNoDelay(true);
        connTimer = setTimeout(() => {
          s.destroy();
          if (!isRetry && !retried && proxyIP) {
            retried = true;
            dial(proxyIP, p, true);
          } else {
            finish();
          }
        }, CONNECT_TIMEOUT);

        s.once('connect', () => {
          clearTimeout(connTimer);
          hardTimer = setTimeout(finish, HARD_DEADLINE);
          armIdle();
          if (uplink && uplink.length) s.write(uplink);
        });

        s.on('data', (d: Buffer) => {
          gotData = true;
          armIdle();
          let out: Buffer = d;
          if (mode === 'vless' && vlessHeaderPending) {
            out = Buffer.concat([vlessHeaderPending, d]);
            vlessHeaderPending = null;
          }
          try {
            controller.enqueue(new Uint8Array(out));
          } catch {
            s.destroy();
          }
        });

        s.on('close', () => {
          if (!gotData && !isRetry && !retried && proxyIP) {
            retried = true;
            dial(proxyIP, p, true);
            return;
          }
          finish();
        });

        s.on('error', () => finish());
      };

      dial(address, port, false);
    },
    cancel() {
      // 客户端中断：由 finish 内部 destroy 兜底（cancel 触发时 controller 已不可用）
    },
  });

  return { stream, dialAddr: address, dialPort: port, uplinkHow: '', uplinkBytes: uplink?.length || 0 };
}

function json(obj: any, status = 200): Response {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { 'Content-Type': 'application/json;charset=utf-8', 'Cache-Control': 'no-store' },
  });
}

/** /xhttp 端点处理器（由 xhttp.ts 路由与 [[default]].ts 兜底调用） */
export async function handleXhttp(context: any): Promise<Response> {
  const { request, env } = context;
  const proxyIP: string = env?.PROXYIP || '';

  try {
    const url = new URL(request.url);
    const sp = url.searchParams;

    // 端点说明
    if (request.method === 'GET' && !sp.get('addr') && !sp.get('d') && sp.get('mode') !== 'vless') {
      return json({
        endpoint: 'XHTTP transaction tunnel (one request = one transaction)',
        platformConstraints: {
          streamingResponse: 'OK (download direction)',
          streamingRequestBody: 'NOT available (uplink must be fully buffered)',
          instanceAffinity: 'NONE (TCP session cannot persist across requests)',
          wallClockLimit: '~30s (hard deadline 25s built-in)',
        },
        usage: {
          raw: 'POST /xhttp?addr=<host>&port=<n>  body=<完整上行字节>  -> 流式下行字节',
          vless: 'POST /xhttp?mode=vless  body=<VLESS 帧>  -> VLESS 响应头 + 流式下行字节',
          earlyUplink: '任意请求可用 d=<base64url> 参数携带小段上行',
          downloadOnly: 'GET /xhttp?addr=<host>&port=<n>  （仅监听目标输出）',
        },
        note: '非 xray xhttp 兼容（平台无跨请求状态）；配合自定义客户端使用。',
      });
    }

    const mode: string = sp.get('mode') || 'raw';

    // 上行数据：body 优先，其次 d= 参数
    const { buf: bodyBuf, how } = await readBodyBuffer(request);
    const dBuf = b64urlToBuffer(sp.get('d') || '');
    const uplink: Buffer | null = bodyBuf && bodyBuf.length ? bodyBuf : dBuf;

    let addr = (sp.get('addr') || '').trim();
    let port = Number(sp.get('port') || 0);

    if (mode === 'vless') {
      if (!uplink || uplink.length < 24) {
        return json({ error: 'mode=vless 需要 VLESS 帧上行（body 或 d= 参数）', uplinkHow: how }, 400);
      }
      const parsed = processVlessHeader(uplink, String(env?.UUID || 'd342d11e-d424-4583-b36e-524ab1f0afa4'));
      if (parsed.hasError) {
        return json({ error: `VLESS 头解析失败: ${parsed.message}` }, 400);
      }
      addr = parsed.addressRemote;
      port = parsed.portRemote;
      // 关键：剥离 VLESS 头，只把 payload 发给远端（此前误发整个帧导致远端收到二进制垃圾）
      const payload: Buffer = uplink.subarray(parsed.rawDataIndex);

      // VLESS UDP 命令（command=2）：复用已验证可用的 dgram 逐包中继
      if (parsed.isUDP) {
        const { relayPacket } = await import('./_udp-core.ts');
        const r = await relayPacket(addr, port, payload);
        if (!r.available) {
          return json({ error: '平台运行时未提供 node:dgram，UDP 不可用', addr, port }, 502);
        }
        if (!r.ok) {
          return json(
            { error: 'UDP 已发送但无响应（可能被环境过滤）', addr, port, sent: r.sent, ms: r.ms, ...(r.error ? { relayError: r.error } : {}) },
            502
          );
        }
        return new Response(r.body, {
          status: 200,
          headers: {
            'Content-Type': 'application/octet-stream',
            'Cache-Control': 'no-store',
            'X-EO-Tunnel': `vless-udp ${addr}:${port} uplink=${payload.length}(${how})`,
            'X-UDP-Responses': String(r.responses),
            'X-UDP-Bytes': String(r.bytes),
            'X-UDP-Ms': String(r.ms),
          },
        });
      }

      const vlessRespHeader = Buffer.from([parsed.vlessVersion[0], 0]);
      const result = runTunnel(addr, port, payload, 'vless', proxyIP, vlessRespHeader);
      return new Response(result.stream, {
        status: 200,
        headers: {
          'Content-Type': 'application/octet-stream',
          'X-Accel-Buffering': 'no',
          'Cache-Control': 'no-store',
          'X-EO-Tunnel': `vless ${addr}:${port} uplink=${payload.length}(${how})`,
        },
      });
    }

    // mode=raw：支持 host:port 简写
    if (addr && addr.includes(':') && !addr.includes(']')) {
      const i = addr.lastIndexOf(':');
      const p = Number(addr.slice(i + 1));
      if (p > 0) { port = p; addr = addr.slice(0, i); }
    }
    if (!addr || !port) {
      return json({ error: '缺少 addr/port 参数', usage: 'POST /xhttp?addr=<host>&port=<n>  body=<上行字节>' }, 400);
    }

    const result = runTunnel(addr, port, uplink, 'raw', proxyIP);
    return new Response(result.stream, {
      status: 200,
      headers: {
        'Content-Type': 'application/octet-stream',
        'X-Accel-Buffering': 'no',
        'Cache-Control': 'no-store',
        'X-EO-Tunnel': `raw ${addr}:${port} uplink=${uplink?.length || 0}(${how})`,
      },
    });
  } catch (err: any) {
    return json({ error: err?.message || String(err) }, 500);
  }
}
