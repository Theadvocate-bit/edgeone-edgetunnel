// XHTTP (splitted-HTTP) 传输 —— 用普通 HTTP POST/GET 承载 VLESS，绕开平台 WS 桥接限制。
//
// 平台约束（v7~v13 实测）：
//   - WS 桥接：握手 101 但服务端 ws.send() 帧不回传客户端（平台硬限制）。
//   - 函数约 30s 硬超时 => 单次下行 GET 流式必须在 ~22s 内结束，由客户端带 seq 重连。
//   - 实例无状态（每请求可能新 isolate）=> 会话存模块级 Map（暖实例窗口内共享）。
//
// 协议：
//   POST   /xhttp/<uuid>      body = VLESS 数据分片（首片含 VLESS 头：目标地址+端口）；返回 200。
//   GET    /xhttp/<uuid>?seq=N  下行长轮询：返回上游回程原始字节（application/octet-stream，无帧）。
//                               X-XHTTP-Error 头 = 上游错误；流结束即本轮终止（客户端带新 seq 重连）。
//   DELETE /xhttp/<uuid>      关闭会话 / 上游 socket。

import net from 'node:net';
import { processVlessHeader, isValidUUID } from './websocket.ts';

interface XhttpSession {
  socket: net.Socket | null;
  buffer: Buffer[];
  bufferedBytes: number;
  nextSeq: number;      // 下行已生成数据的累计序号（供客户端 ?seq= 续传）
  lastSeen: number;
  established: boolean;
  ended: boolean;       // 上游已关闭/出错
  error: string | null; // 上游错误信息（若有）
  target: string;
  port: number;
}

const SESSION_TTL_MS = 120_000;
const STREAM_MAX_MS = 22_000;   // 单次 GET 流式上限（< 平台 30s 硬超时）
const MAX_BUFFER_BYTES = 4 * 1024 * 1024;

const sessions = new Map<string, XhttpSession>();

function getSession(id: string): XhttpSession | undefined { return sessions.get(id); }

function destroySession(id: string) {
  const s = sessions.get(id);
  if (s?.socket) { try { s.socket.destroy(); } catch { /* ignore */ } }
  sessions.delete(id);
}

function sweep() {
  const now = Date.now();
  for (const [id, s] of sessions) if (now - s.lastSeen > SESSION_TTL_MS) destroySession(id);
}

function ensureSession(id: string): XhttpSession {
  let s = sessions.get(id);
  if (!s) {
    s = { socket: null, buffer: [], bufferedBytes: 0, nextSeq: 0, lastSeen: Date.now(), established: false, ended: false, error: null, target: '', port: 0 };
    sessions.set(id, s);
  }
  s.lastSeen = Date.now();
  return s;
}

function ensureUpstream(s: XhttpSession, host: string, port: number, initial: Buffer): void {
  if (s.socket) return;
  s.target = host; s.port = port;
  const socket = net.connect({ host, port });
  s.socket = socket;
  socket.setNoDelay(true);
  socket.on('connect', () => {
    s.established = true;
    if (initial.length) socket.write(initial);
  });
  socket.on('data', (chunk: Buffer) => {
    if (s.bufferedBytes < MAX_BUFFER_BYTES) { s.buffer.push(chunk); s.bufferedBytes += chunk.length; }
  });
  socket.on('error', (err: Error) => { s.ended = true; s.error = err.message; });
  socket.on('close', () => { s.ended = true; });
}

async function handleUpload(id: string, data: Buffer): Promise<void> {
  const s = ensureSession(id);
  if (data.length === 0) return;
  if (!s.socket) {
    const { hasError, message, addressRemote, portRemote, rawDataIndex } = processVlessHeader(data, id);
    if (hasError) { s.ended = true; s.error = 'vless header: ' + message; return; }
    const payload = Buffer.from(data.subarray(rawDataIndex));
    ensureUpstream(s, addressRemote, portRemote, payload);
  } else if (!s.ended) {
    try { s.socket.write(data); } catch { /* ignore */ }
  }
}

/**
 * 处理 XHTTP 请求（由 [[default]].ts catch-all 调用）。
 * 返回 Response，或 null 表示非 /xhttp 路径（交回默认处理）。
 */
export async function handleXhttp(context: any): Promise<Response | null> {
  const { request, env } = context;
  const url = new URL(request.url);
  if (!url.pathname.startsWith('/xhttp')) return null;

  const method = (request as any).method;
  const uid = env?.UUID || 'd342d11e-d424-4583-b36e-524ab1f0afa4';

  if (url.pathname === '/xhttp' || url.pathname === '/xhttp/') {
    return new Response(
      'XHTTP transport.\nPOST   /xhttp/<uuid>        upload VLESS chunks (first carries VLESS header)\nGET    /xhttp/<uuid>?seq=N  downstream poll (raw bytes)\nDELETE /xhttp/<uuid>        close session\n',
      { status: 200, headers: { 'Content-Type': 'text/plain;charset=utf-8' } }
    );
  }

  const id = url.pathname.slice('/xhttp/'.length).split('/')[0];
  if (!id || !isValidUUID(id)) {
    return new Response('bad session id\n', { status: 400, headers: { 'Content-Type': 'text/plain;charset=utf-8' } });
  }

  if (method === 'POST') {
    try { sweep(); } catch { /* ignore */ }
    const raw = await request.arrayBuffer();
    await handleUpload(id, Buffer.from(raw));
    return new Response(null, { status: 200, headers: { 'Cache-Control': 'no-store' } });
  }

  if (method === 'GET') {
    const s = ensureSession(id);
    const since = parseInt(url.searchParams.get('seq') || '0', 10) || 0;
    const started = Date.now();
    let cursor = since;          // 已下发的累计字节序号
    const stream = new ReadableStream({
      start(controller) {
        let closed = false;
        const finish = () => { if (!closed) { closed = true; clearInterval(timer); try { controller.close(); } catch { /* ignore */ } } };
        const timer = setInterval(() => {
          if (closed) return;
          const elapsed = Date.now() - started;
          // 冲刷所有尚未下发的 buffer 数据
          while (s.buffer.length) {
            const item = s.buffer.shift()!;
            s.bufferedBytes -= item.length;
            try { controller.enqueue(new Uint8Array(item)); cursor += item.length; }
            catch { finish(); return; }
          }
          // 上游已结束且无残留数据 => 结束本轮（客户端看到流关闭）
          if (s.ended && s.buffer.length === 0) { finish(); return; }
          // 单次流式上限 => 结束本轮（客户端带 ?seq=cursor 重连）
          if (elapsed >= STREAM_MAX_MS) { finish(); return; }
        }, 150);
      },
    });
    const headers: Record<string, string> = {
      'Content-Type': 'application/octet-stream',
      'X-Accel-Buffering': 'no',
      'Cache-Control': 'no-store',
    };
    if (s.error) headers['X-XHTTP-Error'] = s.error;
    void cursor;
    return new Response(stream, { status: 200, headers });
  }

  if (method === 'DELETE') {
    destroySession(id);
    return new Response(null, { status: 200 });
  }

  return new Response('method not allowed\n', { status: 405, headers: { 'Content-Type': 'text/plain;charset=utf-8' } });
}
