/* global WebSocketPair */
// EdgeOne Pages Node Functions 版 edgetunnel —— WS 端点
//
// 双模式升级（可经 ?ws-mode= 或环境变量 WS_MODE 选择）：
//   - "handlers"（默认）：返回 { websocket: { onopen, onmessage, onclose, onerror } }
//     （官方 pages-ai-voice-chat 模板形态）
//   - "pair"：CF 风格 WebSocketPair + Response(101, webSocket)（若运行时提供 WebSocketPair）
//
// 诊断：所有 WS 事件通过 report() 写入内存缓冲（/debug 可读），并 fire-and-forget
// 外发到 ntfy.sh topic（跨隔离实例可见），用于排查平台事件派发问题。

import net from 'node:net';

// 如何生成自己的 UUID:
// [Windows] Win + R 输入 cmd 运行: Powershell -NoExit -Command "[guid]::NewGuid()"
let userID = 'd342d11e-d424-4583-b36e-524ab1f0afa4';
let proxyIP = '';

const WS_PATH = '/vless';
const DIAG_TOPIC = 'eone-diag-244423673';
const BOOT_ID = Math.random().toString(36).slice(2, 10);

const wsDiag: string[] = [];

function report(event: string, detail?: any) {
  let d = '';
  if (detail !== undefined) {
    try { d = typeof detail === 'string' ? detail : JSON.stringify(detail); } catch { d = String(detail); }
  }
  const line = `[boot:${BOOT_ID}] ${new Date().toISOString()} ${event}${d ? ' ' + d : ''}`;
  wsDiag.push(line);
  if (wsDiag.length > 50) wsDiag.shift();
  console.log('[ws-diag]', line);
  // fire-and-forget 外发（跨隔离实例可见；平台 101 后不派发事件时这是唯一观测手段）
  try {
    void fetch(`https://ntfy.sh/${DIAG_TOPIC}`, {
      method: 'POST',
      body: line.slice(0, 800),
    }).catch(() => { });
  } catch { /* ignore */ }
}

export function getWsDiag(): string[] {
  return wsDiag;
}

/**
 * 兼容读取请求头：onRequest 里 headers 是 Headers 实例（有 .get），
 * 但 WS 回调里平台传入的 request.headers 可能是普通对象（无 .get）。
 */
function readHeader(req: any, name: string): string {
  try {
    const h = req?.headers;
    if (!h) return '';
    if (typeof h.get === 'function') return String(h.get(name) || '');
    const v = h[name] ?? h[name.toLowerCase()] ?? h[name.replace(/\b\w/g, (c: string) => c.toUpperCase())];
    return v ? String(v) : '';
  } catch { return ''; }
}

/**
 * @param {any} context EdgeOne Node Functions EventContext
 */
export async function onRequest(context: any) {
  const { request, env } = context;
  try {
    userID = env?.UUID || userID;
    proxyIP = env?.PROXYIP || proxyIP;

    const upgradeHeader: any = request.headers.get('upgrade');
    if (upgradeHeader?.toLowerCase() === 'websocket') {
      const url = new URL(request.url);
      if (url.pathname !== '/websocket' && url.pathname !== '/ws' && url.pathname !== '/vless') {
        return new Response('Bad WebSocket path', { status: 400 });
      }

      const mode: string = url.searchParams.get('ws-mode') || env?.WS_MODE || 'handlers';
      report('upgrade', `path=${url.pathname} mode=${mode} boot=${BOOT_ID}`);

      // ---- 模式 A：CF 风格 WebSocketPair（若运行时提供）----
      if (mode === 'pair') {
        if (typeof WebSocketPair === 'undefined') {
          report('pair mode requested but WebSocketPair undefined');
          // 落回 handlers 模式
        } else {
          const pair = new WebSocketPair();
          const server = pair[1];
          wirePairServer(server, request);
          return new Response(null, {
            status: 101,
            webSocket: pair[0],
            headers: { 'sec-websocket-protocol': request.headers.get('sec-websocket-protocol') || '' },
          });
        }
      }

      // ---- 模式 B：官方 handlers 形态 ----
      // 重要：必须保持与官方模板完全一致的"内联字面量 + 单一键"返回，
      // 平台构建器靠静态分析 return { websocket: ... } 注入 WS 桥接——
      // 中间变量/别名键可能破坏注入（v10 修复）。
      report('upgrade returning handlers literal');
      return { websocket: createVlessWebSocketHandler(request) };
    }

    return new Response(
      `WebSocket endpoint.\nUse wss://<host>${WS_PATH}?ed=2048\nAnd fetch config at /${userID}\n`,
      { status: 426, headers: { 'Content-Type': 'text/plain;charset=utf-8' } }
    );
  } catch (err: any) {
    report('onRequest error', err?.message || err);
    return new Response(String(err), { status: 500 });
  }
}

// ---------------------------------------------------------------------------
// VLESS 连接状态机（两种升级模式共用）
// ---------------------------------------------------------------------------

interface VlessConnection {
  handleChunk(chunk: Buffer): void;
  handleEarlyData(): void;
  destroy(): void;
}

function createVlessConnection(wsRef: { current: any }, request: any): VlessConnection {
  let address = '';
  let portWithRandomLog = '';
  const log = (info: string, event?: any) => {
    console.log(`[${address}:${portWithRandomLog}] ${info}`, event || '');
  };

  const earlyDataHeader: string = readHeader(request, 'sec-websocket-protocol');

  let remoteSocket: any = null;
  let vlessHeaderPending: Buffer | null = null;
  let isDns = false;
  let dnsBuf: Buffer = Buffer.alloc(0);
  let dnsHeaderPending: Buffer | null = null;
  let retried = false;

  function isWsOpen(ws: any): boolean {
    return ws && (ws.readyState === undefined || ws.readyState === 1 || ws.readyState === 'open');
  }

  function handleDns(chunk: Buffer) {
    dnsBuf = Buffer.concat([dnsBuf, chunk]);
    void (async () => {
      try {
        while (dnsBuf.length >= 2) {
          const len = dnsBuf.readUInt16BE(0);
          if (dnsBuf.length < 2 + len) break;
          const query = dnsBuf.subarray(2, 2 + len);
          dnsBuf = dnsBuf.subarray(2 + len);

          const resp = await fetch('https://1.1.1.1/dns-query', {
            method: 'POST',
            headers: { 'content-type': 'application/dns-message' },
            body: query,
          });
          const result = Buffer.from(await resp.arrayBuffer());
          const udpSizeBuffer = Buffer.from([(result.length >> 8) & 0xff, result.length & 0xff]);
          if (!isWsOpen(wsRef.current)) return;
          log(`doh success and dns message length is ${result.length}`);
          if (dnsHeaderPending) {
            wsRef.current.send(Buffer.concat([dnsHeaderPending, udpSizeBuffer, result]));
            dnsHeaderPending = null;
          } else {
            wsRef.current.send(Buffer.concat([udpSizeBuffer, result]));
          }
        }
      } catch (err: any) {
        log('dns udp has error', err);
      }
    })();
  }

  function connectAndWrite(host: string, port: number, initialData: Buffer, isRetry: boolean) {
    let hasIncomingData = false;
    const socket = net.connect({ host, port: Number(port) });
    socket.setNoDelay(true);
    remoteSocket = socket;
    log(`${isRetry ? 'retry connected' : 'connected'} to ${host}:${port}`);
    report('tcp connect', `${host}:${port} retry=${isRetry}`);

    const CONNECT_TIMEOUT_MS = 8000;
    const connectTimer = setTimeout(() => {
      log(`tcp connect timeout (${CONNECT_TIMEOUT_MS}ms) to ${host}:${port}`);
      socket.destroy();
    }, CONNECT_TIMEOUT_MS);
    socket.once('connect', () => { clearTimeout(connectTimer); report('tcp connected', `${host}:${port}`); });

    socket.on('data', (d: Buffer) => {
      hasIncomingData = true;
      const ws = wsRef.current;
      if (!isWsOpen(ws)) return;
      if (vlessHeaderPending) {
        ws.send(Buffer.concat([vlessHeaderPending, d]));
        vlessHeaderPending = null;
      } else {
        ws.send(d);
      }
    });

    socket.on('close', () => {
      clearTimeout(connectTimer);
      log(`tcp socket closed, hasIncomingData=${hasIncomingData}`);
      report('tcp close', `hasIncomingData=${hasIncomingData} retry=${isRetry}`);
      if (!hasIncomingData && !isRetry && !retried) {
        retried = true;
        connectAndWrite(proxyIP || host, port, initialData, true);
        return;
      }
      const ws = wsRef.current;
      try {
        if (ws && (isWsOpen(ws) || ws.readyState === 2)) ws.close();
      } catch (error) {
        console.error('safeCloseWS error', error);
      }
    });

    socket.on('error', (err: any) => {
      log('tcp socket error', err?.message || err);
      report('tcp error', err?.message || err);
      try { if (isWsOpen(wsRef.current)) wsRef.current.send(`TCP-ERR: ${err?.message || err}`); } catch { /* ignore */ }
      socket.destroy();
    });

    socket.write(initialData);
  }

  function handleChunk(chunk: Buffer) {
    const ws = wsRef.current;
    if (isDns) {
      handleDns(chunk);
      return;
    }
    if (remoteSocket) {
      remoteSocket.write(chunk);
      return;
    }

    const {
      hasError,
      message,
      portRemote = 443,
      addressRemote = '',
      rawDataIndex,
      vlessVersion = new Uint8Array([0, 0]),
      isUDP,
    } = processVlessHeader(chunk, userID);
    address = addressRemote;
    portWithRandomLog = `${portRemote}--${Math.random()} ${isUDP ? 'udp ' : 'tcp '} `;
    if (hasError) {
      log(`vless header error: ${message}`);
      report('vless header error', message);
      try { ws.close(); } catch { /* ignore */ }
      return;
    }

    const vlessResponseHeader = Buffer.from([vlessVersion[0], 0]);
    const rawClientData: Buffer = chunk.subarray(rawDataIndex);

    if (isUDP) {
      if (portRemote !== 53) {
        log('UDP proxy only enable for DNS which is port 53');
        try { ws.close(); } catch { /* ignore */ }
        return;
      }
      isDns = true;
      dnsHeaderPending = vlessResponseHeader;
      handleDns(rawClientData);
      return;
    }

    vlessHeaderPending = vlessResponseHeader;
    connectAndWrite(addressRemote, portRemote, rawClientData, false);
  }

  function handleEarlyData() {
    const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
    if (error) {
      report('early data decode error', error?.message || error);
      try { wsRef.current.close(); } catch { /* ignore */ }
      return;
    }
    if (earlyData) {
      handleChunk(Buffer.from(earlyData));
    }
  }

  function destroy() {
    if (remoteSocket) {
      remoteSocket.destroy();
      remoteSocket = null;
    }
  }

  return { handleChunk, handleEarlyData, destroy };
}

// ---------------------------------------------------------------------------
// 模式 B：官方 handlers 形态
// ---------------------------------------------------------------------------

function createVlessWebSocketHandler(request: any) {
  const wsRef = { current: null as any };
  let conn: VlessConnection | null = null;

  /**
   * 关键诊断/修复：平台的 ws 对象 readyState 可能卡在 CONNECTING(0)。
   * ws 包的 send() 在非 OPEN 状态会静默丢弃数据并触发无人监听的 error 事件，
   * close() 则不发 close 帧直接断 TCP——与线上全部现象吻合。
   * 若不是 OPEN，强制置 1 并补发 open 事件。
   */
  function ensureOpen(ws: any): boolean {
    if (!ws) return false;
    try {
      ws.on?.('error', (e: any) => report('ws emitted error', e?.message || String(e)));
    } catch { /* ignore */ }
    if (ws.readyState === 1 || ws.readyState === 'open') return true;
    report('readyState NOT open', `readyState=${ws.readyState}, ctor=${ws?.constructor?.name}, sockCtor=${ws?._socket?.constructor?.name}, sendSrc=${String(ws?.send).slice(0, 80)}`);
    try {
      ws.readyState = 1;
      ws.emit?.('open');
      report('forced readyState=OPEN');
      return true;
    } catch (err: any) {
      report('force open failed', err?.message || err);
      return false;
    }
  }

  return {
    async onopen(ws: any, req: any) {
      wsRef.current = ws;
      ensureOpen(ws);
      report('onopen', `readyState=${ws?.readyState} send=${typeof ws?.send} headersType=${req?.headers?.constructor?.name || typeof req?.headers}`);
      try {
        ws.send('ready');
        report("onopen sent 'ready'", `bufferedAmount=${ws?.bufferedAmount}`);
        conn = createVlessConnection(wsRef, req || request);
        conn.handleEarlyData();
      } catch (err: any) {
        report('onopen error', err?.message || err);
        try { ws.send(`OPEN-ERR: ${err?.message || err}`); } catch { /* ignore */ }
        try { ws.close(); } catch { /* ignore */ }
      }
    },

    async onmessage(ws: any, message: any, isBinary: boolean) {
      wsRef.current = ws;
      ensureOpen(ws);
      try {
        report('onmessage', `len=${message?.byteLength ?? message?.length ?? '?'} binary=${isBinary} readyState=${ws?.readyState}`);
        const chunk = toBuffer(message, isBinary);
        if (!chunk) return;
        if (!conn) conn = createVlessConnection(wsRef, request);
        conn.handleChunk(chunk);
      } catch (err: any) {
        report('onmessage error', err?.message || err);
        try { ws.send(`MSG-ERR: ${err?.message || err}`); } catch { /* ignore */ }
        try { ws.close(); } catch { /* ignore */ }
      }
    },

    onclose(ws: any, code: number, reason: Buffer) {
      report('onclose', `code=${code} reason=${reason?.toString?.() || ''}`);
      conn?.destroy();
    },

    onerror(ws: any, error: Error) {
      report('onerror', error?.message || error);
      try { if (ws) ws.send(`WS-ERR: ${error?.message || error}`); } catch { /* ignore */ }
      conn?.destroy();
    },
  };
}

// ---------------------------------------------------------------------------
// 模式 A：CF 风格 WebSocketPair
// ---------------------------------------------------------------------------

function wirePairServer(server: any, request: any) {
  const wsRef = { current: server };
  report('pair mode: wiring server socket', `keys=[${Object.keys(server || {}).join(',')}]`);

  try {
    if (typeof server.accept === 'function') {
      server.accept();
      report('pair mode: server.accept() ok');
    } else if (typeof server.addEventListener !== 'function') {
      report('pair mode: server socket has neither accept nor addEventListener');
      return;
    }

    let conn: VlessConnection | null = null;
    const ensureConn = () => conn || (conn = createVlessConnection(wsRef, request));

    // 0-RTT early data
    const { earlyData, error } = base64ToArrayBuffer(readHeader(request, 'sec-websocket-protocol'));
    if (error) {
      report('pair mode: early data decode error', error?.message || error);
    } else if (earlyData) {
      ensureConn().handleChunk(Buffer.from(earlyData));
      report('pair mode: early data dispatched');
    }

    server.addEventListener?.('message', (ev: any) => {
      report('pair onmessage', `len=${ev?.data?.byteLength ?? ev?.data?.length ?? '?'}`);
      try {
        const chunk = toBuffer(ev.data, true);
        if (chunk) ensureConn().handleChunk(chunk);
      } catch (err: any) {
        report('pair onmessage error', err?.message || err);
      }
    });
    server.addEventListener?.('close', (ev: any) => {
      report('pair onclose', `code=${ev?.code ?? '?'}`);
      conn?.destroy();
    });
    server.addEventListener?.('error', (ev: any) => {
      report('pair onerror', ev?.message || JSON.stringify(ev) || 'unknown');
      conn?.destroy();
    });
  } catch (err: any) {
    report('pair wiring error', err?.message || err);
  }
}

// ---------------------------------------------------------------------------
// 工具函数
// ---------------------------------------------------------------------------

function toBuffer(message: any, isBinary: boolean): Buffer | null {
  if (typeof message === 'string') return Buffer.from(message);
  if (Buffer.isBuffer(message)) return message;
  if (message instanceof ArrayBuffer) return Buffer.from(message);
  if (ArrayBuffer.isView(message)) return Buffer.from(message.buffer, message.byteOffset, message.byteLength);
  if (!isBinary && message != null) return Buffer.from(String(message));
  return null;
}

function base64ToArrayBuffer(base64Str: string): { earlyData: ArrayBuffer | null; error: any } {
  if (!base64Str) {
    return { earlyData: null, error: null };
  }
  try {
    base64Str = base64Str.replace(/-/g, '+').replace(/_/g, '/');
    const decode = atob(base64Str);
    const arryBuffer = Uint8Array.from(decode, (c) => c.charCodeAt(0));
    return { earlyData: arryBuffer.buffer, error: null };
  } catch (error) {
    return { earlyData: null, error };
  }
}

/**
 * This is not real UUID validation
 */
export function isValidUUID(uuid: string): boolean {
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  return uuidRegex.test(uuid);
}

const byteToHex: string[] = [];
for (let i = 0; i < 256; ++i) {
  byteToHex.push((i + 256).toString(16).slice(1));
}
function unsafeStringify(arr: Uint8Array, offset = 0): string {
  return (
    byteToHex[arr[offset + 0]] +
    byteToHex[arr[offset + 1]] +
    byteToHex[arr[offset + 2]] +
    byteToHex[arr[offset + 3]] +
    '-' +
    byteToHex[arr[offset + 4]] +
    byteToHex[arr[offset + 5]] +
    '-' +
    byteToHex[arr[offset + 6]] +
    byteToHex[arr[offset + 7]] +
    '-' +
    byteToHex[arr[offset + 8]] +
    byteToHex[arr[offset + 9]] +
    '-' +
    byteToHex[arr[offset + 10]] +
    byteToHex[arr[offset + 11]] +
    byteToHex[arr[offset + 12]] +
    byteToHex[arr[offset + 13]] +
    byteToHex[arr[offset + 14]] +
    byteToHex[arr[offset + 15]]
  ).toLowerCase();
}
function stringify(arr: Uint8Array, offset = 0): string {
  const uuid = unsafeStringify(arr, offset);
  if (!isValidUUID(uuid)) {
    throw new TypeError('Stringified UUID is invalid');
  }
  return uuid;
}

// https://xtls.github.io/development/protocols/vless.html

/**
 * 解析 VLESS 协议头（Buffer 原生实现）
 */
export function processVlessHeader(buf: Buffer, uid: string) {
  if (buf.length < 24) {
    return { hasError: true, message: 'invalid data' };
  }
  const version = new Uint8Array([buf[0]]);
  let isUDP = false;
  let isValidUser = false;
  try {
    if (stringify(buf.subarray(1, 17)) === uid) {
      isValidUser = true;
    }
  } catch {
    isValidUser = false;
  }
  if (!isValidUser) {
    return { hasError: true, message: 'invalid user' };
  }

  const optLength = buf[17];
  const command = buf[18 + optLength];

  if (command === 1) {
  } else if (command === 2) {
    isUDP = true;
  } else {
    return {
      hasError: true,
      message: `command ${command} is not support, command 01-tcp,02-udp,03-mux`,
    };
  }
  const portIndex = 18 + optLength + 1;
  const portRemote = buf.readUInt16BE(portIndex);

  const addressIndex = portIndex + 2;
  const addressType = buf[addressIndex];
  let addressLength = 0;
  let addressValueIndex = addressIndex + 1;
  let addressValue = '';
  switch (addressType) {
    case 1:
      addressLength = 4;
      addressValue = buf.subarray(addressValueIndex, addressValueIndex + addressLength).join('.');
      break;
    case 2:
      addressLength = buf[addressValueIndex];
      addressValueIndex += 1;
      addressValue = buf.subarray(addressValueIndex, addressValueIndex + addressLength).toString('utf8');
      break;
    case 3: {
      addressLength = 16;
      const ipv6: string[] = [];
      for (let i = 0; i < 8; i++) {
        ipv6.push(buf.readUInt16BE(addressValueIndex + i * 2).toString(16));
      }
      addressValue = ipv6.join(':');
      break;
    }
    default:
      return { hasError: true, message: `invild  addressType is ${addressType}` };
  }
  if (!addressValue) {
    return { hasError: true, message: `addressValue is empty, addressType is ${addressType}` };
  }

  return {
    hasError: false,
    addressRemote: addressValue,
    addressType,
    portRemote,
    rawDataIndex: addressValueIndex + addressLength,
    vlessVersion: version,
    isUDP,
  };
}

/**
 * 生成 v2ray / clash-meta 客户端配置文本
 */
export function getVLESSConfig(uid: string, hostName: string): string {
  const xhttpPath = '/xhttp';
  const xhttpUri =
    `vless://${uid}@${hostName}:443` +
    `?encryption=none&security=tls&sni=${hostName}&fp=randomized&type=xhttp&host=${hostName}&path=${encodeURIComponent(xhttpPath)}#${hostName}-xhttp`;

  const xrayJson = JSON.stringify(
    {
      inbounds: [
        {
          tag: 'vless-in',
          port: 10808,
          listen: '127.0.0.1',
          protocol: 'socks',
          settings: { udp: true },
        },
      ],
      outbounds: [
        {
          tag: 'vless-out',
          protocol: 'vless',
          settings: {
            vnext: [
              {
                address: hostName,
                port: 443,
                users: [{ id: uid, encryption: 'none' }],
              },
            ],
          },
          streamSettings: {
            network: 'xhttp',
            security: 'tls',
            tlsSettings: { serverName: hostName, fingerprint: 'chrome' },
            xhttpSettings: {
              path: xhttpPath,
              host: hostName,
              mode: 'splitted',
            },
          },
        },
      ],
    },
    null,
    2
  );

  return `
################################################################
XHTTP 传输（EdgeOne Pages）— 需较新 Xray-core（>= 1.8.x，支持 xhttp）
---------------------------------------------------------------
Xray 客户端配置（JSON）：
${xrayJson}
---------------------------------------------------------------
v2rayNG / NekoBox 分享链接（type=xhttp，需客户端支持）：
${xhttpUri}
---------------------------------------------------------------
说明：
  - 传输层 type=xhttp（splitted-HTTP），路径 ${xhttpPath}
  - 上行：POST /xhttp/<uuid>；下行：GET /xhttp/<uuid>?seq=N（长轮询流式）
  - 平台约 30s 硬超时 => 下行每 ~22s 重连一次（客户端自动处理）
################################################################
`;
}
