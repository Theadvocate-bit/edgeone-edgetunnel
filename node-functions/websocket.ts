/* global WebSocketPair */
// EdgeOne Pages Node Functions 版 edgetunnel —— WS 端点
//
// 重要：本文件刻意复刻官方 WS 模板（TencentEdgeOne/pages-ai-voice-chat）的形态：
//   1) 文件名固定为 websocket.ts——平台的 WebSocket 桥接按此文件名挂接
//      （官方代码同时接受 /websocket 与 /ws 两个路径，说明升级请求统一路由到本文件）；
//   2) package.json 使用 "type": "commonjs" + 依赖 ws——与官方一致，保证构建产物形态一致；
//   3) .ts 文件——官方所有 WS 模板均为 TS，经构建器 esbuild 打包。
//
// WS 升级方式（官方 API）：handler 对升级请求返回
//   { websocket: { onopen, onmessage, onclose, onerror } }
// 由平台完成 101 握手并回调事件（运行时不存在 WebSocketPair）。

import net from 'node:net';

// 如何生成自己的 UUID:
// [Windows] Win + R 输入 cmd 运行: Powershell -NoExit -Command "[guid]::NewGuid()"
let userID = 'd342d11e-d424-4583-b36e-524ab1f0afa4';
let proxyIP = '';

const WS_PATH = '/vless';

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
      // 与官方模板一致：接受 /websocket 与 /ws；另保留 /vless 兼容旧配置。
      // 平台把升级请求统一路由到 websocket.ts，路径校验仅作防御。
      if (url.pathname !== '/websocket' && url.pathname !== '/ws' && url.pathname !== '/vless') {
        return new Response('Bad WebSocket path', { status: 400 });
      }
      // EdgeOne 官方 WS 升级方式：返回 { websocket: handlers }，平台完成 101 握手并回调事件
      const handlers = createVlessWebSocketHandler(request);
      diag(`upgrade accepted at ${url.pathname}, returning handlers`);
      // 兼容不同运行时的键名（官方模板为 websocket；附带别名以防派发键名不同）
      return {
        websocket: handlers,
        webSocket: handlers,
        socket: handlers,
        ws: handlers,
      };
    }

    // 非 WS 请求打到 /vless：给出提示
    return new Response(
      `WebSocket endpoint.\nUse wss://<host>${WS_PATH}?ed=2048\nAnd fetch config at /${userID}\n`,
      { status: 426, headers: { 'Content-Type': 'text/plain;charset=utf-8' } }
    );
  } catch (err: any) {
    return new Response(String(err), { status: 500 });
  }
}

// ---------------------------------------------------------------------------
// 模块级 WS 诊断缓冲：记录最近 50 条 WS 事件/异常，供 /debug 端点读取。
// 用于排查「101 握手成功但事件回调未派发/无数据回传」类平台层问题。
// ---------------------------------------------------------------------------
const wsDiag: string[] = [];

function diag(msg: string) {
  const line = `${new Date().toISOString()} ${msg}`;
  wsDiag.push(line);
  if (wsDiag.length > 50) wsDiag.shift();
  console.log('[ws-diag]', line);
}

export function getWsDiag(): string[] {
  return wsDiag;
}

// ---------------------------------------------------------------------------
// VLESS over WebSocket（事件驱动）
// ---------------------------------------------------------------------------

/**
 * @param {any} request
 */
function createVlessWebSocketHandler(request: any) {
  // ---- 每个连接的独立状态 ----
  let address = '';
  let portWithRandomLog = '';
  const log = (info: string, event?: any) => {
    console.log(`[${address}:${portWithRandomLog}] ${info}`, event || '');
  };

  const earlyDataHeader: any = request.headers.get('sec-websocket-protocol') || '';

  /** 当前 TCP 出站 socket */
  let remoteSocket: any = null;
  /** 首包远端数据前要拼的 VLESS 响应头 */
  let vlessHeaderPending: Buffer | null = null;
  let isDns = false;
  /** DNS 场景下的长度前缀重组缓冲 */
  let dnsBuf: Buffer = Buffer.alloc(0);
  /** DNS 场景首包要拼的 VLESS 响应头 */
  let dnsHeaderPending: Buffer | null = null;
  let retried = false;

  let wsRef: any = null;

  /**
   * 处理客户端发来的一个数据块（VLESS 协议头 / 后续流数据 / DNS 数据）
   */
  function handleClientChunk(ws: any, chunk: Buffer) {
    if (isDns) {
      handleDns(ws, chunk);
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
      handleDns(ws, rawClientData);
      return;
    }

    // 远端首包数据前需拼上 VLESS 响应头 ["version", 0]
    vlessHeaderPending = vlessResponseHeader;
    connectAndWrite(addressRemote, portRemote, rawClientData, false);
  }

  /**
   * 出站 TCP：net.connect，远端数据回写 ws；首连无数据则按 CF 原逻辑用 PROXYIP 重试
   */
  function connectAndWrite(host: string, port: number, initialData: Buffer, isRetry: boolean) {
    let hasIncomingData = false;
    const socket = net.connect({ host, port: Number(port) });
    socket.setNoDelay(true);
    remoteSocket = socket;
    log(`${isRetry ? 'retry connected' : 'connected'} to ${host}:${port}`);

    // 连接超时保护：不可达目标会 SYN 挂起，必须在平台墙钟限制内快速失败
    const CONNECT_TIMEOUT_MS = 8000;
    const connectTimer = setTimeout(() => {
      log(`tcp connect timeout (${CONNECT_TIMEOUT_MS}ms) to ${host}:${port}`);
      socket.destroy(); // 触发 close -> 触发 PROXYIP 重试或关闭
    }, CONNECT_TIMEOUT_MS);
    socket.once('connect', () => clearTimeout(connectTimer));

    socket.on('data', (d: Buffer) => {
      hasIncomingData = true;
      if (!isWsOpen(wsRef)) return;
      if (vlessHeaderPending) {
        wsRef.send(Buffer.concat([vlessHeaderPending, d]));
        vlessHeaderPending = null;
      } else {
        wsRef.send(d);
      }
    });

    socket.on('close', () => {
      clearTimeout(connectTimer);
      log(`tcp socket closed, hasIncomingData=${hasIncomingData}`);
      if (!hasIncomingData && !isRetry && !retried) {
        // 首连无回源数据：按原版逻辑改走 PROXYIP 重试
        retried = true;
        connectAndWrite(proxyIP || host, port, initialData, true);
        return;
      }
      safeCloseWS(wsRef);
    });

    socket.on('error', (err: any) => {
      log('tcp socket error', err?.message || err);
      // 把错误回传客户端，便于线上自诊断（客户端可读到服务端失败原因）
      try { if (isWsOpen(wsRef)) wsRef.send(`TCP-ERR: ${err?.message || err}`); } catch { /* ignore */ }
      socket.destroy();
    });

    // 未连上前 write 会被 Node 缓冲，连上后自动发出
    socket.write(initialData);
  }

  /**
   * DNS over HTTPS：VLESS UDP(53) 数据为 [2字节长度][DNS 查询] 帧
   */
  async function handleDns(ws: any, chunk: Buffer) {
    try {
      dnsBuf = Buffer.concat([dnsBuf, chunk]);
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
        if (!isWsOpen(wsRef)) return;
        log(`doh success and dns message length is ${result.length}`);
        if (dnsHeaderPending) {
          wsRef.send(Buffer.concat([dnsHeaderPending, udpSizeBuffer, result]));
          dnsHeaderPending = null;
        } else {
          wsRef.send(Buffer.concat([udpSizeBuffer, result]));
        }
      }
    } catch (err: any) {
      log('dns udp has error', err);
    }
  }

  return {
    async onopen(ws: any, req: any) {
      wsRef = ws;
      diag(`onopen fired, ws keys=[${Object.keys(ws || {}).join(',')}], typeof send=${typeof ws?.send}`);
      log('websocket opened');
      try {
        // 与官方模板一致：open 后立即回发一条消息，验证服务端 send 通道可用
        ws.send('ready');
        diag("onopen sent 'ready'");
        // 处理 0-RTT early data（v2ray 客户端通过 sec-websocket-protocol 携带）
        const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
        if (error) {
          log('early data decode error', error);
          try { ws.send(`EARLY-DATA-ERR: ${error?.message || error}`); } catch { /* ignore */ }
          try { ws.close(); } catch { /* ignore */ }
          return;
        }
        if (earlyData) {
          handleClientChunk(ws, Buffer.from(earlyData));
        }
      } catch (err: any) {
        diag(`onopen error: ${err?.message || err}`);
        log('onopen error', err);
        try { ws.send(`OPEN-ERR: ${err?.message || err}`); } catch { /* ignore */ }
        try { ws.close(); } catch { /* ignore */ }
      }
    },

    async onmessage(ws: any, message: any, isBinary: boolean) {
      diag(`onmessage fired, len=${message?.byteLength ?? message?.length ?? '?'}, isBinary=${isBinary}`);
      wsRef = ws;
      try {
        const chunk = toBuffer(message, isBinary);
        if (!chunk) return;
        handleClientChunk(ws, chunk);
      } catch (err: any) {
        log('handleClientChunk error', err);
        try { ws.send(`MSG-ERR: ${err?.message || err}`); } catch { /* ignore */ }
        try { ws.close(); } catch { /* ignore */ }
      }
    },

    onclose(ws: any, code: number, reason: Buffer) {
      diag(`onclose fired, code=${code}, reason=${reason?.toString?.() || ''}`);
      log('websocket closed', `${code} ${reason?.toString?.() || ''}`);
      if (remoteSocket) {
        remoteSocket.destroy();
        remoteSocket = null;
      }
    },

    onerror(ws: any, error: Error) {
      diag(`onerror fired: ${error?.message || error}`);
      log('websocket error', error?.message || error);
      try { if (isWsOpen(ws)) ws.send(`WS-ERR: ${error?.message || error}`); } catch { /* ignore */ }
      if (remoteSocket) {
        remoteSocket.destroy();
        remoteSocket = null;
      }
    },
  };
}

// ---------------------------------------------------------------------------
// 工具函数
// ---------------------------------------------------------------------------

function isWsOpen(ws: any): boolean {
  return ws && (ws.readyState === undefined || ws.readyState === 1);
}

function safeCloseWS(ws: any) {
  try {
    if (isWsOpen(ws) || ws?.readyState === 2) {
      ws.close();
    }
  } catch (error) {
    console.error('safeCloseWS error', error);
  }
}

/**
 * 把 ws 消息统一转成 Buffer
 */
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
    // go use modified Base64 for URL rfc4648 which js atob not support
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
  // skip opt for now

  const command = buf[18 + optLength];

  // 0x01 TCP / 0x02 UDP / 0x03 MUX
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
  // port is big-Endian in raw data etc 80 == 0x005d
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
      // 2001:0db8:85a3:0000:0000:8a2e:0370:7334
      const ipv6: string[] = [];
      for (let i = 0; i < 8; i++) {
        ipv6.push(buf.readUInt16BE(addressValueIndex + i * 2).toString(16));
      }
      addressValue = ipv6.join(':');
      // seems no need add [] for ipv6
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
  const protocol = 'vless';
  const wsPath = `${WS_PATH}?ed=2048`;
  const vlessMain =
    `${protocol}` +
    `://${uid}@${hostName}:443` +
    `?encryption=none&security=tls&sni=${hostName}&fp=randomized&type=ws&host=${hostName}&path=${encodeURIComponent(wsPath)}#${hostName}`;

  return `
################################################################
v2ray (EdgeOne Pages)
---------------------------------------------------------------
${vlessMain}
---------------------------------------------------------------
################################################################
clash-meta
---------------------------------------------------------------
- type: vless
  name: ${hostName}
  server: ${hostName}
  port: 443
  uuid: ${uid}
  network: ws
  tls: true
  udp: false
  sni: ${hostName}
  client-fingerprint: chrome
  ws-opts:
    path: "${wsPath}"
    headers:
      host: ${hostName}
---------------------------------------------------------------
################################################################
`;
}
