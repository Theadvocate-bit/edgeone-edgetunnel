// EdgeOne Pages Node Functions 版 edgetunnel
// 由 Cloudflare Worker 版 (worker-vless.js) 改造而来：
//   - cloudflare:sockets  -> Node.js net 模块
//   - CF WebSocketPair    -> EdgeOne 官方 WS API：handler 返回 { websocket: {onopen,onmessage,onclose,onerror} }，
//                            平台负责 101 握手（参考官方 pages-websocket-template）
//   - env.UUID            -> context.env.UUID
//   - WS 路径 /           -> /vless（避免与静态首页路由冲突）
//
// 如何生成自己的 UUID:
// [Windows] Win + R 输入 cmd 运行: Powershell -NoExit -Command "[guid]::NewGuid()"

import net from 'node:net';

let userID = 'd342d11e-d424-4583-b36e-524ab1f0afa4';
let proxyIP = '';

if (!isValidUUID(userID)) {
	throw new Error('uuid is not valid');
}

const WS_PATH = '/vless';

export async function onRequest(context) {
	const { request, env } = context;
	try {
		userID = env?.UUID || userID;
		proxyIP = env?.PROXYIP || proxyIP;

		const upgradeHeader = request.headers.get('upgrade');
		if (upgradeHeader?.toLowerCase() === 'websocket') {
			// EdgeOne 官方 WS 升级方式：返回 { websocket: handlers }，平台完成 101 握手并回调事件
			return { websocket: createVlessWebSocketHandler(request) };
		}

		const url = new URL(request.url);
		switch (url.pathname) {
			case `/${userID}`: {
				const hostName = request.headers.get('Host') || url.hostname;
				const vlessConfig = getVLESSConfig(userID, hostName);
				return new Response(`${vlessConfig}`, {
					status: 200,
					headers: {
						'Content-Type': 'text/plain;charset=utf-8',
					},
				});
			}
			default: {
				// `/` 静态首页优先；这里兜底其它未匹配路径
				return new Response('Not found', { status: 404 });
			}
		}
	} catch (err) {
		/** @type {Error} */ let e = err;
		return new Response(e.toString());
	}
}

// ---------------------------------------------------------------------------
// VLESS over WebSocket（事件驱动）
// ---------------------------------------------------------------------------

/**
 * @param {Request} request
 */
function createVlessWebSocketHandler(request) {
	// ---- 每个连接的独立状态 ----
	let address = '';
	let portWithRandomLog = '';
	const log = (/** @type {string} */ info, /** @type {any} */ event) => {
		console.log(`[${address}:${portWithRandomLog}] ${info}`, event || '');
	};

	const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';

	/** @type {any} 当前 TCP 出站 socket */
	let remoteSocket = null;
	/** @type {Buffer | null} 首包远端数据前要拼的 VLESS 响应头 */
	let vlessHeaderPending = null;
	let isDns = false;
	/** @type {Buffer} DNS 场景下的长度前缀重组缓冲 */
	let dnsBuf = Buffer.alloc(0);
	/** @type {Buffer | null} DNS 场景首包要拼的 VLESS 响应头 */
	let dnsHeaderPending = null;
	let retried = false;

	/** @type {any} */
	let wsRef = null;

	/**
	 * 处理客户端发来的一个数据块（VLESS 协议头 / 后续流数据 / DNS 数据）
	 * @param {any} ws
	 * @param {Buffer} chunk
	 */
	function handleClientChunk(ws, chunk) {
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
			try { ws.close(); } catch (_) {}
			return;
		}

		const vlessResponseHeader = Buffer.from([vlessVersion[0], 0]);
		const rawClientData = chunk.subarray(rawDataIndex);

		if (isUDP) {
			if (portRemote !== 53) {
				log('UDP proxy only enable for DNS which is port 53');
				try { ws.close(); } catch (_) {}
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
	 * @param {string} host
	 * @param {number} port
	 * @param {Buffer} initialData TLS client hello 等
	 * @param {boolean} isRetry
	 */
	function connectAndWrite(host, port, initialData, isRetry) {
		let hasIncomingData = false;
		const socket = net.connect({ host, port: Number(port) });
		socket.setNoDelay(true);
		remoteSocket = socket;
		log(`${isRetry ? 'retry connected' : 'connected'} to ${host}:${port}`);

		socket.on('data', (/** @type {Buffer} */ d) => {
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
			log(`tcp socket closed, hasIncomingData=${hasIncomingData}`);
			if (!hasIncomingData && !isRetry && !retried) {
				// 首连无回源数据：按原版逻辑改走 PROXYIP 重试
				retried = true;
				connectAndWrite(proxyIP || host, port, initialData, true);
				return;
			}
			safeCloseWS(wsRef);
		});

		socket.on('error', (err) => {
			log('tcp socket error', err?.message || err);
			socket.destroy();
		});

		// 未连上前 write 会被 Node 缓冲，连上后自动发出
		socket.write(initialData);
	}

	/**
	 * DNS over HTTPS：VLESS UDP(53) 数据为 [2字节长度][DNS 查询] 帧
	 * @param {any} ws
	 * @param {Buffer} chunk
	 */
	async function handleDns(ws, chunk) {
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
		} catch (err) {
			log('dns udp has error', err);
		}
	}

	return {
		/**
		 * @param {any} ws
		 * @param {any} req
		 */
		async onopen(ws, req) {
			wsRef = ws;
			log('websocket opened');
			// 处理 0-RTT early data（v2ray 客户端通过 sec-websocket-protocol 携带）
			const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
			if (error) {
				log('early data decode error', error);
				try { ws.close(); } catch (_) {}
				return;
			}
			if (earlyData) {
				handleClientChunk(ws, Buffer.from(earlyData));
			}
		},

		/**
		 * @param {any} ws
		 * @param {any} message string | Buffer | ArrayBuffer
		 * @param {boolean} isBinary
		 */
		async onmessage(ws, message, isBinary) {
			wsRef = ws;
			const chunk = toBuffer(message, isBinary);
			if (!chunk) return;
			try {
				handleClientChunk(ws, chunk);
			} catch (err) {
				log('handleClientChunk error', err);
				try { ws.close(); } catch (_) {}
			}
		},

		/**
		 * @param {any} ws
		 * @param {number} code
		 * @param {Buffer} reason
		 */
		onclose(ws, code, reason) {
			log('websocket closed', `${code} ${reason?.toString?.() || ''}`);
			if (remoteSocket) {
				remoteSocket.destroy();
				remoteSocket = null;
			}
		},

		/**
		 * @param {any} ws
		 * @param {Error} error
		 */
		onerror(ws, error) {
			log('websocket error', error?.message || error);
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

/**
 * @param {any} ws
 */
function isWsOpen(ws) {
	return ws && (ws.readyState === undefined || ws.readyState === 1);
}

/**
 * @param {any} ws
 */
function safeCloseWS(ws) {
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
 * @param {any} message
 * @param {boolean} isBinary
 * @returns {Buffer | null}
 */
function toBuffer(message, isBinary) {
	if (typeof message === 'string') return Buffer.from(message);
	if (Buffer.isBuffer(message)) return message;
	if (message instanceof ArrayBuffer) return Buffer.from(message);
	if (ArrayBuffer.isView(message)) return Buffer.from(message.buffer, message.byteOffset, message.byteLength);
	if (!isBinary && message != null) return Buffer.from(String(message));
	return null;
}

/**
 * @param {string} base64Str
 */
function base64ToArrayBuffer(base64Str) {
	if (!base64Str) {
		return { error: null };
	}
	try {
		// go use modified Base64 for URL rfc4648 which js atob not support
		base64Str = base64Str.replace(/-/g, '+').replace(/_/g, '/');
		const decode = atob(base64Str);
		const arryBuffer = Uint8Array.from(decode, (c) => c.charCodeAt(0));
		return { earlyData: arryBuffer.buffer, error: null };
	} catch (error) {
		return { error };
	}
}

/**
 * This is not real UUID validation
 * @param {string} uuid
 */
function isValidUUID(uuid) {
	const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
	return uuidRegex.test(uuid);
}

const byteToHex = [];
for (let i = 0; i < 256; ++i) {
	byteToHex.push((i + 256).toString(16).slice(1));
}
function unsafeStringify(arr, offset = 0) {
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
function stringify(arr, offset = 0) {
	const uuid = unsafeStringify(arr, offset);
	if (!isValidUUID(uuid)) {
		throw TypeError('Stringified UUID is invalid');
	}
	return uuid;
}

// https://xtls.github.io/development/protocols/vless.html

/**
 * 解析 VLESS 协议头（Buffer 原生实现）
 * @param {Buffer} buf
 * @param {string} userID
 */
function processVlessHeader(buf, userID) {
	if (buf.length < 24) {
		return { hasError: true, message: 'invalid data' };
	}
	const version = new Uint8Array([buf[0]]);
	let isUDP = false;
	let isValidUser = false;
	try {
		if (stringify(buf.subarray(1, 17)) === userID) {
			isValidUser = true;
		}
	} catch (_) {
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
			const ipv6 = [];
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
 * @param {string} userID
 * @param {string | null} hostName
 * @returns {string}
 */
function getVLESSConfig(userID, hostName) {
	const protocol = 'vless';
	const wsPath = `${WS_PATH}?ed=2048`;
	const vlessMain =
		`${protocol}` +
		`://${userID}@${hostName}:443` +
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
  uuid: ${userID}
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
