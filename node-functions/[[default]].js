// EdgeOne Pages Node Functions 版 edgetunnel
// 由 Cloudflare Worker 版 (worker-vless.js) 改造而来：
//   - cloudflare:sockets  -> Node.js net 模块 + stream.toWeb 桥接 Web Streams
//   - CF WebSocketPair    -> EdgeOne Node Functions 内置 WebSocketPair ({ socket, response })
//   - env.UUID            -> context.env.UUID
//   - request.cf          -> 移除（返回普通状态信息）
//   - WS 路径 /           -> /vless（避免与静态首页路由冲突）
//
// 如何生成自己的 UUID:
// [Windows] Win + R 输入 cmd 运行: Powershell -NoExit -Command "[guid]::NewGuid()"

import net from 'node:net';
import { Readable, Writable } from 'node:stream';

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

		const upgradeHeader = request.headers.get('Upgrade');
		if (upgradeHeader === 'websocket') {
			return await vlessOverWSHandler(request, context);
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

/**
 * VLESS over WebSocket 入口
 * @param {Request} request
 * @param {any} context EdgeOne Node Functions EventContext
 */
async function vlessOverWSHandler(request, context) {
	const webSocketPair = new WebSocketPair();
	const webSocket = webSocketPair.socket;
	const response = webSocketPair.response;

	// EdgeOne 提供 accept() 则调用（对齐 Cloudflare 语义）
	if (typeof webSocket.accept === 'function') {
		webSocket.accept();
	}

	let address = '';
	let portWithRandomLog = '';
	const log = (/** @type {string} */ info, /** @type {string | undefined} */ event) => {
		console.log(`[${address}:${portWithRandomLog}] ${info}`, event || '');
	};
	const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';

	const readableWebSocketStream = makeReadableWebSocketStream(webSocket, earlyDataHeader, log);

	/** @type {{ value: any | null}} */
	let remoteSocketWapper = {
		value: null,
	};
	let udpStreamWrite = null;
	let isDns = false;

	// ws --> remote
	const piping = readableWebSocketStream
		.pipeTo(
			new WritableStream({
				async write(chunk, controller) {
					if (isDns && udpStreamWrite) {
						return udpStreamWrite(chunk);
					}
					if (remoteSocketWapper.value) {
						const writer = remoteSocketWapper.value.writable.getWriter();
						await writer.write(toWritableChunk(chunk));
						writer.releaseLock();
						return;
					}

					const vlessBuffer = await normalizeToArrayBuffer(chunk);

					const {
						hasError,
						message,
						portRemote = 443,
						addressRemote = '',
						rawDataIndex,
						vlessVersion = new Uint8Array([0, 0]),
						isUDP,
					} = processVlessHeader(vlessBuffer, userID);
					address = addressRemote;
					portWithRandomLog = `${portRemote}--${Math.random()} ${isUDP ? 'udp ' : 'tcp '} `;
					if (hasError) {
						throw new Error(message);
					}
					// if UDP but port not DNS port, close it
					if (isUDP) {
						if (portRemote === 53) {
							isDns = true;
						} else {
							throw new Error('UDP proxy only enable for DNS which is port 53');
						}
					}
					// ["version", "附加信息长度 N"]
					const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
					const rawClientData = vlessBuffer.slice(rawDataIndex);

					if (isDns) {
						const { write } = await handleUDPOutBound(webSocket, vlessResponseHeader, log);
						udpStreamWrite = write;
						udpStreamWrite(rawClientData);
						return;
					}
					handleTCPOutBound(
						remoteSocketWapper,
						addressRemote,
						portRemote,
						rawClientData,
						webSocket,
						vlessResponseHeader,
						log
					);
				},
				close() {
					log(`readableWebSocketStream is close`);
				},
				abort(reason) {
					log(`readableWebSocketStream is abort`, JSON.stringify(reason));
				},
			})
		)
		.catch((err) => {
			log('readableWebSocketStream pipeTo error', err);
		});

	// 尽量延长本次调用的生命周期，保证 WS 隧道不被提前回收
	if (typeof context?.waitUntil === 'function') {
		try {
			context.waitUntil(piping);
		} catch (_) {
			/* ignore */
		}
	}

	return response;
}

/**
 * 出站 TCP 连接（Node.js net 实现，接口对齐 Cloudflare Socket）
 * @param {string} addressRemote
 * @param {number} portRemote
 * @param {Uint8Array} rawClientData
 * @param {any} webSocket
 * @param {Uint8Array} vlessResponseHeader
 * @param {function} log
 */
async function handleTCPOutBound(remoteSocket, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, log) {
	async function connectAndWrite(address, port) {
		const tcpSocket = tcpConnect(address, port);
		remoteSocket.value = tcpSocket;
		log(`connected to ${address}:${port}`);
		const writer = tcpSocket.writable.getWriter();
		// Node 的 Writable.toWeb 只接受 Buffer/TypedArray/DataView，不接受 ArrayBuffer
		await writer.write(toWritableChunk(rawClientData)); // first write, normal is tls client hello
		writer.releaseLock();
		return tcpSocket;
	}

	// if the edge node connect tcp socket have no incoming data, we retry to redirect ip
	async function retry() {
		const tcpSocket = await connectAndWrite(proxyIP || addressRemote, portRemote);
		// no matter retry success or not, close websocket
		tcpSocket.closed.catch((error) => {
			console.log('retry tcpSocket closed error', error);
		}).finally(() => {
			safeCloseWebSocket(webSocket);
		});
		remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, null, log);
	}

	const tcpSocket = await connectAndWrite(addressRemote, portRemote);

	// when remoteSocket is ready, pass to websocket
	// remote --> ws
	remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, retry, log);
}

/**
 * Node.js net.connect -> Cloudflare Socket 风格封装
 * @param {string} hostname
 * @param {number} port
 */
function tcpConnect(hostname, port) {
	const socket = net.connect({ host: hostname, port: Number(port) });
	socket.setNoDelay(true);
	// 吞掉 'error' 事件，错误会通过 readable/writable 流传播，避免 unhandled error 崩掉进程
	socket.on('error', () => {});
	return {
		/** @type {ReadableStream} */
		readable: Readable.toWeb(socket),
		/** @type {WritableStream} */
		writable: Writable.toWeb(socket),
		closed: new Promise((resolve, reject) => {
			socket.once('close', () => resolve());
			socket.once('error', (err) => reject(err));
		}),
		close() {
			socket.destroy();
		},
	};
}

/**
 * @param {any} webSocketServer
 * @param {string} earlyDataHeader for ws 0rtt
 * @param {(info: string) => void} log
 */
function makeReadableWebSocketStream(webSocketServer, earlyDataHeader, log) {
	let readableStreamCancel = false;
	const stream = new ReadableStream({
		start(controller) {
			webSocketServer.addEventListener('message', (event) => {
				if (readableStreamCancel) {
					return;
				}
				const message = event.data;
				controller.enqueue(message);
			});

			// The event means that the client closed the client -> server stream.
			// However, the server -> client stream is still open until you call close() on the server side.
			webSocketServer.addEventListener('close', () => {
				safeCloseWebSocket(webSocketServer);
				if (readableStreamCancel) {
					return;
				}
				controller.close();
			});
			webSocketServer.addEventListener('error', (err) => {
				log('webSocketServer has error');
				controller.error(err);
			});
			// for ws 0rtt
			const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
			if (error) {
				controller.error(error);
			} else if (earlyData) {
				controller.enqueue(earlyData);
			}
		},
		cancel(reason) {
			if (readableStreamCancel) {
				return;
			}
			log(`ReadableStream was canceled, due to ${reason}`);
			readableStreamCancel = true;
			safeCloseWebSocket(webSocketServer);
		},
	});

	return stream;
}

// https://xtls.github.io/development/protocols/vless.html

/**
 * @param {ArrayBuffer} vlessBuffer
 * @param {string} userID
 */
function processVlessHeader(vlessBuffer, userID) {
	if (vlessBuffer.byteLength < 24) {
		return {
			hasError: true,
			message: 'invalid data',
		};
	}
	const version = new Uint8Array(vlessBuffer.slice(0, 1));
	let isValidUser = false;
	let isUDP = false;
	if (stringify(new Uint8Array(vlessBuffer.slice(1, 17))) === userID) {
		isValidUser = true;
	}
	if (!isValidUser) {
		return {
			hasError: true,
			message: 'invalid user',
		};
	}

	const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];
	// skip opt for now

	const command = new Uint8Array(vlessBuffer.slice(18 + optLength, 18 + optLength + 1))[0];

	// 0x01 TCP
	// 0x02 UDP
	// 0x03 MUX
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
	const portBuffer = vlessBuffer.slice(portIndex, portIndex + 2);
	// port is big-Endian in raw data etc 80 == 0x005d
	const portRemote = new DataView(portBuffer).getUint16(0);

	let addressIndex = portIndex + 2;
	const addressBuffer = new Uint8Array(vlessBuffer.slice(addressIndex, addressIndex + 1));

	// 1--> ipv4  addressLength =4
	// 2--> domain name addressLength=addressBuffer[1]
	// 3--> ipv6  addressLength =16
	const addressType = addressBuffer[0];
	let addressLength = 0;
	let addressValueIndex = addressIndex + 1;
	let addressValue = '';
	switch (addressType) {
		case 1:
			addressLength = 4;
			addressValue = new Uint8Array(
				vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)
			).join('.');
			break;
		case 2:
			addressLength = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + 1))[0];
			addressValueIndex += 1;
			addressValue = new TextDecoder().decode(
				vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)
			);
			break;
		case 3: {
			addressLength = 16;
			const dataView = new DataView(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
			// 2001:0db8:85a3:0000:0000:8a2e:0370:7334
			const ipv6 = [];
			for (let i = 0; i < 8; i++) {
				ipv6.push(dataView.getUint16(i * 2).toString(16));
			}
			addressValue = ipv6.join(':');
			// seems no need add [] for ipv6
			break;
		}
		default:
			return {
				hasError: true,
				message: `invild  addressType is ${addressType}`,
			};
	}
	if (!addressValue) {
		return {
			hasError: true,
			message: `addressValue is empty, addressType is ${addressType}`,
		};
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
 * remote --> ws
 * @param {any} remoteSocket
 * @param {any} webSocket
 * @param {ArrayBuffer} vlessResponseHeader
 * @param {(() => Promise<void>) | null} retry
 * @param {*} log
 */
async function remoteSocketToWS(remoteSocket, webSocket, vlessResponseHeader, retry, log) {
	let hasIncomingData = false; // check if remoteSocket has incoming data
	let vlessHeader = vlessResponseHeader;
	await remoteSocket.readable
		.pipeTo(
			new WritableStream({
				async write(chunk, controller) {
					hasIncomingData = true;
					if (isWebSocketClosed(webSocket)) {
						controller.error('webSocket is not open, maybe close');
					}
					if (vlessHeader) {
						webSocket.send(await concatToArrayBuffer(vlessHeader, chunk));
						vlessHeader = null;
					} else {
						webSocket.send(await normalizeToArrayBuffer(chunk));
					}
				},
				close() {
					log(`remoteConnection!.readable is close with hasIncomingData is ${hasIncomingData}`);
				},
				abort(reason) {
					console.error(`remoteConnection!.readable abort`, reason);
				},
			})
		)
		.catch((error) => {
			console.error(`remoteSocketToWS has exception `, error?.stack || error);
			safeCloseWebSocket(webSocket);
		});

	// seems is edge connect socket have error,
	// 1. closed promise will reject
	// 2. socket.readable will be close without any data coming
	if (hasIncomingData === false && retry) {
		log(`retry`);
		retry();
	}
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

/**
 * @param {any} socket
 */
function isWebSocketClosed(socket) {
	// 0 connecting / 1 open / 2 closing / 3 closed
	return socket.readyState !== undefined && socket.readyState >= 2;
}

/**
 * Normally, WebSocket will not has exceptions when close.
 * @param {any} socket
 */
function safeCloseWebSocket(socket) {
	try {
		if (socket.readyState === undefined || socket.readyState === 1 || socket.readyState === 2) {
			socket.close();
		}
	} catch (error) {
		console.error('safeCloseWebSocket error', error);
	}
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

/**
 * 转成 Node 流可写 chunk（Buffer/TypedArray/DataView/string）
 * @param {any} chunk
 * @returns {any}
 */
function toWritableChunk(chunk) {
	if (typeof chunk === 'string') {
		return Buffer.from(chunk);
	}
	if (chunk instanceof ArrayBuffer) {
		return new Uint8Array(chunk);
	}
	return chunk; // TypedArray / DataView / Buffer
}

/**
 * 数据归一化：把 ws/流的任意 chunk 转成 ArrayBuffer
 * @param {any} data
 * @returns {Promise<ArrayBuffer>}
 */
async function normalizeToArrayBuffer(data) {	if (data instanceof ArrayBuffer) return data;
	if (typeof data === 'string') {
		return new TextEncoder().encode(data).buffer;
	}
	if (ArrayBuffer.isView(data)) {
		return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
	}
	if (typeof Blob !== 'undefined' && data instanceof Blob) {
		return await data.arrayBuffer();
	}
	return data;
}

/**
 * 拼接两个二进制块
 * @param {ArrayBuffer | Uint8Array} a
 * @param {Uint8Array | ArrayBuffer} b
 * @returns {Promise<ArrayBuffer>}
 */
async function concatToArrayBuffer(a, b) {
	const ab = a instanceof ArrayBuffer ? new Uint8Array(a) : a;
	const bb = b instanceof ArrayBuffer ? new Uint8Array(b) : b;
	const merged = new Uint8Array(ab.byteLength + bb.byteLength);
	merged.set(ab, 0);
	merged.set(bb, ab.byteLength);
	return merged.buffer;
}

/**
 * UDP(DNS) 出站：通过 DoH 转发 DNS 查询（Node Functions 出站 UDP 不可用）
 * @param {any} webSocket
 * @param {ArrayBuffer} vlessResponseHeader
 * @param {(string) => void} log
 */
async function handleUDPOutBound(webSocket, vlessResponseHeader, log) {
	let isVlessHeaderSent = false;
	const transformStream = new TransformStream({
		transform(chunk, controller) {
			// udp message 2 byte is the the length of udp data
			for (let index = 0; index < chunk.byteLength; ) {
				const lengthBuffer = chunk.slice(index, index + 2);
				const udpPakcetLength = new DataView(lengthBuffer).getUint16(0);
				const udpData = new Uint8Array(chunk.slice(index + 2, index + 2 + udpPakcetLength));
				index = index + 2 + udpPakcetLength;
				controller.enqueue(udpData);
			}
		},
	});

	// only handle dns udp for now
	transformStream.readable.pipeTo(
		new WritableStream({
			async write(chunk) {
				const resp = await fetch('https://1.1.1.1/dns-query', {
					method: 'POST',
					headers: {
						'content-type': 'application/dns-message',
					},
					body: chunk,
				});
				const dnsQueryResult = await resp.arrayBuffer();
				const udpSize = dnsQueryResult.byteLength;
				const udpSizeBuffer = new Uint8Array([(udpSize >> 8) & 0xff, udpSize & 0xff]);
				if (!isWebSocketClosed(webSocket)) {
					log(`doh success and dns message length is ${udpSize}`);
					if (isVlessHeaderSent) {
						webSocket.send(await concatToArrayBuffer(udpSizeBuffer, dnsQueryResult));
					} else {
						webSocket.send(await concatToArrayBuffer(concatToArrayBufferSync(vlessResponseHeader, udpSizeBuffer), dnsQueryResult));
						isVlessHeaderSent = true;
					}
				}
			},
		})
	).catch((error) => {
		log('dns udp has error' + error);
	});

	const writer = transformStream.writable.getWriter();

	return {
		/**
		 * @param {Uint8Array} chunk
		 */
		write(chunk) {
			writer.write(chunk);
		},
	};
}

/**
 * 同步拼接（用于 vlessResponseHeader + udpSizeBuffer）
 * @param {ArrayBuffer | Uint8Array} a
 * @param {Uint8Array} b
 */
function concatToArrayBufferSync(a, b) {
	const aa = a instanceof ArrayBuffer ? new Uint8Array(a) : a;
	const merged = new Uint8Array(aa.byteLength + b.byteLength);
	merged.set(aa, 0);
	merged.set(b, aa.byteLength);
	return merged.buffer;
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
