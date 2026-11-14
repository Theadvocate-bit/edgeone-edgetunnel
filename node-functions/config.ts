// 多节点并存配置生成（v14.2）
//
// 节点清单：
//   节点1 · VLESS + WebSocket   —— 标准协议；平台 WS 数据面修复后立即可用
//   节点2 · XHTTP 事务模式       —— 实验性：一请求一事务（上行整包缓冲 + 下行流式），
//                                  链接对齐 xray xhttp 解剖（type=xhttp + extra JSON），
//                                  但 mode=transaction 为本项目自定义标记，标准客户端不支持
//   节点3 · UDP 逐包中继        —— 依赖平台 dgram 出站（GET /udp 探测）；
//                                  udp-relay:// 自定义 scheme 对齐 hysteria2:// 解剖
//
// Hysteria2 不生成可用节点：其服务端需要 QUIC 协议栈 + 持久 UDP 会话，平台无法承载
//（说明与格式对照见配置文本）。

export const WS_PATH = '/vless';
export const XHTTP_PATH = '/xhttp';
export const UDP_PATH = '/udp';

/** 节点1：VLESS + WebSocket 的 vless:// 分享链接（标准协议，可直接导入） */
export function getWsNodeLink(uid: string, host: string): string {
  const wsPath = `${WS_PATH}?ed=2048`;
  return (
    `vless://${uid}@${host}:443` +
    `?encryption=none&security=tls&sni=${host}&fp=chrome` +
    `&alpn=${encodeURIComponent('h2,http/1.1')}` +
    `&type=ws&host=${host}` +
    `&path=${encodeURIComponent(wsPath)}#EO-WS-${host}`
  );
}

/** 节点1 的 clash-meta 片段 */
export function getWsClashNode(uid: string, host: string): string {
  const wsPath = `${WS_PATH}?ed=2048`;
  return (
    `- name: EO-WS-${host}\n` +
    `  type: vless\n` +
    `  server: ${host}\n` +
    `  port: 443\n` +
    `  uuid: ${uid}\n` +
    `  network: ws\n` +
    `  tls: true\n` +
    `  udp: false\n` +
    `  sni: ${host}\n` +
    `  client-fingerprint: chrome\n` +
    `  alpn: [h2, http/1.1]\n` +
    `  ws-opts:\n` +
    `    path: "${wsPath}"\n` +
    `    headers:\n` +
    `      host: ${host}`
  );
}

/**
 * 节点2：XHTTP 事务模式链接 —— 解剖对齐 xray xhttp 分享链接
 * （type=xhttp + path + mode + extra JSON，参考 xray 1.24+ 链接规范），
 * 但 mode=transaction 是本项目自定义标记：平台沙箱逐请求重建、无跨请求状态，
 * 无法实现 xray 的 packet-up/stream-up 会话语义，标准 xray 客户端不可直连。
 * extra 字段描述本项目真实传输形态，供自定义客户端解析。
 */
export function getXhttpNodeLink(uid: string, host: string): string {
  const xhttpPath = `${XHTTP_PATH}?mode=vless`;
  const extra = JSON.stringify({
    mode: 'transaction',
    uplink: 'buffered',   // 平台无流式上传：上行必须整包
    downlink: 'stream',   // 流式响应可用：下行逐块下发
    tcpCommand: 'vless-frame -> streaming TCP tunnel',
    udpCommand: 'vless-frame cmd=2 -> dgram per-packet relay',
  });
  return (
    `vless://${uid}@${host}:443` +
    `?encryption=none&security=tls&sni=${host}&fp=chrome` +
    `&alpn=${encodeURIComponent('h2,http/1.1')}` +
    `&type=xhttp` +
    `&path=${encodeURIComponent(xhttpPath)}` +
    `&mode=transaction` +
    `&extra=${encodeURIComponent(extra)}` +
    `#EO-XHTTP-${host}`
  );
}

/** 节点2 的 clash-meta 片段（clash 无 xhttp 事务模式类型，输出为注释性说明块） */
export function getXhttpClashNode(uid: string, host: string): string {
  return (
    `- name: EO-XHTTP-${host} (transaction, 需自定义内核)\n` +
    `  # clash.meta 无对应传输类型；标准 xray xhttp 会话语义与本平台不兼容。\n` +
    `  # 仅可用形态: POST https://${host}${XHTTP_PATH}?mode=vless (body=VLESS 帧, 一请求一事务)`
  );
}

/**
 * 节点3：UDP 逐包中继链接 —— 解剖对齐 hysteria2:// 规范
 * （scheme://password@host:port?params#name），scheme 用自定义 udp-relay://：
 * 标准分享协议中不存在「无状态 UDP 中继」节点类型，hysteria2:// 因服务端需
 * QUIC + 持久会话在本平台不可承载，故不伪造 hysteria2 节点。
 */
export function getUdpNodeLink(uid: string, host: string): string {
  return (
    `udp-relay://${uid}@${host}:443${UDP_PATH}` +
    `?probe=${encodeURIComponent(`${UDP_PATH}`)}` +
    `&framing=length-prefix` +
    `&transport=https-post` +
    `&method=POST` +
    `&params=host,port` +
    `#EO-UDP-${host}`
  );
}

/** Hysteria2 格式对照（仅展示为何不可用，非可用节点） */
function getHysteria2FormatNote(host: string): string {
  return (
    `hysteria2://<密码>@${host}:443?sni=${host}#EO-H2-不可用\n` +
    `  ↑ 仅为格式对照。真实 hysteria2 服务端要求: QUIC/TLS1.3 握手 + 连接级状态机\n` +
    `    + 持久 UDP socket。本平台: 沙箱逐请求重建(无跨请求状态) + 运行时无 QUIC，\n` +
    `    故不生成可用 hysteria2:// 节点——导入上述格式将无法建立连接。`
  );
}

/** 多节点并存完整配置文本 */
export function getFullConfig(uid: string, host: string): string {
  const wsLink = getWsNodeLink(uid, host);
  const clash = getWsClashNode(uid, host);
  const xhttpLink = getXhttpNodeLink(uid, host);
  const xhttpClash = getXhttpClashNode(uid, host);
  const udpLink = getUdpNodeLink(uid, host);
  const h2Note = getHysteria2FormatNote(host);
  return `
################################################################
EdgeOne Pages 多节点配置 (v14.2)
域名: ${host}
UUID: ${uid}
################################################################

节点 1 · VLESS + WebSocket（标准协议）
状态: 配置就绪。平台 WS 数据面修复（已知缺陷：握手/回调正常但双向帧不中继）后立即可用。
------------------------------------------------
v2rayN / v2rayNG 分享链接（可直接导入）:
${wsLink}
------------------------------------------------
clash-meta:
${clash}
################################################################

节点 2 · XHTTP 事务模式（实验性，需自定义客户端）
形态: 一请求 = 一事务（上行整包缓冲 + 下行流式）。平台沙箱逐请求重建、无跨请求状态，
      无法实现 xray xhttp 的 packet-up/stream-up 会话语义 —— 标准客户端不可直连。
      链接解剖对齐 xray xhttp 规范（type=xhttp + mode + extra JSON），mode=transaction
      为本项目自定义标记，供自定义客户端解析。
------------------------------------------------
导入链接（xray xhttp 解剖形态）:
${xhttpLink}

clash-meta（说明块，无可用传输类型）:
${xhttpClash}
------------------------------------------------
实际 API（自定义客户端按此实现）:
  POST https://${host}${XHTTP_PATH}?mode=vless
    body: VLESS 请求帧（UUID 鉴权）
    TCP 命令(01) → 流式 TCP 隧道（响应 = VLESS 响应头 + 目标下行流）
    UDP 命令(02) → dgram 逐包中继（X-UDP-* 头回显响应数/字节/耗时）
  POST https://${host}${XHTTP_PATH}?addr=<目标>&port=<端口>
    body: 完整上行字节（raw 模式，如一条完整 HTTP 请求）
    响应: 流式返回目标下行字节（X-EO-Tunnel 头回显会话信息）
  小段上行可用 d=<base64url> 查询参数携带；仅监听下载用 GET ${XHTTP_PATH}?addr=..&port=..

curl 示例（经隧道向 example.com:80 发起 HTTP 请求）:
  curl -s "https://${host}${XHTTP_PATH}?addr=example.com&port=80" \\
    --data-binary $'GET / HTTP/1.1\\r\\nHost: example.com\\r\\nConnection: close\\r\\n\\r\\n'

超时保护: 连接 8s / 空闲 15s / 总时长 25s（平台 30s 墙钟内安全收尾）
################################################################

节点 3 · UDP 逐包中继（可用性以 GET ${UDP_PATH} 探测为准）
形态: 无状态逐包中继——body=UDP 载荷，服务端 dgram 发送并收集响应返回。
      适用于 DNS 等一问一答型 UDP 流量；多响应以 2 字节大端长度前缀分帧。
      链接解剖对齐 hysteria2:// 规范（scheme://password@host:port?params#name），
      scheme 用自定义 udp-relay://（标准协议无此节点类型）。
------------------------------------------------
导入链接（udp-relay:// 解剖形态，需自定义客户端）:
${udpLink}
------------------------------------------------
实际 API（自定义客户端按此实现）:
  探测: GET  https://${host}${UDP_PATH}
  中继: POST https://${host}${UDP_PATH}?host=<IP>&port=<端口>  body=<UDP 载荷>
  经 VLESS 帧: POST ${XHTTP_PATH}?mode=vless，UDP 命令帧（cmd=02）
################################################################

关于 Hysteria2（不生成可用节点）
------------------------------------------------
${h2Note}
UDP 路线的可用产出即节点 3 的逐包中继。
################################################################

自诊断: GET /debug   XHTTP 说明: GET ${XHTTP_PATH}   UDP 探测: GET ${UDP_PATH}
################################################################
`;
}

/** 兼容旧调用的单节点配置（WS） */
export function getVLESSConfig(uid: string, host: string): string {
  const wsPath = `${WS_PATH}?ed=2048`;
  const vlessMain = getWsNodeLink(uid, host);
  return `
################################################################
v2ray (EdgeOne Pages)
---------------------------------------------------------------
${vlessMain}
---------------------------------------------------------------
################################################################
clash-meta
---------------------------------------------------------------
${getWsClashNode(uid, host)}
---------------------------------------------------------------
################################################################
说明: 多节点并存完整配置见 /${uid}（v14 起默认输出多节点版本），本函数保留单节点输出。
XHTTP 事务模式与 UDP 中继用法见多节点配置文本。WS 路径: ${wsPath}
################################################################
`;
}
