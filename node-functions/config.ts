// 多节点并存配置生成（v14）
//
// 节点清单：
//   节点1 · VLESS + WebSocket   —— 标准协议；平台 WS 数据面修复后立即可用
//   节点2 · XHTTP 事务模式       —— 实验性：一请求一事务，需自定义客户端（平台无跨请求状态，
//                                  与 xray xhttp 的会话语义不兼容）
//   节点3 · UDP 逐包中继        —— 依赖平台 dgram 出站（以 GET /udp 探测结果为准）
//
// Hysteria2 不生成节点：其服务端需要 QUIC 协议栈 + 持久 UDP 会话，平台无法承载（说明见配置文本）。

export const WS_PATH = '/vless';

/** 节点1：VLESS + WebSocket 的 vless:// 分享链接 */
export function getWsNodeLink(uid: string, host: string): string {
  const wsPath = `${WS_PATH}?ed=2048`;
  return (
    `vless://${uid}@${host}:443` +
    `?encryption=none&security=tls&sni=${host}&fp=randomized&type=ws&host=${host}` +
    `&path=${encodeURIComponent(wsPath)}#EO-ws-${host}`
  );
}

/** 节点1 的 clash-meta 片段 */
export function getWsClashNode(uid: string, host: string): string {
  const wsPath = `${WS_PATH}?ed=2048`;
  return (
    `- name: EO-ws\n` +
    `  type: vless\n` +
    `  server: ${host}\n` +
    `  port: 443\n` +
    `  uuid: ${uid}\n` +
    `  network: ws\n` +
    `  tls: true\n` +
    `  udp: false\n` +
    `  sni: ${host}\n` +
    `  client-fingerprint: chrome\n` +
    `  ws-opts:\n` +
    `    path: "${wsPath}"\n` +
    `    headers:\n` +
    `      host: ${host}`
  );
}

/** 多节点并存完整配置文本 */
export function getFullConfig(uid: string, host: string): string {
  const wsLink = getWsNodeLink(uid, host);
  const clash = getWsClashNode(uid, host);
  const xhttpPath = encodeURIComponent(`/xhttp`);
  return `
################################################################
EdgeOne Pages 多节点配置 (v14)
域名: ${host}
UUID: ${uid}
################################################################

节点 1 · VLESS + WebSocket（标准协议）
状态: 配置就绪。平台 WS 数据面修复（已知缺陷：握手/回调正常但双向帧不中继）后立即可用。
------------------------------------------------
v2rayN / v2rayNG 分享链接:
${wsLink}
------------------------------------------------
clash-meta:
${clash}
################################################################

节点 2 · XHTTP 事务模式（实验性，需自定义客户端）
形态: 一请求 = 一事务（上行整包 + 下行流式）。平台沙箱逐请求重建、无跨请求状态，
      与 xray xhttp 的会话语义不兼容，故不能用 xray/v2rayN 直连，需按下列 API 自行实现客户端。
------------------------------------------------
上行+下行（raw 模式）:
  POST https://${host}/xhttp?addr=<目标>&port=<端口>
  Content-Type: application/octet-stream
  body: 完整上行字节（如一条完整 HTTP 请求）
  响应: 流式返回目标下行字节（X-EO-Tunnel 头回显会话信息）

VLESS 帧（地址/端口由 VLESS 头解析，回传带 VLESS 响应头）:
  POST https://${host}/xhttp?mode=vless
  body: VLESS 请求帧

小段上行可用 d=<base64url> 查询参数携带；仅监听下载用 GET /xhttp?addr=..&port=..

curl 示例（经隧道向 example.com:80 发起 HTTP 请求）:
  curl -s "https://${host}/xhttp?addr=example.com&port=80" \\
    --data-binary $'GET / HTTP/1.1\\r\\nHost: example.com\\r\\nConnection: close\\r\\n\\r\\n'

超时保护: 连接 8s / 空闲 15s / 总时长 25s（平台 30s 墙钟内安全收尾）
################################################################

节点 3 · UDP 逐包中继（可用性以 GET /udp 探测为准）
形态: 无状态逐包中继——body=UDP 载荷，服务端 dgram 发送并收集响应返回。
      适用于 DNS 等一问一答型 UDP 流量；多响应以 2 字节大端长度前缀分帧。
------------------------------------------------
探测: GET https://${host}/udp
中继: POST https://${host}/udp?host=8.8.8.8&port=53  body=<UDP 载荷>
################################################################

关于 Hysteria2
------------------------------------------------
Hysteria2 服务端需要 QUIC 协议栈（TLS1.3 握手、连接级状态机、持久 UDP socket）。
本平台：沙箱逐请求重建（无跨请求状态）+ 运行时无 QUIC + dgram 存在性待探测
——无法承载 Hysteria2 服务端，故不生成 hysteria2:// 节点。
UDP 路线的可用产出即节点 3 的逐包中继。
################################################################

自诊断: GET /debug   XHTTP 说明: GET /xhttp   UDP 探测: GET /udp
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
