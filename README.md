# Edge Tunnel · EdgeOne Pages

将 Cloudflare Worker 版 [edgetunnel](https://github.com/zizifn/edgetunnel)（VLESS over WebSocket）改造为可部署在**腾讯云 EdgeOne Pages / EdgeOne Makers** 上的版本，使用 **Node Functions**（Node.js 20 运行时）。

## 改造说明

| Cloudflare Worker 原实现 | EdgeOne 版实现 |
|---|---|
| `import { connect } from 'cloudflare:sockets'` | Node.js `net.connect`（事件驱动桥接 WS 回调，带 8 秒连接超时） |
| `WebSocketPair`（CF 专有） | **EdgeOne 官方 WS API**：handler 对升级请求返回 `{ websocket: { onopen, onmessage, onclose, onerror } }`，平台负责 101 握手并回调事件。⚠️ 运行时不存在 `WebSocketPair`，使用会导致升级 500 |
| WS 处理在 fetch 入口 | **官方同形态文件**：`node-functions/websocket.ts`（文件名与官方 WS 模板一致，平台的 WS 桥接按此文件名挂接）。`package.json` 同步对齐官方：`"type": "commonjs"` + 依赖 `ws` |
| WS 升级 API | **双模式**：`handlers`（官方 `{websocket: onopen/onmessage/onclose/onerror}` 形态，默认）与 `pair`（CF 风格 `WebSocketPair` + `Response(101, webSocket)`），可用查询参数 `?ws-mode=pair` 或环境变量 `WS_MODE` 切换 |
| `env.UUID` / `env.PROXYIP` | `context.env.UUID` / `context.env.PROXYIP`（Pages 控制台环境变量） |
| `request.cf`（CF 请求指纹） | 已移除，首页改为静态说明页 |
| WS 路径 `/?ed=2048` | **`/vless?ed=2048`**（`websocket.ts` 同时接受 `/websocket`、`/ws`、`/vless` 三个路径） |
| 流式 pipeTo 管道 | 事件驱动：`onmessage` → 解析/转发；socket `data` → `ws.send`（首包拼 VLESS 响应头） |
| UDP DNS（CF 无 UDP 出站） | 同样通过 DoH（`https://1.1.1.1/dns-query`）转发，含长度前缀重组缓冲 |
| — | **`/debug` 自诊断端点**：实测 DNS 与出站 TCP 连通性 |

## 项目结构

```
├── index.html                    # 静态首页（说明页）
├── package.json                  # "type": "commonjs" + 依赖 ws（与官方 WS 模板一致）
├── test/
│   └── run.mjs                   # 本地冒烟测试（esbuild 打包后模拟运行时，9 项用例）
└── node-functions/
    ├── websocket.ts              # WS 端点（官方形态文件名）+ VLESS 核心 + 配置生成
    ├── vless.ts                  # 兼容路由：转发到 websocket.ts（/vless 路径）
    └── [[default]].ts            # HTTP 兜底：/debug、/<UUID>、404（不处理 WS 升级）
```

## 部署步骤

### 方式一：EdgeOne Makers / Pages 控制台（推荐）

1. 将本项目推送到你的 GitHub 仓库；
2. 打开 [EdgeOne Pages](https://pages.edgeone.ai/zh)（Makers 入口同平台）→「创建项目」→ 选择「从 Git 仓库导入」，授权并选中该仓库；
3. 构建设置保持默认（无需构建命令，`node-functions/` 目录会被自动识别为函数）；
4. 点击「开始部署」，部署完成后获得 `https://<项目名>.edgeone.app` 域名。

### 方式二：EdgeOne CLI

```bash
npm install -g edgeone
cd edgeone-edgetunnel
edgeone pages init   # 关联项目
edgeone pages dev    # 本地调试（注意：本地 net 模块可用，WS 行为以线上为准）
edgeone pages deploy # 部署
```

## 配置

在 Pages 项目控制台 →「环境变量」中添加：

| 变量 | 必填 | 说明 |
|---|---|---|
| `UUID` | 否 | VLESS 鉴权 UUID。不设置时使用内置默认值 `d342d11e-d424-4583-b36e-524ab1f0afa4`，**生产环境务必自定义** |
| `PROXYIP` | 否 | 目标直连失败（无回源数据）时，改用该 IP 出站重试 |
| `CUSTOM_DOMAIN` | 否 | **自定义域名场景强烈推荐**：强制配置生成使用该域名（address/sni/host 全部跟随）。不设置时按 `X-Forwarded-Host` > `url.hostname` > `Host` 自动解析 |

**为什么推荐设置 `CUSTOM_DOMAIN`**：Pages 平台把请求转发给函数时，`Host` 头可能被改写为内部回源地址——不显式指定的话，生成的客户端配置里 SNI/Host/address 可能跟不到自定义域名，导致 TLS 握手失败。设置后（如 `CUSTOM_DOMAIN=my.domain.com`）所有配置项一律使用该域名。可通过 `/debug` 端点的 `resolvedHost` 字段确认当前解析结果。

生成 UUID（Windows PowerShell）：

```powershell
[guid]::NewGuid()
```

## 客户端使用

1. 访问 `https://<你的域名>/<UUID>` 获取 v2ray / clash-meta 配置文本；
2. WebSocket 端点为 `wss://<你的域名>/vless?ed=2048`（支持 0-RTT early data）；
3. 传输层：WS + TLS（EdgeOne 边缘证书），端口 443。

## 已知限制（平台决定）

- **单次调用最长 30 秒墙上时间**（官方 Node Functions 文档）：若实测长连接约 30 秒被断开属平台限制，客户端会自动重连；
- **UDP 仅支持 53 端口 DNS**，通过 DoH 转发；客户端 DNS 策略建议设为「AsIs」；
- 仅支持 HTTP/1.1 WebSocket；
- 函数内发起的 TCP 连接必须从 EdgeOne 节点出站，若目标被墙则需配置 `PROXYIP` 或直接不可达（与 CF 上优选 IP 的玩法不同，EdgeOne 国内节点出站目标通常可达性更好，海外目标视网络情况而定）。

## 本地验证

```bash
npm install
node test/run.mjs
# 9 项用例：/websocket early data / /vless 普通 / /ws IPv4 / 非法路径 400 /
#          /debug / 配置页 / catch-all 升级拒绝 / 自定义域名跟随 ×3
# WS 用例验证：升级返回 { websocket: handlers } -> VLESS 头解析 -> net TCP 连接
#             -> 回传数据首 2 字节为 VLESS 响应头 0x00 0x00，后接 HTTP 响应
```

## 排障记录

### ⚠️ 最终结论（2026-11-14，线上实测定性）：当前平台状态无法承载隧道

经过 v1→v13 共 13 轮迭代与系统化线上诊断，EdgeOne Pages Node Functions 在当前状态下
**无法承载 VLESS/代理类隧道**。全部证据（通过跨隔离 ntfy.sh 诊断流 + raw TLS 帧级观测获得）：

| 平台能力 | 实测结果 |
|---|---|
| WS 101 握手 | ✅ 正常（`websocket.ts` + `commonjs` + `tsconfig.json` 对齐官方形态后建立） |
| `onopen` 回调 | ✅ 触发，`readyState=1`，`ws.send` 不抛错、`bufferedAmount=0` |
| **WS 入站帧（客户端→函数）** | ❌ 从不触发 `onmessage`（唯一例外：升级请求头 `sec-websocket-protocol` 的 early-data 能到达） |
| **WS 出站帧（函数→客户端）** | ❌ 完全不中继——数据帧、close 帧全部丢失，客户端 TCP 0 字节，约 200ms 后服务端 ws 被平台以 1001 关闭 |
| 流式响应（GET 下载） | ✅ 完美（chunk 以精确间隔逐块到达，无缓冲） |
| 流式请求体 | ❌ `request.body` 为 null（缓冲读取 `request.text()` 可用） |
| **实例亲和性（跨请求内存状态）** | ❌ **每个请求都是全新沙箱**（模块级计数器每次 count=1，同一 keepalive 连接内亦如此） |
| 出站 TCP（`node:net`） | ✅ 全通（example.com:80 → ~5ms） |
| 单次调用墙钟 | ~30s |

代理隧道的本质要求（长生命周期 TCP 会话 + 双向实时数据流）与上述约束全面冲突：
WS 数据面死掉 + 无跨请求状态 + 无流式上传，任何标准协议传输（ws/xhttp/grpc）均不可行。

**可行出路**：
1. **向 EdgeOne 官方反馈 WS 桥接缺陷**（证据完备：握手正常/回调正常/双向帧全死/200ms 关闭/readyState=1/send 零异常）。桥接修复后本项目代码**立即可用**（配置生成、域名跟随、PROXYIP 重试、超时保护均已就绪）；
2. 原版 edgetunnel 回归 **Cloudflare Workers**（其原生运行环境，开箱即用）；
3. 或改用支持 WS + raw TCP 的其它运行时（Deno Deploy、VPS 等）。

## V14 新增：多传输端点与多节点并存

在 WS 数据面缺陷未修复期间，v14 提供了基于已验证可用能力（流式响应 / 缓冲请求体 / `node:net` TCP / `node:dgram` 待探测）的替代传输端点：

### 节点 2 · XHTTP 事务模式（`node-functions/xhttp.ts`）

「一请求 = 一事务」的自包含隧道（**非 xray xhttp 兼容**——平台无跨请求状态，xray 的会话语义无法实现，需按下列 API 自建客户端）：

```
POST /xhttp?addr=<host>&port=<n>            # raw 模式：body=完整上行 → 流式下行
POST /xhttp?mode=vless                      # VLESS 帧：地址/端口从头解析，回传带 VLESS 响应头
GET  /xhttp?addr=<host>&port=<n>            # 仅监听下载（不发送上行）
d=<base64url>                               # 小段上行可走查询参数
```

- 下载方向：流式响应逐块下发（已实测完美）✅
- 上行方向：整包缓冲（`arrayBuffer` → `_rawBodyBuffer` → `text` 三路探测兜底）——**渐进式上行受平台限制（`request.body` 为 null）不可用**
- 超时保护：连接 8s / 空闲 15s / 总时长 25s（平台 30s 墙钟内安全收尾）
- PROXYIP 重试、VLESS 响应头拼接与 WS 版同源

### 节点 3 · UDP 逐包中继（`node-functions/udp.ts`）

```
GET  /udp                                   # 探测：dgram 是否可用 + 实测 DNS over UDP
POST /udp?host=<ip>&port=<n>                # body=UDP 载荷 → 收集响应（静默 400ms）→ 返回
                                            # 多响应以 2 字节大端长度前缀分帧（X-UDP-Responses 头标注数量）
```

- `node:dgram` 动态导入（平台不提供时优雅降级为 JSON 说明）
- 适用于 DNS 等一问一答型 UDP 流量

### Hysteria2 为什么没有节点

Hysteria2 服务端需要 **QUIC 协议栈**（TLS1.3 握手、连接级状态机、持久 UDP socket）。本平台：沙箱逐请求重建（无跨请求状态）+ 运行时无 QUIC —— 无法承载 Hysteria2 服务端，配置页不生成虚假的 `hysteria2://` 节点。UDP 路线的可用产出即节点 3 的逐包中继。

### 多节点并存配置

`/<UUID>` 配置页自 v14 起输出**多节点并存**完整文本：节点 1（VLESS+WS，标准协议，平台修复后立即可用）+ 节点 2（XHTTP 事务模式，含 curl 示例）+ 节点 3（UDP 中继）+ Hysteria2 说明与全部自诊断端点索引。

### 诊断端点（保留可用）

- `GET /debug` — DNS / 出站 TCP 自检 + 请求头回显 + WS 诊断缓冲
- `GET /stream-out`、`POST /stream-in`、`GET /affinity` — 平台能力探针（流式响应/请求体/实例亲和性）

### 历史排障

- **WS 升级 500**：根因是在升级请求中使用了 `WebSocketPair`（该 API 在 EdgeOne Node Functions 运行时不存在，属于 Cloudflare 写法）。正确方式是返回 `{ websocket: { onopen, onmessage, onclose, onerror } }`，由平台完成 101 握手。
- **WS 桥接部分建立**：对齐官方形态（`websocket.ts` 文件名 + `"type": "commonjs"` + `node-functions/tsconfig.json` + 内联字面量返回）后，101 握手与 onopen 回调恢复正常——但数据帧仍不中继（见上「最终结论」）。
- **回传数据缺 VLESS 响应头**：客户端无法识别首包。需在 TCP 出站首次收到数据时拼接 `[version, 0x00]` 两字节（DNS 路径同理）。
- **自定义域名跟随**：平台转发会把 Host 改写为内部回源地址，但保留在 **`eo-pages-host`** 头中。域名解析优先级：`CUSTOM_DOMAIN` 环境变量 > `eo-pages-host` > `X-Forwarded-Host` > `url.hostname` > `Host`。

## 免责声明

本项目仅供技术学习与协议验证使用，请遵守当地法律法规。
