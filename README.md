# Edge Tunnel · EdgeOne Pages

将 Cloudflare Worker 版 [edgetunnel](https://github.com/zizifn/edgetunnel)（VLESS over WebSocket）改造为可部署在**腾讯云 EdgeOne Pages / EdgeOne Makers** 上的版本，使用 **Node Functions**（Node.js 20 运行时）。

## 改造说明

| Cloudflare Worker 原实现 | EdgeOne 版实现 |
|---|---|
| `import { connect } from 'cloudflare:sockets'` | Node.js `net.connect`（事件驱动桥接 WS 回调） |
| `WebSocketPair`（CF 专有） | **EdgeOne 官方 WS API**：handler 对升级请求返回 `{ websocket: { onopen, onmessage, onclose, onerror } }`，平台负责 101 握手并回调事件（参考官方 pages-websocket-template）。⚠️ 运行时不存在 `WebSocketPair`，使用会导致升级 500 |
| `env.UUID` / `env.PROXYIP` | `context.env.UUID` / `context.env.PROXYIP`（Pages 控制台环境变量） |
| `request.cf`（CF 请求指纹） | 已移除，首页改为静态说明页 |
| WS 路径 `/?ed=2048` | **`/vless?ed=2048`**（避免与静态首页路由冲突，静态资源优先于函数） |
| 流式 pipeTo 管道 | 事件驱动：`onmessage` → 解析/转发；socket `data` → `ws.send`（首包拼 VLESS 响应头） |
| UDP DNS（CF 无 UDP 出站） | 同样通过 DoH（`https://1.1.1.1/dns-query`）转发，含长度前缀重组缓冲 |

## 项目结构

```
├── index.html                    # 静态首页（说明页）
├── package.json
└── node-functions/
    └── [[default]].js            # 函数入口（catch-all 路由）
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
node test/smoke-test.mjs
# 验证：升级返回 { websocket: handlers } -> VLESS 头解析 -> net TCP 连接 example.com:80
#      -> 回传数据首 2 字节为 VLESS 响应头 0x00 0x00，后接 HTTP 响应
```

## 排障记录

- **WS 升级 500**：根因是在升级请求中使用了 `WebSocketPair`（该 API 在 EdgeOne Node Functions 运行时不存在，属于 Cloudflare 写法）。正确方式是返回 `{ websocket: { onopen, onmessage, onclose, onerror } }`，由平台完成 101 握手。
- **回传数据缺 VLESS 响应头**：客户端无法识别首包。需在 TCP 出站首次收到数据时拼接 `[version, 0x00]` 两字节（DNS 路径同理）。

## 免责声明

本项目仅供技术学习与协议验证使用，请遵守当地法律法规。
