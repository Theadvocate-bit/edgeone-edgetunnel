// 兼容说明：/vless 升级请求由 websocket.ts（官方形态文件名）处理；
// 本文件不再重复导出 onRequest —— 平台把 node-functions/*.ts 合并打包，
// 多个 onRequest 导出会导致 "symbol already declared" 构建失败（v14.1 实测）。
// 所有 HTTP 路由（含 /udp、/xhttp、/vless 相关）统一由 [[default]].ts 的 onRequest 分发。
