// 兼容路由：把 /vless 升级请求转发到官方形态的 websocket.ts。
// 若平台按「路径 = 文件名」精确路由 WS 桥接，此文件保证 /vless 路径同样可用。
export { onRequest } from './websocket.ts';
