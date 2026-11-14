// 最小 WS 回显探针：验证 EdgeOne Node Functions 在「专用路由文件」下能否完成 101 升级
export async function onRequest(context) {
  const upgrade = context.request.headers.get('upgrade');
  if (upgrade?.toLowerCase() === 'websocket') {
    return {
      websocket: {
        onopen(ws) { ws.send('hello-from-edgeone-wstest'); },
        onmessage(ws, message, isBinary) { ws.send(typeof message === 'string' ? ('echo:' + message) : message); },
        onclose() {},
        onerror() {},
      },
    };
  }
  return new Response('WS endpoint. Use ws:// to connect.', { status: 426, headers: { Upgrade: 'websocket' } });
}
