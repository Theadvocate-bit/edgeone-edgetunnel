// 临时：验证平台 WS 桥接是否能把服务端 ws.send 的帧回传给客户端
export async function onRequest(context: any) {
  if (context.request.headers.get('upgrade')?.toLowerCase() !== 'websocket') {
    return new Response('ws echo endpoint', { status: 426 });
  }
  return {
    websocket: {
      onopen(ws: any) { ws.send('echo-server-hello'); },
      onmessage(ws: any, msg: any) { ws.send('echo:' + (typeof msg === 'string' ? msg : Buffer.from(msg).toString().slice(0,40))); },
      onclose() {}, onerror() {},
    },
  };
}
