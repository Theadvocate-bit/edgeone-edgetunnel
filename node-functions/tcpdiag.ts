// 临时诊断：验证 Node Functions 运行时能否 net.connect 出站 TCP，并把结果回传 WS
import net from 'node:net';

export async function onRequest(context: any) {
  const upgrade = context.request.headers.get('upgrade');
  if (upgrade?.toLowerCase() !== 'websocket') {
    return new Response('WS diag endpoint', { status: 426 });
  }
  return {
    websocket: {
      onopen(ws: any) {
        ws.send('diag: onopen fired, attempting net.connect example.com:80');
        const socket = net.connect({ host: 'example.com', port: 80 });
        socket.setNoDelay(true);
        socket.on('connect', () => {
          ws.send('diag: TCP CONNECTED to example.com:80');
          socket.write('GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n');
        });
        socket.on('data', (d: any) => {
          ws.send('diag: got ' + (d.length ?? 0) + ' bytes: ' + Buffer.from(d).toString().slice(0, 60));
          socket.destroy();
        });
        socket.on('error', (err: any) => {
          ws.send('diag: TCP ERROR ' + (err?.code || '') + ' ' + (err?.message || err));
        });
        socket.on('close', () => ws.send('diag: socket closed'));
      },
      onmessage() {},
      onclose() {},
      onerror() {},
    },
  };
}
