// 临时诊断：纯 HTTP 触发 net.connect 出站，结果直接写进 Response（不依赖 WS）
import net from 'node:net';

export async function onRequest(context: any) {
  const result = await new Promise<string>((resolve) => {
    const logs: string[] = [];
    const socket = net.connect({ host: 'example.com', port: 80 });
    socket.setNoDelay(true);
    const done = (m: string) => { logs.push(m); try { socket.destroy(); } catch {} resolve(logs.join(' | ')); };
    socket.on('connect', () => logs.push('TCP CONNECTED example.com:80'));
    socket.on('data', (d: any) => { logs.push('DATA ' + (d.length ?? 0) + 'B: ' + Buffer.from(d).toString().slice(0, 40)); done('done'); });
    socket.on('error', (err: any) => done('TCP ERROR ' + (err?.code || '') + ' ' + (err?.message || err)));
    socket.on('close', () => logs.push('closed'));
    setTimeout(() => done('TIMEOUT after 8s, logs so far: ' + logs.join(' | ')), 8000);
  });
  return new Response(result, { headers: { 'Content-Type': 'text/plain' } });
}
