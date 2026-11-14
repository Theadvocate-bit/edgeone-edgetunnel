// 共享工具函数（v14）

/**
 * 读取请求体为 Buffer，按可靠性顺序尝试多种方式：
 *   1. request.arrayBuffer() —— 标准 Web API（若平台提供）
 *   2. request._rawBodyBuffer —— 平台内部缓冲（/stream-in 内省发现）
 *   3. request.text() —— 兜底（UTF-8 解码，二进制数据有损坏风险）
 */
export async function readBodyBuffer(request: any): Promise<{ buf: Buffer | null; how: string }> {
  try {
    if (typeof request?.arrayBuffer === 'function') {
      const ab = await request.arrayBuffer();
      if (ab && ab.byteLength > 0) return { buf: Buffer.from(ab), how: 'arrayBuffer' };
    }
  } catch { /* try next */ }
  try {
    const raw = request?._rawBodyBuffer;
    if (raw && (raw.length > 0 || raw.byteLength > 0)) return { buf: Buffer.from(raw), how: '_rawBodyBuffer' };
  } catch { /* try next */ }
  try {
    const t = typeof request?.text === 'function' ? await request.text() : '';
    if (t) return { buf: Buffer.from(t, 'utf8'), how: 'text(utf8)' };
  } catch { /* ignore */ }
  return { buf: null, how: 'none' };
}

/** base64url 解码（用于 d= 参数携带上行数据） */
export function b64urlToBuffer(s: string): Buffer | null {
  if (!s) return null;
  try {
    const b64 = s.replace(/-/g, '+').replace(/_/g, '/');
    return Buffer.from(atob(b64), 'binary');
  } catch { return null; }
}

/** 构造极简 DNS A 记录查询包（UDP 探针用） */
export function buildDnsQuery(name: string, id = 0x1234): Buffer {
  const labels = name.split('.').filter(Boolean);
  const qname = Buffer.concat(
    labels.map((l) => Buffer.concat([Buffer.from([l.length]), Buffer.from(l, 'ascii')]))
  );
  return Buffer.concat([
    Buffer.from([(id >> 8) & 0xff, id & 0xff, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
    qname,
    Buffer.from([0x00, 0x01, 0x00, 0x01]), // type A, class IN
  ]);
}
