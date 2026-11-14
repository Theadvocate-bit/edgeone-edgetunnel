export async function onRequest(context: any) {
  return new Response('pong from ping.ts @ ' + new Date().toISOString(), {
    headers: { 'Content-Type': 'text/plain' },
  });
}
