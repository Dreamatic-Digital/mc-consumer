export default {
  async fetch(request, env, ctx) {
    return new Response('Hello, Cloudflare Workers!', {
      headers: { 'Content-Type': 'text/plain' }
    });
  }
}