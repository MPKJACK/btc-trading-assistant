// Optional Cloudflare Worker relay: lets the BTC 5M app (a static page on GitHub Pages) read Polymarket's
// official "price to beat" and final price. Polymarket's endpoint has no CORS header, so a browser can't call it directly.
// No API keys or secrets are involved. It forwards one public, read-only endpoint, locked to your site's origin.
const ALLOWED_ORIGINS = ['https://mpkjack.github.io'];
const UPSTREAM = 'https://polymarket.com/api/crypto/crypto-price';

export default {
  async fetch(request, env, ctx) {
    const origin = request.headers.get('Origin') || '';
    const cors = {
      'Access-Control-Allow-Origin': ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0],
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Vary': 'Origin',
    };
    if (request.method === 'OPTIONS') return new Response(null, { headers: cors });
    const url = new URL(request.url);
    if (request.method !== 'GET' || url.pathname !== '/crypto-price') return new Response('Not found', { status: 404, headers: cors });
    if (origin && !ALLOWED_ORIGINS.includes(origin)) return new Response('Forbidden', { status: 403, headers: cors });

    // Accept only the expected parameters, validated, so this can't be used as an open proxy.
    const symbol = url.searchParams.get('symbol') || 'BTC';
    const variant = url.searchParams.get('variant') || 'fiveminute';
    const start = url.searchParams.get('eventStartTime'), end = url.searchParams.get('endDate');
    if (!/^[A-Z]{2,6}$/.test(symbol) || !/^[a-z]{4,16}$/.test(variant) || !(Date.parse(start) > 0) || !(Date.parse(end) > 0)) {
      return new Response(JSON.stringify({ error: 'bad parameters' }), { status: 400, headers: { ...cors, 'content-type': 'application/json' } });
    }
    const up = new URL(UPSTREAM);
    up.search = new URLSearchParams({ symbol, eventStartTime: start, variant, endDate: end }).toString();
    const r = await fetch(up.toString(), { headers: { accept: 'application/json' }, cf: { cacheTtl: 3, cacheEverything: true } });
    const body = await r.text();
    return new Response(body, { status: r.status, headers: { ...cors, 'content-type': 'application/json', 'cache-control': 'max-age=3' } });
  },
};
