// Informational probe: can a browser-like client reach every data source the app uses?
// Prints a markdown table. Never fails the build (sources can be flaky); read the summary.
const ORIGIN = 'https://mpkjack.github.io';
const t5 = Math.floor(Date.now() / 300000) * 300;
const rows = [];
async function rest(name, url) {
  try {
    const r = await fetch(url, { headers: { Origin: ORIGIN } });
    const acao = r.headers.get('access-control-allow-origin');
    let note = ''; try { const j = await r.json(); note = Array.isArray(j) ? `array(${j.length})` : Object.keys(j).slice(0, 4).join(','); } catch (e) {}
    rows.push([name, r.status, acao ? `CORS ${acao}` : 'NO CORS header', note]);
    return r.ok;
  } catch (e) { rows.push([name, 'ERR', '', e.message]); }
}
function ws(name, url, subs, ms = 9000) {
  return new Promise((res) => {
    let n = 0, first = '', opened = false;
    let s; try { s = new WebSocket(url); } catch (e) { rows.push([name, 'ERR', '', e.message]); return res(); }
    const done = (why) => { try { s.close(); } catch (e) {} rows.push([name, opened ? 'open' : 'no-open', `${n} msgs`, (why || '') + ' ' + first.slice(0, 110).replace(/\|/g, '/')]); res(); };
    const timer = setTimeout(() => done(''), ms);
    s.onopen = () => { opened = true; for (const m of subs) s.send(typeof m === 'string' ? m : JSON.stringify(m)); };
    s.onmessage = (e) => { n++; const d = String(e.data); if (!first && d.length > 5 && !/heartbeat|pong|PONG|subscri/i.test(d)) { first = d; try { const j = JSON.parse(d); if (j.payload) first = 'payload keys: ' + Object.keys(j.payload).join(',') + ' ' + JSON.stringify(j.payload).slice(0, 80); } catch (e) {} } };
    s.onerror = (e) => { if (!opened) { clearTimeout(timer); done('error ' + (e.message || '')); } };
  });
}
const slug = `btc-updown-5m-${t5}`;
await rest('Gamma event (current window)', `https://gamma-api.polymarket.com/events?slug=${slug}`);
let tokens = [];
try { const ev = (await (await fetch(`https://gamma-api.polymarket.com/events?slug=${slug}`)).json())[0]; const m = ev.markets[0]; tokens = JSON.parse(m.clobTokenIds);
  const keys = Object.keys(ev).concat(Object.keys(m)); rows.push(['Gamma fields', '', '', `outcomes=${m.outcomes} feesEnabled=${m.feesEnabled} ptbKey=${keys.filter(k => /price|open|start/i.test(k)).join('/')}`]);
  const meta = ev.eventMetadata || m.eventMetadata; if (meta) rows.push(['Gamma eventMetadata', '', '', JSON.stringify(meta).slice(0, 120)]);
} catch (e) { rows.push(['Gamma parse', 'ERR', '', e.message]); }
await rest('Coinbase candles', `https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=60`);
await rest('Binance.US klines', `https://api.binance.us/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=5`);
await ws('Coinbase WS', 'wss://ws-feed.exchange.coinbase.com', [{ type: 'subscribe', product_ids: ['BTC-USD'], channels: ['ticker', 'matches', 'heartbeat'] }]);
await ws('Kraken WS v2', 'wss://ws.kraken.com/v2', ['ticker', 'trade'].map(ch => ({ method: 'subscribe', params: { channel: ch, symbol: ['BTC/USD'] } })).concat([{ method: 'subscribe', params: { channel: 'book', symbol: ['BTC/USD'], depth: 10 } }]));
await ws('Bitstamp WS', 'wss://ws.bitstamp.net', [{ event: 'bts:subscribe', data: { channel: 'live_trades_btcusd' } }, { event: 'bts:subscribe', data: { channel: 'order_book_btcusd' } }]);
await ws('Binance.US WS', 'wss://stream.binance.us:9443/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker/btcusdt@depth10', []);
await ws('Polymarket RTDS Chainlink', 'wss://ws-live-data.polymarket.com', [{ action: 'subscribe', subscriptions: [{ topic: 'crypto_prices_chainlink', type: '*', filters: JSON.stringify({ symbol: 'btc/usd' }) }] }], 12000);
if (tokens.length) await ws('Polymarket CLOB book', 'wss://ws-subscriptions-clob.polymarket.com/ws/market', [{ assets_ids: tokens, type: 'market' }]);
console.log(`## Data source check ${new Date().toISOString()} (window ${slug})\n\n| Source | Status | Detail | Sample |\n|---|---|---|---|`);
for (const r of rows) console.log(`| ${r.join(' | ')} |`);
