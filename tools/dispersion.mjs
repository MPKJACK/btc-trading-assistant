// Measures how far each exchange's mid sits from the Chainlink price and from the USD-exchange median.
const S = { coinbase: NaN, kraken: NaN, bitstamp: NaN, binanceus: NaN, chainlink: NaN };
const open = (url, subs, on) => { const w = new WebSocket(url); w.onopen = () => subs.forEach(m => w.send(JSON.stringify(m))); w.onmessage = (e) => { try { on(JSON.parse(e.data)); } catch (x) {} }; return w; };
open('wss://ws-feed.exchange.coinbase.com', [{ type: 'subscribe', product_ids: ['BTC-USD'], channels: ['ticker'] }], m => { if (m.type === 'ticker') S.coinbase = (+m.best_bid + +m.best_ask) / 2; });
open('wss://ws.kraken.com/v2', [{ method: 'subscribe', params: { channel: 'ticker', symbol: ['BTC/USD'] } }], m => { if (m.channel === 'ticker' && m.data) S.kraken = (+m.data[0].bid + +m.data[0].ask) / 2; });
open('wss://ws.bitstamp.net', [{ event: 'bts:subscribe', data: { channel: 'order_book_btcusd' } }], m => { if (m.event === 'data' && m.data.bids) S.bitstamp = (+m.data.bids[0][0] + +m.data.asks[0][0]) / 2; });
open('wss://stream.binance.us:9443/stream?streams=btcusdt@bookTicker', [], m => { if (m.data) S.binanceus = (+m.data.b + +m.data.a) / 2; });
open('wss://ws-live-data.polymarket.com', [{ action: 'subscribe', subscriptions: [{ topic: 'crypto_prices_chainlink', type: '*', filters: JSON.stringify({ symbol: 'btc/usd' }) }] }], m => { if (m.payload && m.payload.value) S.chainlink = +m.payload.value; });
const dev = { coinbase: [], kraken: [], bitstamp: [], binanceus: [] }, dispAll = [], dispUsd = [];
const med = (a) => { const s = a.slice().sort((x, y) => x - y); return s.length % 2 ? s[(s.length - 1) / 2] : (s[s.length / 2 - 1] + s[s.length / 2]) / 2; };
const q = (a, p) => { const s = a.slice().sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.floor(p * s.length))]; };
let n = 0; const iv = setInterval(() => {
  if (!(S.chainlink > 0)) return; n++;
  for (const k of Object.keys(dev)) if (S[k] > 0) dev[k].push((S[k] / S.chainlink - 1) * 1e4);
  const all = Object.keys(dev).map(k => S[k]).filter(x => x > 0), usd = ['coinbase', 'kraken', 'bitstamp'].map(k => S[k]).filter(x => x > 0);
  if (all.length > 1) dispAll.push((Math.max(...all) - Math.min(...all)) / med(all) * 1e4);
  if (usd.length > 1) dispUsd.push((Math.max(...usd) - Math.min(...usd)) / med(usd) * 1e4);
  if (n >= 120) { clearInterval(iv);
    console.log('## Exchange vs Chainlink (bps), 120 one-second samples\n\n| Feed | median | p90 |\n|---|---|---|');
    for (const k of Object.keys(dev)) console.log(`| ${k} | ${dev[k].length ? med(dev[k]).toFixed(2) : '—'} | ${dev[k].length ? q(dev[k].map(Math.abs), 0.9).toFixed(2) : '—'} |`);
    console.log(`\nSpread across ALL feeds (old gate, limit 10 bps): median ${med(dispAll).toFixed(1)}, p90 ${q(dispAll, 0.9).toFixed(1)}, share over 10 bps ${(dispAll.filter(x => x > 10).length / dispAll.length * 100).toFixed(0)}%`);
    console.log(`Spread across USD feeds only: median ${med(dispUsd).toFixed(1)}, p90 ${q(dispUsd, 0.9).toFixed(1)}, share over 10 bps ${(dispUsd.filter(x => x > 10).length / dispUsd.length * 100).toFixed(0)}%`);
    process.exit(0); }
}, 1000);
