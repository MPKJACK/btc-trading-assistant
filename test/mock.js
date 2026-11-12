// Injected before the page loads: fakes exchange/Polymarket WebSockets and REST so the UI can be exercised offline.
(() => {
  let P = 71600, drift = 0; const sig = 0.00012;
  const g = () => { let u = 0; while (!u) u = Math.random(); return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * Math.random()); };
  setInterval(() => { drift = 0.95 * drift + 0.05 * g() * sig * 0.3; P *= Math.exp(g() * sig * Math.sqrt(0.2) + drift); }, 200);
  window.__mockP = () => P;
  const tStart = () => Math.floor(Date.now() / 300000) * 300;
  class FakeWS {
    constructor(url) { this.url = url; this.readyState = 0; this.timers = []; setTimeout(() => { this.readyState = 1; this.onopen && this.onopen(); }, 50 + Math.random() * 200); }
    emit(o) { if (this.readyState === 1 && this.onmessage) this.onmessage({ data: typeof o === 'string' ? o : JSON.stringify(o) }); }
    every(ms, fn) { this.timers.push(setInterval(fn, ms)); }
    send(raw) {
      if (raw === 'PING') return; let m; try { m = JSON.parse(raw); } catch (e) { return; }
      const u = this.url, iso = () => new Date().toISOString();
      if (u.includes('coinbase')) this.every(250, () => { const side = Math.random() < 0.5 ? 'buy' : 'sell'; this.emit({ type: 'match', price: P.toFixed(2), size: (Math.random() * 0.2).toFixed(5), side, time: iso() }); this.emit({ type: 'ticker', price: P.toFixed(2), best_bid: (P - 0.01).toFixed(2), best_ask: (P + 0.01).toFixed(2), time: iso() }); });
      else if (u.includes('kraken') && m.params && m.params.channel === 'ticker') this.every(400, () => this.emit({ channel: 'ticker', type: 'update', data: [{ symbol: 'BTC/USD', bid: P - 0.1, ask: P + 0.1, last: P + 0.3 }] }));
      else if (u.includes('kraken') && m.params && m.params.channel === 'book') { this.emit({ channel: 'book', type: 'snapshot', data: [{ bids: [...Array(10)].map((_, i) => ({ price: P - 0.1 - i, qty: Math.random() })), asks: [...Array(10)].map((_, i) => ({ price: P + 0.1 + i, qty: Math.random() })) }] }); }
      else if (u.includes('kraken') && m.params && m.params.channel === 'trade') this.every(700, () => this.emit({ channel: 'trade', type: 'update', data: [{ price: P, qty: Math.random() * 0.5, side: Math.random() < .5 ? 'buy' : 'sell', timestamp: iso() }] }));
      else if (u.includes('bitstamp') && m.data && m.data.channel === 'order_book_btcusd') this.every(1000, () => this.emit({ event: 'data', channel: 'order_book_btcusd', data: { bids: [...Array(20)].map((_, i) => [String(P - 1 - i), String(Math.random())]), asks: [...Array(20)].map((_, i) => [String(P + 1 + i), String(Math.random())]) } }));
      else if (u.includes('ws-live-data')) {
        const t0 = tStart() * 1000; const hist = []; for (let t = t0; t <= Date.now(); t += 1000) hist.push({ timestamp: t, value: P + 2 + (Math.random() - .5) });
        this.emit({ topic: 'crypto_prices_chainlink', type: 'subscribe', timestamp: Date.now(), payload: { symbol: 'btc/usd', data: hist } });
        this.every(1000, () => this.emit({ topic: 'crypto_prices_chainlink', type: 'update', timestamp: Date.now(), payload: { symbol: 'btc/usd', timestamp: Math.floor(Date.now() / 1000) * 1000, value: P + 2 } }));
      }
      else if (u.includes('clob') && m.assets_ids) {
        const book = () => { const fair = 0.5 + (Math.random() - 0.5) * 0.5; const up = Math.min(0.97, Math.max(0.03, fair)); [[m.assets_ids[0], up], [m.assets_ids[1], 1 - up]].forEach(([id, px]) => this.emit([{ event_type: 'book', asset_id: id, bids: [{ price: (px - 0.01).toFixed(2), size: '120' }], asks: [{ price: (px + 0.01).toFixed(2), size: '90' }] }])); };
        book(); this.every(3000, book);
      }
    }
    close() { this.timers.forEach(clearInterval); this.readyState = 3; this.onclose && this.onclose({ code: 1000 }); }
  }
  // Binance.US uses URL-encoded streams: start emitting on open
  const _open = FakeWS.prototype.send;
  window.WebSocket = function (url) { const w = new FakeWS(url); if (url.includes('binance')) setTimeout(() => w.every(300, () => { w.emit({ stream: 'btcusdt@aggTrade', data: { p: (P + 1).toFixed(2), q: (Math.random() * 0.3).toFixed(4), m: Math.random() < .5, T: Date.now() } }); w.emit({ stream: 'btcusdt@bookTicker', data: { b: P + 0.9, a: P + 1.1 } }); }), 300); return w; };
  window.WebSocket.OPEN = 1;
  const realFetch = window.fetch;
  window.fetch = async (url) => {
    url = String(url);
    const json = (o) => new Response(JSON.stringify(o), { status: 200, headers: { 'content-type': 'application/json' } });
    if (url.includes('coinbase.com/products/BTC-USD/candles')) {
      const q = new URL(url); const a = Date.parse(q.searchParams.get('start')) / 1000, b = Date.parse(q.searchParams.get('end')) / 1000; const rows = []; let p = 71000 + (a % 1000);
      for (let t = a; t < b; t += 60) { const o = p; let h = p, l = p; for (let i = 0; i < 6; i++) { p *= Math.exp(g() * sig * Math.sqrt(10)); h = Math.max(h, p); l = Math.min(l, p); } rows.push([t, l, h, o, p, Math.random() * 20]); }
      return json(rows.reverse());
    }
    if (url.includes('gamma-api.polymarket.com/events')) { const slug = new URL(url).searchParams.get('slug'); const t = +slug.split('-').pop();
      return json([{ slug, title: 'Bitcoin Up or Down - mock', markets: [{ outcomes: '["Up", "Down"]', clobTokenIds: `["${t}1", "${t}2"]`, outcomePrices: '["0.52", "0.48"]', endDate: new Date((t + 300) * 1000).toISOString(), closed: false, feesEnabled: true }] }]); }
    return realFetch(url);
  };
})();
