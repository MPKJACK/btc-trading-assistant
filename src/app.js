(function () {
'use strict';
const E = window.Engine;
const $ = (s) => document.querySelector(s);
const APP_VERSION = 'app-1.2.0';
const LS = {
  get(k, d) { try { const v = localStorage.getItem('btc5m:' + k); return v ? JSON.parse(v) : d; } catch (e) { return d; } },
  set(k, v) { try { localStorage.setItem('btc5m:' + k, JSON.stringify(v)); } catch (e) { diag('storage write failed: ' + e.message, 'warn'); } },
};
const DEFAULTS = {
  minEdge: 0.04, marketWeight: 0.3, feeRate: 0.07, minFeeds: 2, maxDispersionBps: 25, alwaysCall: true, minSecondsLeft: 15, volSpikeRatio: 3, microTilt: 0,
  autoPaper: true, stake: 10, sound: true, notify: false, alertMinStrength: 0, relayUrl: '',
  alerts: { strong: true, flip: true, invalid: true, vol: true, expiry: true },
  feeds: { coinbase: true, kraken: true, bitstamp: true, binanceus: true, chainlink: true, polymarket: true },
};
const cfg = Object.assign({}, DEFAULTS, LS.get('cfg', {}));
if (!(cfg.cfgVersion >= 2)) { cfg.maxDispersionBps = 25; cfg.alwaysCall = true; cfg.cfgVersion = 2; } // migrate: old 10 bps gate was too tight
cfg.alerts = Object.assign({}, DEFAULTS.alerts, cfg.alerts || {}); cfg.feeds = Object.assign({}, DEFAULTS.feeds, cfg.feeds || {});
const saveCfg = () => LS.set('cfg', cfg);

// ---------- diagnostics ----------
const diagLines = [];
function diag(msg, lvl = 'info') {
  const line = new Date().toISOString().slice(11, 23) + ' ' + (lvl === 'info' ? '' : lvl.toUpperCase() + ' ') + msg;
  diagLines.push(line); if (diagLines.length > 300) diagLines.shift();
  if (lvl !== 'info') console.warn(line);
}
window.addEventListener('error', (e) => diag('JS error: ' + e.message, 'error'));

// ---------- clock (server-time correction from exchange timestamps) ----------
const skewSamples = [];
let clockOffset = 0;
function noteServerTime(serverMs) {
  if (!(serverMs > 1e12)) return;
  skewSamples.push(serverMs - Date.now()); if (skewSamples.length > 60) skewSamples.shift();
  if (skewSamples.length >= 10) { const s = skewSamples.slice().sort((a, b) => a - b); const med = s[Math.floor(s.length / 2)]; clockOffset = Math.abs(med) > 400 ? med : 0; }
}
const now = () => Date.now() + clockOffset;

// ---------- exchange state ----------
const X = {};
const FEED_DEFS = [
  { id: 'coinbase', label: 'CB', name: 'Coinbase', url: 'wss://ws-feed.exchange.coinbase.com', usd: true },
  { id: 'kraken', label: 'KR', name: 'Kraken', url: 'wss://ws.kraken.com/v2', usd: true },
  { id: 'bitstamp', label: 'BS', name: 'Bitstamp', url: 'wss://ws.bitstamp.net', usd: true },
  { id: 'binanceus', label: 'BN', name: 'Binance.US (USDT)', url: 'wss://stream.binance.us:9443/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker/btcusdt@depth10', usd: false },
  { id: 'chainlink', label: 'CL', name: 'Chainlink (Polymarket RTDS)', url: 'wss://ws-live-data.polymarket.com', usd: true, ref: true },
  { id: 'polymarket', label: 'PM', name: 'Polymarket order book', url: 'wss://ws-subscriptions-clob.polymarket.com/ws/market', pm: true },
];
const FRESH_MS = 4000;
const trades = []; // {t, q, buy, src}
const compTicks = []; // composite mid {t,p}
const clTicks = []; // chainlink {t(payload ms), p, recv}
let basis = null; // EWMA chainlink - composite

class Feed {
  constructor(def, handlers) {
    Object.assign(this, def); this.h = handlers; this.ws = null; this.status = 'off'; this.msgs = 0; this.lastMsg = 0; this.priceMs = 0;
    this.price = NaN; this.bid = NaN; this.ask = NaN; this.reconnects = 0; this.backoff = 1000; this.err = ''; this.book = { bids: new Map(), asks: new Map() };
    this.pingTimer = null; this.retryTimer = null;
  }
  get mid() { return (this.bid > 0 && this.ask > 0 && this.ask >= this.bid) ? (this.bid + this.ask) / 2 : this.price; }
  get fresh() { return this.priceMs > 0 && now() - this.priceMs < FRESH_MS; }
  connect() {
    if (!cfg.feeds[this.id]) { this.status = 'off'; return; }
    if (this.h.canConnect && !this.h.canConnect()) return;
    clearTimeout(this.retryTimer);
    try { this.ws = new WebSocket(this.url); } catch (e) { this.err = e.message; this.scheduleRetry(); return; }
    this.status = 'connecting'; const ws = this.ws;
    ws.onopen = () => { if (ws !== this.ws) return; this.status = 'live'; this.backoff = 1000; diag(this.name + ' connected'); try { this.h.open.call(this); } catch (e) { diag(this.name + ' subscribe error ' + e.message, 'error'); }
      if (this.h.ping) { clearInterval(this.pingTimer); this.pingTimer = setInterval(() => { try { ws.readyState === 1 && ws.send(this.h.ping); } catch (e) {} }, this.h.pingMs || 10000); } };
    ws.onmessage = (ev) => { if (ws !== this.ws) return; this.msgs++; this.lastMsg = Date.now(); try { this.h.msg.call(this, ev.data); } catch (e) { this.err = 'parse: ' + e.message; if (this.msgs % 200 === 1) diag(this.name + ' parse error ' + e.message, 'warn'); } };
    ws.onerror = () => { if (ws !== this.ws) return; this.err = 'socket error'; };
    ws.onclose = (ev) => { if (ws !== this.ws) return; clearInterval(this.pingTimer); this.ws = null; if (this.status === 'off') return;
      this.status = 'down'; diag(`${this.name} closed (code ${ev.code}); retry in ${Math.round(this.backoff / 1000)}s`, 'warn'); this.scheduleRetry(); };
  }
  scheduleRetry() { this.reconnects++; clearTimeout(this.retryTimer); this.retryTimer = setTimeout(() => this.connect(), this.backoff); this.backoff = Math.min(this.backoff * 2, 30000); }
  send(o) { if (this.ws && this.ws.readyState === 1) this.ws.send(typeof o === 'string' ? o : JSON.stringify(o)); }
  close() { this.status = 'off'; clearInterval(this.pingTimer); clearTimeout(this.retryTimer); if (this.ws) { const w = this.ws; this.ws = null; try { w.close(); } catch (e) {} } }
  restart() { this.close(); this.backoff = 1000; this.status = 'connecting'; this.connect(); }
  watchdog() { // heartbeat check: if a live socket goes silent, force a reconnect
    if (this.ws && this.ws.readyState === 1 && this.lastMsg && Date.now() - this.lastMsg > 20000) { diag(this.name + ' silent 20s, reconnecting', 'warn'); this.restart(); }
  }
  setBook(side, levels, replace) { const m = this.book[side]; if (replace) m.clear(); for (const [p, q] of levels) { if (q > 0) m.set(p, q); else m.delete(p); } }
  topImbalance(n = 10) {
    const b = [...this.book.bids.entries()].sort((a, c) => c[0] - a[0]).slice(0, n), a = [...this.book.asks.entries()].sort((x, y) => x[0] - y[0]).slice(0, n);
    if (!b.length || !a.length) return NaN; const bq = b.reduce((s, x) => s + x[1], 0), aq = a.reduce((s, x) => s + x[1], 0);
    // trim maps so they never grow unbounded
    if (this.book.bids.size > 200) this.book.bids = new Map(b.concat([...this.book.bids.entries()].sort((x, y) => y[0] - x[0]).slice(n, 100)));
    if (this.book.asks.size > 200) this.book.asks = new Map(a.concat([...this.book.asks.entries()].sort((x, y) => x[0] - y[0]).slice(n, 100)));
    return (bq - aq) / (bq + aq);
  }
}
function onTrade(src, p, q, buy, tMs) { trades.push({ t: tMs || now(), q, buy, src, p }); if (src === 'coinbase') onCandleTrade(p, q, tMs || now()); }
function setPx(f, p) { if (p > 0) { f.price = p; f.priceMs = now(); } }

const handlers = {
  coinbase: {
    open() { this.send({ type: 'subscribe', product_ids: ['BTC-USD'], channels: ['ticker', 'matches', 'heartbeat'] }); },
    msg(d) { const m = JSON.parse(d);
      if (m.type === 'ticker') { this.bid = +m.best_bid; this.ask = +m.best_ask; setPx(this, +m.price); if (m.time) noteServerTime(Date.parse(m.time)); }
      else if (m.type === 'match') { const p = +m.price; setPx(this, p); onTrade('coinbase', p, +m.size, m.side === 'sell', Date.parse(m.time)); } // maker side 'sell' => taker buy
      else if (m.type === 'error') { this.err = m.message; diag('Coinbase: ' + m.message + ' ' + (m.reason || ''), 'warn'); } },
  },
  kraken: {
    open() { ['ticker', 'trade'].forEach(ch => this.send({ method: 'subscribe', params: { channel: ch, symbol: ['BTC/USD'] } })); this.send({ method: 'subscribe', params: { channel: 'book', symbol: ['BTC/USD'], depth: 10 } }); },
    ping: JSON.stringify({ method: 'ping' }), pingMs: 15000,
    msg(d) { const m = JSON.parse(d); if (!m.channel || !m.data) return;
      if (m.channel === 'ticker') { const t = m.data[0]; this.bid = +t.bid; this.ask = +t.ask; setPx(this, +t.last); }
      else if (m.channel === 'trade') { for (const t of m.data) { setPx(this, +t.price); onTrade('kraken', +t.price, +t.qty, t.side === 'buy', Date.parse(t.timestamp)); } }
      else if (m.channel === 'book') { const b = m.data[0]; const rep = m.type === 'snapshot';
        this.setBook('bids', (b.bids || []).map(x => [+x.price, +x.qty]), rep); this.setBook('asks', (b.asks || []).map(x => [+x.price, +x.qty]), rep); this.priceMs = this.price > 0 ? now() : this.priceMs; } },
  },
  bitstamp: {
    open() { this.send({ event: 'bts:subscribe', data: { channel: 'live_trades_btcusd' } }); this.send({ event: 'bts:subscribe', data: { channel: 'order_book_btcusd' } }); },
    ping: JSON.stringify({ event: 'bts:heartbeat' }), pingMs: 15000,
    msg(d) { const m = JSON.parse(d); if (!m.data) return;
      if (m.event === 'trade') { const t = m.data; setPx(this, +t.price); onTrade('bitstamp', +t.price, +t.amount, t.type === 0, t.microtimestamp ? +t.microtimestamp / 1000 : now()); }
      else if (m.event === 'data' && m.channel === 'order_book_btcusd') { const b = m.data; this.setBook('bids', b.bids.slice(0, 20).map(x => [+x[0], +x[1]]), true); this.setBook('asks', b.asks.slice(0, 20).map(x => [+x[0], +x[1]]), true);
        this.bid = +b.bids[0][0]; this.ask = +b.asks[0][0]; if (!(this.price > 0)) this.price = (this.bid + this.ask) / 2; this.priceMs = now(); }
      else if (m.event === 'bts:request_reconnect') { this.restart(); } },
  },
  binanceus: {
    open() {},
    msg(d) { const w = JSON.parse(d), m = w.data || w, s = w.stream || '';
      if (s.endsWith('@aggTrade') || m.e === 'aggTrade') { setPx(this, +m.p); onTrade('binanceus', +m.p, +m.q, !m.m, m.T); }
      else if (s.endsWith('@bookTicker')) { this.bid = +m.b; this.ask = +m.a; if (!(this.price > 0)) this.price = (this.bid + this.ask) / 2; this.priceMs = now(); }
      else if (s.indexOf('@depth') > 0) { this.setBook('bids', m.bids.map(x => [+x[0], +x[1]]), true); this.setBook('asks', m.asks.map(x => [+x[0], +x[1]]), true); } },
  },
  chainlink: {
    open() { this.send({ action: 'subscribe', subscriptions: [{ topic: 'crypto_prices_chainlink', type: '*', filters: JSON.stringify({ symbol: 'btc/usd' }) }] }); },
    ping: 'PING', pingMs: 5000,
    msg(d) { if (typeof d !== 'string' || d[0] !== '{') return; const m = JSON.parse(d); if (m.topic !== 'crypto_prices_chainlink' || !m.payload) return;
      const pl = m.payload; const list = Array.isArray(pl.data) ? pl.data.map(x => ({ timestamp: x.timestamp, value: x.value, symbol: pl.symbol })) : [pl];
      for (const x of list) { if (x.symbol && String(x.symbol).toLowerCase().replace(/[^a-z]/g, '') !== 'btcusd') continue; onChainlink(+x.timestamp, +x.value); } },
  },
  polymarket: {
    canConnect() { return !!(MK.up.id && MK.down.id); },
    open() { this.send({ assets_ids: [MK.up.id, MK.down.id], type: 'market' }); },
    ping: 'PING', pingMs: 10000,
    msg(d) { if (typeof d !== 'string' || (d[0] !== '{' && d[0] !== '[')) return; let m = JSON.parse(d); (Array.isArray(m) ? m : [m]).forEach(onPmMsg); },
  },
};
FEED_DEFS.forEach(def => { X[def.id] = new Feed(def, handlers[def.id]); });

function onChainlink(tMs, p) {
  if (!(p > 1000) || !(tMs > 1e12)) return;
  const f = X.chainlink; f.price = p; f.priceMs = now(); f.payloadMs = tMs;
  const last = clTicks[clTicks.length - 1]; if (!last || tMs > last.t) clTicks.push({ t: tMs, p, recv: Date.now() });
  else if (tMs < last.t) { clTicks.push({ t: tMs, p, recv: Date.now() }); clTicks.sort((a, b) => a.t - b.t); }
  while (clTicks.length && clTicks[0].t < tMs - 20 * 60e3) clTicks.shift();
  const comp = compositeMid(); if (comp.p > 0) { const b = p - comp.p; basis = basis === null ? b : 0.9 * basis + 0.1 * b; }
  tryCaptureTarget();
}

// ---------- composite price ----------
// Composite = median of fresh USD exchanges. Binance.US trades against USDT (not dollars) and is thin, so it is
// shown for reference only. A single exchange far from the others is flagged as an outlier and dropped, so one
// bad quote can no longer block every call.
const median = (a) => { const s = a.slice().sort((x, y) => x - y); return s.length % 2 ? s[(s.length - 1) / 2] : (s[s.length / 2 - 1] + s[s.length / 2]) / 2; };
function compositeMid() {
  let vals = []; for (const id of ['coinbase', 'kraken', 'bitstamp']) { const f = X[id]; if (f.fresh && f.mid > 0) vals.push({ id, p: f.mid }); }
  if (!vals.length) { const b = X.binanceus; if (b.fresh && b.mid > 0) vals.push({ id: 'binanceus', p: b.mid }); }
  if (!vals.length) return { p: NaN, n: 0, dispBps: 0, vals, outliers: [] };
  const outliers = [];
  if (vals.length >= 3) { const m = median(vals.map(v => v.p)); vals = vals.filter(v => { const off = Math.abs(v.p / m - 1) * 1e4; if (off > 15) { outliers.push(v.id); return false; } return true; }); }
  const ps = vals.map(v => v.p), med = median(ps);
  return { p: med, n: vals.length, dispBps: ps.length > 1 ? (Math.max(...ps) - Math.min(...ps)) / med * 1e4 : 0, vals, outliers };
}
function priceAgo(series, sec) { const t = now() - sec * 1000; for (let i = series.length - 1; i >= 0; i--) if (series[i].t <= t) return series[i].p; return NaN; }

// ---------- 1-minute candles (Coinbase; same source as the backtest) ----------
let candles = []; let cur = null;
function onCandleTrade(p, q, tMs) {
  const m = Math.floor(tMs / 60000) * 60;
  if (!cur || m > cur.t) { if (cur) pushCandle(cur); cur = { t: m, o: p, h: p, l: p, c: p, v: 0 }; }
  if (m < cur.t) return; cur.h = Math.max(cur.h, p); cur.l = Math.min(cur.l, p); cur.c = p; cur.v += q;
}
function pushCandle(c) { const last = candles[candles.length - 1]; if (last && last.t === c.t) candles[candles.length - 1] = c; else if (!last || c.t > last.t) candles.push(c); if (candles.length > 600) candles.splice(0, candles.length - 600); }
function rollCandleIfIdle() { // keep candles moving if Coinbase is quiet/down, using composite
  const m = Math.floor(now() / 60000) * 60, comp = compositeMid().p;
  if (cur && m > cur.t) { pushCandle(cur); cur = comp > 0 ? { t: m, o: comp, h: comp, l: comp, c: comp, v: 0 } : null; }
  else if (!cur && comp > 0) cur = { t: m, o: comp, h: comp, l: comp, c: comp, v: 0 };
  else if (cur && !X.coinbase.fresh && comp > 0) { cur.h = Math.max(cur.h, comp); cur.l = Math.min(cur.l, comp); cur.c = comp; }
}

async function fetchCandles(startSec, endSec, onProgress) {
  // Coinbase: max 300 candles per request. Fallback: Binance.US klines (USDT).
  const out = new Map(); const step = 300 * 60; let ok = true; let n = 0; const total = Math.ceil((endSec - startSec) / step);
  for (let a = startSec; a < endSec; a += step) {
    const b = Math.min(endSec, a + step);
    const url = `https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=60&start=${new Date(a * 1000).toISOString()}&end=${new Date(b * 1000).toISOString()}`;
    let rows = null;
    for (let attempt = 0; attempt < 3 && !rows; attempt++) {
      try { const r = await fetch(url); if (r.status === 429) { await sleep(1000 * (attempt + 1)); continue; } if (!r.ok) throw new Error('HTTP ' + r.status); rows = await r.json(); }
      catch (e) { if (attempt === 2) { ok = false; diag('Coinbase candles failed: ' + e.message, 'warn'); } else await sleep(400); }
    }
    if (!rows) break;
    for (const x of rows) out.set(x[0], { t: x[0], l: +x[1], h: +x[2], o: +x[3], c: +x[4], v: +x[5] });
    n++; onProgress && onProgress(n / total); await sleep(130);
  }
  if (!ok && out.size === 0) {
    diag('Falling back to Binance.US klines', 'warn'); let t = startSec * 1000;
    while (t < endSec * 1000) {
      try { const r = await fetch(`https://api.binance.us/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1000&startTime=${t}`); if (!r.ok) throw new Error('HTTP ' + r.status);
        const rows = await r.json(); if (!rows.length) break;
        for (const k of rows) out.set(k[0] / 1000, { t: k[0] / 1000, o: +k[1], h: +k[2], l: +k[3], c: +k[4], v: +k[5] }); t = rows[rows.length - 1][0] + 60000; onProgress && onProgress((t / 1000 - startSec) / (endSec - startSec)); await sleep(150);
      } catch (e) { diag('Binance.US klines failed: ' + e.message, 'error'); break; }
    }
  }
  return [...out.values()].sort((a, b) => a.t - b.t);
}
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
async function bootstrapCandles() {
  const end = Math.floor(now() / 60000) * 60;
  const c = await fetchCandles(end - 300 * 60, end);
  if (c.length) { const live = candles; candles = c.filter(x => x.t < end); live.forEach(pushCandle); diag(`Loaded ${candles.length} 1-min candles`); }
  else diag('Could not load candle history; indicators will warm up from live trades (~60 min)', 'warn');
}

// ---------- Polymarket market (Gamma + CLOB) ----------
const MK = { tStart: 0, tEnd: 0, slug: '', title: '', gamma: 'idle', gammaMs: 0, feesEnabled: null, manual: null,
  up: { id: '', bids: new Map(), asks: new Map(), bid: NaN, ask: NaN, ms: 0 }, down: { id: '', bids: new Map(), asks: new Map(), bid: NaN, ask: NaN, ms: 0 }, gammaPx: null };
const TG = { value: NaN, src: '', verified: false, win: 0 };
function parseMaybeJSON(v) { if (typeof v === 'string') { try { return JSON.parse(v); } catch (e) { return v; } } return v; }
function findKey(obj, re, depth = 0) { if (!obj || typeof obj !== 'object' || depth > 4) return undefined; for (const k of Object.keys(obj)) { if (re.test(k) && obj[k] !== null && obj[k] !== '') return obj[k]; const r = findKey(obj[k], re, depth + 1); if (r !== undefined) return r; } return undefined; }

async function fetchGammaEvent(slug) {
  const r = await fetch('https://gamma-api.polymarket.com/events?slug=' + encodeURIComponent(slug));
  if (!r.ok) throw new Error('HTTP ' + r.status); const a = await r.json(); return Array.isArray(a) ? a[0] : a;
}
async function loadMarket(tStart) {
  const slug = `btc-updown-5m-${tStart}`;
  Object.assign(MK, { tStart, tEnd: tStart + 300, slug, title: titleFor(tStart), gamma: 'loading', manual: null, gammaPx: null });
  ['up', 'down'].forEach(k => Object.assign(MK[k], { id: '', bids: new Map(), asks: new Map(), bid: NaN, ask: NaN, ms: 0 }));
  X.polymarket.close();
  if (!cfg.feeds.polymarket) { MK.gamma = 'off'; return; }
  for (let attempt = 0; attempt < 4; attempt++) {
    try {
      const ev = await fetchGammaEvent(slug); if (MK.slug !== slug) return;
      if (!ev || !ev.markets || !ev.markets.length) throw new Error('market not found yet');
      const m = ev.markets[0]; const outs = parseMaybeJSON(m.outcomes) || ['Up', 'Down'], ids = parseMaybeJSON(m.clobTokenIds) || [], px = parseMaybeJSON(m.outcomePrices) || [];
      const iUp = outs.findIndex(o => /up/i.test(o)), iDn = outs.findIndex(o => /down/i.test(o));
      if (iUp < 0 || iDn < 0 || !ids[iUp]) throw new Error('unexpected outcomes ' + JSON.stringify(outs));
      MK.up.id = ids[iUp]; MK.down.id = ids[iDn]; MK.title = ev.title || MK.title; MK.feesEnabled = m.feesEnabled ?? null;
      MK.gammaPx = { up: +px[iUp], down: +px[iDn], bestBid: +m.bestBid, bestAsk: +m.bestAsk, ms: now() };
      if (m.endDate) { const e = Math.floor(Date.parse(m.endDate) / 1000); if (e && Math.abs(e - MK.tEnd) > 2) diag(`Gamma endDate ${m.endDate} differs from slug window`, 'warn'); }
      const ptb = +(findKey(ev, /^(priceToBeat|price_to_beat|openPrice|startPrice)$/i));
      if (ptb > 1000 && TG.win === tStart && !(TG.src === 'manual')) setTarget(ptb, 'Polymarket API', true, tStart);
      MK.gamma = 'ok'; MK.gammaMs = now(); diag(`Market ${slug} loaded`); X.polymarket.restart(); return;
    } catch (e) { MK.gamma = 'error: ' + e.message; diag(`Gamma ${slug}: ${e.message}`, 'warn'); await sleep(2500 * (attempt + 1)); if (MK.slug !== slug) return; }
  }
}
function titleFor(t) { const f = (s) => new Date(s * 1000).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' }); return `Bitcoin Up or Down · ${f(t)}–${f(t + 300)}`; }
function onPmMsg(m) {
  const side = (id) => id === MK.up.id ? MK.up : id === MK.down.id ? MK.down : null;
  const best = (o) => { const b = [...o.bids.keys()], a = [...o.asks.keys()]; o.bid = b.length ? Math.max(...b) : NaN; o.ask = a.length ? Math.min(...a) : NaN; o.ms = now(); };
  if (m.event_type === 'book') { const o = side(m.asset_id); if (!o) return; o.bids = new Map((m.bids || []).map(x => [+x.price, +x.size])); o.asks = new Map((m.asks || []).map(x => [+x.price, +x.size])); best(o); X.polymarket.priceMs = now(); }
  else if (m.event_type === 'price_change') {
    const list = m.price_changes || (m.changes || []).map(c => Object.assign({ asset_id: m.asset_id }, c));
    for (const c of list) { const o = side(c.asset_id); if (!o) continue; const book = c.side === 'BUY' ? o.bids : o.asks; const p = +c.price, q = +c.size; if (q > 0) book.set(p, q); else book.delete(p); best(o);
      if (c.best_bid !== undefined && +c.best_bid > 0) o.bid = +c.best_bid; if (c.best_ask !== undefined && +c.best_ask > 0) o.ask = +c.best_ask; }
    X.polymarket.priceMs = now();
  } else if (m.event_type === 'best_bid_ask') { const o = side(m.asset_id); if (!o) return; o.bid = +m.best_bid; o.ask = +m.best_ask; o.ms = now(); X.polymarket.priceMs = now(); }
}
function marketQuotes() {
  if (MK.manual) return { upAsk: MK.manual.up, downAsk: MK.manual.down, upMid: MK.manual.up, src: 'manual' };
  const u = MK.up, d = MK.down, fresh = (o) => o.ms && now() - o.ms < 15000;
  if (fresh(u) && fresh(d) && u.ask > 0 && d.ask > 0 && u.ask < 1 && d.ask < 1) {
    const upMid = (u.bid > 0 ? (u.bid + u.ask) / 2 : u.ask);
    return { upAsk: u.ask, downAsk: d.ask, upMid, upBid: u.bid, downBid: d.bid, src: 'live book', thin: (u.ask - (u.bid || 0)) > 0.06 };
  }
  return { upAsk: NaN, downAsk: NaN, upMid: NaN, src: MK.gammaPx ? 'no live book' : 'unavailable' };
}

// ---------- target (price to beat) ----------
function setTarget(v, src, verified, win) { TG.value = v; TG.src = src; TG.verified = verified; TG.win = win; diag(`Target ${v.toFixed(2)} from ${src}${verified ? '' : ' (unverified)'}`); }
// Polymarket's price to beat is the Chainlink BTC/USD report stamped exactly at the window-open second.
// The app takes that exact report automatically. If it wasn't running then (opened mid-window), it asks the
// optional relay for Polymarket's official value; otherwise it waits for the next window. It never estimates.
let relayTry = 0;
function tryCaptureTarget() {
  const w = MK.tStart; if (!w || (TG.win === w && TG.verified)) return;
  const exact = clTicks.find(x => x.t === w * 1000);
  if (exact) { setTarget(exact.p, 'Chainlink at window open', true, w); return; }
  if (cfg.relayUrl && Date.now() - relayTry > 8000) { relayTry = Date.now(); fetchRelayTarget(w); }
}
async function fetchRelayTarget(w) {
  try { const r = await fetch(relayCryptoUrl(w)); if (!r.ok) throw new Error('HTTP ' + r.status); const j = await r.json();
    if (MK.tStart === w && j.openPrice > 1000 && !(TG.win === w && TG.verified)) setTarget(+j.openPrice, 'Polymarket official (relay)', true, w);
  } catch (e) { diag('Relay target failed: ' + e.message, 'warn'); }
}
function relayCryptoUrl(w) { const iso = (t) => new Date(t * 1000).toISOString(); return cfg.relayUrl.replace(/\/$/, '') + `/crypto-price?symbol=BTC&eventStartTime=${encodeURIComponent(iso(w))}&variant=fiveminute&endDate=${encodeURIComponent(iso(w + 300))}`; }

// ---------- live analysis loop ----------
let lastDecision = null, shown = { rec: 'NO TRADE', side: null }, lastSpikeAlert = 0;
const st = {};
function analyze() {
  const tNow = now(), nowSec = tNow / 1000;
  const ws = Math.floor(nowSec / 300) * 300;
  if (ws !== MK.tStart) onNewWindow(ws);
  rollCandleIfIdle(); tryCaptureTarget();
  const comp = compositeMid();
  if (comp.p > 0) { compTicks.push({ t: tNow, p: comp.p }); while (compTicks.length && compTicks[0].t < tNow - 15 * 60e3) compTicks.shift(); }
  while (trades.length && trades[0].t < tNow - 5 * 60e3) trades.shift();
  const cl = X.chainlink, clFresh = cl.fresh;
  let S = NaN, sSrc = '';
  if (clFresh) { S = cl.price; sSrc = 'Chainlink'; } else if (comp.p > 0) { S = comp.p + (basis || 0); sSrc = basis !== null ? 'Exchanges+basis' : 'Exchanges'; }
  const tRem = MK.tEnd - nowSec;
  const done = candles.slice(-120);
  const sigma = E.sigmaPerSec(done, 60);
  const closes = done.map(c => c.c).concat(cur ? [cur.c] : []);
  const cAll = done.concat(cur ? [cur] : []);
  const S60 = priceAgo(compTicks, 60), S300 = priceAgo(compTicks, 300), S15 = priceAgo(compTicks, 15);
  const compNow = comp.p;
  let f = null;
  if (S > 0 && TG.value > 0 && sigma > 0) {
    f = E.features({ S, K: TG.value, tRem: Math.max(tRem, 1), sigma, closes1m: closes, candles1m: cAll, S60: S60 > 0 ? S60 * (S / compNow) : 0, S300: S300 > 0 ? S300 * (S / compNow) : 0 });
  }
  // short-term vol spike: 30s tick vol relative to its own 10-min level
  const r30 = E.tickVolPerSec(E.resample1s(compTicks, tNow, 30)), r600 = E.tickVolPerSec(E.resample1s(compTicks, tNow, 600));
  const tickVolRatio = (r30 > 0 && r600 > 0) ? r30 / r600 : 0;
  const m15 = (S15 > 0 && sigma > 0) ? Math.log(compNow / S15) / (sigma * Math.sqrt(15)) : 0, m60 = (S60 > 0 && sigma > 0) ? Math.log(compNow / S60) / (sigma * Math.sqrt(60)) : 0;
  const conflict = Math.abs(m15) > 1 && Math.abs(m60) > 1 && Math.sign(m15) !== Math.sign(m60);
  const flow = flowStats(60), obi = bookImb();
  const microScore = (isFinite(flow.imb) ? flow.imb : 0) * 0.5 + (isFinite(obi) ? obi : 0) * 0.5;
  const freshCount = comp.n + (clFresh ? 1 : 0);
  // what matters for settlement is Chainlink; check exchanges against it (not against each other's noise)
  const clVsEx = (clFresh && comp.p > 0) ? Math.abs(cl.price - (comp.p + (basis || 0))) / comp.p * 1e4 : 0;
  const agreeBps = Math.max(comp.dispBps, clVsEx);
  const market = marketQuotes();
  Object.assign(st, { target: TG.value, targetVerified: TG.verified && TG.win === MK.tStart, S, sSrc, comp: compNow, tRem, sigma, f, model: activeModel, market, freshCount, dispersionBps: agreeBps, outliers: comp.outliers, tickVolRatio, conflict, microScore, m15, m60, flow, obi, closes, cAll });
  const d = E.decide(st, cfg);
  // hysteresis: keep an active call unless the edge falls below half the entry threshold or a hard gate fires
  if (shown.side && d.rec === 'NO TRADE' && d.side === shown.side && d.best && d.best.evPerDollar >= cfg.minEdge / 2 && d.gates.every(g => /edge/i.test(g))) { d.rec = shown.side; d.held = true; }
  // Always-on call: when the only blockers are "soft" (price not attractive, feeds disagree, choppy, conflict),
  // still give the direction the analysis favors. Hard blockers (no target, no live price, no volatility) still WAIT.
  d.call = null;
  if (isFinite(d.pUp) && st.targetVerified && st.S > 0 && st.sigma > 0 && st.tRem > 0) {
    d.call = d.pUp >= 0.5 ? 'UP' : 'DOWN'; d.pCall = Math.max(d.pUp, d.pDown);
    d.callMaxPay = E.maxPrice(d.pCall, cfg.minEdge, cfg.feeRate);
    d.cautions = d.rec === 'NO TRADE' ? d.gates.filter(g => !/Best edge/.test(g)) : [];
  }
  d.chart = st.f ? E.chartRead(st.f) : null;
  lastDecision = d; d.text = E.explain(d, st);
  handleSignalEvents(d);
  logSnapshots(d);
  if (tickVolRatio > cfg.volSpikeRatio && cfg.alerts.vol && tNow - lastSpikeAlert > 60000) { lastSpikeAlert = tNow; alertUser(`Volatility spike: ${tickVolRatio.toFixed(1)}× normal`, 'warn'); }
}
function flowStats(sec) { const t = now() - sec * 1000; let b = 0, s = 0; for (const x of trades) if (x.t >= t) { if (x.buy) b += x.q; else s += x.q; } return { buy: b, sell: s, imb: b + s > 0 ? (b - s) / (b + s) : NaN }; }
function bookImb() { const v = ['kraken', 'bitstamp', 'binanceus'].map(id => X[id].fresh || X[id].book.bids.size ? X[id].topImbalance(10) : NaN).filter(isFinite); return v.length ? E.mean(v) : NaN; }

// ---------- window lifecycle, prediction log, paper trades ----------
let preds = LS.get('preds', []), papers = LS.get('papers', []);
const MARKS = [240, 180, 120, 90, 60, 30];
let markDone = new Set();
function onNewWindow(ws) {
  const prev = MK.tStart;
  diag(`New 5-min window ${ws}`);
  markDone = new Set(); shown = { rec: 'NO TRADE', side: null };
  Object.assign(TG, { value: NaN, src: '', verified: false, win: ws });
  loadMarket(ws);
  if (prev) { setTimeout(() => resolveWindow(prev), 4000); setTimeout(() => confirmOfficial(prev), 90000); setTimeout(() => confirmOfficial(prev), 360000); }
}
function windowRecords(win) { return { p: preds.filter(x => x.win === win), t: papers.filter(x => x.win === win) }; }
function resolveWindow(win) {
  const tEnd = (win + 300) * 1000; const endTick = clTicks.find(x => x.t >= tEnd && x.t <= tEnd + 2000);
  let endP = endTick ? endTick.p : NaN, src = endTick ? 'Chainlink' : '';
  if (!(endP > 0)) { const c = candles.find(x => x.t === win + 240); if (c) { endP = c.c; src = 'Coinbase (proxy)'; } }
  const r = windowRecords(win); if (!r.p.length && !r.t.length) return;
  for (const x of r.p.concat(r.t)) { if (x.outcomeSrc === 'official') continue; if (endP > 0 && x.target > 0) { x.endPrice = endP; x.outcome = endP >= x.target ? 'UP' : 'DOWN'; x.outcomeSrc = src; } }
  settlePapers(); persist(); diag(`Window ${win} resolved ${endP > 0 ? (src + ' ' + endP.toFixed(2)) : 'pending'}`);
}
async function confirmOfficial(win) {
  if (cfg.relayUrl) { try { const j = await (await fetch(relayCryptoUrl(win))).json(); if (j.completed && j.closePrice > 0 && j.openPrice > 0) { const out = j.closePrice >= j.openPrice ? 'UP' : 'DOWN';
    const r = windowRecords(win); for (const x of r.p.concat(r.t)) { x.outcome = out; x.outcomeSrc = 'official'; x.endPrice = j.closePrice; } settlePapers(); persist(); return; } } catch (e) { diag('Relay result failed: ' + e.message, 'warn'); } }
  try { const ev = await fetchGammaEvent(`btc-updown-5m-${win}`); const m = ev && ev.markets && ev.markets[0]; if (!m) return;
    const outs = parseMaybeJSON(m.outcomes), px = parseMaybeJSON(m.outcomePrices); if (!m.closed || !Array.isArray(px)) return;
    const iUp = outs.findIndex(o => /up/i.test(o)); const upWon = +px[iUp] > 0.99, dnWon = +px[iUp] < 0.01; if (!upWon && !dnWon) return;
    const out = upWon ? 'UP' : 'DOWN'; const r = windowRecords(win); let changed = 0;
    for (const x of r.p.concat(r.t)) { if (x.outcome && x.outcome !== out) changed++; x.outcome = out; x.outcomeSrc = 'official'; }
    if (changed) diag(`Official result for ${win} differs from our capture on ${changed} record(s)`, 'warn');
    settlePapers(); persist();
  } catch (e) { diag('Official result check failed: ' + e.message, 'warn'); }
}
function settlePapers() { for (const t of papers) if (t.outcome && t.pnl == null) { t.pnl = t.outcome === t.side ? t.shares * 1 - t.stake : -t.stake; } }
function persist() { if (preds.length > 20000) preds = preds.slice(-20000); if (papers.length > 5000) papers = papers.slice(-5000); LS.set('preds', preds); LS.set('papers', papers); }
function snapshot(d, kind) {
  return { id: Date.now() + Math.random().toString(36).slice(2, 6), ts: now(), win: MK.tStart, slug: MK.slug, kind, tRem: Math.round(st.tRem), target: st.target, targetSrc: TG.src, S: st.S, sSrc: st.sSrc,
    pUp: d.pUp, pModel: d.pModel, pMkt: d.pMkt, upAsk: st.market.upAsk, downAsk: st.market.downAsk, rec: d.rec, side: d.side, edge: d.best ? d.best.evPerDollar : null, conf: d.conf, strength: d.strength,
    gates: d.gates.slice(0, 3), model: activeModel.version || 'vol', app: APP_VERSION, engine: E.VERSION, feats: st.f, sigma: st.sigma, disp: st.dispersionBps, outcome: null };
}
function logSnapshots(d) {
  if (!(st.tRem > 0) || !isFinite(d.pUp) || !(st.target > 0)) return;
  for (const m of MARKS) if (st.tRem <= m && st.tRem > m - 5 && !markDone.has(m)) { markDone.add(m); preds.push(snapshot(d, 'mark' + m)); persist(); }
}
function paperBuy(side, auto) {
  const d = lastDecision, q = st.market; const ask = side === 'UP' ? q.upAsk : q.downAsk;
  if (!(ask > 0 && ask < 1)) { toast('No price for ' + side + ' yet', 'warn'); return; }
  if (!(st.target > 0)) { toast('Target unknown', 'warn'); return; }
  const fee = E.takerFee(ask, cfg.feeRate), cost = ask + fee, shares = cfg.stake / cost;
  const t = { id: 'p' + Date.now(), ts: now(), win: MK.tStart, slug: MK.slug, side, ask, fee, cost, stake: cfg.stake, shares, auto: !!auto, tRem: Math.round(st.tRem), target: st.target, targetVerified: st.targetVerified,
    S: st.S, pSide: d ? (side === 'UP' ? d.pUp : d.pDown) : null, rec: d ? d.rec : null, model: activeModel.version || 'vol', outcome: null, pnl: null };
  papers.push(t); persist(); toast(`Paper ${side}: ${shares.toFixed(1)} sh @ ${(ask * 100).toFixed(1)}¢ (+${(fee * 100).toFixed(2)}¢ fee)`, side === 'UP' ? 'up' : 'dn');
}

// ---------- alerts ----------
let lastAlertAt = 0, audioCtx = null;
function handleSignalEvents(d) {
  const prev = shown; const act = d.rec === 'UP' || d.rec === 'DOWN';
  if (act && prev.rec !== d.rec) {
    preds.push(snapshot(d, 'signal')); persist();
    const strong = d.strength >= cfg.alertMinStrength;
    const msg = `BUY ${d.rec} now: ${actionText(d).replace(/^Buy \w+ now /, '')}`;
    if (prev.side && prev.side !== d.rec && cfg.alerts.flip) alertUser('Switched. ' + msg, d.rec === 'UP' ? 'up' : 'dn', true);
    else if (strong && cfg.alerts.strong) alertUser(msg, d.rec === 'UP' ? 'up' : 'dn', true);
    if (cfg.autoPaper && !papers.some(p => p.win === MK.tStart && p.auto)) paperBuy(d.rec, true);
    shown = { rec: d.rec, side: d.rec };
  } else if (!act && prev.side) {
    if (cfg.alerts.invalid) alertUser(`Stop: don't buy ${prev.side} anymore. ${plainReason(d.reason, d)}`, 'warn', true);
    shown = { rec: 'NO TRADE', side: null };
  }
  if (cfg.alerts.expiry && st.tRem <= 30 && st.tRem > 25 && !markDone.has('exp') && markDone.add('exp') && papers.some(p => p.win === MK.tStart && !p.outcome)) alertUser('Market expires in 30s', 'warn', true);
}
function alertUser(msg, kind, force) {
  const t = Date.now(); if (!force && t - lastAlertAt < 20000) return; lastAlertAt = t;
  toast(msg, kind); diag('ALERT ' + msg);
  if (cfg.sound) beep(kind);
  if (cfg.notify && 'Notification' in window && Notification.permission === 'granted' && document.visibilityState !== 'visible') { try { new Notification('BTC 5M', { body: msg, tag: 'btc5m' }); } catch (e) {} }
}
function beep(kind) { try { audioCtx = audioCtx || new (window.AudioContext || window.webkitAudioContext)(); const o = audioCtx.createOscillator(), g = audioCtx.createGain(); o.frequency.value = kind === 'up' ? 880 : kind === 'dn' ? 440 : 660; g.gain.value = 0.08; o.connect(g); g.connect(audioCtx.destination); o.start(); o.stop(audioCtx.currentTime + 0.18); } catch (e) {} }
let toastTimer = null;
function toast(msg, kind) { const el = $('#toast'); el.textContent = msg; el.style.display = 'block'; el.style.background = kind === 'up' ? '#14532d' : kind === 'dn' ? '#7f1d1d' : kind === 'warn' ? '#78350f' : '#1f2937'; clearTimeout(toastTimer); toastTimer = setTimeout(() => el.style.display = 'none', 4500); }

// ---------- model selection ----------
let activeModel = E.defaultModel();
// Default: pre-trained on real data by .github/workflows/research.yml (tools/real_backtest.mjs)
const PRETRAINED = {"fit": {"b0": -0.047612474361572836, "w": [2.405915134742885, -0.07092636341346012, -0.04155198069081413, -0.027698993227202758], "mu": [-0.03046315481131931, -0.03000609160002358, -0.032100144023302725, -0.022809786038333373], "sd": [1.3024369350001026, 0.9568221923774142, 1.0035263211841026, 0.23005414157922743], "names": ["z", "mom15", "ema15", "rsi15"], "n": 34040}, "name": "Distance + 15-min chart (trained on 30 days of real BTC data)", "version": "l15m-2026-11-12", "meta": "Walk-forward out-of-sample log loss 0.4741 vs 0.4747 for distance only (30 days Coinbase BTC-USD, Oct 13 - Nov 12 2026). The chart adds only a small amount; distance to target and time left drive most of the probability."};
function useModel(m) { activeModel = E.makeLogistic(m.fit, m.name); activeModel.version = m.version; activeModel.meta = m.meta; }
(function loadModel() { const saved = LS.get('model', null); useModel(saved && saved.fit ? saved : PRETRAINED); })();

// ---------- formatting ----------
const usd = (v, d = 2) => isFinite(v) ? '$' + v.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d }) : '—';
const pct = (v, d = 0) => isFinite(v) ? (E.clamp(v, 0.01, 0.99) * 100).toFixed(d) + '%' : '—';
const cents = (v) => isFinite(v) && v > 0 ? (v * 100).toFixed(1) + '¢' : '—';
const mmss = (s) => { if (!isFinite(s) || s < 0) return '--:--'; s = Math.floor(s); return String(Math.floor(s / 60)).padStart(2, '0') + ':' + String(s % 60).padStart(2, '0'); };
const fx = (v, d = 2) => isFinite(v) ? (+v).toFixed(d) : '—';
const esc = (s) => String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const row = (k, v, cls = '') => `<div class="row"><span class="k">${k}</span><span class="v ${cls}">${v}</span></div>`;

// ---------- plain-English instructions ----------
function plainReason(g, d) {
  if (/Target price not/.test(g)) return cfg.relayUrl ? 'Getting the target price from Polymarket…' : `Opened mid-window. The target locks automatically when the next window starts (in ${mmss(st.tRem)}).`;
  if (/No active market/.test(g)) return 'Waiting for the next market to open.';
  if (/fresh price feed/.test(g)) return 'Price feeds are reconnecting. No call until live prices are back.';
  if (/disagree by/.test(g)) return 'Exchanges disagree on the price right now. Too risky to call.';
  if (/Volatility not yet/.test(g)) return 'Still measuring volatility (needs a minute of data).';
  if (/Under \d+s left/.test(g)) return 'Too close to expiry to act.';
  if (/Volatility spike/.test(g)) return 'BTC is jumping around too much right now. Too unpredictable.';
  if (/UP\/DOWN prices unavailable/.test(g)) return 'Waiting for Polymarket UP/DOWN prices.';
  if (/momentum disagree/.test(g)) return 'Short-term signals conflict. Sitting this one out.';
  if (/Best edge/.test(g) && d && d.side) { const ask = d.side === 'UP' ? st.market.upAsk : st.market.downAsk, p = d.side === 'UP' ? d.pUp : d.pDown;
    return `No good deal. ${d.side} is ${pct(p)} likely, but costs ${cents(ask)}. It's worth it at ${cents(d.maxPay)} or less.`; }
  return g;
}
const arrow = (x) => x === 'up' ? '↑' : x === 'down' ? '↓' : '→';
function callText(d) {
  const ask = d.call === 'UP' ? st.market.upAsk : st.market.downAsk;
  if (!(ask > 0)) return `Leaning ${d.call} (${pct(d.pCall)}). No Polymarket price yet.`;
  if (ask <= d.callMaxPay) return `Leaning ${d.call} (${pct(d.pCall)}) at ${cents(ask)}. Price is OK, but conditions are shaky (see cautions).`;
  return `Leaning ${d.call} (${pct(d.pCall)}), but ${cents(ask)} is too expensive. Only worth it at ${cents(d.callMaxPay)} or less.`;
}
function actionText(d) {
  if (d.rec === 'UP' || d.rec === 'DOWN') { const ask = d.rec === 'UP' ? st.market.upAsk : st.market.downAsk;
    const pSide = d.rec === 'UP' ? d.pUp : d.pDown, mp = d.held ? E.maxPrice(pSide, cfg.minEdge / 2, cfg.feeRate) : d.maxPay;
    return `Buy ${d.rec} now at ${cents(ask)}. Don't pay more than ${cents(mp)}.`; }
  return d.gates.length ? plainReason(d.gates[0], d) : 'Waiting for data…';
}

// ---------- rendering ----------
let tab = 'live';
function renderDots() {
  $('#dots').innerHTML = FEED_DEFS.map(f => { const F = X[f.id]; let c = F.status === 'off' ? '' : F.status === 'down' ? 'down' : F.fresh ? 'live' : (F.status === 'live' || F.status === 'connecting') ? 'stale' : 'down'; if (f.pm && MK.manual) c = 'stale'; return `<span class="dot ${c}"><i></i>${f.label}</span>`; }).join('');
}
function renderLive() {
  const d = lastDecision; if (!d) return;
  const card = $('#recCard'); const cls = d.rec === 'UP' ? 'UP' : d.rec === 'DOWN' ? 'DOWN' : 'NT';
  card.className = 'card rec ' + cls;
  $('#mktTitle').textContent = MK.title || 'BTC 5M';
  const callMode = cfg.alwaysCall && d.rec === 'NO TRADE' && d.call;
  if (callMode) card.className = 'card rec CALL ' + (d.call === 'UP' ? 'cUP' : 'cDOWN');
  $('#recLabel').textContent = d.rec !== 'NO TRADE' ? 'BUY ' + d.rec : callMode ? d.call : 'WAIT';
  $('#action').textContent = callMode ? callText(d) : actionText(d);
  $('#chartRead').innerHTML = d.chart ? `1-min chart <b class="${d.chart.m1 === 'up' ? 'upc' : d.chart.m1 === 'down' ? 'dnc' : ''}">${arrow(d.chart.m1)} ${d.chart.m1}</b> · 15-min chart <b class="${d.chart.m15 === 'up' ? 'upc' : d.chart.m15 === 'down' ? 'dnc' : ''}">${arrow(d.chart.m15)} ${d.chart.m15}</b>` : '';
  if (isFinite(d.pUp)) {
    const ps = d.rec === 'DOWN' ? d.pDown : d.rec === 'UP' ? d.pUp : Math.max(d.pUp, d.pDown);
    const lbl = (d.rec === 'NO TRADE' ? (d.pUp >= 0.5 ? 'UP' : 'DOWN') : d.rec) + ' ';
    const askSide = d.rec === 'UP' ? st.market.upAsk : d.rec === 'DOWN' ? st.market.downAsk : NaN;
    $('#probLine').innerHTML = `${lbl}${pct(ps)} <small>chance (estimate)</small>` + (isFinite(askSide) && ps < 0.5 ? '<div class="sub">Less likely than not, but the price is low enough to be worth it.</div>' : '');
    $('#confLine').textContent = `UP ${pct(d.pUp)} · DOWN ${pct(d.pDown)} · confidence ${d.conf}${d.held ? ' · holding' : ''}` + (d.best ? ` · edge ${(d.best.evPerDollar * 100).toFixed(1)}%` : '');
  } else { $('#probLine').textContent = '—'; $('#confLine').textContent = ''; }
  $('#strengthBar').style.width = (d.rec === 'NO TRADE' ? 0 : d.strength) + '%';
  $('#strengthBar').style.background = cls === 'UP' ? 'var(--up)' : cls === 'DOWN' ? 'var(--dn)' : 'var(--nt)';
  $('#reason').textContent = d.text || d.reason || '…';
  $('#gates').innerHTML = callMode ? (d.cautions || []).slice(0, 3).map(g => `<li>Caution: ${esc(plainReason(g, d))}</li>`).join('') : d.rec === 'NO TRADE' ? d.gates.slice(1, 4).map(g => `<li>${esc(plainReason(g, d))}</li>`).join('') : '';
  $('#countdown').textContent = mmss(st.tRem);
  $('#countdown').style.color = st.tRem < 30 ? 'var(--warn)' : '';
  $('#target').textContent = usd(st.target);
  const tv = st.targetVerified;
  $('#targetSrc').innerHTML = st.target > 0 ? `<span class="badge ${tv ? 'ok' : 'warn'}">${tv ? 'VERIFIED' : 'UNVERIFIED'}</span> ${esc(TG.src)}` : '<span class="badge warn">WAITING</span> locks at next window';
  $('#spot').textContent = usd(st.S);
  $('#sSrc').textContent = st.sSrc ? st.sSrc.split(' ')[0].toUpperCase() : '';
  const diff = st.S - st.target; $('#diff').innerHTML = isFinite(diff) ? `<span class="${diff >= 0 ? 'upc' : 'dnc'}">${diff >= 0 ? '+' : '−'}${usd(Math.abs(diff))}</span> vs target` : '';
  const q = st.market; $('#upAsk').textContent = cents(q.upAsk); $('#dnAsk').textContent = cents(q.downAsk);
  $('#mktSrc').textContent = 'UP / DOWN · ' + q.src + (q.thin ? ' · thin' : '');
  $('#openPm').innerHTML = MK.slug ? `Open on Polymarket: <a style="color:var(--acc)" href="https://polymarket.com/event/${MK.slug}">${MK.slug}</a>` : '';
  drawSpark();
}
function drawSpark() {
  const cv = $('#spark'), ctx = cv.getContext('2d'), W = cv.clientWidth * devicePixelRatio, H = 110 * devicePixelRatio; if (cv.width !== W) { cv.width = W; cv.height = H; }
  ctx.clearRect(0, 0, W, H); const t0 = MK.tStart * 1000, t1 = MK.tEnd * 1000;
  const src = clTicks.length && X.chainlink.fresh ? clTicks.map(x => ({ t: x.t, p: x.p })) : compTicks.map(x => ({ t: x.t, p: x.p + (basis || 0) }));
  const pts = src.filter(x => x.t >= t0 - 60000); if (pts.length < 2) return;
  const tg = st.target; let lo = Math.min(...pts.map(x => x.p)), hi = Math.max(...pts.map(x => x.p)); if (tg > 0) { lo = Math.min(lo, tg); hi = Math.max(hi, tg); }
  const pad = (hi - lo) * 0.15 || 5; lo -= pad; hi += pad;
  const X_ = (t) => (t - (t0 - 60000)) / (t1 - (t0 - 60000)) * W, Y = (p) => H - (p - lo) / (hi - lo) * H;
  if (tg > 0) { ctx.strokeStyle = '#9ca3af'; ctx.setLineDash([6, 6]); ctx.lineWidth = devicePixelRatio; ctx.beginPath(); ctx.moveTo(0, Y(tg)); ctx.lineTo(W, Y(tg)); ctx.stroke(); ctx.setLineDash([]);
    ctx.fillStyle = '#9ca3af'; ctx.font = `${10 * devicePixelRatio}px monospace`; ctx.fillText('target ' + tg.toFixed(0), 4, Y(tg) - 4); }
  ctx.strokeStyle = '#374151'; ctx.beginPath(); ctx.moveTo(X_(t0), 0); ctx.lineTo(X_(t0), H); ctx.stroke();
  const last = pts[pts.length - 1].p; ctx.strokeStyle = !(tg > 0) ? '#60a5fa' : last >= tg ? '#22c55e' : '#ef4444'; ctx.lineWidth = 2 * devicePixelRatio; ctx.beginPath();
  pts.forEach((x, i) => i ? ctx.lineTo(X_(x.t), Y(x.p)) : ctx.moveTo(X_(x.t), Y(x.p))); ctx.stroke();
}
function renderSignals() {
  const comp = compositeMid();
  $('#feedsTbl').innerHTML = '<table><tr><th>Feed</th><th>Price</th><th>Age</th><th>Msgs</th><th>State</th></tr>' + FEED_DEFS.map(f => { const F = X[f.id]; const age = F.priceMs ? ((now() - F.priceMs) / 1000).toFixed(1) + 's' : '—';
    const px = f.pm ? `${cents(MK.up.ask)}/${cents(MK.down.ask)}` : fx(F.mid, 2);
    return `<tr><td>${f.name}${(st.outliers || []).includes(f.id) ? ' <span class="badge warn">outlier</span>' : ''}${f.id === 'binanceus' ? ' <span class="badge">ref</span>' : ''}</td><td>${px}</td><td class="${F.fresh ? '' : 'wc'}">${age}</td><td>${F.msgs}</td><td>${F.status}${F.reconnects ? ' ↻' + F.reconnects : ''}</td></tr>`; }).join('') + '</table>';
  $('#feedSummary').innerHTML = `Composite (median of ${comp.n} fresh): ${usd(comp.p)} · spread across exchanges ${fx(comp.dispBps, 1)} bps · Chainlink − exchanges basis ${basis === null ? '—' : (basis >= 0 ? '+' : '') + basis.toFixed(2)} · clock offset ${clockOffset} ms`;
  const f = st.f || {}; const contrib = (lastDecision && lastDecision.contributions) || [];
  const cm = Object.fromEntries(contrib.map(c => [c.name, c.v]));
  $('#featTbl').innerHTML = `<div class="sub" style="margin-bottom:6px">Model: ${esc(activeModel.name)} (${esc(activeModel.version || '')})</div><table><tr><th>Feature</th><th>Value</th><th>Pushes logit</th></tr>` +
    E.FEATURE_NAMES.map(n => `<tr><td>${n}</td><td>${fx(f[n], 2)}</td><td class="${(cm[n] || 0) > 0 ? 'upc' : (cm[n] || 0) < 0 ? 'dnc' : ''}">${cm[n] !== undefined ? fx(cm[n], 2) : (n === 'z' && activeModel === E.models.vol ? 'Φ(z)' : '—')}</td></tr>`).join('') + '</table>' +
    `<div class="sub" style="margin-top:6px">σ = ${st.sigma > 0 ? (st.sigma * Math.sqrt(300) * 100).toFixed(3) + '% per 5 min' : '—'} · 15s momentum ${fx(st.m15, 2)}σ · 60s ${fx(st.m60, 2)}σ${st.conflict ? ' · <span class="wc">conflict</span>' : ''}</div>`;
  const cl = st.closes || [], cs = st.cAll || [];
  if (cl.length > 30) {
    const e9 = E.ema(cl.slice(-120), 9), e21 = E.ema(cl.slice(-120), 21), e50 = E.ema(cl.slice(-200), 50), vw = E.vwap(cs.slice(-60)), r = E.rsi(cl.slice(-100)), mc = E.macd(cl.slice(-150));
    const atr = E.atr(cs.slice(-60)), bb = E.bollinger(cl), pa = E.priceAction(cs), roc5 = E.roc(cl, 5);
    const p10 = priceAgo(compTicks, 10), p20 = priceAgo(compTicks, 20), vel = p10 > 0 ? (st.comp - p10) / 10 : NaN, accel = (p20 > 0 && isFinite(vel)) ? (vel - (p10 - p20) / 10) / 10 : NaN;
    const rv = st.sigma * Math.sqrt(300) * 100;
    $('#indTbl').innerHTML = [
      row('EMA 9 / 21 / 50 (1m)', `${fx(e9, 0)} / ${fx(e21, 0)} / ${fx(e50, 0)}`, e9 > e21 && e21 > e50 ? 'upc' : e9 < e21 && e21 < e50 ? 'dnc' : ''),
      row('VWAP (60m)', `${fx(vw, 0)} (${st.comp > vw ? 'above' : 'below'})`), row('RSI 14', fx(r, 1), r > 70 ? 'dnc' : r < 30 ? 'upc' : ''),
      row('MACD hist', `${fx(mc.hist, 2)} ${mc.hist > mc.histPrev ? '↑' : '↓'}`, mc.hist > 0 ? 'upc' : 'dnc'), row('ROC 5m', fx(roc5 * 100, 3) + '%'),
      row('Velocity (10s)', fx(vel, 2) + ' $/s', vel > 0 ? 'upc' : 'dnc'), row('Acceleration', fx(accel, 3) + ' $/s²'),
      row('ATR 14 (1m)', usd(atr)), row('Bollinger %B / width', `${fx(bb.pctB, 2)} / ${fx(bb.width * 100, 2)}%`), row('Realized vol (5m equiv)', fx(rv, 3) + '%'),
      row('Vol spike (30s vs 10m)', fx(st.tickVolRatio, 2) + '×', st.tickVolRatio > cfg.volSpikeRatio ? 'wc' : ''),
      row('Structure', `${pa.trend}${pa.breakout ? ' · breakout ' + pa.breakout : ''}${pa.failedBreakout ? ' · failed ' + pa.failedBreakout : ''}`),
      row('Support / resistance (30m)', `${fx(pa.support, 0)} / ${fx(pa.resistance, 0)}`), row('Last-candle wick', fx(pa.wick, 2) + (pa.wick > 0.3 ? ' upper rejection' : pa.wick < -0.3 ? ' lower rejection' : '')),
    ].join('');
  } else $('#indTbl').innerHTML = '<div class="sub">Warming up: need 30+ one-minute candles.</div>';
  const fl = st.flow || {}, f5 = flowStats(300);
  $('#flowTbl').innerHTML = row('Taker buy / sell (60s)', `${fx(fl.buy, 3)} / ${fx(fl.sell, 3)} BTC`) + row('Flow imbalance 60s / 5m', `${fx(fl.imb, 2)} / ${fx(f5.imb, 2)}`, fl.imb > 0 ? 'upc' : 'dnc') +
    row('Order book imbalance (top 10)', fx(st.obi, 2), st.obi > 0 ? 'upc' : 'dnc') + row('Large trades (≥1 BTC, 5m)', trades.filter(t => t.q >= 1).map(t => (t.buy ? '▲' : '▼') + t.q.toFixed(1)).slice(-6).join(' ') || 'none') +
    `<div class="sub" style="margin-top:6px">These have no historical data here for validation, so they don't affect the probability unless you set a micro-tilt in Settings.</div>`;
}
function renderPaper() {
  const done = papers.filter(t => t.pnl != null), open = papers.filter(t => t.pnl == null);
  const pnl = done.reduce((s, t) => s + t.pnl, 0), staked = done.reduce((s, t) => s + t.stake, 0), wins = done.filter(t => t.pnl > 0).length;
  const sideStats = (s) => { const a = done.filter(t => t.side === s); return a.length ? `${a.filter(t => t.pnl > 0).length}/${a.length} won, ${usd(a.reduce((x, t) => x + t.pnl, 0))}` : '—'; };
  $('#paperStats').innerHTML = row('Settled trades', done.length + (open.length ? ` (+${open.length} open)` : '')) + row('Win rate', done.length ? pct(wins / done.length, 1) : '—') +
    row('P&L after fees', usd(pnl), pnl >= 0 ? 'upc' : 'dnc') + row('ROI', staked ? fx(pnl / staked * 100, 1) + '%' : '—') + row('UP trades', sideStats('UP')) + row('DOWN trades', sideStats('DOWN')) +
    `<div class="sub" style="margin-top:6px">${done.length < 200 ? 'Under 200 settled trades: results are mostly noise. Keep paper trading before trusting any number here.' : 'Sample is getting meaningful; compare model Brier vs market Brier below.'}</div>`;
  const res = preds.filter(p => p.outcome && isFinite(p.pUp)), marks = res.filter(p => p.kind && p.kind.startsWith('mark'));
  const rows = marks.map(p => ({ p: p.pUp, y: p.outcome === 'UP' ? 1 : 0, tRem: p.tRem })), m = E.metrics(rows);
  const mk = marks.filter(p => isFinite(p.pMkt)).map(p => ({ p: p.pMkt, y: p.outcome === 'UP' ? 1 : 0 })), mm = E.metrics(mk);
  const sig = res.filter(p => p.kind === 'signal'), sigW = sig.filter(p => p.rec === p.outcome).length;
  const nt = marks.filter(p => p.rec === 'NO TRADE').length;
  $('#predStats').innerHTML = m ? row('Logged snapshots (resolved)', m.n) + row('Correct / incorrect (≥50% side)', `${Math.round(m.accuracy * m.n)} / ${m.n - Math.round(m.accuracy * m.n)}`) + row('Directional accuracy', pct(m.accuracy, 1)) +
    row('UP calls / accuracy', `${m.upCalls} / ${pct(m.upAcc, 1)}`) + row('DOWN calls / accuracy', `${m.downCalls} / ${pct(m.downAcc, 1)}`) + row('NO TRADE frequency', pct(nt / marks.length, 0)) +
    row('Actionable signals won', sig.length ? `${sigW}/${sig.length} (${pct(sigW / sig.length, 1)})` : '—') + row('Brier: model vs market', `${fx(m.brier, 4)} vs ${mm ? fx(mm.brier, 4) : '—'}`, mm ? (m.brier < mm.brier ? 'upc' : 'dnc') : '') +
    `<div class="sub" style="margin-top:6px">Lower Brier is better. A model only has an edge if it beats the <i>market's</i> Brier, not the coin flip. High directional accuracy is expected because late in a window the distance to target usually decides it, and the market already prices that.</div>`
    : '<div class="sub">No resolved predictions yet. Snapshots log automatically at 4:00, 3:00, 2:00, 1:30, 1:00 and 0:30 left.</div>';
  const cal = m ? m.calibration : [], calM = mm ? mm.calibration : [];
  $('#calTbl').innerHTML = m ? '<table><tr><th>Predicted UP</th><th>N</th><th>Model avg</th><th>Actual</th><th>Mkt N</th><th>Mkt actual</th></tr>' + cal.map((b, i) => `<tr><td>${(b.lo * 100).toFixed(0)}–${(b.hi * 100).toFixed(0)}%</td><td>${b.n}</td><td>${b.n ? pct(b.avgP) : ''}</td><td>${b.n ? pct(b.freq) : ''}</td><td>${calM[i] ? calM[i].n : ''}</td><td>${calM[i] && calM[i].n ? pct(calM[i].freq) : ''}</td></tr>`).join('') + '</table>' : '<div class="sub">—</div>';
  $('#byTTbl').innerHTML = m ? '<table><tr><th>Time left</th><th>N</th><th>Accuracy</th><th>Brier</th></tr>' + MARKS.map(t => { const r = E.metrics(rows.filter(x => Math.abs(x.tRem - t) <= 5)); return r ? `<tr><td>${mmss(t)}</td><td>${r.n}</td><td>${pct(r.accuracy, 1)}</td><td>${fx(r.brier, 4)}</td></tr>` : ''; }).join('') + '</table>' +
    (() => { const byC = ['High', 'Medium', 'Low'].map(c => { const a = marks.filter(p => p.conf === c).map(p => ({ p: p.pUp, y: p.outcome === 'UP' ? 1 : 0 })); const r = E.metrics(a); return r ? `<tr><td>${c}</td><td>${r.n}</td><td>${pct(r.accuracy, 1)}</td><td>${fx(r.brier, 4)}</td></tr>` : ''; }).join(''); return '<h3 style="margin-top:12px">By confidence</h3><table><tr><th>Confidence</th><th>N</th><th>Accuracy</th><th>Brier</th></tr>' + byC + '</table>'; })() : '<div class="sub">—</div>';
  $('#tradeList').innerHTML = papers.length ? '<table><tr><th>Time</th><th>Side</th><th>Price</th><th>Est.</th><th>Result</th><th>P&L</th></tr>' + papers.slice(-25).reverse().map(t => `<tr><td>${new Date(t.ts).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })}${t.auto ? '' : '*'}</td><td class="${t.side === 'UP' ? 'upc' : 'dnc'}">${t.side}</td><td>${cents(t.ask)}</td><td>${pct(t.pSide)}</td><td>${t.outcome || 'open'}</td><td class="${t.pnl > 0 ? 'upc' : t.pnl < 0 ? 'dnc' : ''}">${t.pnl == null ? '' : usd(t.pnl)}</td></tr>`).join('') + '</table><div class="sub">* manual paper trade</div>' : '<div class="sub">No paper trades yet. Auto-paper logs the first actionable signal in each window.</div>';
}
function renderSettings() {
  const num = (key, label, step, mult = 1, suffix = '') => `<div class="row"><span class="k">${label}</span><span><input data-k="${key}" data-m="${mult}" type="number" step="${step}" value="${+(cfg[key] * mult).toFixed(4)}">${suffix}</span></div>`;
  const chk = (path, label) => { const [a, b] = path.split('.'); const v = b ? cfg[a][b] : cfg[a]; return `<div class="row"><span class="k">${label}</span><input type="checkbox" data-c="${path}" ${v ? 'checked' : ''}></div>`; };
  if (!$('#setDecision').dataset.built) {
    $('#setDecision').innerHTML = chk('alwaysCall', 'Always show a direction (UP/DOWN)') + num('minEdge', 'Min expected edge (%)', 0.5, 100) + num('marketWeight', 'Weight on market price (%)', 5, 100) + num('feeRate', 'Taker fee rate (Polymarket crypto: 0.07)', 0.01) +
      num('minFeeds', 'Min fresh price feeds', 1) + num('maxDispersionBps', 'Max exchange disagreement (bps)', 1) + num('minSecondsLeft', 'No trade under (sec left)', 5) +
      num('volSpikeRatio', 'Vol-spike gate (× normal)', 0.5) + num('microTilt', 'Micro-tilt (unvalidated, logit units)', 0.1) +
      '<div class="sub" style="margin-top:6px">Market weight blends the model with the market\'s own probability before edge is computed. 0% trusts the model fully; higher values are more conservative.</div>';
    $('#setAlerts').innerHTML = chk('autoPaper', 'Auto paper-trade first signal per window') + num('stake', 'Paper stake ($)', 1) + num('alertMinStrength', 'Alert min signal strength (0–100)', 5) +
      chk('alerts.strong', 'Alert: strong UP/DOWN') + chk('alerts.flip', 'Alert: direction flips') + chk('alerts.invalid', 'Alert: call becomes invalid') + chk('alerts.vol', 'Alert: volatility spike') + chk('alerts.expiry', 'Alert: 30s to expiry (open paper trade)') +
      chk('sound', 'Sound') + chk('notify', 'System notifications (Home Screen app only)');
    $('#setFeeds').innerHTML = FEED_DEFS.map(f => chk('feeds.' + f.id, f.name)).join('') + '<div class="sub" style="margin-top:6px">Binance.com blocks US users, so Binance.US is used. Chainlink is the settlement source for these markets; exchanges are the cross-check.</div>' +
      `<div class="row"><span class="k">Relay URL (optional)</span><input id="relayUrl" style="width:60%;text-align:left" placeholder="https://…workers.dev" value="${esc(cfg.relayUrl || '')}"></div><div class="sub">Optional. Lets the app get Polymarket's official target when you open it mid-window, and confirms results. See relay/README.md. It needs no keys.</div>`;
    $('#setDecision').dataset.built = 1;
    $('#relayUrl').addEventListener('change', (e) => { const v = e.target.value.trim(); if (v && !/^https:\/\//.test(v)) { toast('Relay URL must start with https://', 'warn'); return; } cfg.relayUrl = v; saveCfg(); relayTry = 0; toast(v ? 'Relay set' : 'Relay removed'); });
    document.querySelectorAll('[data-k]').forEach(el => el.addEventListener('change', () => { const v = parseFloat(el.value); if (isFinite(v)) { cfg[el.dataset.k] = v / +el.dataset.m; saveCfg(); } }));
    document.querySelectorAll('[data-c]').forEach(el => el.addEventListener('change', () => { const [a, b] = el.dataset.c.split('.'); if (b) cfg[a][b] = el.checked; else cfg[a] = el.checked; saveCfg();
      if (a === 'feeds') { const F = X[b]; if (el.checked) F.restart(); else F.close(); if (b === 'polymarket' && el.checked) loadMarket(MK.tStart); }
      if (a === 'notify' && el.checked && 'Notification' in window) Notification.requestPermission().then(p => { if (p !== 'granted') toast('Notifications not allowed. On iPhone this needs the Home Screen app (iOS 16.4+).', 'warn'); });
      if (a === 'sound' && el.checked) beep('up'); }));
  }
  $('#diagSummary').textContent = `${APP_VERSION} · ${E.VERSION} · model ${activeModel.version} · ${preds.length} predictions · ${papers.length} paper trades · clock offset ${clockOffset}ms`;
  $('#diagLog').textContent = diagLines.slice(-120).reverse().join('\n');
}
function renderActiveModel() {
  const m = activeModel; $('#activeModel').innerHTML = row('Model', esc(m.name)) + row('Version', esc(m.version || '')) + (m.fit ? row('Trained on', `${m.fit.n} samples`) + m.fit.names.map((n, i) => row('  coef ' + n, fx(m.fit.w[i], 3))).join('') : '<div class="sub">Pure volatility model: P(UP) = Φ(ln(S/K)/σ√t). Run a backtest to see whether anything beats it out-of-sample.</div>') + (m.meta ? `<div class="sub">${esc(m.meta)}</div>` : '');
}

// ---------- backtest UI ----------
let btResult = null;
async function runBacktest() {
  const days = +$('#btDays').value; const end = Math.floor(now() / 300000) * 300, start = end - days * 86400;
  $('#runBt').disabled = true; $('#btStatus').textContent = 'Downloading candles…'; $('#btOut').innerHTML = '';
  try {
    const c = await fetchCandles(start, end, (p) => $('#btStatus').textContent = `Downloading candles… ${Math.round(p * 100)}%`);
    if (c.length < 2000) throw new Error(`Only ${c.length} candles downloaded`);
    $('#btStatus').textContent = `Got ${c.length} candles. Building samples…`; await sleep(30);
    const samples = E.buildSamples(c); $('#btStatus').textContent = `${samples.length} samples. Walk-forward fitting…`; await sleep(30);
    const r = E.walkForward(samples, { folds: 5 }); if (r.error) throw new Error(r.error);
    btResult = r; renderBacktest(r); $('#btStatus').textContent = `Done: ${r.windows} windows from ${new Date(r.from * 1000).toLocaleDateString()} to ${new Date(r.to * 1000).toLocaleDateString()}; ${r.testWindows} out-of-sample.`;
  } catch (e) { $('#btStatus').textContent = 'Backtest failed: ' + e.message; diag('Backtest failed: ' + e.message, 'error'); }
  $('#runBt').disabled = false;
}
function renderBacktest(r) {
  const base = r.results.find(x => x.id === 'base50').overall;
  let h = '<div class="card"><h3>Out-of-sample results</h3><table><tr><th>Model</th><th>Log loss</th><th>Brier</th><th>Acc</th></tr>' + r.results.map(x => `<tr><td>${x.id === r.bestId ? '★ ' : ''}${x.name}</td><td>${fx(x.overall.logloss, 4)}</td><td>${fx(x.overall.brier, 4)}</td><td>${pct(x.overall.accuracy, 1)}</td></tr>`).join('') + '</table>';
  const best = r.results.find(x => x.id === r.bestId), vol = r.results.find(x => x.id === 'vol'), lz = r.results.find(x => x.id === 'lz');
  const gain = lz.overall.logloss - best.overall.logloss;
  h += `<div class="sub" style="margin-top:6px">Lower is better. Coin flip log loss = ${fx(base.logloss, 4)}. ${best.id === 'lz' || best.id === 'vol' ? 'Momentum and indicator features did <b>not</b> beat distance-to-target out-of-sample: they add noise, not signal.' : `Best model improves log loss by ${fx(gain, 4)} over distance-only${gain < 0.002 ? ' (tiny, likely within noise)' : ''}.`}</div></div>`;
  h += '<div class="card"><h3>Best model by time remaining</h3><table><tr><th>Left</th><th>N</th><th>Acc</th><th>UP acc</th><th>DOWN acc</th><th>Brier</th></tr>' + [240, 180, 120, 60].map(t => { const m = best.byTRem[t]; return m ? `<tr><td>${mmss(t)}</td><td>${m.n}</td><td>${pct(m.accuracy, 1)}</td><td>${pct(m.upAcc, 1)}</td><td>${pct(m.downAcc, 1)}</td><td>${fx(m.brier, 4)}</td></tr>` : ''; }).join('') + '</table>';
  h += `<h3 style="margin-top:12px">Volatility regime</h3><table><tr><th>Regime</th><th>N</th><th>Acc</th><th>Brier</th></tr><tr><td>High vol</td><td>${best.hiVol.n}</td><td>${pct(best.hiVol.accuracy, 1)}</td><td>${fx(best.hiVol.brier, 4)}</td></tr><tr><td>Low vol</td><td>${best.loVol.n}</td><td>${pct(best.loVol.accuracy, 1)}</td><td>${fx(best.loVol.brier, 4)}</td></tr></table></div>`;
  h += '<div class="card"><h3>Calibration (best model, out-of-sample)</h3><table><tr><th>Predicted</th><th>N</th><th>Avg pred</th><th>Actual</th></tr>' + best.overall.calibration.map(b => b.n ? `<tr><td>${(b.lo * 100).toFixed(0)}–${(b.hi * 100).toFixed(0)}%</td><td>${b.n}</td><td>${pct(b.avgP, 1)}</td><td>${pct(b.freq, 1)}</td></tr>` : '').join('') + '</table>';
  const enough = r.testWindows >= 500;
  h += `<div class="btns" style="margin-top:10px">${r.liveFit ? `<button class="pri" id="useModel" ${enough ? '' : 'disabled'}>Use ${esc(best.name)} live</button>` : `<button id="useVol">Keep volatility model</button>`}</div>${enough ? '' : '<div class="sub">Needs 500+ out-of-sample windows before switching models (pick a longer history).</div>'}</div>`;
  $('#btOut').innerHTML = h;
  const u = $('#useModel'); if (u) u.onclick = () => { const ver = `${best.id}-${new Date().toISOString().slice(0, 10)}`; const saved = { fit: r.liveFit, name: best.name, version: ver, meta: `Walk-forward OOS log loss ${fx(best.overall.logloss, 4)} on ${best.overall.n} samples (${r.testWindows} windows).` };
    LS.set('model', saved); activeModel = E.makeLogistic(saved.fit, saved.name); activeModel.version = ver; activeModel.meta = saved.meta; renderActiveModel(); toast('Live model set to ' + ver); diag('Model switched to ' + ver); };
}

// ---------- PiP floating window (closest supported alternative to an overlay) ----------
let pipOn = false;
function drawPip() {
  const cv = $('#pipcanvas'), c = cv.getContext('2d'), d = lastDecision || { rec: 'NO TRADE' };
  const col = d.rec === 'UP' ? '#16a34a' : d.rec === 'DOWN' ? '#dc2626' : '#374151';
  c.fillStyle = '#0b0d10'; c.fillRect(0, 0, 480, 270); c.fillStyle = col; c.fillRect(0, 0, 480, 10);
  c.fillStyle = d.rec === 'UP' ? '#22c55e' : d.rec === 'DOWN' ? '#ef4444' : '#9ca3af'; c.font = 'bold 72px -apple-system,Helvetica'; c.fillText(d.rec === 'NO TRADE' ? 'NO TRADE' : d.rec, 20, 95);
  c.fillStyle = '#e5e7eb'; c.font = 'bold 34px Menlo,monospace';
  const ps = d.rec === 'DOWN' ? d.pDown : d.rec === 'UP' ? d.pUp : (isFinite(d.pUp) ? Math.max(d.pUp, d.pDown) : NaN);
  c.fillText(`${pct(ps)} est   ${mmss(st.tRem)}`, 20, 150);
  c.font = '24px Menlo,monospace'; c.fillStyle = '#9ca3af';
  c.fillText(`T ${usd(st.target)}${st.targetVerified ? '' : ' ?'}`, 20, 195); c.fillText(`S ${usd(st.S)}`, 20, 228);
  c.font = '18px Menlo,monospace'; c.fillText('upd ' + new Date().toLocaleTimeString([], { hour12: false }), 300, 258);
}
async function togglePip() {
  const v = $('#pipvid');
  try {
    if (document.pictureInPictureElement || pipOn) { if (document.exitPictureInPicture) await document.exitPictureInPicture(); else if (v.webkitSetPresentationMode) v.webkitSetPresentationMode('inline'); pipOn = false; return; }
    const cv = $('#pipcanvas'); drawPip();
    if (!cv.captureStream) throw new Error('canvas streaming not supported');
    if (!v.srcObject) v.srcObject = cv.captureStream(4);
    await v.play();
    if (v.requestPictureInPicture) await v.requestPictureInPicture();
    else if (v.webkitSupportsPresentationMode && v.webkitSupportsPresentationMode('picture-in-picture')) v.webkitSetPresentationMode('picture-in-picture');
    else throw new Error('Picture-in-Picture not supported here');
    pipOn = true; toast('Floating window on. If the time in it stops moving, iOS has paused this page; reopen it.', 'info');
  } catch (e) { toast('Float unavailable: ' + e.message, 'warn'); diag('PiP: ' + e.message, 'warn'); }
}

// ---------- wiring ----------
document.querySelectorAll('#nav button').forEach(b => b.onclick = () => { tab = b.dataset.t; document.querySelectorAll('#nav button').forEach(x => x.classList.toggle('on', x === b)); document.querySelectorAll('main section').forEach(s => s.classList.toggle('on', s.id === 'tab-' + tab)); renderTab(true); window.scrollTo(0, 0); });
$('#btnEditTarget').onclick = () => { $('#editTarget').classList.toggle('on'); $('#inTarget').value = TG.value > 0 ? TG.value.toFixed(2) : ''; };
$('#saveTarget').onclick = () => { const v = parseFloat($('#inTarget').value.replace(/[$,\s]/g, '')); if (v > 1000 && v < 10000000) { setTarget(v, 'manual', true, MK.tStart); TG.src = 'manual'; $('#editTarget').classList.remove('on'); } else toast('Enter a valid BTC price', 'warn'); };
$('#btnMktPrices').onclick = () => { $('#editMkt').classList.toggle('on'); };
$('#saveMkt').onclick = () => { const u = parseFloat($('#inUp').value) / 100, d = parseFloat($('#inDn').value) / 100; if (u > 0 && u < 1 && d > 0 && d < 1) { MK.manual = { up: u, down: d, win: MK.tStart }; toast('Manual prices set for this window'); $('#editMkt').classList.remove('on'); } else { MK.manual = null; toast('Cleared manual prices; using live book'); } };
$('#paperUp').onclick = () => paperBuy('UP', false); $('#paperDn').onclick = () => paperBuy('DOWN', false);
$('#btnPip').onclick = togglePip;
$('#runBt').onclick = runBacktest;
$('#resetModel').onclick = () => { LS.set('model', null); useModel(PRETRAINED); renderActiveModel(); toast('Using the default chart model'); };
function csv() { const cols = ['ts', 'kind', 'slug', 'tRem', 'target', 'targetSrc', 'S', 'pUp', 'pModel', 'pMkt', 'upAsk', 'downAsk', 'rec', 'edge', 'conf', 'strength', 'model', 'engine', 'endPrice', 'outcome', 'outcomeSrc'];
  return cols.join(',') + '\n' + preds.map(p => cols.map(c => { let v = p[c]; if (c === 'ts') v = new Date(v).toISOString(); return v == null ? '' : String(v).replace(/,/g, ';'); }).join(',')).join('\n'); }
$('#exportCsv').onclick = () => { try { const b = new Blob([csv()], { type: 'text/csv' }), a = document.createElement('a'); a.href = URL.createObjectURL(b); a.download = 'btc5m-predictions.csv'; document.body.appendChild(a); a.click(); a.remove(); } catch (e) { toast('Download blocked; use Copy JSON', 'warn'); } };
$('#copyJson').onclick = () => { navigator.clipboard.writeText(JSON.stringify({ preds, papers, cfg }, null, 0)).then(() => toast('Copied'), () => toast('Clipboard blocked', 'warn')); };
$('#copyDiag').onclick = () => { navigator.clipboard.writeText(diagLines.join('\n')).then(() => toast('Copied'), () => toast('Clipboard blocked', 'warn')); };
let clearArm = 0; $('#clearData').onclick = () => { if (Date.now() - clearArm < 3000) { preds = []; papers = []; persist(); toast('Cleared'); renderPaper(); } else { clearArm = Date.now(); toast('Tap Clear again within 3s to delete all paper data', 'warn'); } };

function renderTab(force) { renderDots(); if (tab === 'live') renderLive(); else if (tab === 'signals') renderSignals(); else if (tab === 'paper' && force) renderPaper(); else if (tab === 'settings') renderSettings(); else if (tab === 'backtest' && force) renderActiveModel(); }
let paperTick = 0;
function loop() { try { analyze(); } catch (e) { diag('analyze error: ' + e.stack, 'error'); } try { renderTab(false); if (tab === 'paper' && ++paperTick % 10 === 0) renderPaper(); if (pipOn) drawPip(); } catch (e) { diag('render error: ' + e.message, 'error'); } }
function connectAll() { FEED_DEFS.forEach(f => { if (!f.pm) X[f.id].connect(); }); }
document.addEventListener('visibilitychange', () => { if (document.visibilityState === 'visible') { diag('Page visible: checking sockets'); FEED_DEFS.forEach(f => { const F = X[f.id]; if (cfg.feeds[f.id] && (!F.ws || F.ws.readyState > 1)) F.restart(); }); } });
setInterval(() => FEED_DEFS.forEach(f => X[f.id].watchdog()), 5000);
setInterval(loop, 500);
window.__btc5m = { X, MK, TG, st, cfg, E, get decision() { return lastDecision; }, get preds() { return preds; }, get papers() { return papers; }, get candles() { return candles; }, diagLines };
diag(`Start ${APP_VERSION}`);
const unlockAudio = () => { try { audioCtx = audioCtx || new (window.AudioContext || window.webkitAudioContext)(); if (audioCtx.state === 'suspended') audioCtx.resume(); const b = document.getElementById('alertBanner'); if (b) b.style.display = 'none'; } catch (e) {} };
['touchend', 'click'].forEach(ev => document.addEventListener(ev, unlockAudio, { once: true }));
$('#alertBanner').onclick = () => { unlockAudio(); beep('up'); if (!cfg.sound) { cfg.sound = true; saveCfg(); }
  if ('Notification' in window && Notification.permission === 'default') Notification.requestPermission().then(p => { if (p === 'granted') { cfg.notify = true; saveCfg(); } }); toast('Alerts on. You will hear a tone when it says BUY.', 'up'); };
// PWA: service worker only on https (GitHub Pages); harmless elsewhere
if ('serviceWorker' in navigator && (location.protocol === 'https:' || location.hostname === 'localhost')) navigator.serviceWorker.register('./sw.js').then(() => diag('Service worker registered'), (e) => diag('Service worker failed: ' + e.message, 'warn'));
const netBadge = () => { const el = document.getElementById('offlineBadge'); if (el) el.style.display = navigator.onLine ? 'none' : ''; };
window.addEventListener('online', () => { netBadge(); diag('Back online: reconnecting'); FEED_DEFS.forEach(f => { if (cfg.feeds[f.id] && !f.pm) X[f.id].restart(); }); if (cfg.feeds.polymarket) loadMarket(MK.tStart); });
window.addEventListener('offline', () => { netBadge(); diag('Network offline', 'warn'); }); netBadge();
connectAll(); bootstrapCandles(); analyze(); renderTab(true);
})();
