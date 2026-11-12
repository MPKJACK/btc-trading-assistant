/* BTC 5M engine: indicators, probability models, decision gates, backtest.
   Pure functions, no network. Runs in the browser (window.Engine) and in Node (module.exports). */
(function (root) {
  'use strict';
  const E = {};
  E.VERSION = 'engine-1.0.0';

  // ---------- math ----------
  function erf(x) {
    // Abramowitz & Stegun 7.1.26 (max abs error 1.5e-7)
    const s = x < 0 ? -1 : 1; x = Math.abs(x);
    const t = 1 / (1 + 0.3275911 * x);
    const y = 1 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.exp(-x * x);
    return s * y;
  }
  E.normCdf = (z) => 0.5 * (1 + erf(z / Math.SQRT2));
  E.clamp = (x, a, b) => Math.max(a, Math.min(b, x));
  E.mean = (a) => a.length ? a.reduce((s, v) => s + v, 0) / a.length : NaN;
  E.sd = (a) => { if (a.length < 2) return NaN; const m = E.mean(a); return Math.sqrt(a.reduce((s, v) => s + (v - m) * (v - m), 0) / (a.length - 1)); };
  E.sigmoid = (x) => 1 / (1 + Math.exp(-x));
  E.logit = (p) => { p = E.clamp(p, 1e-6, 1 - 1e-6); return Math.log(p / (1 - p)); };

  // ---------- indicators (on arrays of closes / candles {t,o,h,l,c,v}) ----------
  E.emaSeries = function (vals, n) {
    const k = 2 / (n + 1), out = []; let e;
    for (let i = 0; i < vals.length; i++) { e = i === 0 ? vals[0] : vals[i] * k + e * (1 - k); out.push(e); }
    return out;
  };
  E.ema = (vals, n) => { const s = E.emaSeries(vals, n); return s.length ? s[s.length - 1] : NaN; };
  E.rsi = function (closes, n = 14) {
    if (closes.length < n + 1) return NaN;
    let g = 0, l = 0;
    for (let i = 1; i <= n; i++) { const d = closes[i] - closes[i - 1]; if (d > 0) g += d; else l -= d; }
    g /= n; l /= n;
    for (let i = n + 1; i < closes.length; i++) {
      const d = closes[i] - closes[i - 1];
      g = (g * (n - 1) + Math.max(d, 0)) / n; l = (l * (n - 1) + Math.max(-d, 0)) / n;
    }
    if (l === 0) return 100; return 100 - 100 / (1 + g / l);
  };
  E.macd = function (closes) {
    if (closes.length < 35) return { macd: NaN, signal: NaN, hist: NaN };
    const a = E.emaSeries(closes, 12), b = E.emaSeries(closes, 26);
    const m = a.map((v, i) => v - b[i]); const s = E.emaSeries(m, 9);
    const L = m.length - 1; return { macd: m[L], signal: s[L], hist: m[L] - s[L], histPrev: m[L - 1] - s[L - 1] };
  };
  E.atr = function (c, n = 14) {
    if (c.length < n + 1) return NaN;
    const tr = []; for (let i = 1; i < c.length; i++) tr.push(Math.max(c[i].h - c[i].l, Math.abs(c[i].h - c[i - 1].c), Math.abs(c[i].l - c[i - 1].c)));
    let a = E.mean(tr.slice(0, n)); for (let i = n; i < tr.length; i++) a = (a * (n - 1) + tr[i]) / n; return a;
  };
  E.bollinger = function (closes, n = 20, k = 2) {
    if (closes.length < n) return { mid: NaN, up: NaN, lo: NaN, pctB: NaN, width: NaN };
    const w = closes.slice(-n), m = E.mean(w), s = Math.sqrt(w.reduce((a, v) => a + (v - m) ** 2, 0) / n);
    const last = closes[closes.length - 1];
    return { mid: m, up: m + k * s, lo: m - k * s, pctB: s > 0 ? (last - (m - k * s)) / (2 * k * s) : 0.5, width: m ? 2 * k * s / m : NaN };
  };
  E.vwap = function (c) { let pv = 0, v = 0; for (const x of c) { const tp = (x.h + x.l + x.c) / 3; pv += tp * x.v; v += x.v; } return v > 0 ? pv / v : NaN; };
  E.roc = (closes, n) => closes.length > n ? (closes[closes.length - 1] / closes[closes.length - 1 - n] - 1) : NaN;

  // sigma per sqrt(second) from 1-minute candle closes (log returns)
  E.sigmaPerSec = function (c, n = 60) {
    const w = c.slice(-(n + 1)); if (w.length < 11) return NaN;
    const r = []; for (let i = 1; i < w.length; i++) r.push(Math.log(w[i].c / w[i - 1].c));
    const s = Math.sqrt(r.reduce((a, v) => a + v * v, 0) / r.length); // zero-mean estimator
    return s / Math.sqrt(60);
  };

  // 1-second resample of ticks [{t(ms), p}] -> array of prices for the last `sec` seconds
  E.resample1s = function (ticks, nowMs, sec) {
    const out = new Array(sec).fill(NaN); if (!ticks.length) return out;
    let j = 0, last = NaN; const start = nowMs - sec * 1000;
    while (j < ticks.length && ticks[j].t < start) { last = ticks[j].p; j++; }
    for (let i = 0; i < sec; i++) {
      const end = start + (i + 1) * 1000;
      while (j < ticks.length && ticks[j].t < end) { last = ticks[j].p; j++; }
      out[i] = last;
    }
    return out;
  };
  E.tickVolPerSec = function (series) { // series of 1s prices
    const r = []; for (let i = 1; i < series.length; i++) if (series[i] > 0 && series[i - 1] > 0) r.push(Math.log(series[i] / series[i - 1]));
    if (r.length < 10) return NaN; return Math.sqrt(r.reduce((a, v) => a + v * v, 0) / r.length);
  };

  E.priceAction = function (c) {
    const out = { trend: 'flat', hh: false, ll: false, support: NaN, resistance: NaN, breakout: null, failedBreakout: null, wick: 0 };
    if (c.length < 12) return out;
    const last = c[c.length - 1], prev = c.slice(-31, -1);
    out.resistance = Math.max(...prev.map(x => x.h)); out.support = Math.min(...prev.map(x => x.l));
    const a = c.slice(-6, -3), b = c.slice(-3);
    const hA = Math.max(...a.map(x => x.h)), hB = Math.max(...b.map(x => x.h)), lA = Math.min(...a.map(x => x.l)), lB = Math.min(...b.map(x => x.l));
    out.hh = hB > hA && lB > lA; out.ll = hB < hA && lB < lA; out.trend = out.hh ? 'up' : out.ll ? 'down' : 'range';
    if (last.c > out.resistance) out.breakout = 'up'; else if (last.c < out.support) out.breakout = 'down';
    if (last.h > out.resistance && last.c < out.resistance) out.failedBreakout = 'up';
    if (last.l < out.support && last.c > out.support) out.failedBreakout = 'down';
    const done = c[c.length - 2], rng = done.h - done.l;
    out.wick = rng > 0 ? ((done.h - Math.max(done.o, done.c)) - (Math.min(done.o, done.c) - done.l)) / rng : 0; // +: upper-wick rejection
    return out;
  };

  // ---------- features shared by backtest and live (only past data) ----------
  // ctx: {S, K, tRem(sec), sigma(per sqrt sec), closes1m(array incl. current partial), candles1m, S60 (price 60s ago), S300}
  E.FEATURE_NAMES = ['z', 'mom1', 'mom5', 'rsiDev', 'emaSpread', 'volRatio', 'relVol', 'wick'];
  E.features = function (ctx) {
    const sig = ctx.sigma, t = Math.max(ctx.tRem, 1);
    const z = Math.log(ctx.S / ctx.K) / (sig * Math.sqrt(t));
    const mom1 = ctx.S60 ? Math.log(ctx.S / ctx.S60) / (sig * Math.sqrt(60)) : 0;
    const mom5 = ctx.S300 ? Math.log(ctx.S / ctx.S300) / (sig * Math.sqrt(300)) : 0;
    const cl = ctx.closes1m;
    const r = E.rsi(cl.slice(-60), 14); const rsiDev = isFinite(r) ? (r - 50) / 50 : 0;
    const e9 = E.ema(cl.slice(-80), 9), e21 = E.ema(cl.slice(-80), 21);
    const emaSpread = isFinite(e9) ? Math.log(e9 / e21) / (sig * Math.sqrt(300)) : 0;
    const cs = ctx.candles1m;
    const sShort = E.sigmaPerSec(cs, 15), sLong = E.sigmaPerSec(cs, 60);
    const volRatio = (isFinite(sShort) && sLong > 0) ? Math.log(sShort / sLong) : 0;
    let relVol = 0; if (cs.length >= 65) { const v5 = cs.slice(-5).reduce((a, x) => a + x.v, 0), v60 = cs.slice(-65, -5).reduce((a, x) => a + x.v, 0) / 12; relVol = v60 > 0 ? Math.log((v5 + 1e-9) / v60) : 0; }
    const wick = E.priceAction(cs).wick || 0;
    const f = { z: E.clamp(z, -8, 8), mom1: E.clamp(mom1, -6, 6), mom5: E.clamp(mom5, -6, 6), rsiDev, emaSpread: E.clamp(emaSpread, -6, 6), volRatio: E.clamp(volRatio, -3, 3), relVol: E.clamp(relVol, -4, 4), wick };
    return f;
  };

  // ---------- models ----------
  // Every model: (features, ctx) -> P(finish >= target)
  E.models = {
    vol: { name: 'Volatility (driftless)', desc: 'P = Φ(ln(S/K) / σ√t). No fitting; pure distance-vs-volatility.', predict: (f) => E.normCdf(f.z) },
  };
  E.makeLogistic = function (fit, name) {
    return { name, desc: 'Logistic regression on ' + fit.names.join(', ') + ' (walk-forward fit).', fit,
      predict: (f) => { let s = fit.b0; fit.names.forEach((n, i) => { s += fit.w[i] * ((f[n] - fit.mu[i]) / fit.sd[i]); }); return E.sigmoid(s); },
      contributions: (f) => fit.names.map((n, i) => ({ name: n, v: fit.w[i] * ((f[n] - fit.mu[i]) / fit.sd[i]) })) };
  };
  // default when no backtest has run: calibrated-ish vol model (normal z). Kept honest: no momentum assumptions.
  E.defaultModel = () => E.models.vol;

  // L2-regularised logistic regression via Newton/IRLS
  E.logisticFit = function (X, y, names, l2 = 1.0, iters = 30) {
    const n = X.length, d = names.length;
    const mu = [], sd = [];
    for (let j = 0; j < d; j++) { const col = X.map(r => r[j]); mu.push(E.mean(col)); sd.push(E.sd(col) || 1); }
    const Z = X.map(r => [1].concat(r.map((v, j) => (v - mu[j]) / sd[j])));
    let w = new Array(d + 1).fill(0);
    for (let it = 0; it < iters; it++) {
      const g = new Array(d + 1).fill(0), H = Array.from({ length: d + 1 }, () => new Array(d + 1).fill(0));
      for (let i = 0; i < n; i++) {
        let s = 0; for (let j = 0; j <= d; j++) s += w[j] * Z[i][j];
        const p = E.sigmoid(s), r = p * (1 - p);
        for (let j = 0; j <= d; j++) { g[j] += (p - y[i]) * Z[i][j]; for (let k = 0; k <= d; k++) H[j][k] += r * Z[i][j] * Z[i][k]; }
      }
      for (let j = 1; j <= d; j++) { g[j] += l2 * w[j]; H[j][j] += l2; }
      H[0][0] += 1e-9;
      const step = solve(H, g); let mx = 0;
      for (let j = 0; j <= d; j++) { w[j] -= step[j]; mx = Math.max(mx, Math.abs(step[j])); }
      if (mx < 1e-7) break;
    }
    return { b0: w[0], w: w.slice(1), mu, sd, names, n };
  };
  function solve(A, b) { // Gaussian elimination with partial pivoting
    const n = b.length, M = A.map((r, i) => r.slice().concat([b[i]]));
    for (let c = 0; c < n; c++) {
      let p = c; for (let r = c + 1; r < n; r++) if (Math.abs(M[r][c]) > Math.abs(M[p][c])) p = r;
      [M[c], M[p]] = [M[p], M[c]]; const v = M[c][c] || 1e-12;
      for (let r = 0; r < n; r++) if (r !== c) { const f = M[r][c] / v; for (let k = c; k <= n; k++) M[r][k] -= f * M[c][k]; }
    }
    return M.map((r, i) => r[n] / (r[i] || 1e-12));
  }

  // ---------- evaluation metrics ----------
  E.metrics = function (rows) { // rows: [{p, y, tRem, hiVol}]
    if (!rows.length) return null;
    let brier = 0, ll = 0, acc = 0, upN = 0, upW = 0, dnN = 0, dnW = 0;
    for (const r of rows) {
      const p = E.clamp(r.p, 1e-4, 1 - 1e-4);
      brier += (p - r.y) ** 2; ll += -(r.y * Math.log(p) + (1 - r.y) * Math.log(1 - p));
      const callUp = p >= 0.5; if ((callUp ? 1 : 0) === r.y) acc++;
      if (callUp) { upN++; if (r.y === 1) upW++; } else { dnN++; if (r.y === 0) dnW++; }
    }
    const n = rows.length;
    return { n, brier: brier / n, logloss: ll / n, accuracy: acc / n, upCalls: upN, upAcc: upN ? upW / upN : NaN, downCalls: dnN, downAcc: dnN ? dnW / dnN : NaN, calibration: E.calibration(rows) };
  };
  E.calibration = function (rows, nb = 10) {
    const b = Array.from({ length: nb }, (_, i) => ({ lo: i / nb, hi: (i + 1) / nb, n: 0, sumP: 0, hits: 0 }));
    for (const r of rows) { const i = Math.min(nb - 1, Math.floor(r.p * nb)); b[i].n++; b[i].sumP += r.p; b[i].hits += r.y; }
    return b.map(x => ({ lo: x.lo, hi: x.hi, n: x.n, avgP: x.n ? x.sumP / x.n : NaN, freq: x.n ? x.hits / x.n : NaN }));
  };

  // ---------- backtest on 1-minute candles (walk-forward, out-of-sample) ----------
  // candles: [{t(sec, minute open time), o,h,l,c,v}] sorted, contiguous-ish.
  // For each 5-min window aligned to 300s, target K = open of first minute; outcome = close of 5th minute >= K.
  // Decision points at tRem = 240,180,120,60 using only candles completed before the decision time.
  E.buildSamples = function (candles) {
    const byT = new Map(candles.map((c, i) => [c.t, i]));
    const samples = [];
    for (let i = 0; i < candles.length; i++) {
      const c0 = candles[i]; if (c0.t % 300 !== 0) continue;
      const iEnd = byT.get(c0.t + 240); if (iEnd === undefined || iEnd !== i + 4) continue; // need 5 contiguous minutes
      if (i < 70) continue; // need history
      const K = c0.o, y = candles[iEnd].c >= K ? 1 : 0;
      for (let k = 1; k <= 4; k++) {
        const j = i + k - 1; // last completed candle index at decision time c0.t + 60k
        const hist = candles.slice(Math.max(0, j - 90), j + 1);
        const sigma = E.sigmaPerSec(hist, 60); if (!(sigma > 0)) continue;
        const S = candles[j].c, tRem = 300 - 60 * k;
        const ctx = { S, K, tRem, sigma, closes1m: hist.map(x => x.c), candles1m: hist, S60: candles[j - 1].c, S300: candles[j - 5].c };
        samples.push({ t: c0.t + 60 * k, win: c0.t, tRem, y, sigma, f: E.features(ctx) });
      }
    }
    return samples;
  };

  E.MODEL_SPECS = [
    { id: 'base50', name: 'Coin flip (50%)', feats: null },
    { id: 'vol', name: 'Volatility (no fit)', feats: [] },
    { id: 'lz', name: 'Logistic: distance only', feats: ['z'] },
    { id: 'lzm', name: 'Logistic: distance + momentum', feats: ['z', 'mom1', 'mom5'] },
    { id: 'lall', name: 'Logistic: all features', feats: E.FEATURE_NAMES },
  ];

  E.walkForward = function (samples, opts = {}) {
    const folds = opts.folds || 5, warm = opts.warmFrac || 0.4;
    const wins = [...new Set(samples.map(s => s.win))].sort((a, b) => a - b);
    if (wins.length < 60) return { error: 'Not enough history: ' + wins.length + ' windows (need 60+).' };
    const startIdx = Math.floor(wins.length * warm), per = Math.ceil((wins.length - startIdx) / folds);
    const oos = {}; E.MODEL_SPECS.forEach(m => oos[m.id] = []);
    for (let f = 0; f < folds; f++) {
      const a = startIdx + f * per, b = Math.min(wins.length, a + per); if (a >= b) break;
      const trainEnd = wins[a], testA = wins[a], testB = wins[b - 1];
      const train = samples.filter(s => s.win < trainEnd);   // strictly earlier windows => no leakage
      const test = samples.filter(s => s.win >= testA && s.win <= testB);
      const sigMed = median(train.map(s => s.sigma));
      for (const spec of E.MODEL_SPECS) {
        let pred;
        if (spec.feats === null) pred = () => 0.5;
        else if (!spec.feats.length) pred = (s) => E.normCdf(s.f.z);
        else { const fit = E.logisticFit(train.map(s => spec.feats.map(n => s.f[n])), train.map(s => s.y), spec.feats, opts.l2 || 1); const m = E.makeLogistic(fit, spec.name); pred = (s) => m.predict(s.f); }
        for (const s of test) oos[spec.id].push({ p: pred(s), y: s.y, tRem: s.tRem, hiVol: s.sigma > sigMed, t: s.t });
      }
    }
    const results = E.MODEL_SPECS.map(spec => {
      const rows = oos[spec.id], m = E.metrics(rows);
      const byT = {}; [240, 180, 120, 60].forEach(t => byT[t] = E.metrics(rows.filter(r => r.tRem === t)));
      return { id: spec.id, name: spec.name, feats: spec.feats, overall: m, byTRem: byT, hiVol: E.metrics(rows.filter(r => r.hiVol)), loVol: E.metrics(rows.filter(r => !r.hiVol)) };
    });
    const ranked = results.filter(r => r.id !== 'base50').sort((a, b) => a.overall.logloss - b.overall.logloss);
    const best = ranked[0];
    // final model for live use: refit the best spec on ALL samples (for future windows only)
    let live = null;
    if (best.feats && best.feats.length) live = E.logisticFit(samples.map(s => best.feats.map(n => s.f[n])), samples.map(s => s.y), best.feats, opts.l2 || 1);
    return { windows: wins.length, samples: samples.length, testWindows: wins.length - startIdx, from: wins[0], to: wins[wins.length - 1], results, bestId: best.id, liveFit: live, oosBest: oos[best.id] };
  };
  function median(a) { const s = a.slice().sort((x, y) => x - y); return s.length ? s[Math.floor(s.length / 2)] : NaN; }

  // ---------- fees / EV ----------
  E.takerFee = (p, rate) => rate * p * (1 - p); // per share (Polymarket: fee = C × rate × p × (1−p))
  E.ev = function (pWin, ask, rate) { // per share bought at `ask`
    const fee = E.takerFee(ask, rate), cost = ask + fee;
    return { fee, cost, evPerShare: pWin - cost, evPerDollar: (pWin - cost) / cost, breakeven: cost };
  };

  // ---------- decision ----------
  // st: {target, targetVerified, S, sSource, tRem, sigma, f, model, market:{upAsk,downAsk,upMid}, feedsFresh, freshCount,
  //      dispersionBps, tickVolRatio, conflict}
  E.decide = function (st, cfg) {
    const gates = [];
    const R = (rec, extra) => Object.assign({ rec, gates }, extra);
    if (!(st.target > 0)) gates.push('Target price not known');
    else if (!st.targetVerified) gates.push('Target price not verified');
    if (!(st.S > 0)) gates.push('No live BTC price');
    if (!(st.tRem > 0)) gates.push('No active market window');
    if (st.freshCount < cfg.minFeeds) gates.push(`Only ${st.freshCount} fresh price feed(s) (need ${cfg.minFeeds})`);
    if (st.dispersionBps > cfg.maxDispersionBps) gates.push(`Exchanges disagree by ${st.dispersionBps.toFixed(1)} bps`);
    if (!(st.sigma > 0)) gates.push('Volatility not yet measurable');
    if (st.tRem > 0 && st.tRem < cfg.minSecondsLeft) gates.push(`Under ${cfg.minSecondsLeft}s left`);
    if (st.tickVolRatio > cfg.volSpikeRatio) gates.push(`Volatility spike (${st.tickVolRatio.toFixed(1)}× normal)`);
    const mk = st.market || {};
    const haveMarket = mk.upAsk > 0 && mk.upAsk < 1 && mk.downAsk > 0 && mk.downAsk < 1;
    if (!haveMarket) gates.push('UP/DOWN prices unavailable');

    let pModel = NaN, contributions = [];
    if (st.f && st.model && st.sigma > 0 && st.S > 0 && st.target > 0) {
      pModel = st.model.predict(st.f);
      if (st.model.contributions) contributions = st.model.contributions(st.f);
      if (cfg.microTilt && isFinite(st.microScore)) pModel = E.sigmoid(E.logit(pModel) + cfg.microTilt * st.microScore);
    }
    if (!isFinite(pModel)) return R('NO TRADE', { pUp: NaN, pDown: NaN, reason: gates[0] || 'Insufficient data', strength: 0, conf: 'None' });

    const pMkt = haveMarket ? (mk.upMid > 0 ? mk.upMid : (mk.upAsk + (1 - mk.downAsk)) / 2) : NaN;
    const w = haveMarket ? cfg.marketWeight : 0;
    const pUp = isFinite(pMkt) ? E.sigmoid((1 - w) * E.logit(pModel) + w * E.logit(pMkt)) : pModel;
    const up = haveMarket ? E.ev(pUp, mk.upAsk, cfg.feeRate) : null, dn = haveMarket ? E.ev(1 - pUp, mk.downAsk, cfg.feeRate) : null;
    let side = null, best = null;
    if (up && dn) { if (up.evPerDollar >= dn.evPerDollar) { side = 'UP'; best = up; } else { side = 'DOWN'; best = dn; } }
    if (st.conflict && best && best.evPerDollar < 2 * cfg.minEdge) gates.push('Short- and medium-term momentum disagree');
    if (best && best.evPerDollar < cfg.minEdge) gates.push(`Best edge ${(best.evPerDollar * 100).toFixed(1)}% < ${(cfg.minEdge * 100).toFixed(1)}% minimum`);

    const pSide = side === 'DOWN' ? 1 - pUp : pUp;
    const conf = !isFinite(pSide) ? 'None' : Math.abs(pUp - 0.5) >= 0.3 ? 'High' : Math.abs(pUp - 0.5) >= 0.15 ? 'Medium' : 'Low';
    const strength = best ? Math.round(E.clamp(best.evPerDollar / 0.2, 0, 1) * 100) : 0;
    const rec = gates.length ? 'NO TRADE' : side;
    return R(rec, { pUp, pDown: 1 - pUp, pModel, pMkt, side, up, dn, best, conf, strength, contributions, reason: gates.length ? gates[0] : '' });
  };

  E.explain = function (d, st) {
    const parts = [];
    if (st.target > 0 && st.S > 0) {
      const diff = st.S - st.target, zz = st.f ? st.f.z : NaN;
      parts.push(`BTC is $${Math.abs(diff).toFixed(2)} ${diff >= 0 ? 'above' : 'below'} target with ${Math.round(st.tRem)}s left${isFinite(zz) ? ` (${Math.abs(zz).toFixed(1)}σ)` : ''}.`);
    }
    if (d.contributions && d.contributions.length) {
      const nm = { z: 'distance to target', mom1: '1-min momentum', mom5: '5-min momentum', rsiDev: 'RSI', emaSpread: 'EMA 9/21 trend', volRatio: 'volatility regime', relVol: 'relative volume', wick: 'rejection wicks' };
      const top = d.contributions.filter(c => c.name !== 'z' && Math.abs(c.v) > 0.05).sort((a, b) => Math.abs(b.v) - Math.abs(a.v)).slice(0, 2);
      if (top.length) parts.push(top.map(c => `${nm[c.name] || c.name} ${c.v > 0 ? 'favors UP' : 'favors DOWN'}`).join(', ') + '.');
    }
    if (isFinite(d.pMkt)) parts.push(`Market prices UP at ${(d.pMkt * 100).toFixed(0)}%; model ${(d.pModel * 100).toFixed(0)}%.`);
    if (d.best) parts.push(`${d.side} costs ${(d.best.cost * 100).toFixed(1)}¢ incl. fee → est. edge ${(d.best.evPerDollar * 100).toFixed(1)}%.`);
    return parts.join(' ');
  };

  if (typeof module !== 'undefined' && module.exports) module.exports = E; else root.Engine = E;
})(typeof window !== 'undefined' ? window : globalThis);
