const E = require('../src/engine.js');
let fails = 0;
const ok = (c, msg) => { console.log((c ? 'PASS ' : 'FAIL ') + msg); if (!c) fails++; };

// deterministic RNG
let seed = 12345; const rnd = () => { seed = (seed * 1664525 + 1013904223) % 4294967296; return seed / 4294967296; };
const gauss = () => { let u = 0, v = 0; while (u === 0) u = rnd(); v = rnd(); return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v); };

function synth(nMin, sigmaSec, phi = 0) { // 1-min candles from 1s steps; phi = autocorrelation of 1-min drift (planted momentum)
  const out = []; let p = 70000, t0 = 1780000000 - (1780000000 % 300), drift = 0;
  for (let m = 0; m < nMin; m++) {
    const o = p; let h = p, l = p;
    drift = phi * drift + (phi ? Math.sqrt(1 - phi * phi) * gauss() * sigmaSec * 0.6 : 0);
    for (let s = 0; s < 60; s++) { p *= Math.exp(sigmaSec * gauss() + drift); h = Math.max(h, p); l = Math.min(l, p); }
    out.push({ t: t0 + m * 60, o, h, l, c: p, v: 5 + rnd() * 10 });
  }
  return out;
}

// math sanity
ok(Math.abs(E.normCdf(0) - 0.5) < 1e-7 && Math.abs(E.normCdf(1.96) - 0.975) < 1e-4, 'normCdf');
ok(Math.abs(E.takerFee(0.5, 0.07) * 100 - 1.75) < 1e-9 && Math.abs(E.takerFee(0.1, 0.07) * 100 - 0.63) < 1e-9, 'Polymarket crypto fee matches docs table (100 sh @50c = $1.75, @10c = $0.63)');
ok(Math.abs(E.rsi([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]) - 100) < 1e-9, 'RSI all-up = 100');
const fit = E.logisticFit([[0], [1], [2], [3], [0.5], [2.5]], [0, 0, 1, 1, 0, 1], ['x'], 0.01);
ok(fit.w[0] > 0, 'logistic fit learns positive slope');

// 1) pure random walk: vol model should be near-calibrated, momentum should NOT help OOS
const c1 = synth(30 * 24 * 60 / 2, 0.00012); // ~15 days
const s1 = E.buildSamples(c1);
const r1 = E.walkForward(s1, { folds: 5 });
const g = id => r1.results.find(r => r.id === id).overall;
console.log('random walk:', r1.windows, 'windows,', r1.samples, 'samples');
r1.results.forEach(r => console.log('  ', r.name.padEnd(34), 'logloss', r.overall.logloss.toFixed(4), 'brier', r.overall.brier.toFixed(4), 'acc', (r.overall.accuracy * 100).toFixed(1) + '%'));
ok(g('vol').logloss < g('base50').logloss - 0.05, 'vol model beats coin flip on random walk');
ok(g('lzm').logloss > g('lz').logloss - 0.003, 'momentum adds no real OOS value on pure random walk (no false discovery)');
const cal = g('lz').calibration.filter(b => b.n > 200);
ok(cal.every(b => Math.abs(b.avgP - b.freq) < 0.06), 'distance-only logistic is calibrated within 6pts per bucket');

// 2) planted momentum: momentum model should beat distance-only
seed = 999;
const c2 = synth(30 * 24 * 60 / 2, 0.00012, 0.85);
const r2 = E.walkForward(E.buildSamples(c2), { folds: 5 });
const g2 = id => r2.results.find(r => r.id === id).overall;
console.log('planted momentum:'); r2.results.forEach(r => console.log('  ', r.name.padEnd(34), 'logloss', r.overall.logloss.toFixed(4)));
ok(g2('lzm').logloss < g2('lz').logloss - 0.003, 'momentum model detects planted momentum OOS');

// 3) leakage check: feature that peeks at the outcome must be impossible — features only see candles <= decision time
const smp = s1[100]; ok(smp.t <= smp.win + 240 && smp.tRem >= 60, 'samples are taken before window end');

// 4) decision gates
const cfg = { minFeeds: 2, maxDispersionBps: 8, minSecondsLeft: 15, volSpikeRatio: 3, marketWeight: 0.3, feeRate: 0.07, minEdge: 0.04, microTilt: 0 };
const base = { target: 70000, targetVerified: true, S: 70050, tRem: 120, sigma: 0.00012, freshCount: 3, dispersionBps: 1, tickVolRatio: 1, model: E.models.vol };
base.f = E.features({ S: base.S, K: base.target, tRem: 120, sigma: base.sigma, closes1m: [70000], candles1m: [] });
let d = E.decide(Object.assign({}, base, { market: { upAsk: 0.60, downAsk: 0.42, upMid: 0.59 } }), cfg);
console.log('  pModel', d.pModel.toFixed(3), 'pUp', d.pUp.toFixed(3), 'rec', d.rec, d.reason);
ok(d.rec === 'UP', 'underpriced UP -> UP');
d = E.decide(Object.assign({}, base, { targetVerified: false, market: { upAsk: 0.60, downAsk: 0.42, upMid: 0.59 } }), cfg);
ok(d.rec === 'NO TRADE' && /verified/.test(d.reason), 'unverified target -> NO TRADE');
d = E.decide(Object.assign({}, base, { market: { upAsk: d.pUp + 0.01, downAsk: 1 - d.pUp + 0.01, upMid: d.pUp } }), cfg);
ok(d.rec === 'NO TRADE', 'fairly priced market -> NO TRADE (edge below threshold after fees)');
d = E.decide(Object.assign({}, base, { freshCount: 1, market: { upAsk: 0.6, downAsk: 0.42 } }), cfg);
ok(d.rec === 'NO TRADE', 'stale feeds -> NO TRADE');
d = E.decide(Object.assign({}, base, { tRem: 8, market: { upAsk: 0.6, downAsk: 0.42 } }), cfg);
ok(d.rec === 'NO TRADE', 'too little time -> NO TRADE');
const mp = E.maxPrice(0.7, 0.04, 0.07); const evAt = E.ev(0.7, mp, 0.07);
ok(Math.abs(evAt.evPerDollar - 0.04) < 1e-6, 'maxPrice: buying at the max price leaves exactly the minimum edge (' + (mp * 100).toFixed(2) + 'c)');
console.log(fails ? `\n${fails} FAILED` : '\nALL PASSED'); process.exit(fails ? 1 : 0);
