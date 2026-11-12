// Walk-forward backtest on real Coinbase 1-min BTC-USD candles. Prints results and the fitted live coefficients.
import { createRequire } from 'module'; const E = createRequire(import.meta.url)('../src/engine.js');
const days = +(process.argv[2] || 30); const end = Math.floor(Date.now() / 300000) * 300, start = end - days * 86400;
const out = new Map(); const sleep = (ms) => new Promise(r => setTimeout(r, ms));
for (let a = start; a < end; a += 18000) {
  const b = Math.min(end, a + 18000); const url = `https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=60&start=${new Date(a * 1000).toISOString()}&end=${new Date(b * 1000).toISOString()}`;
  for (let k = 0; k < 4; k++) { try { const r = await fetch(url); if (r.status === 429) { await sleep(1500); continue; } const rows = await r.json(); for (const x of rows) out.set(x[0], { t: x[0], l: +x[1], h: +x[2], o: +x[3], c: +x[4], v: +x[5] }); break; } catch (e) { await sleep(800); } }
  await sleep(140);
}
const c = [...out.values()].sort((a, b) => a.t - b.t);
const samples = E.buildSamples(c); const r = E.walkForward(samples, { folds: 6, warmFrac: 0.4 });
const f = (x, d = 4) => (+x).toFixed(d);
console.log(`## Real-data walk-forward backtest (Coinbase BTC-USD, ${days} days)\n\n${c.length} candles, ${r.windows} windows, ${r.samples} samples, ${r.testWindows} out-of-sample windows (${new Date(r.from * 1000).toISOString()} → ${new Date(r.to * 1000).toISOString()})\n`);
console.log('| Model | Log loss | Brier | Accuracy | UP acc | DOWN acc |\n|---|---|---|---|---|---|');
for (const m of r.results) console.log(`| ${m.id === r.bestId ? '**' + m.name + '**' : m.name} | ${f(m.overall.logloss)} | ${f(m.overall.brier)} | ${f(m.overall.accuracy * 100, 1)}% | ${f(m.overall.upAcc * 100, 1)}% | ${f(m.overall.downAcc * 100, 1)}% |`);
const lz = r.results.find(m => m.id === 'lz');
console.log('\nBy time remaining (log loss):\n\n| Model | 4:00 | 3:00 | 2:00 | 1:00 |\n|---|---|---|---|---|');
for (const m of r.results) console.log(`| ${m.name} | ${[240, 180, 120, 60].map(t => f(m.byTRem[t].logloss)).join(' | ')} |`);
// Chart-only skill: how often does the chart model call direction right when distance is ~0 (|z|<0.25)?
console.log('\nGain vs distance-only (positive = chart features help): ' + r.results.filter(m => m.feats && m.feats.length > 1).map(m => `${m.id}: ${f(lz.overall.logloss - m.overall.logloss, 5)}`).join(', '));
// Bootstrap the log-loss difference for the chart model to see if it's distinguishable from noise
const ch = r.results.find(m => m.id === 'lchart');
// Refit chosen chart model on all data for live use
const spec = E.MODEL_SPECS.find(m => m.id === 'lchart');
const fit = E.logisticFit(samples.map(s => spec.feats.map(n => s.f[n])), samples.map(s => s.y), spec.feats, 1);
const fitBest = r.liveFit;
console.log('\nLIVEFIT_CHART ' + JSON.stringify({ fit, name: spec.name, oosLogloss: ch.overall.logloss, lzLogloss: lz.overall.logloss, windows: r.windows, testWindows: r.testWindows, to: r.to }));
console.log('LIVEFIT_BEST ' + JSON.stringify({ id: r.bestId, fit: fitBest }));
