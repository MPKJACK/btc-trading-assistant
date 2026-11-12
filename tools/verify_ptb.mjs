// Checks that the app's target capture (Chainlink RTDS tick at the window-open second)
// equals Polymarket's official "price to beat" (openPrice) for real windows.
const WINDOWS = +(process.argv[2] || 2);
const ticks = [];
const ws = new WebSocket('wss://ws-live-data.polymarket.com');
ws.onopen = () => { ws.send(JSON.stringify({ action: 'subscribe', subscriptions: [{ topic: 'crypto_prices_chainlink', type: '*', filters: JSON.stringify({ symbol: 'btc/usd' }) }] })); setInterval(() => ws.send('PING'), 5000); };
ws.onmessage = (e) => { try { const m = JSON.parse(e.data); const p = m.payload; if (!p) return; const list = Array.isArray(p.data) ? p.data : [p]; for (const x of list) ticks.push({ t: +x.timestamp, v: +x.value, recv: Date.now() }); } catch (err) {} };
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const iso = (t) => new Date(t * 1000).toISOString();
let first = Math.floor(Date.now() / 300000) * 300 + 300;
console.log('| Window start | Official openPrice | Tick @ exact second | First tick ≥ open | Last tick < open | Match |\n|---|---|---|---|---|---|');
let allOk = true;
for (let k = 0; k < WINDOWS; k++) {
  const t0 = first + 300 * k;
  while (Date.now() < (t0 + 20) * 1000) await sleep(1000);
  let off = null;
  for (let i = 0; i < 8 && !(off && off.openPrice); i++) { try { off = await (await fetch(`https://polymarket.com/api/crypto/crypto-price?symbol=BTC&eventStartTime=${encodeURIComponent(iso(t0))}&variant=fiveminute&endDate=${encodeURIComponent(iso(t0 + 300))}`)).json(); } catch (e) {} if (!(off && off.openPrice)) await sleep(5000); }
  const exact = ticks.find(x => x.t === t0 * 1000), ge = ticks.find(x => x.t >= t0 * 1000), lt = ticks.filter(x => x.t < t0 * 1000).pop();
  const ok = exact && off && Math.abs(exact.v - off.openPrice) < 1e-6; allOk = allOk && !!ok;
  console.log(`| ${iso(t0)} | ${off && off.openPrice} | ${exact ? exact.v : 'none'} | ${ge ? ge.v + ' @' + (ge.t - t0 * 1000) + 'ms' : 'none'} | ${lt ? lt.v : 'none'} | ${ok ? 'EXACT' : 'NO'} |`);
}
const gaps = []; for (let i = 1; i < ticks.length; i++) gaps.push(ticks[i].t - ticks[i - 1].t);
console.log(`\nTicks: ${ticks.length}; timestamp spacing ms (min/median/max): ${Math.min(...gaps)}/${gaps.sort((a, b) => a - b)[gaps.length >> 1]}/${Math.max(...gaps)}; all second-aligned: ${ticks.every(x => x.t % 1000 === 0)}`);
process.exit(allOk ? 0 : 1);
