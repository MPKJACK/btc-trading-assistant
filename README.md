# BTC 5M Intelligence

A phone web app (installable PWA) that watches Polymarket's **Bitcoin Up or Down 5-minute** markets and shows **UP / DOWN / NO TRADE** with an estimated probability, a cost after fees, and the reasons behind it. It is paper trading only. It never places trades or touches any account.

**Live app:** https://mpkjack.github.io/btc-trading-assistant/

## Install on iPhone
1. Open the live URL in **Safari**.
2. Tap Share, then **Add to Home Screen**. It opens full-screen with its own icon.
3. Keep it in the foreground while you use it. iOS pauses web apps in the background.

## Repository layout
| Path | What it is |
|---|---|
| `src/engine.js` | Indicators, probability models, decision gates, fees, walk-forward backtest. Pure functions, no network |
| `src/app.js` | Live feeds, market tracking, UI, paper trading, alerts |
| `src/index.html` | Page template, styles |
| `src/sw.js` | Service worker template (offline shell; never touches API traffic) |
| `index.html`, `sw.js` | **Built output** served by Pages. Rebuild with `python3 build.py` after editing `src/` |
| `manifest.webmanifest`, `icons/` | PWA manifest and app icons (`tools/make_icons.py` regenerates them) |
| `test/` | `node test/test_engine.js` (engine) and `python3 test/ui_test.py` (headless browser with simulated feeds) |
| `.github/workflows/pages.yml` | Tests, then deploys to GitHub Pages, then checks the live site |
| `.github/workflows/endpoints.yml` | Probes every data source from a GitHub US runner (WebSockets, REST, CORS) |

## Secrets and backend
**The app holds no secrets, and there are none in this repository.** Every data source it uses is public and needs no key, so no backend is required today. Everything runs in your browser, and paper-trading data stays in your phone's local storage.

You'd only need a backend if Polymarket shuts down the public Chainlink price stream (RTDS). Polymarket has marked it deprecated in favor of PolyBolt, which needs CLOB API credentials. Those credentials must never ship in this public front-end. The plan in that case:

- **Relay:** a small serverless relay (for example a Cloudflare Worker) that stores the credentials as encrypted Worker secrets.
- **Connection:** the relay connects to PolyBolt and rebroadcasts only the public BTC/USD reference price to the app over its own WebSocket.
- **Lockdown:** restrict the relay to this site's origin and rate-limit it.

The "Check live data sources" workflow shows whether this is needed.

## Data sources (all public, no keys)
| Source | Use |
|---|---|
| Polymarket RTDS `crypto_prices_chainlink` (btc/usd) | Settlement-source price; captures the price to beat at window open |
| Polymarket Gamma `events?slug=btc-updown-5m-{start}` | Market and token IDs; official result after close |
| Polymarket CLOB market WebSocket | Live UP/DOWN best bid and ask |
| Coinbase, Kraken, Bitstamp, Binance.US WebSockets | Cross-check prices, trade flow, order books, outage and disagreement checks |
| Coinbase REST 1-min candles (fallback: Binance.US) | Indicator history and backtest |

## How the decision works
1. **Probability.** P(UP) = Φ(ln(S/K) / σ√t), where S = Chainlink price, K = price to beat, t = seconds left, and σ comes from the last 60 one-minute returns. After you run a backtest, you can switch to a logistic model, but only if it beat this one on held-out data.
2. **Blend with the market** (default weight 30%), then compute expected value against the actual ask **plus Polymarket's crypto taker fee** (fee per share = 0.07 × p × (1 − p)).
3. **NO TRADE** if any of these hold:
   - the target is unverified
   - fewer than 2 fresh price feeds
   - exchanges disagree by more than 10 bps
   - under 15 s left
   - a volatility spike (over 3× normal)
   - momentum conflict
   - edge below the minimum (default 4%)

   All of these thresholds can be changed in Settings.

**Target safety:** the target counts as verified only when one of these is true:
- it was captured from Chainlink within 1 s of the window opening
- Polymarket's API supplied it
- you confirmed it or typed it in

An estimated target always gives NO TRADE.

## What was tested here, and what wasn't
- **Engine (Node, synthetic data): all tests pass.**
  - The fee matches Polymarket's published table.
  - On a pure random walk, the momentum features did *not* produce a false edge.
  - When momentum was planted in the data, the walk-forward test found it.
  - The gates block stale feeds, unverified targets and fairly priced markets.
- **UI (headless phone browser with simulated feeds):** all tabs render with no JS errors. The flows tested were: capturing the target, entering it manually, paper trades, and the backtest.
- **Not tested against the real services.** This build environment can't reach exchange or Polymarket servers. Things to watch on first launch:
  - **Gamma API from a browser (CORS).** If it fails, the PM dot stays red. Use **Enter prices** and **Edit** target.
  - **Polymarket RTDS.** Polymarket has marked its price topics deprecated in favor of PolyBolt, which needs API credentials. If the CL dot goes red, the app falls back to exchange prices plus the last measured basis, and asks you to confirm the target each window.
  - **Picture-in-Picture** on iOS.
  - If something fails, check **Settings → Diagnostics → Copy log** and send it to Claude.

## Honest expectations
These markets are heavily traded by bots that see the same Chainlink and exchange data. The market price already includes distance to target and time left. That's why high "accuracy" (often 75%+) means nothing here. The only test that matters is on the Paper tab: does the **model's Brier score beat the market's** over a few hundred windows, and is paper P&L positive after fees? Until both are true, treat any recommendation as unproven.
