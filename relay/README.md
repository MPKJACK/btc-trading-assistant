# Optional relay (Cloudflare Worker)

**Why:** Polymarket's official "price to beat" endpoint (`polymarket.com/api/crypto/crypto-price`) works, but it sends no CORS header, so the app (running in Safari) isn't allowed to read it directly.

**What the app does without the relay:** it captures the exact Chainlink price at each window's open, which is the same value Polymarket uses, and the "Verify target capture" workflow checks this. If you open the app mid-window, it waits for the next window (at most 5 minutes) before making a call.

**What the relay adds:**
- Official targets even when you open the app mid-window.
- Official final prices for the Paper results.

**Secrets:** none. It forwards one public read-only endpoint and only answers requests from `https://mpkjack.github.io`.

## Deploy (free, about 3 minutes, works from a phone browser)
1. Sign up at dash.cloudflare.com (free plan).
2. Go to **Workers & Pages**, then **Create**, then **Create Worker**. Name it `btc5m-relay`, then **Deploy**.
3. Tap **Edit code**, replace everything with the contents of `relay/worker.js`, then **Deploy**.
4. Copy the URL, which looks like `https://btc5m-relay.<you>.workers.dev`.
5. In the app, open **Settings**, then **Data sources**, then **Relay URL**, and paste it.

Test it in Safari: `https://btc5m-relay.<you>.workers.dev/crypto-price?symbol=BTC&variant=fiveminute&eventStartTime=2026-11-12T15:00:00Z&endDate=2026-11-12T15:05:00Z`
