// server.js
require("dotenv").config();
const express = require("express");
const WebSocket = require("ws");
const crypto = require("crypto");
const { RSI, SMA } = require("technicalindicators");

const app = express();
const PORT = process.env.APP_PORT || 3000;

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const TRADE_PASSWORD = process.env.TRADE_PASSWORD || "admin123";

const SYMBOLS = (process.env.SYMBOLS || "btcusdt,ethusdt,solusdt,dogeusdt,xrpusdt").split(",").map((s) => s.trim().toLowerCase());

const INTERVAL = "1m";

const marketData = {};
SYMBOLS.forEach((sym) => {
  marketData[sym.toUpperCase()] = {
    prices: [],
    volumes: [],
    lastRsi: null,
    lastVolumeSurge: false,
    lastSignalTime: 0,
  };
});

// Replace sendTelegramAlert in server.js
async function sendTelegramAlert(message) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.warn("[TELEGRAM SKIPPED] Missing TELEGRAM_TOKEN or TELEGRAM_CHAT_ID in .env");
    return;
  }

  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHAT_ID,
        text: message,
        parse_mode: "HTML",
      }),
    });

    const data = await res.json();

    if (!res.ok || !data.ok) {
      console.error("[TELEGRAM ERROR RESPONSE]", data);
    } else {
      console.log("[TELEGRAM SENT SUCCESS]", data.result.message_id);
    }
  } catch (err) {
    console.error("Telegram Network Error:", err.message);
  }
}

async function bootstrapHistoricalData() {
  console.log(`Bootstrapping historical candle data for: ${SYMBOLS.map((s) => s.toUpperCase()).join(", ")}...`);
  for (const symbol of SYMBOLS) {
    const symUpper = symbol.toUpperCase();
    try {
      const response = await fetch(`https://data-api.binance.vision/api/v3/klines?symbol=${symUpper}&interval=${INTERVAL}&limit=50`);
      const klines = await response.json();

      if (Array.isArray(klines) && klines.length >= 15) {
        const prices = klines.map((k) => parseFloat(k[4]));
        const volumes = klines.map((k) => parseFloat(k[5]));

        marketData[symUpper].prices = prices;
        marketData[symUpper].volumes = volumes;

        const rsiVals = RSI.calculate({ values: prices, period: 14 });
        if (rsiVals.length > 0) {
          marketData[symUpper].lastRsi = rsiVals[rsiVals.length - 1];
          console.log(`[${symUpper}] Bootstrapped ${prices.length} candles. Initial RSI: ${marketData[symUpper].lastRsi.toFixed(2)}`);
        }
      }
    } catch (err) {
      console.error(`Failed to bootstrap ${symbol}:`, err.message);
    }
  }
}

function connectMultiStreamWS() {
  const streamNames = SYMBOLS.map((s) => `${s}@kline_${INTERVAL}`).join("/");
  const wsUrl = `wss://data-stream.binance.com/stream?streams=${streamNames}`;

  console.log(`Connecting to multi-stream: ${wsUrl}`);
  const ws = new WebSocket(wsUrl);

  ws.on("open", () => {
    console.log(`Connected to Binance Multi-Stream for: ${SYMBOLS.map((s) => s.toUpperCase()).join(", ")}`);
  });

  ws.on("message", (data) => {
    try {
      const payload = JSON.parse(data);
      const kline = payload.data?.k;

      if (kline && kline.x) {
        const sym = kline.s;
        const closePrice = parseFloat(kline.c);
        const volume = parseFloat(kline.v);

        const target = marketData[sym];
        if (!target) return;

        target.prices.push(closePrice);
        target.volumes.push(volume);

        if (target.prices.length > 100) target.prices.shift();
        if (target.volumes.length > 100) target.volumes.shift();

        if (target.prices.length >= 15) {
          const rsiValues = RSI.calculate({ values: target.prices, period: 14 });
          if (rsiValues && rsiValues.length > 0) {
            target.lastRsi = rsiValues[rsiValues.length - 1];
          }
        }

        let isVolumeSurge = false;
        if (target.volumes.length >= 20) {
          const volSmaValues = SMA.calculate({ values: target.volumes, period: 20 });
          if (volSmaValues && volSmaValues.length > 0) {
            const avgVolume = volSmaValues[volSmaValues.length - 1];
            isVolumeSurge = volume > avgVolume * 1.8;
          }
        }
        target.lastVolumeSurge = isVolumeSurge;

        const rsiDisplay = typeof target.lastRsi === "number" ? target.lastRsi.toFixed(2) : "Calculating...";
        console.log(`[${sym}] Close: $${closePrice} | RSI: ${rsiDisplay} | Vol Surge: ${isVolumeSurge ? "YES" : "No"}`);

        // Signal Checks (5-min cooldown)
        const now = Date.now();
        if (target.lastRsi !== null && now - target.lastSignalTime > 5 * 60 * 1000) {
          // BUY SIGNAL: RSI <= 32
          if (target.lastRsi <= 32 && isVolumeSurge) {
            const entry = closePrice;
            const msg =
              `⚡ <b>BUY SIGNAL (${sym})</b>\n\n` +
              `<b>RSI:</b> ${target.lastRsi.toFixed(2)} (Oversold)\n` +
              `<b>Price:</b> $${entry}\n` +
              `🎯 <b>Take Profit (+2%):</b> $${(entry * 1.02).toFixed(4)}\n` +
              `🛡️ <b>Stop Loss (-0.8%):</b> $${(entry * 0.992).toFixed(4)}`;

            sendTelegramAlert(msg);
            target.lastSignalTime = now;
          }
          // SELL SIGNAL: RSI >= 70 (Overbought / Take Profit Zone)
          else if (target.lastRsi >= 70) {
            const msg =
              `🚨 <b>SELL / TAKE PROFIT SIGNAL (${sym})</b>\n\n` +
              `<b>RSI:</b> ${target.lastRsi.toFixed(2)} (Overbought)\n` +
              `<b>Price:</b> $${closePrice}\n` +
              `💡 Consider cashing out your position to USDT!`;

            sendTelegramAlert(msg);
            target.lastSignalTime = now;
          }
        }
      }
    } catch (err) {
      console.error("Message processing error:", err.message);
    }
  });

  ws.on("error", (err) => console.error("WebSocket Error:", err.message));
  ws.on("close", () => {
    console.warn("Multi-stream WS closed. Reconnecting in 3s...");
    setTimeout(connectMultiStreamWS, 3000);
  });
}

app.get("/api/status", (req, res) => {
  const marketsList = Object.keys(marketData).map((sym) => {
    const prices = marketData[sym].prices;
    const rsiVal = marketData[sym].lastRsi;
    return {
      symbol: sym,
      price: prices.length > 0 ? prices[prices.length - 1] : null,
      history: prices.slice(-20),
      rsi: typeof rsiVal === "number" ? parseFloat(rsiVal.toFixed(2)) : null,
      volSurge: marketData[sym].lastVolumeSurge,
    };
  });

  res.json({ interval: INTERVAL, markets: marketsList });
});

app.post("/api/trade", express.json(), async (req, res) => {
  const { symbol, side, usdtAmount, password } = req.body;

  if (!password || password !== TRADE_PASSWORD) {
    console.warn(`[UNAUTHORIZED ATTEMPT] Bad trading password provided for ${symbol}`);
    return res.status(401).json({ success: false, error: "Unauthorized: Incorrect trading password." });
  }

  const apiKey = process.env.BINANCE_API_KEY;
  const secretKey = process.env.BINANCE_SECRET_KEY;

  if (!apiKey || !secretKey) {
    return res.status(500).json({ success: false, error: "Binance API keys missing from .env" });
  }

  try {
    const timestamp = Date.now();
    const tradeAmount = usdtAmount || 5.5;

    const query = `symbol=${symbol.toUpperCase()}&side=${side.toUpperCase()}&type=MARKET&quoteOrderQty=${tradeAmount}&timestamp=${timestamp}`;
    const signature = crypto.createHmac("sha256", secretKey).update(query).digest("hex");

    const response = await fetch(`https://api.binance.com/api/v3/order?${query}&signature=${signature}`, {
      method: "POST",
      headers: {
        "X-MBX-APIKEY": apiKey,
        "Content-Type": "application/x-www-form-urlencoded",
      },
    });

    const result = await response.json();

    if (result.orderId) {
      console.log(`[TRADE EXECUTED] ${side} ${symbol} ($${tradeAmount}) - OrderID: ${result.orderId}`);
      sendTelegramAlert(
        `✅ <b>AUTHORIZED TRADE EXECUTED</b>\n\n` +
          `<b>Symbol:</b> ${symbol.toUpperCase()}\n` +
          `<b>Type:</b> MARKET ${side.toUpperCase()}\n` +
          `<b>Amount:</b> $${tradeAmount} USDT\n` +
          `<b>Order ID:</b> <code>${result.orderId}</code>`,
      );
      return res.json({ success: true, orderId: result.orderId, details: result });
    } else {
      console.error("[TRADE REJECTED]", result);
      return res.status(400).json({ success: false, error: result.msg || "Order rejected by Binance" });
    }
  } catch (err) {
    console.error("Execution exception:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.use(express.static("public"));

(async () => {
  await bootstrapHistoricalData();
  connectMultiStreamWS();

  app.listen(PORT, () => {
    console.log(`Dashboard running on http://localhost:${PORT}`);

    // Test Telegram connection on startup
    sendTelegramAlert("🔔 <b>Trader Engine Online</b>\nTelegram notifications are configured successfully!");
  });
})();
