// server.js
require("dotenv").config();
const express = require("express");
const WebSocket = require("ws");
const { RSI, SMA } = require("technicalindicators");
const crypto = require("crypto");

const app = express();
const PORT = process.env.APP_PORT || 3000;

// Telegram Config
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

// Multi-pair setup
const SYMBOLS = ["btcusdt", "ethusdt", "solusdt", "dogeusdt"];
const INTERVAL = "1m";

// Memory store for prices & volumes
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

// Send Telegram Message
async function sendTelegramAlert(message) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;

  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  try {
    await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHAT_ID,
        text: message,
        parse_mode: "HTML",
      }),
    });
  } catch (err) {
    console.error("Telegram Notification Error:", err.message);
  }
}

// Step 1: Pre-fill historical candle data via REST API
async function bootstrapHistoricalData() {
  console.log("Bootstrapping historical candle data...");
  for (const symbol of SYMBOLS) {
    const symUpper = symbol.toUpperCase();
    try {
      // Use data-stream mirror host for REST or fallback
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
      } else {
        console.warn(`[${symUpper}] Could not fetch historical candles, starting fresh via WS.`);
      }
    } catch (err) {
      console.error(`Failed to bootstrap ${symbol}:`, err.message);
    }
  }
}

// Step 2: Connect to Binance Combined WebSocket Stream
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

        // Calculate RSI safely
        if (target.prices.length >= 15) {
          const rsiValues = RSI.calculate({ values: target.prices, period: 14 });
          if (rsiValues && rsiValues.length > 0) {
            target.lastRsi = rsiValues[rsiValues.length - 1];
          }
        }

        // Calculate 20-period Volume Moving Average safely
        let isVolumeSurge = false;
        if (target.volumes.length >= 20) {
          const volSmaValues = SMA.calculate({ values: target.volumes, period: 20 });
          if (volSmaValues && volSmaValues.length > 0) {
            const avgVolume = volSmaValues[volSmaValues.length - 1];
            isVolumeSurge = volume > avgVolume * 1.8;
          }
        }
        target.lastVolumeSurge = isVolumeSurge;

        // Format RSI display safely
        const rsiDisplay = typeof target.lastRsi === "number" ? target.lastRsi.toFixed(2) : "Calculating...";

        console.log(`[${sym}] Close: $${closePrice} | RSI: ${rsiDisplay} | Vol Surge: ${isVolumeSurge ? "YES" : "No"}`);

        // Check for Buy Signals (5-min cooldown)
        const now = Date.now();
        if (target.lastRsi !== null && now - target.lastSignalTime > 5 * 60 * 1000) {
          if (target.lastRsi <= 32 && isVolumeSurge) {
            const entry = closePrice;
            const takeProfit = (entry * 1.02).toFixed(4);
            const stopLoss = (entry * 0.992).toFixed(4);

            const msg =
              `⚡ <b>HIGH PROBABILITY BUY SIGNAL</b>\n\n` +
              `<b>Coin:</b> ${sym}\n` +
              `<b>RSI:</b> ${target.lastRsi.toFixed(2)} (Oversold)\n` +
              `<b>Volume:</b> Surge Detected (>1.8x avg)\n` +
              `<b>Entry Price:</b> $${entry}\n\n` +
              `🎯 <b>Take Profit (+2%):</b> $${takeProfit}\n` +
              `🛡️ <b>Stop Loss (-0.8%):</b> $${stopLoss}\n\n` +
              `<a href="https://www.binance.com/en/trade/${sym}">Trade on Binance</a>`;

            sendTelegramAlert(msg);
            target.lastSignalTime = now;
          }
        }
      }
    } catch (err) {
      console.error("Message processing error:", err.message);
    }
  });

  ws.on("error", (err) => {
    console.error("WebSocket Error:", err.message);
  });

  ws.on("close", () => {
    console.warn("Multi-stream WS closed. Reconnecting in 3s...");
    setTimeout(connectMultiStreamWS, 3000);
  });
}

// Dashboard API Endpoint
app.get("/api/status", (req, res) => {
  const marketsList = Object.keys(marketData).map((sym) => {
    const prices = marketData[sym].prices;
    const rsiVal = marketData[sym].lastRsi;
    return {
      symbol: sym,
      price: prices.length > 0 ? prices[prices.length - 1] : null,
      history: prices.slice(-20), // Send the last 20 price points for instant charts
      rsi: typeof rsiVal === "number" ? parseFloat(rsiVal.toFixed(2)) : null,
      volSurge: marketData[sym].lastVolumeSurge,
    };
  });

  res.json({
    interval: INTERVAL,
    markets: marketsList,
  });
});

// Trade Execution Endpoint
app.post("/api/trade", express.json(), async (req, res) => {
  const { symbol, side, usdtAmount } = req.body;
  const apiKey = process.env.BINANCE_API_KEY;
  const secretKey = process.env.BINANCE_SECRET_KEY;

  if (!apiKey || !secretKey) {
    return res.status(500).json({ success: false, error: "Binance API keys missing from .env" });
  }

  try {
    const timestamp = Date.now();
    const tradeAmount = usdtAmount || process.env.DEFAULT_TRADE_AMOUNT_USDT || 5.0;

    // Construct signed query for Market Order using quoteOrderQty (USDT amount)
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

      // Notify Telegram on execution
      sendTelegramAlert(
        `✅ <b>TRADE EXECUTED VIA APP</b>\n\n` +
          `<b>Symbol:</b> ${symbol.toUpperCase()}\n` +
          `<b>Type:</b> MARKET ${side}\n` +
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

// Start server after bootstrapping
(async () => {
  await bootstrapHistoricalData();
  connectMultiStreamWS();

  app.listen(PORT, () => {
    console.log(`Dashboard running on http://localhost:${PORT}`);
  });
})();
