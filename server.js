// server.js
require("dotenv").config();
const express = require("express");
const WebSocket = require("ws");
const crypto = require("crypto");
const { RSI, SMA } = require("technicalindicators");
const Database = require("better-sqlite3");

const app = express();
const PORT = process.env.APP_PORT || 3000;

// Database Setup
const DB_PATH = process.env.DB_PATH || "trades.db";
const db = new Database(DB_PATH);

// Initialize DB Table for Trade Logging
db.exec(`
  CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    price REAL NOT NULL,
    qty REAL NOT NULL,
    usdt_amount REAL NOT NULL,
    order_id TEXT NOT NULL,
    timestamp INTEGER NOT NULL
  )
`);

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

async function sendTelegramAlert(message) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
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
    if (!res.ok || !data.ok) console.error("[TELEGRAM ERROR]", data);
  } catch (err) {
    console.error("Telegram Error:", err.message);
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

        const now = Date.now();
        if (target.lastRsi !== null && now - target.lastSignalTime > 5 * 60 * 1000) {
          if (target.lastRsi <= 32 && isVolumeSurge) {
            sendTelegramAlert(`⚡ <b>BUY SIGNAL (${sym})</b>\n\n` + `<b>RSI:</b> ${target.lastRsi.toFixed(2)} | <b>Price:</b> $${closePrice}`);
            target.lastSignalTime = now;
          } else if (target.lastRsi >= 70) {
            sendTelegramAlert(`🚨 <b>SELL SIGNAL (${sym})</b>\n\n` + `<b>RSI:</b> ${target.lastRsi.toFixed(2)} | <b>Price:</b> $${closePrice}`);
            target.lastSignalTime = now;
          }
        }
      }
    } catch (err) {
      console.error("WS Message processing error:", err.message);
    }
  });

  ws.on("error", (err) => console.error("WebSocket Error:", err.message));
  ws.on("close", () => setTimeout(connectMultiStreamWS, 3000));
}

// 1. Dashboard Status API Endpoint
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

// 2. Binance Spot Holdings API Endpoint
app.get("/api/holdings", async (req, res) => {
  const apiKey = process.env.BINANCE_API_KEY;
  const secretKey = process.env.BINANCE_SECRET_KEY;

  if (!apiKey || !secretKey) {
    return res.status(500).json({ error: "Missing Binance API credentials" });
  }

  try {
    const timestamp = Date.now();
    const query = `timestamp=${timestamp}`;
    const signature = crypto.createHmac("sha256", secretKey).update(query).digest("hex");

    const response = await fetch(`https://api.binance.com/api/v3/account?${query}&signature=${signature}`, {
      headers: { "X-MBX-APIKEY": apiKey },
    });

    const data = await response.json();

    if (!data.balances) {
      return res.status(400).json({ error: data.msg || "Could not fetch balances" });
    }

    // Filter non-zero balances for USDT and tracked symbols
    const trackedAssets = new Set(["USDT", ...SYMBOLS.map((s) => s.toUpperCase().replace("USDT", ""))]);

    const activeBalances = data.balances
      .filter((b) => {
        const total = parseFloat(b.free) + parseFloat(b.locked);
        return trackedAssets.has(b.asset) && total > 0.0001;
      })
      .map((b) => {
        const free = parseFloat(b.free);
        const locked = parseFloat(b.locked);
        const total = free + locked;
        let usdVal = total;

        if (b.asset !== "USDT") {
          const pair = `${b.asset}USDT`;
          const currentPrice = marketData[pair]?.prices.slice(-1)[0] || 0;
          usdVal = total * currentPrice;
        }

        return {
          asset: b.asset,
          free: free.toFixed(4),
          locked: locked.toFixed(4),
          total: total.toFixed(4),
          usdValue: usdVal.toFixed(2),
        };
      });

    res.json({ success: true, balances: activeBalances });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 3. Trade History & PnL Analytics Endpoint
app.get("/api/pnl", (req, res) => {
  const timeframe = req.query.tf || "all"; // 'day', 'week', 'month', 'all'
  let timeFilter = 0;
  const now = Date.now();

  if (timeframe === "day") timeFilter = now - 24 * 60 * 60 * 1000;
  else if (timeframe === "week") timeFilter = now - 7 * 24 * 60 * 60 * 1000;
  else if (timeframe === "month") timeFilter = now - 30 * 24 * 60 * 60 * 1000;

  const trades = db.prepare("SELECT * FROM trades WHERE timestamp >= ? ORDER BY timestamp ASC").all(timeFilter);

  // Group trades per symbol to compute FIFO Realized PnL
  const assetPnL = {};
  let totalRealizedUsdt = 0;
  let totalVolumeTraded = 0;

  trades.forEach((t) => {
    const sym = t.symbol.toUpperCase();
    if (!assetPnL[sym]) {
      assetPnL[sym] = { symbol: sym, buyQty: 0, buyCost: 0, realizedPnl: 0, tradeCount: 0 };
    }

    assetPnL[sym].tradeCount++;
    totalVolumeTraded += t.usdt_amount;

    if (t.side.toUpperCase() === "BUY") {
      assetPnL[sym].buyQty += t.qty;
      assetPnL[sym].buyCost += t.usdt_amount;
    } else if (t.side.toUpperCase() === "SELL") {
      if (assetPnL[sym].buyQty > 0) {
        const avgBuyPrice = assetPnL[sym].buyCost / assetPnL[sym].buyQty;
        const costBasisForSale = t.qty * avgBuyPrice;
        const pnl = t.usdt_amount - costBasisForSale;

        assetPnL[sym].realizedPnl += pnl;
        totalRealizedUsdt += pnl;

        // Reduce open buy basis proportion
        assetPnL[sym].buyQty = Math.max(0, assetPnL[sym].buyQty - t.qty);
        assetPnL[sym].buyCost = Math.max(0, assetPnL[sym].buyCost - costBasisForSale);
      }
    }
  });

  res.json({
    timeframe,
    totalRealizedPnl: parseFloat(totalRealizedUsdt.toFixed(2)),
    totalVolumeTraded: parseFloat(totalVolumeTraded.toFixed(2)),
    assets: Object.values(assetPnL).map((a) => ({
      ...a,
      realizedPnl: parseFloat(a.realizedPnl.toFixed(2)),
    })),
    tradeCount: trades.length,
  });
});

// 4. Password Protected Trade Endpoint with SQLite Logging
app.post("/api/trade", express.json(), async (req, res) => {
  const { symbol, side, usdtAmount, password } = req.body;

  if (!password || password !== TRADE_PASSWORD) {
    return res.status(401).json({ success: false, error: "Unauthorized: Incorrect password." });
  }

  const apiKey = process.env.BINANCE_API_KEY;
  const secretKey = process.env.BINANCE_SECRET_KEY;

  if (!apiKey || !secretKey) {
    return res.status(500).json({ success: false, error: "Missing API keys." });
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
      const executedPrice = parseFloat(result.fills?.[0]?.price || marketData[symbol.toUpperCase()]?.prices.slice(-1)[0] || 0);
      const executedQty = parseFloat(result.executedQty || tradeAmount / executedPrice);

      // Save Trade Record into SQLite
      db.prepare(
        `
        INSERT INTO trades (symbol, side, price, qty, usdt_amount, order_id, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `,
      ).run(symbol.toUpperCase(), side.toUpperCase(), executedPrice, executedQty, tradeAmount, String(result.orderId), timestamp);

      sendTelegramAlert(
        `✅ <b>TRADE EXECUTED (${side.toUpperCase()})</b>\n\n` +
          `<b>Symbol:</b> ${symbol.toUpperCase()}\n` +
          `<b>Amount:</b> $${tradeAmount} USDT\n` +
          `<b>Executed Price:</b> $${executedPrice}\n` +
          `<b>Order ID:</b> <code>${result.orderId}</code>`,
      );

      return res.json({ success: true, orderId: result.orderId, details: result });
    } else {
      return res.status(400).json({ success: false, error: result.msg || "Order rejected by Binance" });
    }
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.use(express.static("public"));

(async () => {
  await bootstrapHistoricalData();
  connectMultiStreamWS();
  app.listen(PORT, () => console.log(`Terminal running on http://localhost:${PORT}`));
})();
