// server.js
require("dotenv").config();
const express = require("express");
const WebSocket = require("ws");
const crypto = require("crypto");
const { RSI, SMA } = require("technicalindicators");
const initSqlJs = require("sql.js");
const fs = require("fs");
const path = require("path");

const app = express();
const PORT = process.env.APP_PORT || 3000;

const DB_PATH = process.env.DB_PATH || path.join(__dirname, "trades.db");
let db;

// Memory store for user's active Binance spot balances
const availableBalances = { USDT: 0 };

// Default Strategy Config (fallback if database is empty)
let config = {
  rsiOversold: 32,
  rsiOverbought: 70,
  volumeSurgeMultiplier: 1.8,
  tradeAmountUsdt: 5.5,
  cooldownMinutes: 5,
  takeProfitPercent: 1.5, // Min gain % required to trigger SELL alert
  stopLossPercent: 2.0, // Max loss % before triggering Stop Loss alert
};

function saveDatabase() {
  if (!db) return;
  const data = db.export();
  fs.writeFileSync(DB_PATH, Buffer.from(data));
}

async function initDatabase() {
  const SQL = await initSqlJs();

  if (fs.existsSync(DB_PATH)) {
    db = new SQL.Database(fs.readFileSync(DB_PATH));
    console.log("Loaded existing database from disk.");
  } else {
    db = new SQL.Database();
    console.log("Created fresh SQLite database.");
  }

  // Trades table
  db.run(`
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

  // Settings table for dynamic UI configuration
  db.run(`
    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )
  `);

  loadSettingsFromDb();
  saveDatabase();
}

function loadSettingsFromDb() {
  try {
    const stmt = db.prepare("SELECT key, value FROM settings");
    while (stmt.step()) {
      const row = stmt.getAsObject();
      if (row.key in config) {
        config[row.key] = parseFloat(row.value);
      }
    }
    stmt.free();
    console.log("Loaded strategy config from DB:", config);
  } catch (err) {
    console.error("Error loading settings from DB:", err.message);
  }
}

function saveSettingToDb(key, value) {
  db.run(`INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)`, [key, String(value)]);
  saveDatabase();
}

// Calculate FIFO Average Entry Price for currently open position
function getAverageEntryPrice(symbol) {
  try {
    const stmt = db.prepare("SELECT side, qty, usdt_amount FROM trades WHERE UPPER(symbol) = ? ORDER BY timestamp ASC");
    stmt.bind([symbol.toUpperCase()]);

    let totalQty = 0;
    let totalCost = 0;

    while (stmt.step()) {
      const trade = stmt.getAsObject();
      if (trade.side === "BUY") {
        totalQty += trade.qty;
        totalCost += trade.usdt_amount;
      } else if (trade.side === "SELL") {
        if (totalQty > 0) {
          const avgPrice = totalCost / totalQty;
          const costBasisForSale = trade.qty * avgPrice;
          totalCost = Math.max(0, totalCost - costBasisForSale);
          totalQty = Math.max(0, totalQty - trade.qty);
        }
      }
    }
    stmt.free();

    return totalQty > 0 ? totalCost / totalQty : null;
  } catch (err) {
    console.error(`Error calculating entry price for ${symbol}:`, err.message);
    return null;
  }
}

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
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
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.warn("[TELEGRAM SKIPPED] Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in .env");
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
      console.error("[TELEGRAM API REJECTED]", data);
    }
  } catch (err) {
    console.error("Telegram Network Error:", err.message);
  }
}

async function updateAccountBalances() {
  const apiKey = process.env.BINANCE_API_KEY;
  const secretKey = process.env.BINANCE_SECRET_KEY;
  if (!apiKey || !secretKey) return;

  try {
    const timestamp = Date.now();
    const query = `timestamp=${timestamp}`;
    const signature = crypto.createHmac("sha256", secretKey).update(query).digest("hex");

    const response = await fetch(`https://api.binance.com/api/v3/account?${query}&signature=${signature}`, {
      headers: { "X-MBX-APIKEY": apiKey },
    });

    const data = await response.json();
    if (data.balances) {
      data.balances.forEach((b) => {
        const free = parseFloat(b.free);
        if (free > 0) availableBalances[b.asset] = free;
        else delete availableBalances[b.asset];
      });
    }
  } catch (err) {
    console.error("Failed to update background balances:", err.message);
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
            isVolumeSurge = volume > avgVolume * config.volumeSurgeMultiplier;
          }
        }
        target.lastVolumeSurge = isVolumeSurge;

        const now = Date.now();
        const cooldownMs = config.cooldownMinutes * 60 * 1000;

        if (target.lastRsi !== null && now - target.lastSignalTime > cooldownMs) {
          const baseAsset = sym.replace("USDT", "");
          const currentAssetBalance = availableBalances[baseAsset] || 0;
          const currentAssetUsdVal = currentAssetBalance * closePrice;
          const usdtBalance = availableBalances["USDT"] || 0;

          // 1. CONDITIONAL BUY SIGNAL: RSI <= Oversold AND Vol Surge AND Free USDT >= config.tradeAmountUsdt
          if (target.lastRsi <= config.rsiOversold && isVolumeSurge && usdtBalance >= config.tradeAmountUsdt) {
            sendTelegramAlert(
              `⚡ <b>BUY SIGNAL (${sym})</b>\n\n` + `<b>RSI:</b> ${target.lastRsi.toFixed(2)} | <b>Price:</b> $${closePrice}\n` + `<b>Available Cash:</b> $${usdtBalance.toFixed(2)} USDT`,
            );
            target.lastSignalTime = now;
          }
          // 2. COST-BASIS AWARE SELL SIGNALS (Take Profit & Stop Loss)
          else if (currentAssetUsdVal >= 5.0) {
            const avgEntryPrice = getAverageEntryPrice(sym);

            if (avgEntryPrice) {
              const pnlPercent = ((closePrice - avgEntryPrice) / avgEntryPrice) * 100;

              // A. Take Profit Signal (RSI Overbought AND in profit by at least takeProfitPercent)
              if (target.lastRsi >= config.rsiOverbought && pnlPercent >= (config.takeProfitPercent || 1.5)) {
                sendTelegramAlert(
                  `🎯 <b>TAKE PROFIT SIGNAL (${sym})</b>\n\n` +
                    `<b>Price:</b> $${closePrice} (Entry: $${avgEntryPrice.toFixed(2)})\n` +
                    `<b>Unrealized Gain:</b> +${pnlPercent.toFixed(2)}%\n` +
                    `<b>RSI:</b> ${target.lastRsi.toFixed(2)}`,
                );
                target.lastSignalTime = now;
              }
              // B. Stop Loss Signal (Price dropped below cost basis limit)
              else if (pnlPercent <= -(config.stopLossPercent || 2.0)) {
                sendTelegramAlert(
                  `🛑 <b>STOP LOSS ALERT (${sym})</b>\n\n` +
                    `<b>Price:</b> $${closePrice} (Entry: $${avgEntryPrice.toFixed(2)})\n` +
                    `<b>Unrealized Loss:</b> ${pnlPercent.toFixed(2)}%\n` +
                    `<b>Action:</b> Consider selling to protect capital.`,
                );
                target.lastSignalTime = now;
              }
            } else {
              // Fallback if entry price is unrecorded in DB but asset is held
              if (target.lastRsi >= config.rsiOverbought) {
                sendTelegramAlert(
                  `🚨 <b>SELL SIGNAL (${sym})</b>\n\n` + `<b>RSI:</b> ${target.lastRsi.toFixed(2)} | <b>Price:</b> $${closePrice}\n` + `<b>Holding Value:</b> $${currentAssetUsdVal.toFixed(2)} USD`,
                );
                target.lastSignalTime = now;
              }
            }
          }
        }
      }
    } catch (err) {
      console.error("WS processing error:", err.message);
    }
  });

  ws.on("error", (err) => console.error("WebSocket Error:", err.message));
  ws.on("close", () => setTimeout(connectMultiStreamWS, 3000));
}

// Get/Update Settings Endpoints
app.get("/api/settings", (req, res) => {
  res.json({ success: true, config });
});

app.post("/api/settings", express.json(), (req, res) => {
  const { password, settings } = req.body;

  if (!password || password !== TRADE_PASSWORD) {
    return res.status(401).json({ success: false, error: "Unauthorized password." });
  }

  if (settings && typeof settings === "object") {
    Object.keys(settings).forEach((key) => {
      if (key in config) {
        config[key] = parseFloat(settings[key]);
        saveSettingToDb(key, config[key]);
      }
    });
    console.log("Updated runtime strategy settings:", config);
    return res.json({ success: true, config });
  }

  res.status(400).json({ success: false, error: "Invalid settings payload." });
});

// Dashboard Status API
app.get("/api/status", (req, res) => {
  const marketsList = Object.keys(marketData).map((sym) => {
    const prices = marketData[sym].prices;
    const rsiVal = marketData[sym].lastRsi;
    const baseAsset = sym.replace("USDT", "");
    const holdingQty = availableBalances[baseAsset] || 0;
    const currentPrice = prices.length > 0 ? prices[prices.length - 1] : 0;
    const holdingUsd = holdingQty * currentPrice;
    const avgEntryPrice = getAverageEntryPrice(sym);

    return {
      symbol: sym,
      price: currentPrice || null,
      history: prices.slice(-20),
      rsi: typeof rsiVal === "number" ? parseFloat(rsiVal.toFixed(2)) : null,
      volSurge: marketData[sym].lastVolumeSurge,
      canBuy: (availableBalances["USDT"] || 0) >= config.tradeAmountUsdt,
      canSell: holdingUsd >= 5.0,
      holdingUsd: parseFloat(holdingUsd.toFixed(2)),
      avgEntryPrice: avgEntryPrice ? parseFloat(avgEntryPrice.toFixed(2)) : null,
    };
  });

  res.json({
    interval: INTERVAL,
    markets: marketsList,
    config,
    usdtBalance: parseFloat((availableBalances["USDT"] || 0).toFixed(2)),
  });
});

app.get("/api/holdings", async (req, res) => {
  await updateAccountBalances();
  const trackedAssets = new Set(["USDT", ...SYMBOLS.map((s) => s.toUpperCase().replace("USDT", ""))]);

  const activeBalances = Object.keys(availableBalances)
    .filter((asset) => trackedAssets.has(asset) && availableBalances[asset] > 0.0001)
    .map((asset) => {
      const free = availableBalances[asset];
      let usdVal = free;

      if (asset !== "USDT") {
        const pair = `${asset}USDT`;
        const currentPrice = marketData[pair]?.prices.slice(-1)[0] || 0;
        usdVal = free * currentPrice;
      }

      return {
        asset,
        free: free.toFixed(4),
        locked: "0.0000",
        total: free.toFixed(4),
        usdValue: usdVal.toFixed(2),
      };
    });

  res.json({ success: true, balances: activeBalances });
});

app.get("/api/pnl", (req, res) => {
  const timeframe = req.query.tf || "all";
  let timeFilter = 0;
  const now = Date.now();

  if (timeframe === "day") timeFilter = now - 24 * 60 * 60 * 1000;
  else if (timeframe === "week") timeFilter = now - 7 * 24 * 60 * 60 * 1000;
  else if (timeframe === "month") timeFilter = now - 30 * 24 * 60 * 60 * 1000;

  try {
    const stmt = db.prepare("SELECT * FROM trades WHERE timestamp >= :tf ORDER BY timestamp ASC");
    stmt.bind({ ":tf": timeFilter });

    const trades = [];
    while (stmt.step()) {
      trades.push(stmt.getAsObject());
    }
    stmt.free();

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
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Protected Trade Execution Endpoint
app.post("/api/trade", express.json(), async (req, res) => {
  const { symbol, side, usdtAmount, quantity, sellAll, password } = req.body;

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
    const symUpper = symbol.toUpperCase();
    const baseAsset = symUpper.replace("USDT", "");
    const isSell = side.toUpperCase() === "SELL";

    let queryParams = `symbol=${symUpper}&side=${side.toUpperCase()}&type=MARKET&timestamp=${timestamp}`;

    // Handle SELL ALL or specific Token Quantity vs USDT Dollar Amount
    if (isSell && (sellAll || quantity)) {
      await updateAccountBalances(); // Ensure fresh balance
      let sellQty = quantity || availableBalances[baseAsset] || 0;

      if (sellQty <= 0) {
        return res.status(400).json({ success: false, error: `No available ${baseAsset} balance to sell.` });
      }

      // Format quantity to prevent precision overflow errors
      queryParams += `&quantity=${sellQty}`;
    } else {
      const tradeAmount = usdtAmount || config.tradeAmountUsdt;
      queryParams += `&quoteOrderQty=${tradeAmount}`;
    }

    const signature = crypto.createHmac("sha256", secretKey).update(queryParams).digest("hex");

    const response = await fetch(`https://api.binance.com/api/v3/order?${queryParams}&signature=${signature}`, {
      method: "POST",
      headers: {
        "X-MBX-APIKEY": apiKey,
        "Content-Type": "application/x-www-form-urlencoded",
      },
    });

    const result = await response.json();

    if (result.orderId) {
      const executedPrice = parseFloat(result.fills?.[0]?.price || marketData[symUpper]?.prices.slice(-1)[0] || 0);
      const executedQty = parseFloat(result.executedQty || 0);
      const executedUsdt = parseFloat(result.cummulativeQuoteQty || tradeAmount || executedQty * executedPrice);

      db.run(`INSERT INTO trades (symbol, side, price, qty, usdt_amount, order_id, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)`, [
        symUpper,
        side.toUpperCase(),
        executedPrice,
        executedQty,
        executedUsdt,
        String(result.orderId),
        timestamp,
      ]);
      saveDatabase();
      await updateAccountBalances();

      sendTelegramAlert(
        `✅ <b>TRADE EXECUTED (${side.toUpperCase()})</b>\n\n` +
          `<b>Symbol:</b> ${symUpper}\n` +
          `<b>Amount:</b> $${executedUsdt.toFixed(2)} USDT (${executedQty} ${baseAsset})\n` +
          `<b>Executed Price:</b> $${executedPrice}\n` +
          `<b>Order ID:</b> <code>${result.orderId}</code>`,
      );

      return res.json({
        success: true,
        orderId: result.orderId,
        symbol: symUpper,
        side: side.toUpperCase(),
        executedPrice,
        details: result,
      });
    } else {
      return res.status(400).json({ success: false, error: result.msg || "Order rejected by Binance" });
    }
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.use(express.static("public"));

(async () => {
  await initDatabase();
  await updateAccountBalances();
  setInterval(updateAccountBalances, 15000);
  await bootstrapHistoricalData();
  connectMultiStreamWS();

  app.listen(PORT, async () => {
    console.log(`Terminal running on http://localhost:${PORT}`);
    const symbolsList = SYMBOLS.map((s) => s.toUpperCase()).join(", ");
    await sendTelegramAlert(`🟢 <b>Binance Trader Online</b>\n\nMonitoring: <code>${symbolsList}</code>`);
  });
})();
