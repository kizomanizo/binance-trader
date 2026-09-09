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

// Memory store for user's active Binance spot balances & lot size rules
const availableBalances = { USDT: 0 };
const symbolLotSizes = {}; // Stores stepSize precision for each symbol

// Default Strategy Config (fallback if database is empty)
let config = {
  rsiOversold: 32,
  rsiOverbought: 70,
  volumeSurgeMultiplier: 1.8,
  tradeAmountUsdt: 5.5,
  cooldownMinutes: 5,
  takeProfitPercent: 1.5,
  stopLossPercent: 2.0,
  takerFeePercent: 0.1,
};

let alertConfig = {
  cryptoTelegramEnabled: true,
  stocksTelegramEnabled: true,
  cryptoAllowedStart: "",
  cryptoAllowedEnd: "",
  stocksAllowedStart: "",
  stocksAllowedEnd: "",
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

  db.run(`
    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS strategy_alerts (
      id TEXT PRIMARY KEY,
      symbol TEXT NOT NULL,
      action TEXT NOT NULL,
      signal_type TEXT NOT NULL,
      price REAL NOT NULL,
      rsi REAL,
      created_time INTEGER NOT NULL,
      executed INTEGER NOT NULL DEFAULT 0,
      order_id TEXT,
      source TEXT NOT NULL DEFAULT 'crypto'
    )
  `);

  ensureColumn("strategy_alerts", "source", "TEXT NOT NULL DEFAULT 'crypto'");
  loadSettingsFromDb();
  saveDatabase();
}

function ensureColumn(table, column, type) {
  const stmt = db.prepare(`PRAGMA table_info(${table})`);
  let exists = false;
  while (stmt.step()) {
    if (stmt.getAsObject().name === column) exists = true;
  }
  stmt.free();
  if (!exists) db.run(`ALTER TABLE ${table} ADD COLUMN ${column} ${type}`);
}

function parseStoredAlertValue(key, value) {
  if (key.endsWith("Enabled")) return value === "true" || value === "1";
  return value == null ? "" : String(value);
}

function loadSettingsFromDb() {
  try {
    const stmt = db.prepare("SELECT key, value FROM settings");
    while (stmt.step()) {
      const row = stmt.getAsObject();
      if (row.key in config) {
        config[row.key] = parseFloat(row.value);
      } else if (row.key in alertConfig) {
        alertConfig[row.key] = parseStoredAlertValue(row.key, row.value);
      }
    }
    stmt.free();
    console.log("Loaded strategy config from DB:", config);
    console.log("Loaded alert config from DB:", alertConfig);
  } catch (err) {
    console.error("Error loading settings from DB:", err.message);
  }
}

function saveSettingToDb(key, value) {
  db.run(`INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)`, [key, String(value)]);
  saveDatabase();
}

const ALERT_RETENTION = 50;

function recordStrategyAlert({ symbol, action, signalType, price, rsi, source = "crypto" }) {
  if (!db) return null;
  const createdTime = Date.now();
  const id = `${String(symbol).toUpperCase()}_${action}_${createdTime}`;
  db.run(`INSERT INTO strategy_alerts (id, symbol, action, signal_type, price, rsi, created_time, executed, source) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)`, [
    id,
    String(symbol).toUpperCase(),
    action,
    signalType || action,
    parseFloat(price) || 0,
    typeof rsi === "number" ? rsi : null,
    createdTime,
    source === "stocks" ? "stocks" : "crypto",
  ]);

  const stmt = db.prepare("SELECT id FROM strategy_alerts ORDER BY created_time DESC");
  const ids = [];
  while (stmt.step()) ids.push(stmt.getAsObject().id);
  stmt.free();
  ids.slice(ALERT_RETENTION).forEach((oldId) => {
    db.run("DELETE FROM strategy_alerts WHERE id = ?", [oldId]);
  });
  saveDatabase();
  return id;
}

function listStrategyAlerts() {
  if (!db) return [];
  const stmt = db.prepare("SELECT * FROM strategy_alerts ORDER BY created_time DESC");
  const alerts = [];
  while (stmt.step()) {
    const row = stmt.getAsObject();
    alerts.push({
      id: row.id,
      symbol: row.symbol,
      action: row.action,
      signalType: row.signal_type,
      price: row.price,
      rsi: row.rsi === null || row.rsi === undefined ? null : parseFloat(Number(row.rsi).toFixed(2)),
      createdTime: row.created_time,
      executed: Boolean(row.executed),
      orderId: row.order_id || null,
      source: row.source === "stocks" ? "stocks" : "crypto",
    });
  }
  stmt.free();
  return alerts;
}

function markAlertExecuted(alertId, orderId) {
  if (!db || !alertId) return;
  db.run(`UPDATE strategy_alerts SET executed = 1, order_id = ? WHERE id = ?`, [orderId ? String(orderId) : "", alertId]);
  saveDatabase();
}

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

function getTakerFeeRate() {
  const pct = parseFloat(config.takerFeePercent);
  return (Number.isFinite(pct) && pct >= 0 ? pct : 0.1) / 100;
}

function getAssetUsdPrice(asset) {
  if (!asset || asset === "USDT") return 1;
  const tick = marketData[`${asset}USDT`];
  if (tick?.prices?.length) return tick.prices[tick.prices.length - 1];
  return null;
}

function getNetPnlPercent(avgEntryPrice, closePrice) {
  if (!avgEntryPrice || !closePrice) return null;
  const netExit = closePrice * (1 - getTakerFeeRate());
  return ((netExit - avgEntryPrice) / avgEntryPrice) * 100;
}

function evaluateSellSignal(symbol, closePrice, rsi) {
  const avgEntryPrice = getAverageEntryPrice(symbol);
  const netPnl = getNetPnlPercent(avgEntryPrice, closePrice);
  const takeProfit = config.takeProfitPercent || 1.5;
  const stopLoss = config.stopLossPercent || 2.0;

  if (avgEntryPrice && netPnl !== null) {
    if (typeof rsi === "number" && rsi >= config.rsiOverbought && netPnl >= takeProfit) {
      return { type: "TAKE_PROFIT", avgEntryPrice, netPnl };
    }
    if (netPnl <= -stopLoss) {
      return { type: "STOP_LOSS", avgEntryPrice, netPnl };
    }
    return null;
  }

  if (typeof rsi === "number" && rsi >= config.rsiOverbought) {
    return { type: "SELL", avgEntryPrice: null, netPnl: null };
  }
  return null;
}

function summarizeOrderFills(result, symbol, side) {
  const baseAsset = symbol.replace("USDT", "");
  const fills = Array.isArray(result.fills) ? result.fills : [];
  let commissionUsdt = 0;
  let baseCommission = 0;

  fills.forEach((fill) => {
    const commission = parseFloat(fill.commission || 0);
    if (!commission) return;
    const asset = fill.commissionAsset;
    if (asset === "USDT") commissionUsdt += commission;
    else if (asset === baseAsset) baseCommission += commission;
    else {
      const px = getAssetUsdPrice(asset);
      if (px) commissionUsdt += commission * px;
    }
  });

  const executedQty = parseFloat(result.executedQty || 0);
  const quoteQty = parseFloat(result.cummulativeQuoteQty || 0);
  const isBuy = side.toUpperCase() === "BUY";

  return {
    executedQty,
    netQty: isBuy ? Math.max(0, executedQty - baseCommission) : executedQty,
    recordedUsdt: isBuy ? quoteQty + commissionUsdt : Math.max(0, quoteQty - commissionUsdt),
    commissionUsdt,
    baseCommission,
  };
}

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const TRADE_PASSWORD = process.env.TRADE_PASSWORD || "admin123";

const ALPACA_API_KEY = process.env.ALPACA_API_KEY;
const ALPACA_SECRET_KEY = process.env.ALPACA_SECRET_KEY;
const ALPACA_BASE_URL = process.env.ALPACA_BASE_URL || "https://paper-api.alpaca.markets/v2";
const ALPACA_DATA_URL = process.env.ALPACA_DATA_URL || "https://data.alpaca.markets/v2";
const ALPACA_STOCK_WS_URL = "wss://stream.data.alpaca.markets/v2/iex";

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
    lastStopLossPnl: null,
  };
});

const STOCK_SYMBOLS = (process.env.STOCK_SYMBOLS || "AAPL,TSLA,NVDA,SPY")
  .split(",")
  .map((s) => s.trim().toUpperCase())
  .filter(Boolean);
const stockMarketData = {};

STOCK_SYMBOLS.forEach((symbol) => {
  stockMarketData[symbol] = {
    prices: [],
    volumes: [],
    lastPrice: null,
    lastBarTime: null,
    lastRsi: null,
    lastVolumeSurge: false,
    lastSignalTime: 0,
    lastSignal: null,
    sawLiveBar: false,
  };
});

let alpacaStockWs = null;
let alpacaReconnectTimer = null;
let alpacaClock = { is_open: null, next_open: null, next_close: null };

function alpacaAuthHeaders() {
  return {
    "APCA-API-KEY-ID": ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
  };
}

// Fetch LOT_SIZE rules to format trade quantities precisely for Binance
async function fetchExchangeInfo() {
  try {
    const res = await fetch("https://api.binance.com/api/v3/exchangeInfo");
    const data = await res.json();
    if (data.symbols) {
      data.symbols.forEach((s) => {
        const lotFilter = s.filters.find((f) => f.filterType === "LOT_SIZE");
        const notionalFilter = s.filters.find((f) => f.filterType === "NOTIONAL" || f.filterType === "MIN_NOTIONAL");
        if (lotFilter) {
          const stepSize = parseFloat(lotFilter.stepSize);
          const stepStr = Number(lotFilter.stepSize).toString();
          const precision = stepStr.includes(".") ? stepStr.split(".")[1].length : 0;
          symbolLotSizes[s.symbol] = {
            stepSize,
            precision,
            minQty: parseFloat(lotFilter.minQty),
            minNotional: parseFloat(notionalFilter?.minNotional || notionalFilter?.notional || 5),
          };
        }
      });
      console.log("Loaded exchange LOT_SIZE rules for precision formatting.");
    }
  } catch (err) {
    console.error("Failed to fetch exchange info:", err.message);
  }
}

function formatQuantity(symbol, qty) {
  const rule = symbolLotSizes[symbol.toUpperCase()];
  if (!rule) return String(qty);
  const steps = Math.floor((Number(qty) + Number.EPSILON) / rule.stepSize);
  return parseFloat((steps * rule.stepSize).toFixed(rule.precision)).toFixed(rule.precision);
}

function isDustQty(symbol, qty, price) {
  const amount = Number(qty);
  if (!Number.isFinite(amount) || amount <= 0) return true;
  const rule = symbolLotSizes[symbol.toUpperCase()];
  if (rule?.minQty && amount + Number.EPSILON < rule.minQty) return true;
  const minNotional = rule?.minNotional || 5;
  return amount * (Number(price) || 0) < minNotional;
}

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

function parseHHMM(value) {
  if (value == null || value === "") return null;
  const match = String(value).trim().match(/^([01]?\d|2[0-3]):([0-5]\d)(?::[0-5]\d)?$/);
  if (!match) return null;
  return parseInt(match[1], 10) * 60 + parseInt(match[2], 10);
}

function normalizeAllowedTime(value) {
  if (value == null || String(value).trim() === "") return "";
  const match = String(value).trim().match(/^([01]?\d|2[0-3]):([0-5]\d)(?::[0-5]\d)?$/);
  if (!match) return null;
  return `${String(match[1]).padStart(2, "0")}:${match[2]}`;
}

function isWithinAllowedHours(startStr, endStr, now = new Date()) {
  const start = parseHHMM(startStr);
  const end = parseHHMM(endStr);
  if (start === null || end === null) return true;
  const current = now.getHours() * 60 + now.getMinutes();
  if (start === end) return true;
  if (start < end) return current >= start && current < end;
  return current >= start || current < end;
}

function shouldSendTelegram(channel) {
  const isStocks = channel === "stocks";
  const enabled = isStocks ? alertConfig.stocksTelegramEnabled : alertConfig.cryptoTelegramEnabled;
  if (!enabled) return false;
  const start = isStocks ? alertConfig.stocksAllowedStart : alertConfig.cryptoAllowedStart;
  const end = isStocks ? alertConfig.stocksAllowedEnd : alertConfig.cryptoAllowedEnd;
  return isWithinAllowedHours(start, end);
}

function sendChannelTelegram(channel, message) {
  if (!shouldSendTelegram(channel)) return Promise.resolve();
  return sendTelegramAlert(message);
}

function parseAlertSettingsPayload(payload) {
  const values = {};

  if ("cryptoTelegramEnabled" in payload) values.cryptoTelegramEnabled = Boolean(payload.cryptoTelegramEnabled);
  if ("stocksTelegramEnabled" in payload) values.stocksTelegramEnabled = Boolean(payload.stocksTelegramEnabled);

  const next = { ...alertConfig, ...values };
  const timeKeys = ["cryptoAllowedStart", "cryptoAllowedEnd", "stocksAllowedStart", "stocksAllowedEnd"];
  for (const key of timeKeys) {
    if (key in payload) {
      const normalized = normalizeAllowedTime(payload[key]);
      if (normalized === null) return { error: "Allowed hours must be HH:MM or empty." };
      next[key] = normalized;
      values[key] = normalized;
    }
  }

  if ((next.cryptoAllowedStart && !next.cryptoAllowedEnd) || (!next.cryptoAllowedStart && next.cryptoAllowedEnd)) {
    return { error: "Set both crypto start and end times, or leave both empty for 24/7." };
  }
  if ((next.stocksAllowedStart && !next.stocksAllowedEnd) || (!next.stocksAllowedStart && next.stocksAllowedEnd)) {
    return { error: "Set both stock start and end times, or leave both empty for 24/7." };
  }

  return { values };
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
    } else {
      // Print Binance rejection message directly in terminal logs
      console.error("[BINANCE ACCOUNT API ERROR]:", data);
    }
  } catch (err) {
    console.error("Failed to update background balances:", err.message);
  }
}

async function convertDustToBnb(asset) {
  const apiKey = process.env.BINANCE_API_KEY;
  const secretKey = process.env.BINANCE_SECRET_KEY;
  if (!apiKey || !secretKey || !asset || asset === "USDT" || asset === "BNB") return null;

  try {
    const timestamp = Date.now();
    const queryParams = `asset=${asset}&timestamp=${timestamp}`;
    const signature = crypto.createHmac("sha256", secretKey).update(queryParams).digest("hex");
    const response = await fetch(`https://api.binance.com/sapi/v1/asset/dust?${queryParams}&signature=${signature}`, {
      method: "POST",
      headers: { "X-MBX-APIKEY": apiKey },
    });
    const result = await response.json();
    if (!response.ok || result.code) {
      console.warn(`[DUST CONVERT SKIPPED ${asset}]`, result.msg || JSON.stringify(result));
      return null;
    }
    console.log(`[DUST CONVERTED] ${asset} leftover swept to BNB`);
    return result;
  } catch (err) {
    console.warn(`[DUST CONVERT ERROR ${asset}]`, err.message);
    return null;
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

function applyStockBars(symbol, bars) {
  const target = stockMarketData[symbol];
  if (!target || !Array.isArray(bars) || bars.length === 0) return 0;

  const prices = [];
  const volumes = [];
  let lastBarTime = null;
  for (const bar of bars) {
    const close = parseFloat(bar.c);
    const volume = parseFloat(bar.v);
    if (!Number.isFinite(close)) continue;
    prices.push(close);
    if (Number.isFinite(volume)) volumes.push(volume);
    if (bar.t) lastBarTime = bar.t;
  }
  if (prices.length === 0) return 0;

  target.prices = prices.slice(-100);
  target.volumes = volumes.slice(-100);
  target.lastPrice = prices[prices.length - 1];
  target.lastBarTime = lastBarTime;

  if (target.prices.length >= 15) {
    const rsiVals = RSI.calculate({ values: target.prices, period: 14 });
    if (rsiVals.length > 0) {
      target.lastRsi = rsiVals[rsiVals.length - 1];
    }
  }
  return prices.length;
}

function applyStockSnapshot(symbol, snapshot) {
  const target = stockMarketData[symbol];
  if (!target || !snapshot || typeof snapshot !== "object") return false;

  const lastTrade = parseFloat(snapshot.latestTrade?.p);
  const minuteClose = parseFloat(snapshot.minuteBar?.c);
  const dailyClose = parseFloat(snapshot.dailyBar?.c);
  const prevClose = parseFloat(snapshot.prevDailyBar?.c);
  const price = [lastTrade, minuteClose, dailyClose, prevClose].find(Number.isFinite);
  if (!Number.isFinite(price)) return false;

  target.lastPrice = price;
  if (target.prices.length === 0) target.prices = [price];
  return true;
}

async function refreshAlpacaClock() {
  if (!ALPACA_API_KEY || !ALPACA_SECRET_KEY) return;
  try {
    const res = await fetch(`${ALPACA_BASE_URL}/clock`, { headers: alpacaAuthHeaders() });
    const data = await res.json();
    if (!res.ok) {
      console.error("[ALPACA CLOCK]", data.message || data);
      return;
    }
    alpacaClock = {
      is_open: Boolean(data.is_open),
      next_open: data.next_open || null,
      next_close: data.next_close || null,
    };
  } catch (err) {
    console.error("[ALPACA CLOCK]", err.message);
  }
}

async function refreshStockSnapshots() {
  if (!ALPACA_API_KEY || !ALPACA_SECRET_KEY || STOCK_SYMBOLS.length === 0) return;

  try {
    const symbols = encodeURIComponent(STOCK_SYMBOLS.join(","));
    const res = await fetch(`${ALPACA_DATA_URL}/stocks/snapshots?symbols=${symbols}&feed=iex`, {
      headers: alpacaAuthHeaders(),
    });
    const data = await res.json();
    if (!res.ok) {
      console.error("[ALPACA SNAPSHOTS]", data.message || JSON.stringify(data));
      return;
    }

    const snapshots = data.snapshots && typeof data.snapshots === "object" ? data.snapshots : data;
    STOCK_SYMBOLS.forEach((symbol) => applyStockSnapshot(symbol, snapshots[symbol]));
  } catch (err) {
    console.error("[ALPACA SNAPSHOTS]", err.message);
  }
}

async function bootstrapStockHistoricalData() {
  if (!ALPACA_API_KEY || !ALPACA_SECRET_KEY) {
    console.warn("[ALPACA] Missing ALPACA_API_KEY or ALPACA_SECRET_KEY — stock bootstrap skipped.");
    return;
  }
  if (STOCK_SYMBOLS.length === 0) return;

  console.log(`Bootstrapping Alpaca IEX stock data for: ${STOCK_SYMBOLS.join(", ")}...`);
  await refreshAlpacaClock();

  const start = new Date(Date.now() - 10 * 24 * 60 * 60 * 1000).toISOString();
  await Promise.all(
    STOCK_SYMBOLS.map(async (symbol) => {
      try {
        const barsUrl =
          `${ALPACA_DATA_URL}/stocks/bars?symbols=${encodeURIComponent(symbol)}` +
          `&timeframe=1Min&limit=50&adjustment=raw&feed=iex&sort=desc&start=${encodeURIComponent(start)}`;
        const res = await fetch(barsUrl, { headers: alpacaAuthHeaders() });
        const data = await res.json();
        if (!res.ok) {
          console.error(`[ALPACA BARS ${symbol}]`, data.message || JSON.stringify(data));
          return;
        }
        const rawBars = data.bars?.[symbol];
        const bars = Array.isArray(rawBars) ? rawBars.slice().reverse() : [];
        const count = applyStockBars(symbol, bars);
        if (count > 0) {
          console.log(`[ALPACA] Seeded ${symbol} with ${count} 1m bars, last $${stockMarketData[symbol].lastPrice}`);
        }
      } catch (err) {
        console.error(`[ALPACA BARS ${symbol}]`, err.message);
      }
    }),
  );

  await refreshStockSnapshots();

  STOCK_SYMBOLS.forEach((symbol) => {
    const target = stockMarketData[symbol];
    if (!Number.isFinite(target.lastPrice)) {
      console.warn(`[ALPACA] No seed price for ${symbol} — UI will show -- until a live bar or snapshot arrives.`);
    }
  });

  if (alpacaClock.is_open === false) {
    console.log(
      `[ALPACA] US market is closed. Showing last IEX prices until next open${alpacaClock.next_open ? ` (${alpacaClock.next_open})` : ""}.`,
    );
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

          if (target.lastRsi <= config.rsiOversold && isVolumeSurge && usdtBalance >= config.tradeAmountUsdt) {
            recordStrategyAlert({ symbol: sym, action: "BUY", signalType: "BUY", price: closePrice, rsi: target.lastRsi });
            sendChannelTelegram(
              "crypto",
              `⚡ <b>BUY SIGNAL (${sym})</b>\n\n` + `<b>RSI:</b> ${target.lastRsi.toFixed(2)} | <b>Price:</b> $${closePrice}\n` + `<b>Available Cash:</b> $${usdtBalance.toFixed(2)} USDT`,
            );
            target.lastSignalTime = now;
          } else if (currentAssetUsdVal >= 5.0) {
            const sellSignal = evaluateSellSignal(sym, closePrice, target.lastRsi);

            if (sellSignal?.type === "TAKE_PROFIT") {
              recordStrategyAlert({ symbol: sym, action: "SELL", signalType: "TAKE_PROFIT", price: closePrice, rsi: target.lastRsi });
              sendChannelTelegram(
                "crypto",
                `🎯 <b>TAKE PROFIT SIGNAL (${sym})</b>\n\n` +
                  `<b>Price:</b> $${closePrice} (Fee-adjusted entry: $${sellSignal.avgEntryPrice.toFixed(4)})\n` +
                  `<b>Net PnL after fees:</b> +${sellSignal.netPnl.toFixed(2)}%\n` +
                  `<b>RSI:</b> ${target.lastRsi.toFixed(2)}`,
              );
              target.lastSignalTime = now;
              target.lastStopLossPnl = null;
            } else if (sellSignal?.type === "STOP_LOSS") {
              const lastPnl = target.lastStopLossPnl;
              const isDeeperDip = lastPnl !== null && sellSignal.netPnl <= lastPnl - 1.0;

              if (lastPnl === null || isDeeperDip) {
                recordStrategyAlert({ symbol: sym, action: "SELL", signalType: "STOP_LOSS", price: closePrice, rsi: target.lastRsi });
                sendChannelTelegram(
                  "crypto",
                  `🛑 <b>STOP LOSS ALERT (${sym})</b>\n\n` +
                    `<b>Price:</b> $${closePrice} (Fee-adjusted entry: $${sellSignal.avgEntryPrice.toFixed(4)})\n` +
                    `<b>Net PnL after fees:</b> ${sellSignal.netPnl.toFixed(2)}%\n` +
                    `<b>Action:</b> Consider selling to protect capital.`,
                );
                target.lastSignalTime = now;
                target.lastStopLossPnl = sellSignal.netPnl;
              }
            } else if (target.lastStopLossPnl !== null && (sellSignal === null || sellSignal.netPnl > -(config.stopLossPercent || 2.0))) {
              target.lastStopLossPnl = null;
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

function scheduleAlpacaReconnect() {
  if (alpacaReconnectTimer) return;
  alpacaReconnectTimer = setTimeout(() => {
    alpacaReconnectTimer = null;
    connectAlpacaStockWS();
  }, 5000);
}

function processAlpacaTrade(msg) {
  const symbol = msg.S;
  const price = parseFloat(msg.p);
  const target = stockMarketData[symbol];
  if (!target || !Number.isFinite(price)) return;
  target.lastPrice = price;
}

function processAlpacaBar(msg) {
  const symbol = msg.S;
  const closePrice = parseFloat(msg.c);
  const volume = parseFloat(msg.v);
  const target = stockMarketData[symbol];
  if (!target || !Number.isFinite(closePrice)) return;
  if (msg.t && target.lastBarTime === msg.t) return;

  if (!target.sawLiveBar) {
    target.sawLiveBar = true;
    console.log(`[ALPACA] first live bar ${symbol} $${closePrice}`);
  }

  target.prices.push(closePrice);
  if (Number.isFinite(volume)) target.volumes.push(volume);
  target.lastPrice = closePrice;
  if (msg.t) target.lastBarTime = msg.t;

  if (target.prices.length > 100) target.prices.shift();
  if (target.volumes.length > 100) target.volumes.shift();

  let rsi = null;
  if (target.prices.length >= 15) {
    const rsiValues = RSI.calculate({ values: target.prices, period: 14 });
    if (rsiValues && rsiValues.length > 0) {
      rsi = rsiValues[rsiValues.length - 1];
      target.lastRsi = rsi;
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

  if (rsi === null) return;

  const now = Date.now();
  const cooldownMs = config.cooldownMinutes * 60 * 1000;
  if (now - target.lastSignalTime <= cooldownMs) return;

  if (rsi <= config.rsiOversold && isVolumeSurge) {
    target.lastSignalTime = now;
    target.lastSignal = "BUY";
    recordStrategyAlert({ symbol, action: "BUY", signalType: "STOCK_BUY", price: closePrice, rsi, source: "stocks" });
    sendChannelTelegram(
      "stocks",
      `📈 <b>STOCK BUY SIGNAL (${symbol})</b>\n\n` +
        `<b>RSI:</b> ${rsi.toFixed(2)} | <b>Price:</b> $${closePrice}\n` +
        `<b>Source:</b> Alpaca Market Data`,
    );
  } else if (rsi >= config.rsiOverbought) {
    target.lastSignalTime = now;
    target.lastSignal = "OVERBOUGHT";
    recordStrategyAlert({ symbol, action: "OVERBOUGHT", signalType: "STOCK_OVERBOUGHT", price: closePrice, rsi, source: "stocks" });
    sendChannelTelegram(
      "stocks",
      `📊 <b>STOCK OVERBOUGHT ALERT (${symbol})</b>\n\n` +
        `<b>RSI:</b> ${rsi.toFixed(2)} | <b>Price:</b> $${closePrice}\n` +
        `<b>Action:</b> Consider evaluating profit targets.`,
    );
  }
}

function handleAlpacaMessage(msg, ws) {
  if (!msg || typeof msg !== "object") return;

  if (msg.T === "success" && msg.msg === "authenticated") {
    ws.send(JSON.stringify({ action: "subscribe", bars: STOCK_SYMBOLS, trades: STOCK_SYMBOLS }));
    console.log(`Alpaca IEX subscribed to 1m bars + trades: ${STOCK_SYMBOLS.join(", ")}`);
    return;
  }

  if (msg.T === "success") {
    console.log(`[ALPACA] ${msg.msg || "success"}`);
    return;
  }

  if (msg.T === "subscription") {
    console.log("[ALPACA] Subscription confirmed:", {
      bars: msg.bars || [],
      trades: msg.trades || [],
    });
    return;
  }

  if (msg.T === "error") {
    console.error(`[ALPACA ERROR] ${msg.code || ""} ${msg.msg || JSON.stringify(msg)}`);
    return;
  }

  if (msg.T === "t") {
    processAlpacaTrade(msg);
    return;
  }

  if (msg.T === "b") {
    processAlpacaBar(msg);
  }
}

function connectAlpacaStockWS() {
  if (!ALPACA_API_KEY || !ALPACA_SECRET_KEY) {
    console.warn("[ALPACA] Missing ALPACA_API_KEY or ALPACA_SECRET_KEY — stock stream disabled.");
    return;
  }

  if (alpacaReconnectTimer) {
    clearTimeout(alpacaReconnectTimer);
    alpacaReconnectTimer = null;
  }

  if (alpacaStockWs) {
    try {
      alpacaStockWs.removeAllListeners();
      alpacaStockWs.close();
    } catch {
      // ignore stale socket cleanup errors
    }
    alpacaStockWs = null;
  }

  const ws = new WebSocket(ALPACA_STOCK_WS_URL);
  alpacaStockWs = ws;

  ws.on("open", () => {
    console.log(`Connected to Alpaca IEX stream (${ALPACA_BASE_URL})`);
    ws.send(
      JSON.stringify({
        action: "auth",
        key: ALPACA_API_KEY,
        secret: ALPACA_SECRET_KEY,
      }),
    );
  });

  ws.on("message", (data) => {
    try {
      const parsed = JSON.parse(data);
      const messages = Array.isArray(parsed) ? parsed : [parsed];
      messages.forEach((msg) => handleAlpacaMessage(msg, ws));
    } catch (err) {
      console.error("Alpaca WS processing error:", err.message);
    }
  });

  ws.on("error", (err) => {
    console.error("Alpaca WebSocket Error:", err.message);
    scheduleAlpacaReconnect();
  });

  ws.on("close", () => {
    console.warn("Alpaca WebSocket closed. Reconnecting in 5s...");
    if (alpacaStockWs === ws) alpacaStockWs = null;
    scheduleAlpacaReconnect();
  });
}

app.get("/api/settings", (req, res) => {
  res.json({
    success: true,
    config,
    alertConfig,
    timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
  });
});

app.post("/api/settings", express.json(), (req, res) => {
  const { password, settings, alertSettings } = req.body;

  if (!password || password !== TRADE_PASSWORD) {
    return res.status(401).json({ success: false, error: "Unauthorized password." });
  }

  const hasStrategy = settings && typeof settings === "object";
  const hasAlerts = alertSettings && typeof alertSettings === "object";
  if (!hasStrategy && !hasAlerts) {
    return res.status(400).json({ success: false, error: "Invalid settings payload." });
  }

  if (hasStrategy) {
    Object.keys(settings).forEach((key) => {
      if (key in config) {
        config[key] = parseFloat(settings[key]);
        saveSettingToDb(key, config[key]);
      }
    });
    console.log("Updated runtime strategy settings:", config);
  }

  if (hasAlerts) {
    const parsed = parseAlertSettingsPayload(alertSettings);
    if (parsed.error) {
      return res.status(400).json({ success: false, error: parsed.error });
    }
    Object.assign(alertConfig, parsed.values);
    Object.keys(parsed.values).forEach((key) => saveSettingToDb(key, alertConfig[key]));
    console.log("Updated runtime alert settings:", alertConfig);
  }

  return res.json({ success: true, config, alertConfig });
});

app.get("/api/alerts", (req, res) => {
  res.json({ success: true, alerts: listStrategyAlerts() });
});

app.get("/api/stocks", (req, res) => {
  const stocks = STOCK_SYMBOLS.map((symbol) => {
    const data = stockMarketData[symbol];
    const prices = data.prices;
    const rsiVal = data.lastRsi;
    const barPrice = prices.length > 0 ? prices[prices.length - 1] : null;
    const currentPrice = Number.isFinite(data.lastPrice) ? data.lastPrice : barPrice;

    return {
      symbol,
      price: currentPrice,
      history: prices.slice(-20),
      rsi: typeof rsiVal === "number" ? parseFloat(rsiVal.toFixed(2)) : null,
      volSurge: data.lastVolumeSurge,
      lastSignal: data.lastSignal || null,
      lastSignalTime: data.lastSignalTime || 0,
    };
  });

  res.json({
    success: true,
    source: "alpaca",
    feed: "iex",
    interval: "1m",
    baseUrl: ALPACA_BASE_URL,
    connected: Boolean(alpacaStockWs && alpacaStockWs.readyState === WebSocket.OPEN),
    marketOpen: alpacaClock.is_open,
    nextOpen: alpacaClock.next_open,
    nextClose: alpacaClock.next_close,
    stocks,
  });
});

app.get("/api/status", (req, res) => {
  const marketsList = Object.keys(marketData).map((sym) => {
    const prices = marketData[sym].prices;
    const rsiVal = marketData[sym].lastRsi;
    const baseAsset = sym.replace("USDT", "");
    const holdingQty = availableBalances[baseAsset] || 0;
    const currentPrice = prices.length > 0 ? prices[prices.length - 1] : 0;
    const holdingUsd = holdingQty * currentPrice;
    const avgEntryPrice = getAverageEntryPrice(sym);
    const netPnl = getNetPnlPercent(avgEntryPrice, currentPrice);
    const sellSignal = holdingUsd >= 5.0 ? evaluateSellSignal(sym, currentPrice, rsiVal) : null;

    return {
      symbol: sym,
      price: currentPrice || null,
      history: prices.slice(-20),
      rsi: typeof rsiVal === "number" ? parseFloat(rsiVal.toFixed(2)) : null,
      volSurge: marketData[sym].lastVolumeSurge,
      canBuy: (availableBalances["USDT"] || 0) >= config.tradeAmountUsdt,
      canSell: holdingUsd >= 5.0,
      holdingUsd: parseFloat(holdingUsd.toFixed(2)),
      avgEntryPrice: avgEntryPrice ? parseFloat(avgEntryPrice.toFixed(4)) : null,
      netPnlPercent: netPnl !== null ? parseFloat(netPnl.toFixed(2)) : null,
      sellSignal: sellSignal?.type || null,
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

  const activeBalances = Object.keys(availableBalances)
    .filter((asset) => availableBalances[asset] > 0.0001) // Show any asset with balance > 0.0001
    .map((asset) => {
      const free = availableBalances[asset];
      let usdVal = free;

      if (asset !== "USDT" && asset !== "USD") {
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
  const { symbol, side, usdtAmount, quantity, sellAll, password, alertId } = req.body;

  if (!password || password !== TRADE_PASSWORD) {
    return res.status(401).json({ success: false, error: "Unauthorized: Incorrect password." });
  }

  const apiKey = process.env.BINANCE_API_KEY;
  const secretKey = process.env.BINANCE_SECRET_KEY;

  if (!apiKey || !secretKey) {
    return res.status(500).json({ success: false, error: "Missing API keys." });
  }

  try {
    const symUpper = symbol.toUpperCase();
    const baseAsset = symUpper.replace("USDT", "");
    const isSell = side.toUpperCase() === "SELL";
    const clearWallet = isSell && (sellAll || !quantity);
    let tradeAmount = usdtAmount || config.tradeAmountUsdt;

    let queryParams = `symbol=${symUpper}&side=${side.toUpperCase()}&type=MARKET`;

    if (isSell) {
      await updateAccountBalances();
      const freeQty = availableBalances[baseAsset] || 0;
      let rawQty = clearWallet ? freeQty : Math.min(parseFloat(quantity) || 0, freeQty);

      if (rawQty <= 0) {
        return res.status(400).json({ success: false, error: `No available ${baseAsset} balance to sell.` });
      }

      const markPrice = marketData[symUpper]?.prices.slice(-1)[0] || 0;
      const formattedQty = formatQuantity(symUpper, rawQty);

      if (!parseFloat(formattedQty) || isDustQty(symUpper, formattedQty, markPrice)) {
        const dust = await convertDustToBnb(baseAsset);
        await updateAccountBalances();
        if (dust) {
          return res.json({
            success: true,
            orderId: null,
            dustConverted: true,
            symbol: symUpper,
            side: "SELL",
            details: dust,
          });
        }
        return res.status(400).json({ success: false, error: `${baseAsset} balance is below Binance LOT_SIZE / min notional.` });
      }

      queryParams += `&quantity=${formattedQty}`;
    } else {
      queryParams += `&quoteOrderQty=${tradeAmount}`;
    }

    const timestamp = Date.now();
    queryParams += `&timestamp=${timestamp}`;
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
      const economics = summarizeOrderFills(result, symUpper, side);
      const executedPrice = parseFloat(result.fills?.[0]?.price || marketData[symUpper]?.prices.slice(-1)[0] || 0);
      const executedQty = economics.netQty;
      const executedUsdt = economics.recordedUsdt || parseFloat(result.cummulativeQuoteQty || tradeAmount || executedQty * executedPrice);

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
      markAlertExecuted(alertId, result.orderId);
      await updateAccountBalances();

      let dustConverted = false;
      if (clearWallet) {
        const leftover = availableBalances[baseAsset] || 0;
        if (leftover > 0 && isDustQty(symUpper, leftover, executedPrice)) {
          dustConverted = Boolean(await convertDustToBnb(baseAsset));
          await updateAccountBalances();
        }
      }

      sendTelegramAlert(
        `✅ <b>TRADE EXECUTED (${side.toUpperCase()})</b>\n\n` +
          `<b>Symbol:</b> ${symUpper}\n` +
          `<b>Amount:</b> $${executedUsdt.toFixed(2)} USDT (${executedQty} ${baseAsset})\n` +
          `<b>Executed Price:</b> $${executedPrice}\n` +
          (economics.commissionUsdt || economics.baseCommission
            ? `<b>Fees:</b> $${economics.commissionUsdt.toFixed(4)} USDT` + (economics.baseCommission ? ` + ${economics.baseCommission} ${baseAsset}` : "") + `\n`
            : "") +
          (dustConverted ? `<b>Dust:</b> leftover ${baseAsset} converted to BNB\n` : "") +
          `<b>Order ID:</b> <code>${result.orderId}</code>`,
      );

      return res.json({
        success: true,
        orderId: result.orderId,
        symbol: symUpper,
        side: side.toUpperCase(),
        executedPrice,
        dustConverted,
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
  await fetchExchangeInfo();
  await updateAccountBalances();
  setInterval(updateAccountBalances, 15000);
  await bootstrapHistoricalData();
  await bootstrapStockHistoricalData();
  connectMultiStreamWS();
  connectAlpacaStockWS();
  setInterval(refreshStockSnapshots, 60000);
  setInterval(refreshAlpacaClock, 60000);

  app.listen(PORT, async () => {
    console.log(`Terminal running on http://localhost:${PORT}`);
    const symbolsList = SYMBOLS.map((s) => s.toUpperCase()).join(", ");
    await sendTelegramAlert(
      `🟢 <b>Binance Trader Online</b>\n\n` +
        `Monitoring: <code>${symbolsList}</code>\n` +
        `Stocks: <code>${STOCK_SYMBOLS.join(", ")}</code>`,
    );
  });
})();
