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
const lockedBalances = {};
const symbolLotSizes = {}; // Stores stepSize precision for each symbol
const tickerPriceCache = {};
const binanceStockAssets = new Set();

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

let botConfig = {
  defaultBudgetUsdt: 20,
  alertProfitPercent: 3,
  alertLossPercent: 3,
};

let botSession = createIdleBotSession();

function createIdleBotSession() {
  return {
    enabled: false,
    startedAt: null,
    budgetUsdt: 0,
    alertProfitPercent: botConfig.alertProfitPercent,
    alertLossPercent: botConfig.alertLossPercent,
    startingEquity: 0,
    cashUsdt: 0,
    positions: {},
    lastAction: "Idle",
    lastAlertKind: null,
    lastAlertAt: 0,
    realizedPnl: 0,
    busy: false,
  };
}

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
  ensureColumn("trades", "source", "TEXT NOT NULL DEFAULT 'manual'");
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
      } else if (row.key in botConfig) {
        const n = parseFloat(row.value);
        if (Number.isFinite(n)) botConfig[row.key] = n;
      } else if (row.key === "botSession") {
        restoreBotSession(row.value);
      }
    }
    stmt.free();
    console.log("Loaded strategy config from DB:", config);
    console.log("Loaded alert config from DB:", alertConfig);
    console.log("Loaded bot config from DB:", botConfig);
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
  if (!asset || asset === "USDT" || asset === "USD") return 1;
  const upper = String(asset).toUpperCase();
  const tick = marketData[`${upper}USDT`];
  if (tick?.prices?.length) return tick.prices[tick.prices.length - 1];
  if (tickerPriceCache[`${upper}USDT`]?.price) return tickerPriceCache[`${upper}USDT`].price;
  if (tickerPriceCache[upper]?.price) return tickerPriceCache[upper].price;
  const stock = stockMarketData[upper];
  if (Number.isFinite(stock?.lastPrice)) return stock.lastPrice;
  return null;
}

function stockTickerFromAsset(asset) {
  const a = String(asset || "").toUpperCase();
  if (!a) return "";
  if (STOCK_SYMBOLS.includes(a)) return a;
  for (const ticker of STOCK_SYMBOLS) {
    if (a === `${ticker}X` || a === `${ticker}B` || a === `B${ticker}`) return ticker;
  }
  return a.replace(/[XB]$/, "");
}

function isStockAsset(asset) {
  const a = String(asset || "").toUpperCase();
  if (!a) return false;
  if (STOCK_SYMBOLS.includes(a) || binanceStockAssets.has(a)) return true;
  return STOCK_SYMBOLS.some((s) => a === s || a === `${s}X` || a === `${s}B` || a === `B${s}`);
}

function getNetPnlPercent(avgEntryPrice, closePrice) {
  if (!avgEntryPrice || !closePrice) return null;
  const netExit = closePrice * (1 - getTakerFeeRate());
  return ((netExit - avgEntryPrice) / avgEntryPrice) * 100;
}

function getMinNotional(symbol) {
  return symbolLotSizes[String(symbol || "").toUpperCase()]?.minNotional || 5;
}

function restoreBotSession(raw) {
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return;
    botSession = {
      ...createIdleBotSession(),
      ...parsed,
      positions: parsed.positions && typeof parsed.positions === "object" ? parsed.positions : {},
      busy: false,
    };
    if (botSession.enabled) {
      console.log(`[AUTOPILOT] Restored enabled session · cash $${Number(botSession.cashUsdt || 0).toFixed(2)}`);
    }
  } catch (err) {
    console.warn("[AUTOPILOT] Failed to restore session:", err.message);
    botSession = createIdleBotSession();
  }
}

function persistBotSession() {
  const { busy, ...rest } = botSession;
  saveSettingToDb("botSession", JSON.stringify(rest));
}

function markToMarketBotPositions() {
  let value = 0;
  const open = [];
  Object.entries(botSession.positions || {}).forEach(([symbol, pos]) => {
    if (!pos) return;
    const qty = parseFloat(pos.qty) || 0;
    const fallback = parseFloat(pos.entryPrice) || 0;
    const px = marketData[symbol]?.prices?.slice(-1)[0] || fallback;
    const usd = qty * px;
    value += usd;
    open.push({
      symbol,
      qty,
      usd: parseFloat(usd.toFixed(2)),
      entryPrice: fallback,
      orderId: pos.orderId || null,
    });
  });
  return { value, open };
}

function describeBotWatch() {
  const need = config.rsiOversold;
  const rows = Object.keys(marketData)
    .map((sym) => ({
      sym,
      rsi: marketData[sym]?.lastRsi,
      surge: Boolean(marketData[sym]?.lastVolumeSurge),
    }))
    .filter((row) => typeof row.rsi === "number");

  if (!rows.length) return "Watching · RSI not ready yet (need ~15 closed 1m candles)";

  const best = rows.reduce((a, b) => (a.rsi <= b.rsi ? a : b));
  const rsiTxt = best.rsi.toFixed(1);
  if (best.rsi <= need) return `Ready ${best.sym} RSI ${rsiTxt} · placing clip`;
  return `Watching ${best.sym} RSI ${rsiTxt} · need ≤${need}`;
}

function getBotStatus() {
  const mt = markToMarketBotPositions();
  const cash = parseFloat(botSession.cashUsdt) || 0;
  const equity = cash + mt.value;
  const starting = parseFloat(botSession.startingEquity) || 0;
  const pnl = botSession.startedAt ? equity - starting : 0;
  const pnlPercent = starting > 0 ? (pnl / starting) * 100 : 0;
  let lastAction = botSession.lastAction || "Idle";
  if (botSession.enabled && mt.open.length === 0) {
    lastAction = describeBotWatch();
  }
  return {
    enabled: Boolean(botSession.enabled),
    startedAt: botSession.startedAt,
    budgetUsdt: parseFloat((botSession.budgetUsdt || 0).toFixed(2)),
    cashUsdt: parseFloat(cash.toFixed(2)),
    equity: parseFloat(equity.toFixed(2)),
    pnl: parseFloat(pnl.toFixed(2)),
    pnlPercent: parseFloat(pnlPercent.toFixed(2)),
    realizedPnl: parseFloat((botSession.realizedPnl || 0).toFixed(2)),
    lastAction,
    positions: mt.open,
    alertProfitPercent: botSession.alertProfitPercent,
    alertLossPercent: botSession.alertLossPercent,
    usdtWallet: parseFloat((availableBalances.USDT || 0).toFixed(2)),
    defaults: { ...botConfig },
    clipUsdt: config.tradeAmountUsdt,
  };
}

function evaluateBotSellSignal(symbol, closePrice, rsi) {
  const pos = botSession.positions[symbol];
  if (!pos) return null;
  const entry = parseFloat(pos.entryPrice) || 0;
  const netPnl = getNetPnlPercent(entry, closePrice);
  const takeProfit = config.takeProfitPercent || 1.5;
  const stopLoss = config.stopLossPercent || 2.0;

  if (entry && netPnl !== null) {
    if (typeof rsi === "number" && rsi >= config.rsiOverbought && netPnl >= takeProfit) {
      return { type: "TAKE_PROFIT", avgEntryPrice: entry, netPnl };
    }
    if (netPnl <= -stopLoss) {
      return { type: "STOP_LOSS", avgEntryPrice: entry, netPnl };
    }
    return null;
  }
  return null;
}

function maybeSessionPnlAlert() {
  if (!botSession.enabled || !botSession.startedAt) return;
  const snap = getBotStatus();
  const now = Date.now();
  if (now - (botSession.lastAlertAt || 0) < 60 * 1000) return;

  if (snap.pnlPercent >= botSession.alertProfitPercent && botSession.lastAlertKind !== "profit") {
    botSession.lastAlertKind = "profit";
    botSession.lastAlertAt = now;
    persistBotSession();
    sendTelegramAlert(
      `🤖 <b>AUTOPILOT SESSION +${snap.pnlPercent.toFixed(2)}%</b>\n\n` +
        `<b>Session P/L:</b> ${snap.pnl >= 0 ? "+" : ""}$${snap.pnl.toFixed(2)}\n` +
        `<b>Capital:</b> $${snap.equity.toFixed(2)} / $${snap.budgetUsdt.toFixed(2)} budget\n` +
        `<b>Cash:</b> $${snap.cashUsdt.toFixed(2)} USDT\n` +
        `<b>Action:</b> Tighten alerts or turn Autopilot OFF to park in USDT.`,
    );
  } else if (snap.pnlPercent <= -botSession.alertLossPercent && botSession.lastAlertKind !== "loss") {
    botSession.lastAlertKind = "loss";
    botSession.lastAlertAt = now;
    persistBotSession();
    sendTelegramAlert(
      `🤖 <b>AUTOPILOT SESSION ${snap.pnlPercent.toFixed(2)}%</b>\n\n` +
        `<b>Session P/L:</b> $${snap.pnl.toFixed(2)}\n` +
        `<b>Capital:</b> $${snap.equity.toFixed(2)} / $${snap.budgetUsdt.toFixed(2)} budget\n` +
        `<b>Cash:</b> $${snap.cashUsdt.toFixed(2)} USDT\n` +
        `<b>Action:</b> Consider OFF to flatten back to USDT, or raise the loss alert.`,
    );
  }
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
const BOT_UNIVERSE_SIZE = 25;
const BOT_SCAN_MS = 5 * 60 * 1000;
const BOT_MIN_QUOTE_VOLUME = 3_000_000;
const LEVERAGE_USDT_RE = /(UP|DOWN|BULL|BEAR)USDT$/;
const STABLE_USDT_PAIRS = new Set(["USDTUSDT", "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "FDUSDUSDT", "DAIUSDT", "USDPUSDT", "USDEUSDT", "USD1USDT"]);

const marketData = {};
let binanceKlineWs = null;
const subscribedKlines = new Set();
const botHuntSymbols = new Set();

function emptyMarketSlot(hunt = false) {
  return {
    prices: [],
    volumes: [],
    lastRsi: null,
    lastVolumeSurge: false,
    lastSignalTime: 0,
    lastStopLossPnl: null,
    hunt: Boolean(hunt),
  };
}

SYMBOLS.forEach((sym) => {
  marketData[sym.toUpperCase()] = emptyMarketSlot(false);
});

function isDashboardSymbol(symbol) {
  return SYMBOLS.includes(String(symbol || "").toLowerCase());
}

function ensureMarketSlot(symbol, hunt = false) {
  const sym = String(symbol || "").toUpperCase();
  if (!marketData[sym]) marketData[sym] = emptyMarketSlot(hunt);
  else if (hunt) marketData[sym].hunt = true;
  return marketData[sym];
}

function isTradableUsdtSpot(symbol) {
  const sym = String(symbol || "").toUpperCase();
  if (!sym.endsWith("USDT")) return false;
  if (LEVERAGE_USDT_RE.test(sym) || STABLE_USDT_PAIRS.has(sym)) return false;
  return Boolean(symbolLotSizes[sym]);
}

function subscribeKline(symbol) {
  const stream = `${String(symbol).toLowerCase()}@kline_${INTERVAL}`;
  if (subscribedKlines.has(stream)) return;
  subscribedKlines.add(stream);
  if (binanceKlineWs && binanceKlineWs.readyState === WebSocket.OPEN) {
    binanceKlineWs.send(JSON.stringify({ method: "SUBSCRIBE", params: [stream], id: Date.now() }));
    console.log(`[AUTOPILOT] subscribed ${String(symbol).toUpperCase()}`);
  }
}

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
        if (s.status && s.status !== "TRADING") return;
        if (s.isSpotTradingAllowed === false) return;
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
        const locked = parseFloat(b.locked);
        if (free > 0) availableBalances[b.asset] = free;
        else delete availableBalances[b.asset];
        if (Number.isFinite(locked) && locked > 0) lockedBalances[b.asset] = locked;
        else delete lockedBalances[b.asset];
      });
    } else {
      // Print Binance rejection message directly in terminal logs
      console.error("[BINANCE ACCOUNT API ERROR]:", data);
    }
  } catch (err) {
    console.error("Failed to update background balances:", err.message);
  }
}

function collectSpotTradeSymbols() {
  const symbols = new Set(SYMBOLS.map((s) => s.toUpperCase()));

  Object.keys(availableBalances).forEach((asset) => {
    if (!asset || asset === "USDT" || asset === "USD") return;
    symbols.add(`${asset}USDT`);
    if (isStockAsset(asset)) {
      symbols.add(`${asset}USDT`);
      symbols.add(`${String(asset).toUpperCase().replace(/X$/, "")}USDT`);
    }
  });

  STOCK_SYMBOLS.forEach((ticker) => {
    symbols.add(`${ticker}USDT`);
    symbols.add(`${ticker}XUSDT`);
  });

  try {
    const stmt = db.prepare("SELECT DISTINCT symbol FROM trades");
    while (stmt.step()) {
      const row = stmt.getAsObject();
      if (row.symbol) symbols.add(String(row.symbol).toUpperCase());
    }
    stmt.free();
  } catch (err) {
    console.warn("[TRADE HISTORY] Failed to read stored symbols:", err.message);
  }

  const listed = [...symbols];
  if (!Object.keys(symbolLotSizes).length) return listed;
  return listed.filter((sym) => Boolean(symbolLotSizes[sym]));
}

function knownTradeOrderIds() {
  const ids = new Set();
  try {
    const stmt = db.prepare("SELECT order_id FROM trades");
    while (stmt.step()) {
      const row = stmt.getAsObject();
      if (row.order_id) ids.add(String(row.order_id));
    }
    stmt.free();
  } catch (err) {
    console.warn("[TRADE HISTORY] Failed to read stored order ids:", err.message);
  }
  return ids;
}

async function fetchBinanceMyTrades(symbol, limit = 200) {
  const apiKey = process.env.BINANCE_API_KEY;
  const secretKey = process.env.BINANCE_SECRET_KEY;
  if (!apiKey || !secretKey) throw new Error("Missing API keys.");

  const timestamp = Date.now();
  const query = `symbol=${symbol}&limit=${limit}&timestamp=${timestamp}`;
  const signature = crypto.createHmac("sha256", secretKey).update(query).digest("hex");
  const response = await fetch(`https://api.binance.com/api/v3/myTrades?${query}&signature=${signature}`, {
    headers: { "X-MBX-APIKEY": apiKey },
  });
  const data = await response.json();

  if (!Array.isArray(data)) {
    if (data?.code !== -1121) {
      console.warn(`[BINANCE MYTRADES ${symbol}]`, data.msg || JSON.stringify(data));
    }
    return [];
  }
  return data;
}

function summarizeImportedFills(fills, symbol, side) {
  const baseAsset = symbol.replace("USDT", "");
  let commissionUsdt = 0;
  let baseCommission = 0;
  let qty = 0;
  let quoteQty = 0;
  let timestamp = Date.now();

  fills.forEach((fill) => {
    qty += parseFloat(fill.qty || 0);
    quoteQty += parseFloat(fill.quoteQty || 0);
    if (fill.time) timestamp = Math.min(timestamp, fill.time);

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

  const isBuy = side.toUpperCase() === "BUY";
  return {
    qty: isBuy ? Math.max(0, qty - baseCommission) : qty,
    usdt: isBuy ? quoteQty + commissionUsdt : Math.max(0, quoteQty - commissionUsdt),
    price: qty > 0 ? quoteQty / qty : 0,
    timestamp,
  };
}

function aggregateBinanceFills(fills) {
  const byOrder = new Map();

  fills.forEach((fill) => {
    const orderId = String(fill.orderId);
    if (!byOrder.has(orderId)) byOrder.set(orderId, []);
    byOrder.get(orderId).push(fill);
  });

  return [...byOrder.entries()].map(([orderId, orderFills]) => {
    const first = orderFills[0];
    const symbol = String(first.symbol || "").toUpperCase();
    const side = first.isBuyer ? "BUY" : "SELL";
    const economics = summarizeImportedFills(orderFills, symbol, side);

    return {
      id: `binance_${orderId}`,
      timestamp: economics.timestamp,
      symbol,
      side,
      amount: parseFloat(economics.usdt.toFixed(2)),
      status: "SUCCESS",
      outcome: `Order #${orderId}`,
      orderId: Number.isFinite(Number(orderId)) ? Number(orderId) : orderId,
      qty: economics.qty,
      price: economics.price,
      source: "binance",
    };
  });
}

function persistImportedTrades(trades) {
  const known = knownTradeOrderIds();
  let imported = 0;

  trades.forEach((trade) => {
    const orderId = trade.orderId == null ? "" : String(trade.orderId);
    if (!orderId || known.has(orderId)) return;

    db.run(`INSERT INTO trades (symbol, side, price, qty, usdt_amount, order_id, timestamp, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [
      trade.symbol,
      trade.side,
      trade.price || 0,
      trade.qty || 0,
      trade.amount || 0,
      orderId,
      trade.timestamp,
      trade.source || "binance",
    ]);
    known.add(orderId);
    imported++;
  });

  if (imported) saveDatabase();
  return imported;
}

async function syncBinanceTradeHistory() {
  await updateAccountBalances();
  const symbols = collectSpotTradeSymbols();
  const fills = [];

  for (let i = 0; i < symbols.length; i += 5) {
    const batch = symbols.slice(i, i + 5);
    const results = await Promise.all(
      batch.map(async (symbol) => {
        try {
          return await fetchBinanceMyTrades(symbol);
        } catch (err) {
          console.warn(`[BINANCE MYTRADES ${symbol}]`, err.message);
          return [];
        }
      }),
    );
    results.forEach((trades) => fills.push(...trades));
  }

  const trades = aggregateBinanceFills(fills).sort((a, b) => b.timestamp - a.timestamp);
  const imported = persistImportedTrades(trades);
  return { trades, imported, symbols };
}

async function binanceSignedRequest(method, path, params = {}) {
  const apiKey = process.env.BINANCE_API_KEY;
  const secretKey = process.env.BINANCE_SECRET_KEY;
  if (!apiKey || !secretKey) throw new Error("Missing API keys.");

  const timestamp = Date.now();
  const search = new URLSearchParams();
  Object.entries({ ...params, timestamp }).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") return;
    search.append(key, String(value));
  });
  const signature = crypto.createHmac("sha256", secretKey).update(search.toString()).digest("hex");
  search.append("signature", signature);
  const response = await fetch(`https://api.binance.com${path}?${search.toString()}`, {
    method,
    headers: { "X-MBX-APIKEY": apiKey },
  });
  return response.json();
}

async function fetchTickerPrice(symbol) {
  const sym = String(symbol || "").toUpperCase();
  if (!sym) return null;
  const cached = tickerPriceCache[sym];
  if (cached && Date.now() - cached.at < 30000) return cached.price;
  try {
    const res = await fetch(`https://api.binance.com/api/v3/ticker/price?symbol=${sym}`);
    const data = await res.json();
    const price = parseFloat(data.price);
    if (Number.isFinite(price)) {
      tickerPriceCache[sym] = { price, at: Date.now() };
      return price;
    }
  } catch (err) {
    console.warn(`[TICKER ${sym}]`, err.message);
  }
  return null;
}

async function fetchEquityQuote(symbol) {
  const ticker = stockTickerFromAsset(symbol);
  if (!ticker) return null;
  const cached = tickerPriceCache[`EQ_${ticker}`];
  if (cached && Date.now() - cached.at < 30000) return cached.price;
  try {
    const data = await binanceSignedRequest("GET", "/sapi/v1/equity/market/quote", { symbol: ticker });
    const price = parseFloat(data?.price || data?.lastPrice || data?.data?.price || data?.[0]?.price);
    if (Number.isFinite(price)) {
      tickerPriceCache[`EQ_${ticker}`] = { price, at: Date.now() };
      return price;
    }
  } catch (err) {
    console.warn(`[BINANCE EQUITY QUOTE ${ticker}]`, err.message);
  }
  const alpacaPx = stockMarketData[ticker]?.lastPrice;
  return Number.isFinite(alpacaPx) ? alpacaPx : null;
}

async function resolveUsdValue(asset, qty) {
  const amount = parseFloat(qty) || 0;
  if (!amount) return 0;
  const direct = getAssetUsdPrice(asset);
  if (direct) return amount * direct;
  const upper = String(asset || "").toUpperCase();
  if (isStockAsset(upper)) {
    const equityPx = await fetchEquityQuote(upper);
    if (equityPx) return amount * equityPx;
  }
  const price =
    (await fetchTickerPrice(`${upper}USDT`)) ||
    (await fetchTickerPrice(`${upper.replace(/[XB]$/, "")}USDT`)) ||
    (await fetchTickerPrice(upper));
  return price ? amount * price : 0;
}

function normalizeEquityTrades(payload) {
  const rows = Array.isArray(payload)
    ? payload
    : Array.isArray(payload?.list)
      ? payload.list
      : Array.isArray(payload?.trades)
        ? payload.trades
        : Array.isArray(payload?.data)
          ? payload.data
          : [];
  return rows
    .map((row) => {
      const symbol = String(row.symbol || row.s || "").toUpperCase();
      const orderId = row.orderId || row.order_id || row.id;
      const qty = parseFloat(row.qty || row.executedQty || row.quantity || 0);
      const price = parseFloat(row.price || row.avgPrice || 0);
      const quote = parseFloat(row.quoteQty || row.quoteQuantity || qty * price || 0);
      const sideRaw = String(row.side || "").toUpperCase();
      const side = sideRaw === "BUY" || sideRaw === "SELL" ? sideRaw : row.isBuyer === false ? "SELL" : "BUY";
      const timestamp = parseInt(row.time || row.tradeTime || row.updateTime || Date.now(), 10);
      if (!symbol || !orderId) return null;
      const base = symbol.replace(/USDT$|USD$|USDC$/, "");
      if (base) binanceStockAssets.add(base);
      return {
        id: `binance_eq_${orderId}`,
        timestamp: Number.isFinite(timestamp) ? timestamp : Date.now(),
        symbol,
        side,
        amount: parseFloat(quote.toFixed(2)),
        status: "SUCCESS",
        outcome: `Binance stock #${orderId}`,
        orderId,
        qty,
        price,
        source: "binance-stock",
      };
    })
    .filter(Boolean);
}

async function fetchBinanceEquityTrades() {
  const trades = [];
  const now = Date.now();
  const week = 7 * 24 * 60 * 60 * 1000;
  for (let i = 0; i < 4; i++) {
    const endTime = now - i * week;
    const startTime = endTime - week;
    try {
      const data = await binanceSignedRequest("GET", "/sapi/v1/equity/trade/history", {
        startTime,
        endTime,
        size: 100,
      });
      if (data?.code && !Array.isArray(data) && !data.list && !data.trades) {
        if (i === 0) console.warn("[BINANCE EQUITY TRADES]", data.msg || JSON.stringify(data));
        break;
      }
      trades.push(...normalizeEquityTrades(data));
    } catch (err) {
      console.warn("[BINANCE EQUITY TRADES]", err.message);
      break;
    }
  }
  return trades;
}

function mapBinanceAssetRow(row, venue) {
  const asset = String(row.asset || row.tokenizedAsset || row.symbol || "").toUpperCase();
  const free = parseFloat(row.free || row.available || row.qty || 0);
  const locked = parseFloat(row.locked || row.freeze || row.freezeAmount || 0);
  if (!asset || free + locked <= 0.0001) return null;
  if (isStockAsset(asset)) binanceStockAssets.add(asset);
  return { asset, free, locked, venue: isStockAsset(asset) ? "binance-stock" : venue };
}

async function fetchBinanceFundingStocks() {
  try {
    const data = await binanceSignedRequest("POST", "/sapi/v1/asset/get-funding-asset", {});
    const rows = Array.isArray(data) ? data : [];
    return rows.map((row) => mapBinanceAssetRow(row, "binance-spot")).filter((row) => row && row.venue === "binance-stock");
  } catch (err) {
    console.warn("[BINANCE FUNDING STOCKS]", err.message);
    return [];
  }
}

async function fetchBinanceUserAssets() {
  try {
    const data = await binanceSignedRequest("POST", "/sapi/v1/asset/getUserAsset", { needBtcValuation: "false" });
    const rows = Array.isArray(data) ? data : [];
    return rows.map((row) => mapBinanceAssetRow(row, "binance-spot")).filter(Boolean);
  } catch (err) {
    console.warn("[BINANCE USER ASSETS]", err.message);
    return [];
  }
}

async function loadBinanceStockUniverse() {
  try {
    const res = await fetch("https://api.binance.com/sapi/v1/equity/market/tokenized-assets");
    const data = await res.json();
    const rows = Array.isArray(data) ? data : data?.list || data?.assets || data?.data || [];
    rows.forEach((row) => {
      ["asset", "tokenizedAsset", "symbol", "baseAsset"].forEach((key) => {
        const value = String(row[key] || "").toUpperCase().replace(/USDT$|USD$/, "");
        if (value) binanceStockAssets.add(value);
      });
    });
    if (binanceStockAssets.size) {
      console.log(`[BINANCE STOCKS] Loaded ${binanceStockAssets.size} tokenized equity assets.`);
    }
  } catch (err) {
    console.warn("[BINANCE STOCK UNIVERSE]", err.message);
  }
}

async function buildHoldingsList() {
  await Promise.all([updateAccountBalances(), loadBinanceStockUniverse()]);
  const [fundingStocks, userAssets] = await Promise.all([fetchBinanceFundingStocks(), fetchBinanceUserAssets()]);
  const merged = new Map();

  const addRow = (row) => {
    if (!row) return;
    const key = `${row.venue}:${row.asset}`;
    const prev = merged.get(key);
    if (prev) {
      prev.free += row.free;
      prev.locked += row.locked;
      return;
    }
    merged.set(key, { ...row });
  };

  Object.keys(availableBalances).forEach((asset) => {
    addRow({
      asset,
      free: availableBalances[asset] || 0,
      locked: lockedBalances[asset] || 0,
      venue: isStockAsset(asset) ? "binance-stock" : "binance-spot",
    });
  });
  userAssets.forEach(addRow);
  fundingStocks.forEach(addRow);

  const rows = [];
  for (const row of merged.values()) {
    const total = row.free + row.locked;
    if (total <= 0.0001) continue;
    const usdValue = await resolveUsdValue(row.asset, total);
    rows.push({
      asset: row.asset,
      free: row.free.toFixed(4),
      locked: row.locked.toFixed(4),
      total: total.toFixed(4),
      usdValue: usdValue.toFixed(2),
      venue: row.venue,
    });
  }

  rows.sort((a, b) => {
    if (a.venue !== b.venue) return a.venue === "binance-stock" ? -1 : 1;
    return a.asset.localeCompare(b.asset);
  });
  return rows;
}

async function syncAllTradeHistory() {
  const [spot, equity] = await Promise.all([syncBinanceTradeHistory(), fetchBinanceEquityTrades()]);
  persistImportedTrades(equity);
  const trades = [...spot.trades, ...equity].sort((a, b) => b.timestamp - a.timestamp);
  return {
    trades,
    imported: spot.imported,
    symbols: spot.symbols,
  };
}

async function placeMarketOrder({ symbol, side, usdtAmount, quantity, sellAll = false, alertId = null, source = "manual" }) {
  const apiKey = process.env.BINANCE_API_KEY;
  const secretKey = process.env.BINANCE_SECRET_KEY;
  if (!apiKey || !secretKey) return { success: false, error: "Missing API keys." };

  const symUpper = String(symbol || "").toUpperCase();
  const sideUpper = String(side || "").toUpperCase();
  const baseAsset = symUpper.replace("USDT", "");
  const isSell = sideUpper === "SELL";
  const clearWallet = isSell && (sellAll || !quantity);
  const tradeAmount = usdtAmount || config.tradeAmountUsdt;
  let queryParams = `symbol=${symUpper}&side=${sideUpper}&type=MARKET`;

  try {
    if (isSell) {
      await updateAccountBalances();
      const freeQty = availableBalances[baseAsset] || 0;
      const rawQty = clearWallet ? freeQty : Math.min(parseFloat(quantity) || 0, freeQty);

      if (rawQty <= 0) return { success: false, error: `No available ${baseAsset} balance to sell.` };

      const markPrice = marketData[symUpper]?.prices.slice(-1)[0] || 0;
      const formattedQty = formatQuantity(symUpper, rawQty);

      if (!parseFloat(formattedQty) || isDustQty(symUpper, formattedQty, markPrice)) {
        const dust = await convertDustToBnb(baseAsset);
        await updateAccountBalances();
        if (dust) {
          return { success: true, orderId: null, dustConverted: true, symbol: symUpper, side: "SELL", details: dust };
        }
        return { success: false, error: `${baseAsset} balance is below Binance LOT_SIZE / min notional.` };
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

    if (!result.orderId) {
      return { success: false, error: result.msg || "Order rejected by Binance", details: result };
    }

    const economics = summarizeOrderFills(result, symUpper, sideUpper);
    const executedPrice = parseFloat(result.fills?.[0]?.price || marketData[symUpper]?.prices.slice(-1)[0] || 0);
    const executedQty = economics.netQty;
    const executedUsdt = economics.recordedUsdt || parseFloat(result.cummulativeQuoteQty || tradeAmount || executedQty * executedPrice);

    db.run(`INSERT INTO trades (symbol, side, price, qty, usdt_amount, order_id, timestamp, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [
      symUpper,
      sideUpper,
      executedPrice,
      executedQty,
      executedUsdt,
      String(result.orderId),
      timestamp,
      source === "bot" ? "bot" : source === "binance" ? "binance" : "manual",
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

    if (source !== "bot") {
      sendTelegramAlert(
        `✅ <b>TRADE EXECUTED (${sideUpper})</b>\n\n` +
          `<b>Symbol:</b> ${symUpper}\n` +
          `<b>Amount:</b> $${executedUsdt.toFixed(2)} USDT (${executedQty} ${baseAsset})\n` +
          `<b>Executed Price:</b> $${executedPrice}\n` +
          (economics.commissionUsdt || economics.baseCommission
            ? `<b>Fees:</b> $${economics.commissionUsdt.toFixed(4)} USDT` + (economics.baseCommission ? ` + ${economics.baseCommission} ${baseAsset}` : "") + `\n`
            : "") +
          (dustConverted ? `<b>Dust:</b> leftover ${baseAsset} converted to BNB\n` : "") +
          `<b>Order ID:</b> <code>${result.orderId}</code>`,
      );
    }

    return {
      success: true,
      orderId: result.orderId,
      symbol: symUpper,
      side: sideUpper,
      executedPrice,
      executedQty,
      executedUsdt,
      dustConverted,
      details: result,
      economics,
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

function botOpenPositionCount() {
  return Object.keys(botSession.positions || {}).length;
}

async function botMaybeBuy(symbol, closePrice, rsi) {
  if (!botSession.enabled || botSession.busy) return;
  if (botOpenPositionCount() > 0) return;

  const minNotional = getMinNotional(symbol);
  const amount = Math.min(config.tradeAmountUsdt, botSession.cashUsdt);
  const wallet = availableBalances.USDT || 0;

  if (amount + 1e-8 < minNotional) {
    botSession.lastAction = `Skipped BUY ${symbol} · cash $${botSession.cashUsdt.toFixed(2)} below min notional`;
    persistBotSession();
    return;
  }
  if (wallet + 1e-8 < amount) {
    botSession.lastAction = `Skipped BUY ${symbol} · wallet has $${wallet.toFixed(2)} USDT`;
    persistBotSession();
    return;
  }

  botSession.busy = true;
  try {
    const result = await placeMarketOrder({ symbol, side: "BUY", usdtAmount: amount, source: "bot" });
    if (!result.success) {
      botSession.lastAction = `BUY failed ${symbol}: ${result.error}`;
      persistBotSession();
      return;
    }

    botSession.cashUsdt = Math.max(0, botSession.cashUsdt - result.executedUsdt);
    botSession.positions[result.symbol] = {
      qty: result.executedQty,
      costUsdt: result.executedUsdt,
      entryPrice: result.executedPrice,
      orderId: result.orderId,
    };
    botSession.lastAction = `BOUGHT ${result.symbol} $${result.executedUsdt.toFixed(2)}`;
    persistBotSession();

    const alertId = recordStrategyAlert({ symbol: result.symbol, action: "BUY", signalType: "BOT_BUY", price: closePrice, rsi });
    markAlertExecuted(alertId, result.orderId);
    sendChannelTelegram(
      "crypto",
      `🤖 <b>AUTOPILOT BUY (${result.symbol})</b>\n\n` +
        `<b>Spent:</b> $${result.executedUsdt.toFixed(2)} USDT\n` +
        `<b>Price:</b> $${result.executedPrice}\n` +
        `<b>RSI:</b> ${typeof rsi === "number" ? rsi.toFixed(2) : "--"}\n` +
        `<b>Bot cash left:</b> $${botSession.cashUsdt.toFixed(2)}\n` +
        `<b>Order:</b> <code>${result.orderId}</code>`,
    );
    maybeSessionPnlAlert();
  } finally {
    botSession.busy = false;
  }
}

async function botMaybeSell(symbol, reason, rsi, { force = false } = {}) {
  const pos = botSession.positions[symbol];
  if (!pos) return { success: false, error: "No autopilot position." };
  if (!force && (!botSession.enabled || botSession.busy)) return { success: false, error: "Autopilot busy." };

  const ownedBusy = !botSession.busy;
  botSession.busy = true;
  try {
    const result = await placeMarketOrder({ symbol, side: "SELL", quantity: pos.qty, source: "bot" });
    if (!result.success) {
      botSession.lastAction = `SELL failed ${symbol}: ${result.error}`;
      persistBotSession();
      return result;
    }

    const proceeds = result.executedUsdt || 0;
    const pnl = proceeds - (parseFloat(pos.costUsdt) || 0);
    botSession.cashUsdt += proceeds;
    botSession.realizedPnl += pnl;
    delete botSession.positions[symbol];
    botSession.lastAction = `${reason || "SOLD"} ${symbol} $${proceeds.toFixed(2)} (${pnl >= 0 ? "+" : ""}$${pnl.toFixed(2)})`;
    persistBotSession();

    const alertId = recordStrategyAlert({
      symbol,
      action: "SELL",
      signalType: reason === "STOP_LOSS" ? "BOT_STOP_LOSS" : reason === "TAKE_PROFIT" ? "BOT_TAKE_PROFIT" : "BOT_SELL",
      price: result.executedPrice,
      rsi,
    });
    markAlertExecuted(alertId, result.orderId);
    sendChannelTelegram(
      "crypto",
      `🤖 <b>AUTOPILOT ${reason || "SELL"} (${symbol})</b>\n\n` +
        `<b>Proceeds:</b> $${proceeds.toFixed(2)} USDT\n` +
        `<b>Clip P/L:</b> ${pnl >= 0 ? "+" : ""}$${pnl.toFixed(2)}\n` +
        `<b>Price:</b> $${result.executedPrice}\n` +
        `<b>Bot cash:</b> $${botSession.cashUsdt.toFixed(2)}\n` +
        `<b>Order:</b> <code>${result.orderId}</code>`,
    );
    maybeSessionPnlAlert();
    return result;
  } finally {
    if (ownedBusy) botSession.busy = false;
  }
}

function parseBotBudget(value) {
  const n = parseFloat(value);
  if (!Number.isFinite(n)) return { error: "Enter a USDT budget." };
  if (n < 5) return { error: "Autopilot budget must be at least $5 (Binance min notional)." };
  if (n > 200) return { error: "Autopilot budget is capped at $200 for this terminal." };
  return { value: parseFloat(n.toFixed(2)) };
}

function parseBotAlertPercent(value, label) {
  const n = parseFloat(value);
  if (!Number.isFinite(n) || n <= 0 || n > 50) return { error: `${label} must be between 0.1 and 50.` };
  return { value: parseFloat(n.toFixed(2)) };
}

async function startBotSession({ budgetUsdt, alertProfitPercent, alertLossPercent }) {
  await updateAccountBalances();
  const wallet = availableBalances.USDT || 0;
  const hasOpen = botOpenPositionCount() > 0;

  if (hasOpen) {
    botSession.enabled = true;
    botSession.alertProfitPercent = alertProfitPercent;
    botSession.alertLossPercent = alertLossPercent;
    botSession.lastAlertKind = null;
    botSession.lastAction = `Resumed · ${Object.keys(botSession.positions).join(", ")} still open`;
    persistBotSession();
    console.log(`[AUTOPILOT] RESUMED · ${Object.keys(botSession.positions).join(", ")}`);
    void scanAutopilotUniverse().catch((err) => console.error("[AUTOPILOT] scan", err.message));
    sendTelegramAlert(
      `🤖 <b>AUTOPILOT RESUMED</b>\n\n` +
        `<b>Open:</b> ${Object.keys(botSession.positions).join(", ")}\n` +
        `<b>Cash:</b> $${botSession.cashUsdt.toFixed(2)} USDT\n` +
        `<b>Alerts:</b> +${alertProfitPercent}% / −${alertLossPercent}%`,
    );
    return { success: true, resumed: true, status: getBotStatus() };
  }

  if (wallet + 1e-8 < budgetUsdt) {
    return { success: false, error: `Need $${budgetUsdt.toFixed(2)} USDT. Spot wallet has $${wallet.toFixed(2)}.` };
  }

  botSession = {
    ...createIdleBotSession(),
    enabled: true,
    startedAt: Date.now(),
    budgetUsdt,
    alertProfitPercent,
    alertLossPercent,
    startingEquity: budgetUsdt,
    cashUsdt: budgetUsdt,
    lastAction: `Armed · $${budgetUsdt.toFixed(2)} USDT · watching RSI ≤ ${config.rsiOversold}`,
  };
  persistBotSession();
  console.log(
    `[AUTOPILOT] ON · budget $${budgetUsdt.toFixed(2)} · clip $${Number(config.tradeAmountUsdt).toFixed(2)} · buy when RSI ≤ ${config.rsiOversold} · TP ${config.takeProfitPercent}% / SL ${config.stopLossPercent}%`,
  );
  sendTelegramAlert(
    `🤖 <b>AUTOPILOT ON</b>\n\n` +
      `<b>Budget:</b> $${budgetUsdt.toFixed(2)} USDT\n` +
      `<b>Clip size:</b> $${Number(config.tradeAmountUsdt).toFixed(2)} (strategy default)\n` +
      `<b>Alerts:</b> +${alertProfitPercent}% / −${alertLossPercent}% session P/L\n` +
      `<b>Pairs:</b> monitored + top ${BOT_UNIVERSE_SIZE} USDT by 24h volume\n` +
      `<b>Exits:</b> fee-aware TP ${config.takeProfitPercent}% / SL ${config.stopLossPercent}%`,
  );
  void scanAutopilotUniverse().catch((err) => console.error("[AUTOPILOT] scan", err.message));
  return { success: true, resumed: false, status: getBotStatus() };
}

async function stopBotSession() {
  botSession.enabled = false;
  botSession.busy = true;
  const symbols = Object.keys(botSession.positions || {});
  const errors = [];

  for (const symbol of symbols) {
    const result = await botMaybeSell(symbol, "FLATTEN", null, { force: true });
    if (result && result.success === false) errors.push(`${symbol}: ${result.error}`);
  }

  const parked = parseFloat(botSession.cashUsdt) || 0;
  const pnl = (parked) - (parseFloat(botSession.startingEquity) || 0);
  botSession.lastAction = errors.length
    ? `Stop incomplete · ${errors.join("; ")}`
    : `Stopped · parked $${parked.toFixed(2)} USDT · session ${pnl >= 0 ? "+" : ""}$${pnl.toFixed(2)}`;
  persistBotSession();
  botSession.busy = false;

  sendTelegramAlert(
    `🤖 <b>AUTOPILOT OFF</b>\n\n` +
      `<b>Parked:</b> $${parked.toFixed(2)} USDT\n` +
      `<b>Session P/L:</b> ${pnl >= 0 ? "+" : ""}$${pnl.toFixed(2)}\n` +
      (errors.length ? `<b>Unsold:</b> ${errors.join("; ")}\n` : "") +
      `<b>Note:</b> Autopilot crypto was flattened. Other Spot holdings were left alone.`,
  );

  return { success: errors.length === 0, error: errors[0] || null, status: getBotStatus() };
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

function applyKlineBootstrap(symbol, klines) {
  const target = ensureMarketSlot(symbol);
  const prices = klines.map((k) => parseFloat(k[4])).filter(Number.isFinite);
  const volumes = klines.map((k) => parseFloat(k[5])).filter(Number.isFinite);
  if (prices.length < 15) return target;
  target.prices = prices.slice(-100);
  target.volumes = volumes.slice(-100);
  const rsiVals = RSI.calculate({ values: target.prices, period: 14 });
  if (rsiVals.length > 0) target.lastRsi = rsiVals[rsiVals.length - 1];
  if (target.volumes.length >= 20) {
    const volSma = SMA.calculate({ values: target.volumes, period: 20 });
    const avg = volSma[volSma.length - 1];
    const lastVol = target.volumes[target.volumes.length - 1];
    target.lastVolumeSurge = Boolean(avg && lastVol > avg * config.volumeSurgeMultiplier);
  }
  return target;
}

async function fetchKlines(symbol, limit = 50) {
  const res = await fetch(`https://data-api.binance.vision/api/v3/klines?symbol=${String(symbol).toUpperCase()}&interval=${INTERVAL}&limit=${limit}`);
  const klines = await res.json();
  return Array.isArray(klines) ? klines : [];
}

async function bootstrapHistoricalData() {
  console.log(`Bootstrapping historical candle data for: ${SYMBOLS.map((s) => s.toUpperCase()).join(", ")}...`);
  for (const symbol of SYMBOLS) {
    try {
      const klines = await fetchKlines(symbol);
      if (klines.length >= 15) applyKlineBootstrap(symbol, klines);
    } catch (err) {
      console.error(`Failed to bootstrap ${symbol}:`, err.message);
    }
  }
}

async function pickHottestUsdtSymbols(limit = BOT_UNIVERSE_SIZE) {
  const res = await fetch("https://api.binance.com/api/v3/ticker/24hr");
  const tickers = await res.json();
  if (!Array.isArray(tickers)) {
    console.warn("[AUTOPILOT] 24h ticker scan failed:", tickers?.msg || "unexpected payload");
    return [];
  }

  return tickers
    .filter((t) => isTradableUsdtSpot(t.symbol) && parseFloat(t.quoteVolume) >= BOT_MIN_QUOTE_VOLUME)
    .sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume))
    .slice(0, limit)
    .map((t) => ({
      symbol: t.symbol,
      quoteVolume: parseFloat(t.quoteVolume) || 0,
      change: parseFloat(t.priceChangePercent) || 0,
    }));
}

async function scanAutopilotUniverse() {
  if (!botSession.enabled) return;
  try {
    const hottest = await pickHottestUsdtSymbols();
    if (!hottest.length) {
      console.warn("[AUTOPILOT] universe scan found no liquid USDT pairs");
      return;
    }

    const scored = [];
    for (let i = 0; i < hottest.length; i += 5) {
      const batch = hottest.slice(i, i + 5);
      const part = await Promise.all(
        batch.map(async (row) => {
          try {
            const hunt = !isDashboardSymbol(row.symbol);
            ensureMarketSlot(row.symbol, hunt);
            if (hunt) botHuntSymbols.add(row.symbol);
            const klines = await fetchKlines(row.symbol);
            if (klines.length >= 15) applyKlineBootstrap(row.symbol, klines);
            subscribeKline(row.symbol);
            const rsi = marketData[row.symbol]?.lastRsi;
            return { ...row, rsi: typeof rsi === "number" ? rsi : null };
          } catch (err) {
            console.warn(`[AUTOPILOT] scan ${row.symbol} failed:`, err.message);
            return { ...row, rsi: null };
          }
        }),
      );
      scored.push(...part);
    }

    const ranked = scored.filter((row) => typeof row.rsi === "number").sort((a, b) => a.rsi - b.rsi);
    const preview = ranked
      .slice(0, 5)
      .map((row) => `${row.symbol} ${row.rsi.toFixed(1)}`)
      .join(", ");
    console.log(`[AUTOPILOT] universe ${ranked.length} pairs · lowest RSI: ${preview || "n/a"}`);

    const buy = ranked.find((row) => row.rsi <= config.rsiOversold);
    if (buy && botOpenPositionCount() === 0) {
      const price = marketData[buy.symbol]?.prices.slice(-1)[0] || 0;
      console.log(`[AUTOPILOT] BUY trigger ${buy.symbol} RSI ${buy.rsi.toFixed(2)} ≤ ${config.rsiOversold} (universe scan)`);
      await botMaybeBuy(buy.symbol, price, buy.rsi);
    }
  } catch (err) {
    console.error("[AUTOPILOT] universe scan error:", err.message);
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
  binanceKlineWs = ws;

  ws.on("open", () => {
    SYMBOLS.forEach((s) => subscribedKlines.add(`${s}@kline_${INTERVAL}`));
    console.log(`Connected to Binance Multi-Stream for: ${SYMBOLS.map((s) => s.toUpperCase()).join(", ")}`);
    botHuntSymbols.forEach((sym) => {
      subscribedKlines.delete(`${String(sym).toLowerCase()}@kline_${INTERVAL}`);
      subscribeKline(sym);
    });
    Object.keys(botSession.positions || {}).forEach((sym) => {
      if (!isDashboardSymbol(sym)) {
        botHuntSymbols.add(sym);
        subscribedKlines.delete(`${String(sym).toLowerCase()}@kline_${INTERVAL}`);
        subscribeKline(sym);
      }
    });
  });

  ws.on("message", (data) => {
    try {
      const payload = JSON.parse(data);
      const kline = payload.data?.k;

      if (kline && kline.x) {
        const sym = kline.s;
        const closePrice = parseFloat(kline.c);
        const volume = parseFloat(kline.v);

        const target = ensureMarketSlot(sym);

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
        const botFlat = botOpenPositionCount() === 0;
        const cooledDown = now - target.lastSignalTime > cooldownMs;

        if (
          botSession.enabled &&
          target.lastRsi !== null &&
          (isDashboardSymbol(sym) || target.lastRsi <= config.rsiOversold + 8)
        ) {
          console.log(
            `[AUTOPILOT] ${sym} RSI ${target.lastRsi.toFixed(2)} surge=${isVolumeSurge ? "yes" : "no"} cooldown=${cooledDown ? "ready" : "blocked"} pos=${botSession.positions[sym] ? "open" : "flat"}`,
          );
        }

        if (botSession.enabled && botSession.positions[sym] && target.lastRsi !== null) {
          const botSell = evaluateBotSellSignal(sym, closePrice, target.lastRsi);
          if (botSell?.type === "TAKE_PROFIT" || botSell?.type === "STOP_LOSS") {
            void botMaybeSell(sym, botSell.type, target.lastRsi).catch((err) => console.error("[AUTOPILOT SELL]", err.message));
            target.lastSignalTime = now;
            target.lastStopLossPnl = botSell.type === "STOP_LOSS" ? botSell.netPnl : null;
          }
        }

        if (botSession.enabled && botFlat && cooledDown && target.lastRsi !== null && target.lastRsi <= config.rsiOversold) {
          console.log(`[AUTOPILOT] BUY trigger ${sym} RSI ${target.lastRsi.toFixed(2)} ≤ ${config.rsiOversold}`);
          void botMaybeBuy(sym, closePrice, target.lastRsi).catch((err) => console.error("[AUTOPILOT BUY]", err.message));
          target.lastSignalTime = now;
        }

        if (target.lastRsi !== null && cooledDown) {
          const baseAsset = sym.replace("USDT", "");
          const currentAssetBalance = availableBalances[baseAsset] || 0;
          const currentAssetUsdVal = currentAssetBalance * closePrice;
          const usdtBalance = availableBalances["USDT"] || 0;

          if (!botSession.enabled && target.lastRsi <= config.rsiOversold && isVolumeSurge && usdtBalance >= config.tradeAmountUsdt) {
            recordStrategyAlert({ symbol: sym, action: "BUY", signalType: "BUY", price: closePrice, rsi: target.lastRsi });
            sendChannelTelegram(
              "crypto",
              `⚡ <b>BUY SIGNAL (${sym})</b>\n\n` + `<b>RSI:</b> ${target.lastRsi.toFixed(2)} | <b>Price:</b> $${closePrice}\n` + `<b>Available Cash:</b> $${usdtBalance.toFixed(2)} USDT`,
            );
            target.lastSignalTime = now;
          } else if (currentAssetUsdVal >= 5.0 && !botSession.positions[sym]) {
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
    botConfig,
    timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
  });
});

app.post("/api/settings", express.json(), (req, res) => {
  const { password, settings, alertSettings, botSettings } = req.body;

  if (!password || password !== TRADE_PASSWORD) {
    return res.status(401).json({ success: false, error: "Unauthorized password." });
  }

  const hasStrategy = settings && typeof settings === "object";
  const hasAlerts = alertSettings && typeof alertSettings === "object";
  const hasBot = botSettings && typeof botSettings === "object";
  if (!hasStrategy && !hasAlerts && !hasBot) {
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

  if (hasBot) {
    const budget = botSettings.defaultBudgetUsdt != null ? parseBotBudget(botSettings.defaultBudgetUsdt) : null;
    const profit = botSettings.alertProfitPercent != null ? parseBotAlertPercent(botSettings.alertProfitPercent, "Profit alert %") : null;
    const loss = botSettings.alertLossPercent != null ? parseBotAlertPercent(botSettings.alertLossPercent, "Loss alert %") : null;
    if (budget?.error) return res.status(400).json({ success: false, error: budget.error });
    if (profit?.error) return res.status(400).json({ success: false, error: profit.error });
    if (loss?.error) return res.status(400).json({ success: false, error: loss.error });
    if (budget) botConfig.defaultBudgetUsdt = budget.value;
    if (profit) botConfig.alertProfitPercent = profit.value;
    if (loss) botConfig.alertLossPercent = loss.value;
    Object.keys(botConfig).forEach((key) => saveSettingToDb(key, botConfig[key]));
    console.log("Updated runtime bot settings:", botConfig);
  }

  return res.json({ success: true, config, alertConfig, botConfig });
});

app.get("/api/bot/status", (req, res) => {
  res.json({ success: true, ...getBotStatus() });
});

app.post("/api/bot/start", express.json(), async (req, res) => {
  const { password, budgetUsdt, alertProfitPercent, alertLossPercent } = req.body || {};
  if (!password || password !== TRADE_PASSWORD) {
    return res.status(401).json({ success: false, error: "Unauthorized: Incorrect password." });
  }
  if (botSession.enabled) {
    return res.status(400).json({ success: false, error: "Autopilot is already on.", status: getBotStatus() });
  }

  const budget = parseBotBudget(budgetUsdt ?? botConfig.defaultBudgetUsdt);
  const profit = parseBotAlertPercent(alertProfitPercent ?? botConfig.alertProfitPercent, "Profit alert %");
  const loss = parseBotAlertPercent(alertLossPercent ?? botConfig.alertLossPercent, "Loss alert %");
  if (budget.error) return res.status(400).json({ success: false, error: budget.error });
  if (profit.error) return res.status(400).json({ success: false, error: profit.error });
  if (loss.error) return res.status(400).json({ success: false, error: loss.error });

  botConfig.defaultBudgetUsdt = budget.value;
  botConfig.alertProfitPercent = profit.value;
  botConfig.alertLossPercent = loss.value;
  Object.keys(botConfig).forEach((key) => saveSettingToDb(key, botConfig[key]));

  const result = await startBotSession({
    budgetUsdt: budget.value,
    alertProfitPercent: profit.value,
    alertLossPercent: loss.value,
  });
  if (!result.success) return res.status(400).json(result);
  res.json(result);
});

app.post("/api/bot/stop", express.json(), async (req, res) => {
  const { password } = req.body || {};
  if (!password || password !== TRADE_PASSWORD) {
    return res.status(401).json({ success: false, error: "Unauthorized: Incorrect password." });
  }
  if (!botSession.enabled && botOpenPositionCount() === 0) {
    return res.json({ success: true, status: getBotStatus() });
  }
  const result = await stopBotSession();
  res.json(result);
});

app.post("/api/bot/alerts", express.json(), (req, res) => {
  const { password, alertProfitPercent, alertLossPercent } = req.body || {};
  if (!password || password !== TRADE_PASSWORD) {
    return res.status(401).json({ success: false, error: "Unauthorized: Incorrect password." });
  }
  const profit = parseBotAlertPercent(alertProfitPercent ?? botSession.alertProfitPercent, "Profit alert %");
  const loss = parseBotAlertPercent(alertLossPercent ?? botSession.alertLossPercent, "Loss alert %");
  if (profit.error) return res.status(400).json({ success: false, error: profit.error });
  if (loss.error) return res.status(400).json({ success: false, error: loss.error });

  botSession.alertProfitPercent = profit.value;
  botSession.alertLossPercent = loss.value;
  botSession.lastAlertKind = null;
  botConfig.alertProfitPercent = profit.value;
  botConfig.alertLossPercent = loss.value;
  Object.keys(botConfig).forEach((key) => saveSettingToDb(key, botConfig[key]));
  persistBotSession();
  res.json({ success: true, status: getBotStatus() });
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
  const marketsList = SYMBOLS.map((s) => s.toUpperCase()).map((sym) => {
    if (!marketData[sym]) return null;
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
  }).filter(Boolean);

  res.json({
    interval: INTERVAL,
    markets: marketsList,
    config,
    usdtBalance: parseFloat((availableBalances["USDT"] || 0).toFixed(2)),
  });
});

app.get("/api/holdings", async (req, res) => {
  try {
    const balances = await buildHoldingsList();
    res.json({ success: true, balances });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/api/trades", async (req, res) => {
  const apiKey = process.env.BINANCE_API_KEY;
  const secretKey = process.env.BINANCE_SECRET_KEY;
  if (!apiKey || !secretKey) {
    return res.status(500).json({ success: false, error: "Missing API keys." });
  }

  try {
    const { trades, imported, symbols } = await syncAllTradeHistory();
    res.json({
      success: true,
      imported,
      symbols,
      trades: trades.map(({ qty, price, ...entry }) => entry),
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/api/pnl", (req, res) => {
  const timeframe = req.query.tf || "all";
  let timeFilter = 0;
  const now = Date.now();

  if (timeframe === "day") timeFilter = now - 24 * 60 * 60 * 1000;
  else if (timeframe === "week") timeFilter = now - 7 * 24 * 60 * 60 * 1000;
  else if (timeframe === "month") timeFilter = now - 30 * 24 * 60 * 60 * 1000;

  try {
    // Walk the full trade history so cost basis includes buys that happened
    // before the selected window. Realized PnL is attributed to the sell time.
    const stmt = db.prepare("SELECT * FROM trades ORDER BY timestamp ASC");

    const trades = [];
    while (stmt.step()) {
      trades.push(stmt.getAsObject());
    }
    stmt.free();

    const assetPnL = {};
    let totalRealizedUsdt = 0;
    let totalVolumeTraded = 0;
    let tradeCount = 0;

    trades.forEach((t) => {
      const sym = t.symbol.toUpperCase();
      if (!assetPnL[sym]) {
        assetPnL[sym] = { symbol: sym, buyQty: 0, buyCost: 0, realizedPnl: 0, tradeCount: 0 };
      }

      const inWindow = t.timestamp >= timeFilter;
      if (inWindow) {
        assetPnL[sym].tradeCount++;
        totalVolumeTraded += t.usdt_amount;
        tradeCount++;
      }

      if (t.side.toUpperCase() === "BUY") {
        assetPnL[sym].buyQty += t.qty;
        assetPnL[sym].buyCost += t.usdt_amount;
      } else if (t.side.toUpperCase() === "SELL") {
        if (assetPnL[sym].buyQty > 0) {
          const avgBuyPrice = assetPnL[sym].buyCost / assetPnL[sym].buyQty;
          const costBasisForSale = t.qty * avgBuyPrice;
          const pnl = t.usdt_amount - costBasisForSale;

          if (inWindow) {
            assetPnL[sym].realizedPnl += pnl;
            totalRealizedUsdt += pnl;
          }

          assetPnL[sym].buyQty = Math.max(0, assetPnL[sym].buyQty - t.qty);
          assetPnL[sym].buyCost = Math.max(0, assetPnL[sym].buyCost - costBasisForSale);
        }
      }
    });

    res.json({
      timeframe,
      totalRealizedPnl: parseFloat(totalRealizedUsdt.toFixed(2)),
      totalVolumeTraded: parseFloat(totalVolumeTraded.toFixed(2)),
      assets: Object.values(assetPnL)
        .filter((a) => a.tradeCount > 0)
        .map((a) => ({
          ...a,
          realizedPnl: parseFloat(a.realizedPnl.toFixed(2)),
        })),
      tradeCount,
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

  const result = await placeMarketOrder({ symbol, side, usdtAmount, quantity, sellAll, alertId, source: "manual" });
  if (!result.success) {
    return res.status(result.error && /Unauthorized|Missing API/.test(result.error) ? 500 : 400).json(result);
  }
  return res.json(result);
});

app.use(express.static("public"));

(async () => {
  await initDatabase();
  await fetchExchangeInfo();
  await loadBinanceStockUniverse();
  await updateAccountBalances();
  setInterval(updateAccountBalances, 15000);
  setInterval(() => {
    if (botSession.enabled) maybeSessionPnlAlert();
  }, 30000);
  setInterval(() => {
    if (botSession.enabled) void scanAutopilotUniverse().catch((err) => console.error("[AUTOPILOT] scan", err.message));
  }, BOT_SCAN_MS);
  await bootstrapHistoricalData();
  await bootstrapStockHistoricalData();
  connectMultiStreamWS();
  connectAlpacaStockWS();
  setInterval(refreshStockSnapshots, 60000);
  setInterval(refreshAlpacaClock, 60000);
  if (botSession.enabled) {
    setTimeout(() => {
      void scanAutopilotUniverse().catch((err) => console.error("[AUTOPILOT] scan", err.message));
    }, 4000);
  }

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
